import { createServer } from 'node:http';
import { setTimeout as sleep } from 'node:timers/promises';
import { describe, expect, it } from 'vitest';
import { ExecutorEngineService, IsolatedExecutorService } from '@enfyra/kernel';
import { DynamicContextFactory } from '../../src/shared/services/dynamic-context.factory';
import { DynamicService } from '../../src/modules/dynamic-api/services/dynamic.service';
import { RuntimeScriptExecutorService } from '../../src/engines/cache/services/runtime-script-executor.service';
import { compileScriptSource } from '../../src/shared/utils/script-code.util';
import {
  acknowledgeRuntimeLog,
  peekRuntimeLogs,
} from '../../src/shared/runtime-log-buffer';
import { emptySharedHelperRegistry } from '../helpers/shared-helper-registry';

for (const mode of ['success', 'downstream-close', 'source-error']) {
  describe(`native delivery diagnostics: ${mode}`, () => {
    it('preserves host milestones through the installed executor and retains response ownership', async () => {
      const isolated = new IsolatedExecutorService({
        packageCacheService: { getPackages: async () => [] },
        packageCdnLoaderService: { getPackageSources: () => [] },
        tuning: {
          maxConcurrentWorkers: 1,
          isolateMemoryLimitMb: 64,
          isolatesPerWorker: 2,
        },
      });
      const executor = new RuntimeScriptExecutorService({
        runtimeRegistryService: emptySharedHelperRegistry(),
        kernelExecutorEngineService: new ExecutorEngineService({
          isolatedExecutorService: isolated,
        }),
      });
      const factory = new DynamicContextFactory({
        bcryptService: {},
        apiTokenService: {},
        userCacheService: {},
        envService: { get: () => 'fixture' },
        websocketContextFactory: { createGlobalProxy: () => ({}) },
      } as any);
      const dynamic = new DynamicService({
        executorEngineService: executor,
        dynamicCancellationService: { run: async () => {} },
        loggingService: { error() {} },
      } as any);
      const handler = String.raw`
        await @HELPERS.$delivery('upstream_headers', { statusCode: 200, secret: 'private-upstream-key' });
        const relay = async function* () {
          await @HELPERS.$delivery('upstream_terminal');
          yield 'data: private-output-and-tool-arguments\n\n';
          await @HELPERS.$sleep(100);
          ${mode === 'source-error' ? "throw new Error('private-source-payload');" : mode === 'downstream-close' ? 'await @HELPERS.$sleep(2000);' : "await @HELPERS.$delivery('upstream_eof'); yield 'data: [DONE]\\n\\n';"}
        };
        return await @RES.stream(require('node:stream').Readable.from(relay()), { mimetype: 'text/event-stream', timeoutMs: 3000 });
      `;
      let executionId: string | undefined;
      let finishedResolve!: () => void;
      const finished = new Promise<void>((resolve) => {
        finishedResolve = resolve;
      });
      const errors: unknown[] = [];
      const gateway = createServer(async (req: any, res: any) => {
        req.body = { model: 'fixture' };
        req.path = '/v2/fixture';
        res.status = (code) => {
          res.statusCode = code;
          return res;
        };
        res.json = (value) => {
          res.end(JSON.stringify(value));
          return res;
        };
        res.setHeader('x-correlation-id', 'req_native_delivery');
        const context = factory.createHttp(req, {
          params: {},
          realClientIP: '127.0.0.1',
        });
        executionId = context.$api?.request?.executionId;
        req.routeData = {
          res,
          context,
          handler: compileScriptSource(handler, 'javascript'),
          handlerRecord: {
            id: 1,
            sourceCode: handler,
            scriptLanguage: 'javascript',
          },
          route: { path: '/v2/fixture' },
          routeMethodConfig: { timeout: 4000 },
          postHooks: [],
          preHooks: [],
        };
        try {
          await dynamic.runHandler(req);
        } catch (error) {
          errors.push(error);
        } finally {
          finishedResolve();
        }
      });
      await new Promise<void>((resolve) =>
        gateway.listen(0, '127.0.0.1', resolve),
      );
      const controller = new AbortController();
      const timer = setTimeout(() => controller.abort(), 5000);
      try {
        const response = await fetch(
          `http://127.0.0.1:${(gateway.address() as any).port}/v2/fixture`,
          { signal: controller.signal },
        );
        expect(response.status).toBe(200);
        if (mode === 'downstream-close') {
          await response.body!.getReader().read();
          controller.abort();
        } else await response.text().catch(() => '');
        await Promise.race([
          finished,
          sleep(3000).then(() => {
            throw new Error('native completion deadline');
          }),
        ]);
        await sleep(10);
        const logs = peekRuntimeLogs(2000).filter(
          (row) =>
            row.record.component === 'DynamicHttpDelivery' &&
            (row.record.details as any)?.executionId === executionId,
        );
        expect(errors).toEqual([]);
        if (mode === 'success') expect(logs).toHaveLength(0);
        else {
          expect(logs).toHaveLength(1);
          const details = logs[0].record.details as any;
          expect(details.gateway.upstream_headers.statusCode).toBe(200);
          expect(details.gateway.upstream_terminal).toBeDefined();
          expect(details.gateway.upstream_eof).toBeUndefined();
          expect(details.transport.streamBytes).toBeGreaterThan(0);
          expect(details.correlationId).toBe('req_native_delivery');
          expect(details.failure.code).toBe(
            mode === 'source-error'
              ? 'response_stream_error'
              : 'client_closed_request',
          );
          expect(logs[0].record.statusCode).toBe(
            mode === 'source-error' ? 502 : 499,
          );
          expect(JSON.stringify(logs)).not.toContain('private');
          acknowledgeRuntimeLog(logs[0].record.eventId);
        }
      } finally {
        clearTimeout(timer);
        controller.abort();
        gateway.closeAllConnections();
        await new Promise<void>((resolve) => gateway.close(resolve));
        isolated.onDestroy();
      }
    }, 6500);
  });
}
