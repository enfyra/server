import { emptySharedHelperRegistry } from '../helpers/shared-helper-registry';
import { createServer } from 'node:http';
import { mkdtemp, writeFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { setTimeout as sleep } from 'node:timers/promises';
import { describe, expect, it, vi } from 'vitest';
import {
  ExecutorEngineService,
  IsolatedExecutorService,
  getIoAbortSignal,
} from '@enfyra/kernel';
import { DynamicContextFactory } from '../../src/shared/services/dynamic-context.factory';
import { DynamicCancellationService } from '../../src/modules/dynamic-api/services/dynamic-cancellation.service';
import { DynamicService } from '../../src/modules/dynamic-api/services/dynamic.service';
import { dynamicInterceptorBegin } from '../../src/http/middlewares/dynamic-interceptor.middleware';
import { RuntimeScriptExecutorService } from '../../src/engines/cache/services/runtime-script-executor.service';
import { compileScriptSource } from '../../src/shared/utils/script-code.util';

const cases = [
  { cause: 'source-error', status: 502, code: 'response_stream_error' },
  {
    cause: 'source-before-headers',
    status: 502,
    code: 'response_stream_error',
    beforeHeaders: true,
  },
  {
    cause: 'task-before-headers',
    status: 504,
    code: 'execution_timeout',
    beforeHeaders: true,
  },
  {
    cause: 'task-before-stream',
    status: 504,
    code: 'execution_timeout',
    beforeHeaders: true,
  },
  { cause: 'relay-timeout', status: 504, code: 'response_stream_timeout' },
  { cause: 'idle-timeout', status: 504, code: 'response_stream_timeout' },
  { cause: 'package-source-error', status: 502, code: 'response_stream_error' },
  { cause: 'package-client-close', status: 499, code: 'client_closed_request' },
  { cause: 'task-timeout', status: 504, code: 'execution_timeout' },
  { cause: 'client-close', status: 499, code: 'client_closed_request' },
  { cause: 'client-before-error', status: 499, code: 'client_closed_request' },
  { cause: 'success', status: 200, code: undefined },
];

for (const boundary of ['handler', 'pre-hook']) {
  describe(`HTTP failure audit through ${boundary}`, () => {
    for (const fixture of cases) {
      it(`${fixture.cause} retains its owner, cancels I/O and audits once`, async () => {
        const temp = fixture.cause.startsWith('package-')
          ? await mkdtemp(join(tmpdir(), 'enfyra-http-failure-'))
          : undefined;
        const packagePath = temp ? join(temp, 'fixture.mjs') : undefined;
        if (packagePath)
          await writeFile(
            packagePath,
            `import { Readable } from 'node:stream'; export default { stream() { return Readable.from((async function* () { yield ': keepalive\\n\\n'; await new Promise(resolve => setTimeout(resolve, 100)); ${fixture.cause === 'package-source-error' ? "throw new Error('private-package-error');" : 'await new Promise(() => {});'} })()); } };`,
          );
        const packages = packagePath
          ? [
              {
                name: 'fixture',
                safeName: 'fixture',
                version: 'fixture',
                sourceCode: '',
                filePath: packagePath,
                fileUrl: packagePath,
              },
            ]
          : [];
        const isolated = new IsolatedExecutorService({
          packageCacheService: {
            getPackages: async () => packages.map((value) => value.name),
          },
          packageCdnLoaderService: { getPackageSources: () => packages },
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
        if (boundary === 'pre-hook' && fixture.cause.startsWith('task-')) {
          const runBatch = executor.runBatch.bind(executor);
          executor.runBatch = (req, timeout, options) =>
            runBatch(req, timeout ?? 500, options);
        }
        const factory = new DynamicContextFactory({
          bcryptService: {},
          apiTokenService: {},
          userCacheService: {},
          envService: { get: () => 'fixture' },
          websocketContextFactory: { createGlobalProxy: () => ({}) },
        } as any);
        const rows: any[] = [];
        let hostAborted = false,
          ordinaryRuns = 0,
          nextCalls = 0;
        let enteredResolve!: () => void, finishedResolve!: () => void;
        const entered = new Promise<void>((resolve) => {
          enteredResolve = resolve;
        });
        const finished = new Promise<void>((resolve) => {
          finishedResolve = resolve;
        });
        const finalizer = new DynamicCancellationService({
          executorEngineService: executor,
          dynamicContextFactory: factory,
          repoRegistryService: {
            createReposProxy: () => ({
              audit: {
                create: async ({ data }) => {
                  expect(getIoAbortSignal()?.aborted).toBe(false);
                  rows.push(data);
                  return { data: [{ id: 1 }] };
                },
              },
            }),
          },
        } as any);
        const dynamic = new DynamicService({
          executorEngineService: executor,
          dynamicCancellationService: finalizer,
          loggingService: { error: vi.fn() },
        } as any);
        const source = packagePath
          ? "return await @RES.stream($ctx.$pkgs.fixture.stream(), { mimetype: 'text/event-stream' });"
          : fixture.cause === 'task-before-stream'
            ? 'await $ctx.$helpers.wait();'
            : `const relay = async function* () { ${'beforeHeaders' in fixture ? '' : "yield ': keepalive\\n\\n';"} await $ctx.$helpers.wait(); }; return await @RES.stream(require('node:stream').Readable.from(relay()), { mimetype: 'text/event-stream', timeoutMs: ${fixture.cause === 'relay-timeout' ? 100 : 2000}, idleTimeoutMs: ${fixture.cause === 'idle-timeout' ? 100 : 2000} });`;
        const audit =
          "'enfyra:client-disconnect-audit'; if (!$ctx.$error) return; await $ctx.$repos.audit.create({ data: { status: $ctx.$statusCode, code: $ctx.$error.code, message: $ctx.$error.message } });";
        let request: any;
        const gateway = createServer(async (req: any, res: any) => {
          req.body = { model: 'fixture' };
          req.user = { id: 'fixture' };
          req.path = '/v2/fixture';
          res.status = (code) => {
            res.statusCode = code;
            return res;
          };
          res.json = (data) => {
            res.end(JSON.stringify(data));
            return res;
          };
          const context = factory.createHttp(req, {
            params: {},
            realClientIP: '127.0.0.1',
          });
          context.$helpers = {
            wait: async () => {
              enteredResolve();
              const signal = getIoAbortSignal();
              if (fixture.cause === 'success') return;
              if (fixture.cause.startsWith('source-')) {
                await sleep(50);
                throw new Error('private-source-error');
              }
              await new Promise<void>((resolve, reject) => {
                if (signal?.aborted) {
                  hostAborted = true;
                  resolve();
                  return;
                }
                signal?.addEventListener(
                  'abort',
                  () => {
                    hostAborted = true;
                    if (fixture.cause === 'client-before-error') {
                      reject(new Error('late-private-error'));
                    } else {
                      resolve();
                    }
                  },
                  { once: true },
                );
              });
            },
            ordinary: async () => {
              ordinaryRuns++;
            },
          } as any;
          const record = {
            id: 1,
            code: compileScriptSource(source, 'javascript'),
            sourceCode: source,
            scriptLanguage: 'javascript',
          };
          req.routeData = {
            res,
            context,
            route: { path: '/v2/fixture' },
            routeMethodConfig: {
              timeout: fixture.cause.startsWith('task-') ? 500 : 3000,
            },
            handler:
              boundary === 'handler'
                ? record.code
                : 'throw new Error("handler must not execute");',
            handlerRecord: record,
            preHooks: boundary === 'pre-hook' ? [record] : [],
            postHooks: [
              {
                id: 2,
                sourceCode: audit,
                code: compileScriptSource(audit, 'javascript'),
              },
              {
                id: 3,
                sourceCode: 'await $ctx.$helpers.ordinary();',
                code: 'await $ctx.$helpers.ordinary();',
              },
            ],
          };
          request = req;
          try {
            if (boundary === 'handler') await dynamic.runHandler(req);
            else
              await dynamicInterceptorBegin(
                executor as any,
                undefined,
                finalizer,
              )(req, res, () => {
                nextCalls++;
              });
          } catch (error) {
            res.status(500).json({ message: String(error) });
          } finally {
            finishedResolve();
          }
        });
        await new Promise<void>((resolve) =>
          gateway.listen(0, '127.0.0.1', resolve),
        );
        const controller = new AbortController();
        const deadline = setTimeout(() => controller.abort(), 4000);
        try {
          const response = await fetch(
            `http://127.0.0.1:${(gateway.address() as any).port}/v2/fixture`,
            { signal: controller.signal },
          );
          expect(response.status).toBe(
            'beforeHeaders' in fixture ? fixture.status : 200,
          );
          if (!packagePath) await entered;
          if (fixture.cause.includes('client-')) controller.abort();
          const text = await response.text().catch(() => '');
          if ('beforeHeaders' in fixture) {
            const body = JSON.parse(text);
            expect(body.error.code).toBe(fixture.code);
            expect(body.error.should_retry).toBe(true);
            expect(body.error.retry_after_seconds).toBe(5);
            expect(response.headers.get('content-type')).toContain(
              'application/json',
            );
            expect(response.headers.get('x-should-retry')).toBe('true');
            expect(response.headers.get('retry-after')).toBe('5');
            expect(text).not.toContain('private');
          }
          await Promise.race([
            finished,
            sleep(2500).then(() => {
              throw new Error('audit deadline');
            }),
          ]);
          if (fixture.cause === 'success') {
            expect(rows).toEqual([]);
            await finalizer.run(request);
            expect(rows).toEqual([]);
            expect(hostAborted).toBe(false);
            expect(nextCalls).toBe(0);
            expect(ordinaryRuns).toBe(boundary === 'handler' ? 1 : 0);
            await vi.waitFor(
              () => expect(isolated.getMetrics().pool.activeTasks).toBe(0),
              { timeout: 1000 },
            );
            return;
          }
          expect(rows).toEqual([
            {
              status: fixture.status,
              code: fixture.code,
              message: expect.any(String),
            },
          ]);
          expect(rows[0].message).not.toContain('private');
          await finalizer.run(request);
          expect(rows).toHaveLength(1);
          expect(ordinaryRuns).toBe(0);
          expect(nextCalls).toBe(0);
          if (!packagePath && !fixture.cause.startsWith('source-'))
            expect(hostAborted).toBe(true);
          await vi.waitFor(
            () => expect(isolated.getMetrics().pool.activeTasks).toBe(0),
            { timeout: 1000 },
          );
        } finally {
          clearTimeout(deadline);
          controller.abort();
          gateway.closeAllConnections();
          await new Promise<void>((resolve) => gateway.close(resolve));
          isolated.onDestroy();
          if (temp) await rm(temp, { recursive: true, force: true });
        }
      }, 6000);
    }
  });
}
