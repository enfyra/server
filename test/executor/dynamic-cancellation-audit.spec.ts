import { EventEmitter } from 'node:events';
import { ExecutorEngineService, IsolatedExecutorService, getIoAbortSignal } from '@enfyra/kernel';
import { describe, expect, it, vi } from 'vitest';
import { RuntimeScriptExecutorService } from '../../src/engines/cache/services/runtime-script-executor.service';
import { DynamicService } from '../../src/modules/dynamic-api/services/dynamic.service';
import { dynamicInterceptorBegin } from '../../src/http/middlewares/dynamic-interceptor.middleware';
import { DynamicContextFactory } from '../../src/shared/services/dynamic-context.factory';
import { recordDynamicClientDisconnect } from '../../src/modules/dynamic-api/utils/dynamic-http-failure.util';

function factory() {
  return new DynamicContextFactory({
    bcryptService: {}, apiTokenService: {}, userCacheService: {},
    envService: { get: () => 'fixture' },
    websocketContextFactory: { createGlobalProxy: () => ({}) },
  } as any);
}

function httpRequest() {
  const res = Object.assign(new EventEmitter(), { writableEnded: false, destroyed: false, statusCode: 200 });
  const context = factory().createHttp({
    method: 'POST', url: '/v2/chat/completions', headers: { 'x-correlation-id': 'caller-controlled' },
    body: { model: 'fixture', messages: [{ role: 'user', content: 'private prompt' }] },
    user: { id: 'member' },
  }, { params: {}, realClientIP: '127.0.0.1' });
  return Object.assign(new EventEmitter(), {
    method: 'POST', url: '/v2/chat/completions', body: context.$body, user: { id: 'member' },
    routeData: { context, res, handler: '', postHooks: [], preHooks: [], routeMethodConfig: { timeout: 3000 } },
  });
}

describe('cancelled dynamic request audit', () => {
  it('allocates an internal execution ID independent from caller correlation IDs', () => {
    const first = httpRequest();
    const second = httpRequest();
    expect(first.routeData.context.$api?.request?.executionId).toMatch(/^[0-9a-f-]{36}$/);
    expect(first.routeData.context.$api?.request?.executionId).not.toBe(second.routeData.context.$api?.request?.executionId);
    expect(first.routeData.context.$api?.request?.executionId).not.toBe('caller-controlled');
  });

  it.each(['request', 'response'])('runs cancellation finalization once after %s disconnect', async (event) => {
    const req = httpRequest();
    req.routeData.handler = 'await wait();';
    const finalize = vi.fn(async () => {});
    const executor = {
      register: vi.fn(),
      runBatch: vi.fn((_req, _timeout, options) => new Promise((_resolve, reject) => {
        options.signal.addEventListener('abort', () => reject(Object.assign(new Error('aborted'), { code: 'ERR_EXECUTION_ABORTED' })), { once: true });
      })),
    };
    const service = new DynamicService({ executorEngineService: executor, loggingService: { error: vi.fn() }, dynamicCancellationService: { run: finalize } } as any);
    const running = service.runHandler(req as any);
    if (event === 'request') req.emit('aborted'); else req.routeData.res.emit('close');
    await running;
      expect(finalize).toHaveBeenCalledExactlyOnceWith(
        req,
        expect.objectContaining({
          origin: 'client',
          statusCode: 499,
          code: 'client_closed_request',
        }),
      );
    expect(req.listenerCount('aborted')).toBe(0);
    expect(req.routeData.res.listenerCount('close')).toBe(0);
  });

  it('finalizes a cancelled pre-hook without dispatching the handler or sending a response', async () => {
    const req = httpRequest();
    req.routeData.preHooks = [{ code: 'await wait();' }] as any;
    const json = vi.fn();
    Object.assign(req.routeData.res, { json });
    const run = vi.fn(async () => {});
    const executor = {
      register: vi.fn(),
      runBatch: vi.fn((_req, _timeout, options) => new Promise((_resolve, reject) => {
        options.signal.addEventListener('abort', () => reject(Object.assign(new Error('aborted'), { code: 'ERR_EXECUTION_ABORTED' })), { once: true });
      })),
    };
    const next = vi.fn();
    const running = dynamicInterceptorBegin(executor as any, undefined, { run } as any)(req, req.routeData.res as any, next);
    req.routeData.res.emit('close');
    await running;
    expect(run).toHaveBeenCalledExactlyOnceWith(
      req,
      expect.objectContaining({
        origin: 'client',
        statusCode: 499,
        code: 'client_closed_request',
      }),
    );
    expect(next).not.toHaveBeenCalled();
    expect(json).not.toHaveBeenCalled();
  });

  it('does not replay ordinary post-hooks or dispatch without the internal execution identity', async () => {
    const { DynamicCancellationService } = await import('../../src/modules/dynamic-api/services/dynamic-cancellation.service');
    const runBatch = vi.fn();
    const finalizer = new DynamicCancellationService({ executorEngineService: { register: vi.fn(), runBatch }, dynamicContextFactory: factory(), repoRegistryService: { createReposProxy: vi.fn() } } as any);
    const noCause = httpRequest();
    noCause.routeData.postHooks = [
      { id: 1, code: "'enfyra:client-disconnect-audit'; await writeAudit();" },
    ] as any;
    await finalizer.run(noCause as any);
    const ordinary = httpRequest();
    ordinary.routeData.postHooks = [{ id: 1, code: 'await sendNotification();' }] as any;
    recordDynamicClientDisconnect(ordinary.routeData.res);
    await finalizer.run(ordinary as any);
    const missingId = httpRequest();
    delete missingId.routeData.context.$api?.request?.executionId;
    missingId.routeData.postHooks = [{ id: 1, code: "'enfyra:client-disconnect-audit'; await writeAudit();" }] as any;
    recordDynamicClientDisconnect(missingId.routeData.res);
    await finalizer.run(missingId as any);
    expect(runBatch).not.toHaveBeenCalled();
  });

  it('keeps an audit preparation failure from turning a client disconnect into an HTTP 500', async () => {
    const { DynamicCancellationService } = await import('../../src/modules/dynamic-api/services/dynamic-cancellation.service');
    const req = httpRequest();
    req.routeData.postHooks = [{ id: 1, code: "'enfyra:client-disconnect-audit';" }] as any;
    const finalizer = new DynamicCancellationService({ executorEngineService: {}, dynamicContextFactory: { createBase: () => { throw new Error('private fixture diagnostic'); } }, repoRegistryService: {} } as any);
    recordDynamicClientDisconnect(req.routeData.res);
    await expect(finalizer.run(req as any)).resolves.toBeUndefined();
  });

  it('uses a fresh bounded executor task after real host I/O cancellation', async () => {
    const { DynamicCancellationService } = await import('../../src/modules/dynamic-api/services/dynamic-cancellation.service');
    const isolated = new IsolatedExecutorService({
      packageCacheService: { getPackages: async () => [] },
      packageCdnLoaderService: { getPackageSources: () => [] },
      tuning: { maxConcurrentWorkers: 1, isolateMemoryLimitMb: 64, isolatesPerWorker: 2 },
    });
    const executor = new RuntimeScriptExecutorService({ kernelExecutorEngineService: new ExecutorEngineService({ isolatedExecutorService: isolated }) });
    const req = httpRequest();
    let entered = false;
    let aborted = false;
    let release!: () => void;
    req.routeData.context.$helpers = {
      wait: async () => {
        entered = true;
        await new Promise<void>((resolve) => {
          release = resolve;
          getIoAbortSignal()?.addEventListener('abort', () => { aborted = true; resolve(); }, { once: true });
        });
      },
    } as any;
    req.routeData.handler = 'await $ctx.$helpers.wait();';
    req.routeData.postHooks = [{ id: 'ordinary', sourceCode: 'await $ctx.$helpers.ordinary();', code: 'await $ctx.$helpers.ordinary();' }, { sourceCode: '"enfyra:client-disconnect-audit";', code: 'await $ctx.$repos.audit.create({ data: { status: $ctx.$statusCode, code: $ctx.$error.code, executionId: $ctx.$api.request.executionId, model: $ctx.$body.model } });', id: 'audit' }] as any;
    const rows: any[] = [];
    const registered: any[] = [];
    const originalRegister = executor.register.bind(executor);
    executor.register = (request, block) => { registered.push(block); originalRegister(request, block); };
    const create = vi.fn(async (value) => {
      expect(getIoAbortSignal()?.aborted).toBe(false);
      rows.push(value.data);
      return { data: [{ id: 'audit' }] };
    });
    const finalizer = new DynamicCancellationService({
      executorEngineService: executor, dynamicContextFactory: factory(),
      repoRegistryService: { createReposProxy: () => ({ audit: { create } }) },
    } as any);
    const dynamic = new DynamicService({ executorEngineService: executor, loggingService: { error: vi.fn() }, dynamicCancellationService: finalizer } as any);
    const running = dynamic.runHandler(req as any);
    try {
      await vi.waitFor(() => expect(entered).toBe(true), { timeout: 2000 });
      req.routeData.res.emit('close');
      await running;
      expect(aborted).toBe(true);
      expect(rows).toEqual([{ status: 499, code: 'client_closed_request', executionId: req.routeData.context.$api?.request?.executionId, model: 'fixture' }]);
      await finalizer.run(req as any);
      expect(create).toHaveBeenCalledTimes(1);
      expect(registered.filter(block => block.scriptId === 'ordinary')).toHaveLength(1);
      await vi.waitFor(() => expect(isolated.getMetrics().pool.activeTasks).toBe(0), { timeout: 1000 });
    } finally {
      release?.();
      await running;
      isolated.onDestroy();
    }
  });
});
