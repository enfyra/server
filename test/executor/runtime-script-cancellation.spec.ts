import { EventEmitter } from 'node:events';
import {
  ExecutorEngineService,
  IsolatedExecutorService,
  getIoAbortSignal,
} from '@enfyra/kernel';
import { describe, expect, it, vi } from 'vitest';
import { RuntimeScriptExecutorService } from '../../src/engines/cache/services/runtime-script-executor.service';
import { dynamicInterceptorBegin } from '../../src/http/middlewares/dynamic-interceptor.middleware';
import { DynamicService } from '../../src/modules/dynamic-api/services/dynamic.service';

function request(code = 'return true;', helpers = {}) {
  return {
    routeData: {
      context: { $helpers: helpers, $share: { $logs: [] } },
      __codeBlocks: [{ type: 'handler', code, sourceCode: 'return true;' }],
    },
  };
}

function createExecutor() {
  const isolated = new IsolatedExecutorService({
    packageCacheService: { getPackages: async () => [] },
    packageCdnLoaderService: { getPackageSources: () => [] },
    tuning: {
      maxConcurrentWorkers: 1,
      isolateMemoryLimitMb: 64,
      isolatePoolSize: 1,
      tasksPerIsolate: 6,
      tasksPerWorkerCap: 6,
    },
  });
  const service = new RuntimeScriptExecutorService({
    kernelExecutorEngineService: new ExecutorEngineService({
      isolatedExecutorService: isolated,
    }),
  });
  return { isolated, service };
}

describe('runtime script request cancellation', () => {
  it.each([false, true])('forwards the caller signal through batch execution and repair (%s)', async (repair) => {
    const kernel = { runBatch: vi.fn().mockResolvedValue({ value: true, shortCircuit: false }) };
    if (repair) kernel.runBatch.mockRejectedValueOnce({ details: { errorName: 'SyntaxError', executionStage: 'compile' } });
    const service = new RuntimeScriptExecutorService({ kernelExecutorEngineService: kernel as any });
    const signal = new AbortController().signal;
    await service.runBatch(request(repair ? 'invalid source!' : 'return true;'), 1000, { signal });
    expect(kernel.runBatch).toHaveBeenCalledTimes(repair ? 2 : 1);
    for (const call of kernel.runBatch.mock.calls) expect(call[2]?.signal).toBe(signal);
  });

  it('aborts host I/O immediately without waiting for the upstream deadline', async () => {
    const { isolated, service } = createExecutor();
    const controller = new AbortController();
    let entered = false;
    let hostAborted = false;
    let release: () => void = () => {};
    const pending = service.runBatch(request('await $ctx.$helpers.wait(); return true;', {
      wait: async () => {
        entered = true;
        await new Promise<void>((resolve) => {
          release = resolve;
          getIoAbortSignal()?.addEventListener('abort', () => { hostAborted = true; resolve(); }, { once: true });
        });
      },
    }), 3000, { signal: controller.signal }).then(
      (value: unknown) => ({ value, error: undefined }),
      (error: Error & { code: string }) => ({ value: undefined, error }),
    );
    try {
      await vi.waitFor(() => expect(entered).toBe(true), { timeout: 2000 });
      const pid = isolated.getMetrics().pool.workers[0].pid;
      controller.abort();
      await vi.waitFor(() => expect(hostAborted).toBe(true), { timeout: 500 });
      expect((await pending).error?.code).toBe('ERR_EXECUTION_ABORTED');
      await vi.waitFor(() => expect(isolated.getMetrics().pool.activeTasks).toBe(0));
      expect((await service.runBatch(request('return 42;'), 1000)).value).toBe(42);
      expect(isolated.getMetrics().pool.workers[0].pid).toBe(pid);
    } finally {
      release();
      await pending;
      isolated.onDestroy();
    }
  });

  it('removes a disconnected request from a full pool without dispatching it', async () => {
    const { isolated, service } = createExecutor();
    let entered = 0;
    let release!: () => void;
    const gate = new Promise<void>((resolve) => { release = resolve; });
    const active = Array.from({ length: 6 }, () => service.runBatch(request('await $ctx.$helpers.wait(); return true;', {
      wait: async () => { entered++; await gate; },
    }), 4000));
    const controller = new AbortController();
    let queued: Promise<unknown> | undefined;
    let dispatched = false;
    try {
      await vi.waitFor(() => expect(entered).toBe(6), { timeout: 2000 });
      queued = service.runBatch(request('await $ctx.$helpers.start();', {
        start: () => { dispatched = true; },
      }), 2000, { signal: controller.signal }).catch((error: { code: string }) => error.code);
      await vi.waitFor(() => expect(isolated.getMetrics().pool.waitingTasks).toBe(1));
      controller.abort();
      await vi.waitFor(() => expect(isolated.getMetrics().pool.waitingTasks).toBe(0), { timeout: 500 });
      expect(await queued).toBe('ERR_EXECUTION_ABORTED');
      expect(dispatched).toBe(false);
      expect(isolated.getMetrics().pool.activeTasks).toBe(6);
    } finally {
      release();
      await Promise.allSettled([...active, queued]);
      isolated.onDestroy();
    }
  });

  it.each(['aborted', 'close'])('cancels a pre-hook on request %s without continuing the HTTP pipeline', async (event) => {
    const req = Object.assign(new EventEmitter(), {
      method: 'POST', path: '/v1/chat/completions',
      routeData: { context: { $share: { $logs: [] } }, preHooks: [{ code: 'await wait();' }] },
    });
    const json = vi.fn();
    const res = Object.assign(new EventEmitter(), { statusCode: 200, writableEnded: false, json });
    const next = vi.fn();
    let signal: AbortSignal | undefined;
    let finish!: () => void;
    const executor = {
      register: vi.fn(),
      runBatch: vi.fn((_req, _timeout, options) => {
        signal = options?.signal;
        return new Promise((resolve, reject) => {
          finish = () => resolve({ value: true, shortCircuit: false });
          signal?.addEventListener('abort', () => reject(Object.assign(new Error('aborted'), { code: 'ERR_EXECUTION_ABORTED' })), { once: true });
        });
      }),
    };
    const running = dynamicInterceptorBegin(executor as any)(req, res as any, next);
    try {
      if (event === 'aborted') req.emit(event); else res.emit(event);
      expect(signal?.aborted).toBe(true);
      await running;
      expect(next).not.toHaveBeenCalled();
      expect(json).not.toHaveBeenCalled();
      expect(req.listenerCount('aborted')).toBe(0);
      expect(res.listenerCount('close')).toBe(0);
      expect(req.routeData.context).not.toHaveProperty('$res');
    } finally {
      finish();
      await running;
    }
  });
  it.each(['request', 'response'])('does not dispatch a handler whose %s already disconnected', async (disconnected) => {
    const req = Object.assign(new EventEmitter(), {
      method: 'POST', url: '/v1/chat/completions', aborted: disconnected === 'request',
      routeData: {
        handler: 'return true;', context: { $share: { $logs: [] } }, postHooks: [],
        res: Object.assign(new EventEmitter(), { destroyed: disconnected === 'response', writableEnded: false }),
      },
    });
    const runBatch = vi.fn(async (_req, _timeout, options) => {
      expect(options.signal.aborted).toBe(true);
      throw Object.assign(new Error('aborted'), { code: 'ERR_EXECUTION_ABORTED' });
    });
    const loggingService = { error: vi.fn() };
    const dynamic = new DynamicService({ executorEngineService: { register: vi.fn(), runBatch }, loggingService } as any);
    await expect(dynamic.runHandler(req as any)).resolves.toBeUndefined();
    expect(loggingService.error).not.toHaveBeenCalled();
    expect(req.listenerCount('aborted')).toBe(0);
    expect(req.routeData.res.listenerCount('close')).toBe(0);
  });

});
