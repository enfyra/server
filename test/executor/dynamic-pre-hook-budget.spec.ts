import { EventEmitter } from 'node:events';
import { setTimeout as sleep } from 'node:timers/promises';
import {
  ExecutorEngineService,
  IsolatedExecutorService,
  getIoAbortSignal,
} from '@enfyra/kernel';
import { expect, it, vi } from 'vitest';
import { RuntimeScriptExecutorService } from '../../src/engines/cache/services/runtime-script-executor.service';
import { dynamicInterceptorBegin } from '../../src/http/middlewares/dynamic-interceptor.middleware';
import { DynamicService } from '../../src/modules/dynamic-api/services/dynamic.service';
import { emptySharedHelperRegistry } from '../helpers/shared-helper-registry';

it('allows a method-budgeted pre-hook beyond the default task tolerance', async () => {
  const isolated = new IsolatedExecutorService({
    packageCacheService: { getPackages: async () => [] },
    packageCdnLoaderService: { getPackageSources: () => [] },
    tuning: {
      maxConcurrentWorkers: 1,
      isolateMemoryLimitMb: 64,
      isolatesPerWorker: 1,
    },
  });
  const executor = new RuntimeScriptExecutorService({
    runtimeRegistryService: emptySharedHelperRegistry(),
    kernelExecutorEngineService: new ExecutorEngineService({
      isolatedExecutorService: isolated,
    }),
  });
  const req = Object.assign(new EventEmitter(), {
    method: 'POST',
    path: '/v2/embeddings',
    headers: {},
    routeData: {
      routeMethodConfig: { timeout: 60000 },
      context: {
        $helpers: { wait: () => sleep(35200) },
        $share: { $logs: [] },
      },
      preHooks: [
        {
          code: 'await $ctx.$helpers.wait(); return { ready: true };',
          scriptLanguage: 'javascript',
        },
      ],
      postHooks: [],
    },
  });
  const json = vi.fn();
  const res = Object.assign(new EventEmitter(), {
    statusCode: 200,
    destroyed: false,
    writableEnded: false,
    writableFinished: false,
    json,
    req,
    setHeader: vi.fn(),
    getHeader: vi.fn(),
    status: vi.fn(),
  });
  res.status.mockImplementation((code: number) => {
    res.statusCode = code;
    return res;
  });
  const next = vi.fn();
  try {
    await dynamicInterceptorBegin(executor as any)(req, res as any, next);
    expect(json).toHaveBeenCalledWith({ ready: true });
    expect(next).not.toHaveBeenCalled();
  } finally {
    res.writableEnded = true;
    res.writableFinished = true;
    res.emit('close');
    isolated.onDestroy();
  }
}, 45000);

it('shares one native deadline across pre-hooks, handler and post-hooks', async () => {
  const isolated = new IsolatedExecutorService({
    packageCacheService: { getPackages: async () => [] },
    packageCdnLoaderService: { getPackageSources: () => [] },
    tuning: {
      maxConcurrentWorkers: 1,
      isolateMemoryLimitMb: 64,
      isolatesPerWorker: 1,
    },
  });
  const executor = new RuntimeScriptExecutorService({
    runtimeRegistryService: emptySharedHelperRegistry(),
    kernelExecutorEngineService: new ExecutorEngineService({
      isolatedExecutorService: isolated,
    }),
  });
  const phases: string[] = [];
  const aborted = vi.fn();
  const req = Object.assign(new EventEmitter(), {
    method: 'POST',
    path: '/v2/budget',
    url: '/v2/budget',
    headers: {},
    routeData: {
      routeMethodConfig: { timeout: 1400 },
      context: {
        $helpers: {
          mark: (phase: string) => {
            phases.push(phase);
          },
          wait: async (ms: number) => {
            const signal = getIoAbortSignal();
            try {
              await sleep(ms, undefined, { signal });
            } catch (error) {
              if (signal?.aborted) aborted();
              throw error;
            }
          },
        },
        $share: { $logs: [] },
      },
      handler:
        'await $ctx.$helpers.mark("handler"); await $ctx.$helpers.wait(450); return true;',
      preHooks: [
        {
          code: 'await $ctx.$helpers.mark("pre"); await $ctx.$helpers.wait(450);',
          scriptLanguage: 'javascript',
        },
      ],
      postHooks: [
        {
          code: 'await $ctx.$helpers.mark("post"); await $ctx.$helpers.wait(1000); await $ctx.$helpers.mark("post_end");',
          scriptLanguage: 'javascript',
        },
      ],
      res: undefined as any,
    },
  });
  const json = vi.fn();
  const res = Object.assign(new EventEmitter(), {
    statusCode: 200,
    headersSent: false,
    destroyed: false,
    writableEnded: false,
    writableFinished: false,
    req,
    json,
    setHeader: vi.fn(),
    getHeader: vi.fn(),
    status: vi.fn(),
  });
  res.status.mockImplementation((code: number) => {
    res.statusCode = code;
    return res;
  });
  req.routeData.res = res;
  const audit = { run: vi.fn() };
  const next = vi.fn();
  try {
    await dynamicInterceptorBegin(executor as any, undefined, audit as any)(
      req,
      res as any,
      next,
    );
    expect(next).toHaveBeenCalledOnce();
    await new DynamicService({
      executorEngineService: executor,
      dynamicCancellationService: audit,
      loggingService: { error: vi.fn() },
    } as any).runHandler(req as any);
    expect(phases).toEqual(['pre', 'handler', 'post']);
    expect(aborted).toHaveBeenCalledOnce();
    expect(audit.run).toHaveBeenCalledExactlyOnceWith(
      req,
      expect.objectContaining({
        origin: 'execution',
        code: 'execution_timeout',
      }),
    );
    expect(json).toHaveBeenCalledWith(
      expect.objectContaining({
        statusCode: 504,
        error: expect.objectContaining({ code: 'execution_timeout' }),
      }),
    );
    await vi.waitFor(() =>
      expect(isolated.getMetrics().pool.activeTasks).toBe(0),
    );
  } finally {
    res.writableEnded = true;
    res.writableFinished = true;
    res.emit('close');
    isolated.onDestroy();
  }
}, 5000);
