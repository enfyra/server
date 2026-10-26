import { EventEmitter } from 'node:events';
import { setTimeout as sleep } from 'node:timers/promises';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { dynamicInterceptorBegin } from '../../src/http/middlewares/dynamic-interceptor.middleware';
import { DynamicService } from '../../src/modules/dynamic-api/services/dynamic.service';

function fixture(timeout = 200) {
  const req = Object.assign(new EventEmitter(), {
    method: 'POST',
    path: '/budget',
    url: '/budget',
    headers: {},
    routeData: {
      routeMethodConfig: { timeout },
      handler: 'return true;',
      context: { $share: { $logs: [] } },
      preHooks: [{ code: 'await wait();' }],
      postHooks: [{ code: 'return undefined;' }],
      res: undefined as any,
    },
  });
  const json = vi.fn();
  const res = Object.assign(new EventEmitter(), {
    statusCode: 200,
    headersSent: false,
    writableEnded: false,
    writableFinished: false,
    destroyed: false,
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
  return { req, res, json };
}
const responses: ReturnType<typeof fixture>['res'][] = [];
afterEach(() => {
  for (const res of responses.splice(0)) {
    res.writableEnded = true;
    res.writableFinished = true;
    res.emit('finish');
    res.emit('close');
  }
});

describe('one dynamic route execution budget', () => {
  it('does not renew the method timeout between pre-hooks and handler/post-hooks', async () => {
    const { req, res } = fixture();
    responses.push(res);
    const calls: Array<{ timeout: number; signal: AbortSignal }> = [];
    const executor = {
      register: vi.fn(),
      runBatch: vi.fn(async (_req, timeout, options) => {
        calls.push({ timeout, signal: options.signal });
        if (calls.length === 1) await sleep(60);
        return { value: true, shortCircuit: false };
      }),
    };
    const audit = { run: vi.fn() };
    await dynamicInterceptorBegin(executor as any, undefined, audit as any)(
      req,
      res as any,
      vi.fn(),
    );
    await new DynamicService({
      executorEngineService: executor,
      loggingService: { error: vi.fn() },
      dynamicCancellationService: audit,
    } as any).runHandler(req as any);
    expect(calls).toHaveLength(2);
    expect(calls[0].timeout).toBeGreaterThan(150);
    expect(calls[1].timeout).toBeLessThan(160);
    expect(calls[1].signal).toBe(calls[0].signal);
    expect(audit.run).not.toHaveBeenCalled();
  });

  it('counts time spent between batches and never dispatches after the route deadline', async () => {
    const { req, res, json } = fixture(50);
    responses.push(res);
    const executor = {
      register: vi.fn(),
      runBatch: vi.fn(async () => ({ value: true, shortCircuit: false })),
    };
    const audit = { run: vi.fn() };
    await dynamicInterceptorBegin(executor as any, undefined, audit as any)(
      req,
      res as any,
      vi.fn(),
    );
    await sleep(80);
    await new DynamicService({
      executorEngineService: executor,
      loggingService: { error: vi.fn() },
      dynamicCancellationService: audit,
    } as any).runHandler(req as any);
    expect(executor.runBatch).toHaveBeenCalledTimes(1);
    expect(json).toHaveBeenCalledWith(
      expect.objectContaining({
        statusCode: 504,
        error: expect.objectContaining({ code: 'execution_timeout' }),
      }),
    );
  });

  it('keeps native post-hooks inside the budget after a pre-hook short circuit', async () => {
    const { req, res, json } = fixture();
    responses.push(res);
    req.routeData.handler = '';
    const budgets: number[] = [];
    const executor = {
      register: vi.fn(),
      runBatch: vi.fn(async (_req, timeout) => {
        budgets.push(timeout);
        await sleep(60);
        return { value: { ok: true }, shortCircuit: budgets.length === 1 };
      }),
    };
    await dynamicInterceptorBegin(executor as any)(req, res as any, vi.fn());
    await vi.waitFor(() => expect(json).toHaveBeenCalledWith({ ok: true }));
    expect(budgets).toHaveLength(2);
    expect(budgets[1]).toBeLessThan(160);
  });

  it('aborts an active second batch at the route deadline and audits once', async () => {
    const { req, res, json } = fixture(100);
    responses.push(res);
    let calls = 0;
    const executor = {
      register: vi.fn(),
      runBatch: vi.fn(async (_req, _timeout, options) => {
        if (++calls === 1) {
          await sleep(60);
          return { value: true, shortCircuit: false };
        }
        return await new Promise((_resolve, reject) => {
          options.signal.addEventListener(
            'abort',
            () =>
              reject(
                Object.assign(new Error('aborted'), {
                  code: 'ERR_EXECUTION_ABORTED',
                }),
              ),
            { once: true },
          );
        });
      }),
    };
    const audit = { run: vi.fn() };
    await dynamicInterceptorBegin(executor as any, undefined, audit as any)(
      req,
      res as any,
      vi.fn(),
    );
    await new DynamicService({
      executorEngineService: executor,
      loggingService: { error: vi.fn() },
      dynamicCancellationService: audit,
    } as any).runHandler(req as any);
    expect(calls).toBe(2);
    expect(audit.run).toHaveBeenCalledExactlyOnceWith(
      req,
      expect.objectContaining({
        origin: 'execution',
        code: 'execution_timeout',
      }),
    );
    expect(json).toHaveBeenCalledWith(
      expect.objectContaining({ statusCode: 504 }),
    );
    expect(req.listenerCount('aborted')).toBe(1);
    res.emit('close');
    expect(req.listenerCount('aborted')).toBe(0);
  });

  it('uses the same remaining budget for post-hooks around a native route', async () => {
    const { req, res, json } = fixture();
    responses.push(res);
    req.routeData.handler = '';
    const budgets: number[] = [];
    const executor = {
      register: vi.fn(),
      runBatch: vi.fn(async (_req, timeout) => {
        budgets.push(timeout);
        await sleep(60);
        return { shortCircuit: false };
      }),
    };
    await dynamicInterceptorBegin(executor as any)(req, res as any, vi.fn());
    res.json({ ok: true });
    await vi.waitFor(() => expect(json).toHaveBeenCalledWith({ ok: true }));
    expect(budgets).toHaveLength(2);
    expect(budgets[1]).toBeLessThan(160);
  });
});
