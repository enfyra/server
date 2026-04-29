import { describe, expect, it, vi } from 'vitest';
import { dynamicInterceptorBegin } from '../../src/http/middlewares/dynamic-interceptor.middleware';

describe('dynamicInterceptorBegin admin test run isolation', () => {
  it('does not run global hooks or wrap responses for /admin/test/run', async () => {
    const executorEngineService = {
      register: vi.fn(),
      runBatch: vi.fn(),
    };
    const req = {
      method: 'POST',
      path: '/admin/test/run',
      originalUrl: '/admin/test/run',
      routeData: {
        context: {
          $share: { $logs: [] },
          $data: undefined,
        },
        preHooks: [
          {
            code: '$ctx.$body.changed = true',
          },
        ],
        postHooks: [
          {
            code: '@DATA = { statusCode: @STATUS, ...@DATA }',
          },
        ],
      },
    };
    const json = vi.fn();
    const res = {
      statusCode: 200,
      json,
    };
    const next = vi.fn();

    await dynamicInterceptorBegin(executorEngineService as any)(
      req,
      res as any,
      next,
    );
    res.json({ success: true, result: { ok: true } });

    expect(next).toHaveBeenCalledTimes(1);
    expect(executorEngineService.register).not.toHaveBeenCalled();
    expect(executorEngineService.runBatch).not.toHaveBeenCalled();
    expect(json).toHaveBeenCalledWith({ success: true, result: { ok: true } });
  });

  it('still runs post hooks for normal built-in routes with routeData', async () => {
    const executorEngineService = {
      register: vi.fn(),
      runBatch: vi.fn(async (req: any) => {
        req.routeData.context.$data = {
          statusCode: req.routeData.context.$statusCode,
          ...req.routeData.context.$data,
        };
      }),
    };
    const req = {
      method: 'GET',
      path: '/metadata',
      originalUrl: '/metadata',
      routeData: {
        context: {
          $share: { $logs: [] },
          $data: undefined,
        },
        preHooks: [],
        postHooks: [
          {
            code: '@DATA = { statusCode: @STATUS, ...@DATA }',
          },
        ],
      },
    };
    const json = vi.fn();
    const res = {
      statusCode: 200,
      json,
    };
    const next = vi.fn();

    await dynamicInterceptorBegin(executorEngineService as any)(
      req,
      res as any,
      next,
    );
    res.json({ data: [] });
    await new Promise((resolve) => setTimeout(resolve, 0));

    expect(next).toHaveBeenCalledTimes(1);
    expect(executorEngineService.register).toHaveBeenCalledWith(req, {
      code: '@DATA = { statusCode: @STATUS, ...@DATA }',
      type: 'postHook',
    });
    expect(json).toHaveBeenCalledWith({ statusCode: 200, data: [] });
  });
});
