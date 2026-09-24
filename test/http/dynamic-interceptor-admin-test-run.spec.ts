import { PassThrough, Readable } from 'node:stream';
import { describe, expect, it, vi } from 'vitest';
import { dynamicInterceptorBegin } from '../../src/http/middlewares/dynamic-interceptor.middleware';
import * as runtimeLogs from '../../src/shared/runtime-log-buffer';

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
      sourceCode: '@DATA = { statusCode: @STATUS, ...@DATA }',
      scriptLanguage: 'typescript',
      onCompiledCodeRepair: undefined,
      type: 'postHook',
    });
    expect(json).toHaveBeenCalledWith({ statusCode: 200, data: [] });
  });

  it('does not run success post hooks for error responses from built-in routes', async () => {
    const executorEngineService = {
      register: vi.fn(),
      runBatch: vi.fn(async (req: any) => {
        req.routeData.context.$data = {
          statusCode: req.routeData.context.$statusCode,
          ...req.routeData.context.$data,
          message: 'Success',
        };
      }),
    };
    const req = {
      method: 'POST',
      path: '/auth/token/exchange',
      originalUrl: '/auth/token/exchange',
      routeData: {
        context: {
          $share: { $logs: [] },
          $data: undefined,
        },
        preHooks: [],
        postHooks: [
          {
            code: "@DATA = { statusCode: @STATUS, ...@DATA, message: 'Success' }",
          },
        ],
      },
    };
    const json = vi.fn();
    const res = {
      statusCode: 401,
      json,
    };
    const next = vi.fn();
    const errorBody = {
      success: false,
      message: 'Invalid API token',
      statusCode: 401,
      error: { code: 'UNAUTHORIZED', message: 'Invalid API token' },
    };

    await dynamicInterceptorBegin(executorEngineService as any)(
      req,
      res as any,
      next,
    );
    res.json(errorBody);
    await new Promise((resolve) => setTimeout(resolve, 0));

    expect(next).toHaveBeenCalledTimes(1);
    expect(executorEngineService.register).not.toHaveBeenCalled();
    expect(executorEngineService.runBatch).not.toHaveBeenCalled();
    expect(json).toHaveBeenCalledWith(errorBody);
  });

  it('persists pre-hook logs when a pre-hook throws a client error', async () => {
    const executorEngineService = {
      register: vi.fn(),
      runBatch: vi.fn(async (req: any) => {
        req.routeData.context.$share.$logs.push('unsupported model');
        const error: any = new Error('Unsupported model');
        error.statusCode = 400;
        throw error;
      }),
    };
    const record = vi.spyOn(runtimeLogs, 'recordUserLog');
    const req = {
      method: 'POST',
      path: '/gateway/v1/chat/completions',
      originalUrl: '/gateway/v1/chat/completions',
      routeData: {
        route: { path: '/gateway/v1/*' },
        context: { $share: { $logs: [] } },
        preHooks: [{ code: "@THROW400('Unsupported model')" }],
        postHooks: [],
      },
    };
    const next = vi.fn();

    await dynamicInterceptorBegin(executorEngineService as any)(
      req,
      { statusCode: 200, json: vi.fn() } as any,
      next,
    );

    expect(next).toHaveBeenCalledWith(
      expect.objectContaining({ statusCode: 400 }),
    );
    expect(record).toHaveBeenCalledWith(
      ['unsupported model'],
      expect.objectContaining({ statusCode: 400 }),
    );
    record.mockRestore();
  });

  it('lets a pre-hook stream an early response and stops the request pipeline', async () => {
    const req = {
      method: 'POST',
      path: '/gateway/v1/chat/completions',
      originalUrl: '/gateway/v1/chat/completions',
      routeData: {
        context: { $share: { $logs: [] } },
        preHooks: [{ code: 'await @RES.stream(errorStream)' }],
        postHooks: [],
      },
    };
    const executorEngineService = {
      register: vi.fn(),
      runBatch: vi.fn(async () => {
        await req.routeData.context.$res.stream(
          Readable.from([
            JSON.stringify({ error: { code: 'insufficient_quota' } }),
          ]),
          { statusCode: 429, mimetype: 'application/json' },
        );
        return { shortCircuit: false, value: undefined };
      }),
    };
    const response = new PassThrough() as PassThrough & {
      headersSent: boolean;
      statusCode: number;
      status: ReturnType<typeof vi.fn>;
      setHeader: ReturnType<typeof vi.fn>;
      json: ReturnType<typeof vi.fn>;
    };
    response.headersSent = false;
    response.statusCode = 200;
    response.status = vi.fn((statusCode: number) => {
      response.statusCode = statusCode;
      return response;
    });
    response.setHeader = vi.fn();
    const json = vi.fn();
    response.json = json;
    const chunks: Buffer[] = [];
    response.on('data', (chunk) => chunks.push(Buffer.from(chunk)));
    const next = vi.fn();

    await dynamicInterceptorBegin(executorEngineService as any)(
      req,
      response as any,
      next,
    );

    expect(response.status).toHaveBeenCalledWith(429);
    expect(response.setHeader).toHaveBeenCalledWith(
      'Content-Type',
      'application/json',
    );
    expect(next).not.toHaveBeenCalled();
    expect(json).not.toHaveBeenCalled();
    expect(JSON.parse(Buffer.concat(chunks).toString('utf8'))).toEqual({
      error: { code: 'insufficient_quota' },
    });
    expect(req.routeData.context).not.toHaveProperty('$res');
  });
});
