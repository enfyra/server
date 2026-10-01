import { describe, expect, it, vi } from 'vitest';
import { EventEmitter } from 'events';
import {
  RawScriptErrorCarrier,
  ValidationException as KernelValidationException,
} from '@enfyra/kernel';
import { DynamicService } from '../../src/modules/dynamic-api/services/dynamic.service';
import { HttpException } from '../../src/domain/exceptions';
import * as runtimeLogs from '../../src/shared/runtime-log-buffer';

function createRequest(overrides: any = {}) {
  return {
    method: 'GET',
    url: '/api/test',
    user: { id: 1 },
    routeData: {
      handler: 'return true;',
      postHooks: [],
      context: {
        $share: { $logs: [] },
        $query: {},
      },
      ...overrides.routeData,
    },
    ...overrides,
  } as any;
}

describe('DynamicService error reporting', () => {
  it('persists dynamic script diagnostics for client errors', async () => {
    const executorError: any = new Error('upstream rejected request');
    executorError.statusCode = 400;
    const service = new DynamicService({
      executorEngineService: {
        register: vi.fn(),
        runBatch: vi.fn(async () => {
          throw executorError;
        }),
      },
      loggingService: {
        error: vi.fn(),
      },
    } as any);
    const record = vi.spyOn(runtimeLogs, 'recordUserLog');
    const request = createRequest({
      routeData: {
        context: {
          $share: { $logs: ['upstream status=400'] },
          $query: {},
        },
      },
    });

    await expect(service.runHandler(request)).rejects.toMatchObject({
      statusCode: 400,
    });

    expect(record).toHaveBeenCalledWith(['upstream status=400'], expect.objectContaining({ statusCode: 400 }));
    record.mockRestore();
  });

  it('keeps script client error message separate from object details', async () => {
    const executorError: any = new Error(
      'Script execution failed: missingValue is not defined (handler, line 2)',
    );
    executorError.statusCode = 400;
    executorError.details = {
      scriptId: '(batch execution)',
      phase: 'handler',
      line: 2,
      codeFrame: [
        '  1. const first = "row 1";',
        '> 2. const second = missingValue + 1;',
        '  3. return { first, second };',
      ].join('\n'),
    };

    const service = new DynamicService({
      executorEngineService: {
        register: vi.fn(),
        runBatch: vi.fn(async () => {
          throw executorError;
        }),
      },
      loggingService: {
        error: vi.fn(),
      },
    } as any);

    await expect(service.runHandler(createRequest())).rejects.toMatchObject({
      statusCode: 400,
    });

    try {
      await service.runHandler(createRequest());
      throw new Error('should have thrown');
    } catch (error) {
      expect(error).toBeInstanceOf(HttpException);
      const response = (error as HttpException).getResponse() as any;
      expect(response.message).toContain('missingValue is not defined');
      expect(response.message).not.toBe('{"scriptId":"(batch execution)"}');
      expect(response.details).toMatchObject({
        scriptId: '(batch execution)',
        phase: 'handler',
        line: 2,
      });
      expect(response.details.codeFrame).toContain(
        '> 2. const second = missingValue + 1;',
      );
    }
  });

  it('normalizes a Kernel HTTP carrier into the generic ESV envelope contract', async () => {
    const executorError = new RawScriptErrorCarrier(
      'Retry in five seconds',
      503,
      'HTTP_503',
      '$throw.http',
      { retry_after_seconds: 5 },
      'handler-1',
    );
    const service = new DynamicService({
      executorEngineService: {
        register: vi.fn(),
        runBatch: vi.fn(async () => {
          throw executorError;
        }),
      },
      loggingService: {
        error: vi.fn(),
      },
    } as any);

    try {
      await service.runHandler(createRequest());
      throw new Error('should have thrown');
    } catch (error) {
      expect(error).toBeInstanceOf(HttpException);
      expect(error).not.toBe(executorError);
      expect(error).toMatchObject({
        statusCode: 503,
        errorCode: 'HTTP_ERROR',
        message: 'Retry in five seconds',
      });
      expect((error as HttpException).details).toBeUndefined();
    }
  });

  it('writes a Kernel custom JSON error carrier through the ESV error boundary', async () => {
    const executorError = new RawScriptErrorCarrier(
      'Custom JSON error response',
      502,
      'HTTP_502',
      '$throw.json',
      {
        errorJsonText: '{"error":{"code":"upstream_error","should_retry":true}}',
        errorJsonOptions: {
          statusCode: 502,
          headers: { 'x-should-retry': 'true' },
        },
      },
      'handler-1',
    );
    const response: any = new EventEmitter();
    response.writableEnded = false;
    response.headersSent = false;
    response.statusCode = 200;
    response.status = vi.fn((statusCode: number) => {
      response.statusCode = statusCode;
      return response;
    });
    response.setHeader = vi.fn();
    response.end = vi.fn(() => {
      response.writableEnded = true;
      response.headersSent = true;
    });
    const service = new DynamicService({
      executorEngineService: {
        register: vi.fn(),
        runBatch: vi.fn(async () => {
          throw executorError;
        }),
      },
      loggingService: { error: vi.fn() },
    } as any);
    const request = createRequest({
      method: 'POST',
      url: '/v1/chat/completions',
      originalUrl: '/api/v1/chat/completions',
      correlationId: 'req_custom_error',
    });
    response.req = request;
    request.routeData.res = response;

    await expect(service.runHandler(request)).resolves.toBeUndefined();
    expect(response.status).toHaveBeenCalledWith(502);
    expect(response.setHeader).toHaveBeenCalledWith(
      'x-should-retry',
      'true',
    );
    const responseBody = JSON.parse(response.end.mock.calls[0][0]);
    expect(responseBody).toMatchObject({
      error: {
        code: 'upstream_error',
        should_retry: true,
        path: '/api/v1/chat/completions',
        method: 'POST',
        correlationId: 'req_custom_error',
      },
    });
    expect(Date.parse(responseBody.error.timestamp)).not.toBeNaN();
    expect(response.setHeader).toHaveBeenCalledWith(
      'X-Correlation-ID',
      'req_custom_error',
    );
  });

  it('preserves non-HTTP cross-boundary domain errors', async () => {
    const executorError = new KernelValidationException('Referral code is invalid', {
      field: 'code',
    });
    const service = new DynamicService({
      executorEngineService: {
        register: vi.fn(),
        runBatch: vi.fn(async () => {
          throw executorError;
        }),
      },
      loggingService: {
        error: vi.fn(),
      },
    } as any);

    await expect(service.runHandler(createRequest())).rejects.toBe(executorError);
  });

  it('treats response-close cancellation as a normal disconnect', async () => {
    const response: any = new EventEmitter();
    response.writableEnded = false;
    const request: any = Object.assign(new EventEmitter(), createRequest({
      routeData: {
        handler: 'return true;',
        postHooks: [],
        context: { $share: { $logs: [] }, $query: {} },
        res: response,
      },
    }));
    const loggingService = { error: vi.fn() };
    const runBatch = vi.fn(
      async (_req: any, _timeout: number, options: { signal: AbortSignal }) => {
        return new Promise((_resolve, reject) => {
          options.signal.addEventListener(
            'abort',
            () => {
              const error: any = new Error('Execution aborted after client disconnect');
              error.code = 'ERR_EXECUTION_ABORTED';
              reject(error);
            },
            { once: true },
          );
          queueMicrotask(() => response.emit('close'));
        });
      },
    );
    const service = new DynamicService({
      executorEngineService: {
        register: vi.fn(),
        runBatch,
      },
      loggingService,
    } as any);

    await expect(service.runHandler(request)).resolves.toBeUndefined();
    expect(runBatch).toHaveBeenCalledWith(
      request,
      60_000,
      expect.objectContaining({ signal: expect.any(AbortSignal) }),
    );
    expect(loggingService.error).not.toHaveBeenCalled();
  });
});
