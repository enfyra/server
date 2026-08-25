import { describe, expect, it, vi } from 'vitest';
import { EventEmitter } from 'events';
import {
  HttpException as KernelHttpException,
  ValidationException as KernelValidationException,
} from '@enfyra/kernel';
import { DynamicService } from '../../src/modules/dynamic-api/services/dynamic.service';
import { HttpException } from '../../src/domain/exceptions';

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
    const warn = vi.spyOn((service as any).logger, 'warn');
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

    expect(warn).toHaveBeenCalledWith(
      expect.objectContaining({
        message: 'Dynamic script log',
        statusCode: 400,
        entry: 'upstream status=400',
      }),
    );
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

  it.each([
    new KernelHttpException('Retry in five seconds', 503, {
      retry_after_seconds: 5,
    }),
    new KernelValidationException('Referral code is invalid', {
      field: 'code',
    }),
  ])('preserves cross-boundary custom throws', async (executorError) => {
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

    await expect(service.runHandler(createRequest())).rejects.toBe(
      executorError,
    );
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
