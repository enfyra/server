import { describe, expect, it, vi } from 'vitest';
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
});
