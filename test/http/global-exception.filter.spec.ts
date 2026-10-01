import { describe, expect, it, vi } from 'vitest';
import { HttpException as KernelHttpException } from '@enfyra/kernel';
import { HttpException } from '../../src/domain/exceptions';
import { globalExceptionMiddleware } from '../../src/domain/exceptions/filters/global-exception.filter';

type ErrorResponse = {
  success: boolean;
  statusCode: number;
  message?: string | string[];
  retry_after_seconds?: number;
  error: {
    code: string;
    message?: string | string[];
    details?: Record<string, unknown>;
  };
};

function runFilterWithResponse(
  exception: unknown,
  request: Record<string, unknown>,
) {
  const response = {
    status: vi.fn().mockReturnThis(),
    setHeader: vi.fn(),
    getHeader: vi.fn(),
    json: vi.fn(),
  };

  globalExceptionMiddleware(
    exception,
    request as any,
    response as any,
    vi.fn(),
  );

  return {
    body: response.json.mock.calls[0]?.[0] as ErrorResponse,
    response,
  };
}

function runFilter(exception: unknown, request: Record<string, unknown>) {
  return runFilterWithResponse(exception, request).body;
}

describe('global exception middleware request-size errors', () => {
  it('omits empty details while retaining server-owned trace fields', () => {
    const { body: response, response: httpResponse } = runFilterWithResponse(
      new HttpException('Upstream service failed', 502),
      {
        method: 'POST',
        url: '/v1/chat/completions?api_key=private',
        headers: {},
        correlationId: 'req_generic_error',
      },
    );

    expect(response.statusCode).toBe(502);
    expect(response.error).toMatchObject({
      code: 'HTTP_ERROR',
      message: 'Upstream service failed',
      path: '/v1/chat/completions',
      method: 'POST',
      correlationId: 'req_generic_error',
    });
    expect(response.error).not.toHaveProperty('details');
    expect(response.error).not.toHaveProperty('statusCode');
    expect(Date.parse((response.error as any).timestamp)).not.toBeNaN();
    expect(httpResponse.setHeader).toHaveBeenCalledWith(
      'X-Correlation-ID',
      'req_generic_error',
    );
  });

  it('keeps GraphQL-native errors while adding the same server trace', () => {
    const { body: response } = runFilterWithResponse(
      new HttpException('Query failed', 400),
      {
        method: 'POST',
        url: '/graphql?token=private',
        headers: {},
        correlationId: 'req_graphql_error',
      },
    );

    expect(response).toMatchObject({
      errors: [
        {
          message: 'Query failed',
          extensions: {
            code: 'HTTP_ERROR',
            statusCode: 400,
            path: '/graphql',
            method: 'POST',
            correlationId: 'req_graphql_error',
          },
        },
      ],
    });
    expect((response as any).errors[0].extensions).not.toHaveProperty('details');
    expect(Date.parse((response as any).errors[0].extensions.timestamp)).not.toBeNaN();
  });

  it('preserves a custom exception that crossed the ESV boundary', () => {
    const response = runFilter(
      new KernelHttpException('Retry in five seconds', 503, {
        retry_after_seconds: 5,
      }),
      {
        method: 'POST',
        url: '/v1/chat/completions',
        headers: {},
      },
    );

    expect(response.statusCode).toBe(503);
    expect(response.error.code).toBe('HTTP_503');
    expect(response.error.details).toEqual({ retry_after_seconds: 5 });
    expect(response.retry_after_seconds).toBe(5);
  });

  it('reports the configured request-body limit for parser rejections', () => {
    const error = Object.assign(new Error('request entity too large'), {
      name: 'PayloadTooLargeError',
      type: 'entity.too.large',
      status: 413,
      limit: 2 * 1024 * 1024,
      received: 2.25 * 1024 * 1024,
    });

    const response = runFilter(error, {
      method: 'POST',
      url: '/gateway/v1/chat/completions',
      originalUrl: '/gateway/v1/chat/completions',
      headers: { 'content-type': 'application/json' },
      requestBodyLimitBytes: 2 * 1024 * 1024,
    });

    expect(response.statusCode).toBe(413);
    expect(response.error.code).toBe('REQUEST_ENTITY_TOO_LARGE');
    expect(response.error.details).toMatchObject({
      phase: 'body_parser',
      configuredLimitBytes: 2 * 1024 * 1024,
      configuredLimitMb: 2,
      receivedMb: 2.25,
    });
    expect(JSON.stringify(response)).not.toContain('request entity too large');
  });

  it('reports the effective multipart upload limit in MB', () => {
    const fileContent = 'private-file-content-should-not-appear';
    const error = Object.assign(new Error('File too large'), {
      code: 'LIMIT_FILE_SIZE',
    });

    const response = runFilter(error, {
      method: 'POST',
      url: '/files',
      originalUrl: '/files',
      headers: {
        'content-type': 'multipart/form-data; boundary=test',
        'content-length': String(12 * 1024 * 1024),
      },
      uploadFileSizeLimitBytes: 10 * 1024 * 1024,
      uploadProgressLoaded: 12 * 1024 * 1024,
      body: fileContent,
    });

    expect(response.statusCode).toBe(413);
    expect(response.error.code).toBe('FILE_TOO_LARGE');
    expect(response.error.details).toMatchObject({
      phase: 'multipart_upload',
      configuredLimitBytes: 10 * 1024 * 1024,
      configuredLimitMb: 10,
      receivedBytes: 12 * 1024 * 1024,
      receivedMb: 12,
    });
    expect(JSON.stringify(response)).not.toContain(fileContent);
  });

  it('maps a strict-mode JSON parse failure to a 400 invalid-body error', () => {
    const error = Object.assign(
      new SyntaxError("Unexpected token 'n', \"null\" is not valid JSON"),
      {
        type: 'entity.parse.failed',
        status: 400,
        statusCode: 400,
        body: 'null',
      },
    );

    const response = runFilter(error, {
      method: 'POST',
      url: '/enfyra_role',
      originalUrl: '/enfyra_role',
      headers: { 'content-type': 'application/json' },
    });

    expect(response.statusCode).toBe(400);
    expect(response.error.code).toBe('BAD_REQUEST');
    expect(response.error.message).toEqual(['Invalid JSON body']);
  });
});
