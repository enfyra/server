import { describe, expect, it, vi } from 'vitest';
import { HttpException as KernelHttpException } from '@enfyra/kernel';
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

function runFilter(exception: unknown, request: Record<string, unknown>) {
  const response = {
    status: vi.fn().mockReturnThis(),
    json: vi.fn(),
  };

  globalExceptionMiddleware(
    exception,
    request as any,
    response as any,
    vi.fn(),
  );

  return response.json.mock.calls[0]?.[0] as ErrorResponse;
}

describe('global exception middleware request-size errors', () => {
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
