import { EventEmitter } from 'node:events';
import { PassThrough, Readable } from 'node:stream';
import { describe, expect, it, vi } from 'vitest';
import { attachStreamResponseHelper } from '../../src/modules/dynamic-api/services/dynamic.service';

function makeResponse() {
  const response = new PassThrough() as PassThrough & {
    headersSent: boolean;
    __enfyraStreamStarted?: boolean;
    status: ReturnType<typeof vi.fn>;
    setHeader: ReturnType<typeof vi.fn>;
    json: ReturnType<typeof vi.fn>;
    destroy: ReturnType<typeof vi.fn>;
    req?: {
      method: string;
      url: string;
      originalUrl?: string;
      correlationId?: string;
      headers: Record<string, string>;
    };
  };
  response.headersSent = false;
  response.status = vi.fn(() => response);
  response.setHeader = vi.fn();
  response.json = vi.fn((body: unknown) => {
    response.headersSent = true;
    response.end(JSON.stringify(body));
    return response;
  });
  response.destroy = vi.fn((error?: Error) => {
    response.headersSent = true;
    response.emit('close');
    return response;
  });
  return response;
}

describe('attachStreamResponseHelper', () => {
  it('writes an exact custom success without calling res.json', async () => {
    const response = makeResponse() as ReturnType<typeof makeResponse> & {
      __enfyraJson: (jsonText: string, options?: unknown) => Promise<void>;
    };
    const body = { data: { id: 'project-1' }, success: true };
    attachStreamResponseHelper(response);

    await response.__enfyraJson(JSON.stringify(body), {
      statusCode: 201,
      headers: { 'x-resource-created': 'true' },
    });

    expect(response.__enfyraStreamStarted).toBe(true);
    expect(response.status).toHaveBeenCalledWith(201);
    expect(response.setHeader).toHaveBeenCalledWith(
      'Content-Type',
      'application/json; charset=utf-8',
    );
    expect(response.setHeader).toHaveBeenCalledWith(
      'x-resource-created',
      'true',
    );
    expect(response.json).not.toHaveBeenCalled();
    expect(JSON.parse(response.read().toString())).toEqual(body);
  });

  it('merges custom error fields with server-owned trace fields', async () => {
    const response = makeResponse() as ReturnType<typeof makeResponse> & {
      __enfyraErrorJson: (jsonText: string, options?: unknown) => Promise<void>;
    };
    response.req = {
      method: 'POST',
      url: '/v1/chat/completions?debug=true',
      originalUrl: '/api/v1/chat/completions?debug=true',
      correlationId: 'req_custom_error',
      headers: { 'x-correlation-id': 'spoofed-header-id' },
    };
    const body = {
      success: true,
      statusCode: 200,
      request_id: 'public-request-id',
      error: {
        code: 'upstream_error',
        message: 'Please retry shortly.',
        should_retry: true,
        statusCode: 200,
        timestamp: 'spoofed-timestamp',
        path: '/spoofed',
        method: 'GET',
        correlationId: 'spoofed-body-id',
      },
    };
    attachStreamResponseHelper(response);

    await response.__enfyraErrorJson(JSON.stringify(body), {
      statusCode: 502,
      headers: {
        'x-should-retry': 'true',
        'X-Correlation-ID': 'spoofed-options-id',
      },
    });

    expect(response.__enfyraStreamStarted).toBe(true);
    expect(response.status).toHaveBeenCalledWith(502);
    expect(response.setHeader).toHaveBeenCalledWith(
      'Content-Type',
      'application/json; charset=utf-8',
    );
    expect(response.setHeader).toHaveBeenCalledWith(
      'x-should-retry',
      'true',
    );
    expect(response.setHeader).toHaveBeenCalledWith(
      'X-Correlation-ID',
      'req_custom_error',
    );
    expect(response.json).not.toHaveBeenCalled();
    const result = JSON.parse(response.read().toString());
    expect(result).toMatchObject({
      success: false,
      statusCode: 502,
      request_id: 'public-request-id',
      error: {
        code: 'upstream_error',
        message: 'Please retry shortly.',
        should_retry: true,
        path: '/api/v1/chat/completions',
        method: 'POST',
        correlationId: 'req_custom_error',
      },
    });
    expect(result.error).not.toHaveProperty('statusCode');
    expect(result.error.timestamp).not.toBe('spoofed-timestamp');
    expect(Date.parse(result.error.timestamp)).not.toBeNaN();
  });

  it('creates the error object when a custom body omits it', async () => {
    const response = makeResponse() as ReturnType<typeof makeResponse> & {
      __enfyraErrorJson: (jsonText: string, options?: unknown) => Promise<void>;
    };
    response.req = {
      method: 'DELETE',
      url: '/projects/1',
      correlationId: 'req_missing_error',
      headers: {},
    };
    attachStreamResponseHelper(response);

    await response.__enfyraErrorJson('{"reason":"conflict"}', {
      statusCode: 409,
    });

    expect(JSON.parse(response.read().toString())).toMatchObject({
      success: false,
      statusCode: 409,
      reason: 'conflict',
      error: {
        path: '/projects/1',
        method: 'DELETE',
        correlationId: 'req_missing_error',
      },
    });
  });

  it('rejects custom error bodies that cannot carry an error object', async () => {
    const primitiveResponse = makeResponse() as ReturnType<typeof makeResponse> & {
      __enfyraErrorJson: (jsonText: string, options?: unknown) => Promise<void>;
    };
    const scalarErrorResponse = makeResponse() as ReturnType<typeof makeResponse> & {
      __enfyraErrorJson: (jsonText: string, options?: unknown) => Promise<void>;
    };
    attachStreamResponseHelper(primitiveResponse);
    attachStreamResponseHelper(scalarErrorResponse);

    await expect(
      primitiveResponse.__enfyraErrorJson('[]', { statusCode: 500 }),
    ).rejects.toThrow('@THROW.json body must be a JSON object');
    await expect(
      scalarErrorResponse.__enfyraErrorJson('{"error":"failed"}', {
        statusCode: 500,
      }),
    ).rejects.toThrow('@THROW.json body.error must be a JSON object');
    expect(primitiveResponse.__enfyraStreamStarted).not.toBe(true);
    expect(scalarErrorResponse.__enfyraStreamStarted).not.toBe(true);
  });

  it('keeps success and error JSON status ranges separate', async () => {
    const successResponse = makeResponse() as ReturnType<typeof makeResponse> & {
      __enfyraJson: (jsonText: string, options?: unknown) => Promise<void>;
    };
    const errorResponse = makeResponse() as ReturnType<typeof makeResponse> & {
      __enfyraErrorJson: (jsonText: string, options?: unknown) => Promise<void>;
    };
    attachStreamResponseHelper(successResponse);
    attachStreamResponseHelper(errorResponse);

    await expect(
      successResponse.__enfyraJson('{"error":true}', { statusCode: 400 }),
    ).rejects.toThrow('@RES.json statusCode must be from 200 to 399');
    await expect(
      errorResponse.__enfyraErrorJson('{"ok":true}', { statusCode: 200 }),
    ).rejects.toThrow('@THROW.json statusCode must be from 400 to 599');
  });

  it('writes binary bytes through the native response boundary unchanged', async () => {
    const response = makeResponse() as ReturnType<typeof makeResponse> & {
      __enfyraBytes: (bytes: Uint8Array, options?: unknown) => Promise<void>;
    };
    attachStreamResponseHelper(response);

    await response.__enfyraBytes(new Uint8Array([0, 255, 1, 2]), {
      mimetype: 'image/png',
      filename: 'image.png',
    });

    expect(response.__enfyraStreamStarted).toBe(true);
    expect(response.setHeader).toHaveBeenCalledWith('Content-Type', 'image/png');
    expect(response.setHeader).toHaveBeenCalledWith(
      'Content-Disposition',
      'attachment; filename="image.png"',
    );
    expect(Buffer.from(response.read())).toEqual(Buffer.from([0, 255, 1, 2]));
  });

  it('rejects a second native response boundary', async () => {
    const response = makeResponse() as ReturnType<typeof makeResponse> & {
      __enfyraJson: (jsonText: string, options?: unknown) => Promise<void>;
      __enfyraBytes: (bytes: Uint8Array, options?: unknown) => Promise<void>;
    };
    attachStreamResponseHelper(response);

    await response.__enfyraJson('{"ok":true}');

    await expect(
      response.__enfyraBytes(new Uint8Array([1])),
    ).rejects.toThrow('Dynamic response has already started');
  });

  it('returns a Promise that resolves when the readable ends', async () => {
    const response = makeResponse();
    attachStreamResponseHelper(response);

    const completion = response.stream(Readable.from(['a', 'b']), {
      mimetype: 'text/plain',
      statusCode: 201,
    });

    expect(completion).toBeInstanceOf(Promise);
    expect(response.__enfyraStreamStarted).toBe(true);
    await expect(completion).resolves.toBeUndefined();
    expect(response.status).toHaveBeenCalledWith(201);
    expect(response.setHeader).toHaveBeenCalledWith('Content-Type', 'text/plain');
  });

  it('rejects when the readable errors and destroys an already-started response', async () => {
    const response = makeResponse();
    attachStreamResponseHelper(response);
    response.headersSent = true;

    const source = new Readable({
      read() {
        this.destroy(new Error('upstream failed'));
      },
    });
    const completion = response.stream(source);

    await expect(completion).rejects.toThrow('upstream failed');
    expect(response.destroy).toHaveBeenCalledWith(expect.any(Error));
  });

  it('rejects when the readable errors before response headers are sent', async () => {
    const response = makeResponse();
    attachStreamResponseHelper(response);
    const source = new Readable({
      read() {
        this.destroy(new Error('source failed'));
      },
    });

    await expect(response.stream(source)).rejects.toThrow('source failed');
    expect(response.json).toHaveBeenCalledWith({
      success: false,
      message: 'The response stream failed.',
      statusCode: 500,
      error: {
        code: 'STREAM_FAILED',
        message: 'The response stream failed.',
      },
    });
  });

  it('resolves when the response closes before the readable ends', async () => {
    const response = makeResponse();
    attachStreamResponseHelper(response);
    const source = new EventEmitter() as NodeJS.ReadableStream & {
      pipe: ReturnType<typeof vi.fn>;
    };
    source.pipe = vi.fn(() => response);

    const completion = response.stream(source);
    response.emit('close');

    await expect(completion).resolves.toBeUndefined();
  });
});
