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
    expect(response.json).toHaveBeenCalledWith(
      expect.objectContaining({
        statusCode: 500,
        error: expect.objectContaining({ code: 'STREAM_FAILED' }),
      }),
    );
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
