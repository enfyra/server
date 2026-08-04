import { mkdtemp, rm, writeFile } from 'fs/promises';
import { tmpdir } from 'os';
import path from 'path';
import { EventEmitter } from 'events';
import { afterEach, describe, expect, it } from 'vitest';
import { IsolatedExecutorService } from '@enfyra/kernel';

let tempDir: string | null = null;

afterEach(async () => {
  if (tempDir) {
    await rm(tempDir, { recursive: true, force: true });
    tempDir = null;
  }
});

function makeService(modulePath: string, packageName = 'sse-package') {
  return new IsolatedExecutorService({
    packageCacheService: {
      getPackages: async () => [packageName],
    } as any,
    packageCdnLoaderService: {
      getPackageSources: () => [
        {
          name: packageName,
          safeName: packageName.replace(/[^a-z0-9_]/gi, '_'),
          version: '1.0.0',
          sourceCode: '',
          filePath: modulePath,
          fileUrl: modulePath,
        },
      ],
    } as any,
  });
}

describe('stream observer callback', () => {
  it('lets the sandbox observe every streamed chunk without blocking the relay', async () => {
    tempDir = await mkdtemp(path.join(tmpdir(), 'enfyra-stream-observer-'));
    const modulePath = path.join(tempDir, 'sse-package.mjs');
    await writeFile(
      modulePath,
      `
        class SseStream {
          constructor(chunks) {
            this.chunks = chunks;
          }
          async *[Symbol.asyncIterator]() {
            for (const chunk of this.chunks) yield chunk;
          }
        }
        export default {
          createSseStream() {
            return new SseStream([
              'data: {"delta":"a"}\\n\\n',
              'data: {"delta":"b"}\\n\\n',
              'data: {"usage":{"prompt":3,"completion":5}}\\n\\n',
            ]);
          },
        };
      `,
      'utf8',
    );

    const service = makeService(modulePath);
    const received: Buffer[] = [];
    const observed: Array<{ text: string; kind: string }> = [];

    const ctx: any = {
      $body: {},
      $query: {},
      $params: {},
      $share: { $logs: [] },
      $helpers: {},
      $cache: {},
      $repos: {},
      $user: null,
      $res: {
        stream: (stream: any, options: any) =>
          new Promise<void>((resolve, reject) => {
            expect(options.mimetype).toBe('text/event-stream');
            expect(options.observer).toBeUndefined();
            stream.on('data', (chunk: Buffer) => received.push(chunk));
            stream.on('end', () => resolve());
            stream.on('error', (error: Error) => reject(error));
          }),
      },
    };

    try {
      const result = await service.run(
        `
          const pkg = $ctx.$pkgs['sse-package'];
          const stream = pkg.createSseStream();
          const observed = [];
          await $ctx.$res.stream(stream, {
            mimetype: 'text/event-stream',
            observer: (text, kind) => { observed.push({ text, kind }); },
          });
          return { observed };
        `,
        ctx,
        5000,
      );

      expect(result).toEqual({
        observed: [
          { text: 'data: {"delta":"a"}\n\n', kind: 'chunk' },
          { text: 'data: {"delta":"b"}\n\n', kind: 'chunk' },
          { text: 'data: {"usage":{"prompt":3,"completion":5}}\n\n', kind: 'chunk' },
          { text: '', kind: 'end' },
        ],
      });
      expect(Buffer.concat(received).toString('utf8')).toBe(
        'data: {"delta":"a"}\n\n' +
          'data: {"delta":"b"}\n\n' +
          'data: {"usage":{"prompt":3,"completion":5}}\n\n',
      );
    } finally {
      service.onDestroy();
    }
  });

  it('observes local Readable.from chunks before relaying them', async () => {
    const service = new IsolatedExecutorService({
      packageCacheService: {
        getPackages: async () => [],
      } as any,
      packageCdnLoaderService: {
        getPackageSources: () => [],
      } as any,
    });
    const received: Buffer[] = [];
    const ctx: any = {
      $body: {},
      $query: {},
      $params: {},
      $share: { $logs: [] },
      $helpers: {},
      $cache: {},
      $repos: {},
      $user: null,
      $res: {
        stream: (stream: any, options: any) =>
          new Promise<void>((resolve, reject) => {
            expect(options.observer).toBeUndefined();
            stream.on('data', (chunk: Buffer) => received.push(chunk));
            stream.on('end', () => resolve());
            stream.on('error', (error: Error) => reject(error));
          }),
      },
    };

    try {
      const result = await service.run(
        `
          const { Readable } = require('stream');
          const observed = [];
          await $ctx.$res.stream(Readable.from(['a', 'b']), {
            observer: (text, kind) => { observed.push({ text, kind }); },
          });
          return { observed };
        `,
        ctx,
        5000,
      );

      expect(result).toEqual({
        observed: [
          { text: 'a', kind: 'chunk' },
          { text: 'b', kind: 'chunk' },
          { text: '', kind: 'end' },
        ],
      });
      expect(Buffer.concat(received).toString('utf8')).toBe('ab');
    } finally {
      service.onDestroy();
    }
  });

  it('cancels a package-backed stream when the response closes', async () => {
    tempDir = await mkdtemp(path.join(tmpdir(), 'enfyra-stream-cancel-'));
    const modulePath = path.join(tempDir, 'undici.mjs');
    await writeFile(
      modulePath,
      `
        class ControlledBody {
          constructor(signal) {
            this.aborted = false;
            this.release = null;
            signal.addEventListener('abort', () => {
              this.aborted = true;
              this.release?.();
            }, { once: true });
          }
          async *[Symbol.asyncIterator]() {
            yield 'first';
            await new Promise((resolve) => { this.release = resolve; });
            if (this.aborted) throw new Error('upstream aborted');
          }
        }
        export default {
          async request(_url, options) {
            return {
              statusCode: 200,
              headers: { 'content-type': 'text/event-stream' },
              body: new ControlledBody(options.signal),
            };
          },
        };
      `,
      'utf8',
    );

    const service = makeService(modulePath, 'undici');
    const response: any = new EventEmitter();
    response.writableEnded = false;
    let ended = false;
    let firstChunk = false;
    response.stream = (stream: any) =>
      new Promise<void>((resolve, reject) => {
        stream.on('data', () => {
          if (!firstChunk) {
            firstChunk = true;
            queueMicrotask(() => response.emit('close'));
          }
        });
        stream.on('end', () => {
          ended = true;
          resolve();
        });
        stream.on('error', reject);
      });

    try {
      const result = service.run(
        `
          const upstream = await $ctx.$pkgs.undici.request('https://upstream.test', { method: 'GET' });
          await $ctx.$res.stream(upstream.body, { mimetype: 'text/event-stream' });
        `,
        {
          $body: {},
          $query: {},
          $params: {},
          $share: { $logs: [] },
          $helpers: {},
          $cache: {},
          $repos: {},
          $user: null,
          $res: response,
        },
        5000,
      );

      await expect(result).rejects.toMatchObject({
        code: 'ERR_EXECUTION_ABORTED',
      });
      expect(firstChunk).toBe(true);
      expect(ended).toBe(false);
    } finally {
      service.onDestroy();
    }
  });

  it('cancels a pending package request through the external task signal', async () => {
    tempDir = await mkdtemp(path.join(tmpdir(), 'enfyra-request-cancel-'));
    const modulePath = path.join(tempDir, 'undici.mjs');
    await writeFile(
      modulePath,
      `
        export default {
          request(_url, options) {
            if (options.signal.aborted) return Promise.reject(new Error('upstream aborted'));
            return new Promise((_resolve, reject) => {
              options.signal.addEventListener('abort', () => reject(new Error('upstream aborted')), { once: true });
            });
          },
        };
      `,
      'utf8',
    );

    const service = makeService(modulePath, 'undici');
    const controller = new AbortController();
    const context: any = {
      $body: {},
      $query: {},
      $params: {},
      $share: { $logs: [] },
      $helpers: {},
      $cache: {},
      $repos: {},
      $user: null,
    };

    try {
      const result = service.runBatch(
        [
          {
            type: 'handler',
            code: `await $ctx.$pkgs.undici.request('https://upstream.test', { method: 'GET' });`,
          },
        ],
        context,
        5000,
        { signal: controller.signal },
      );
      setTimeout(() => controller.abort(), 20);

      await expect(result).rejects.toMatchObject({
        code: 'ERR_EXECUTION_ABORTED',
      });
    } finally {
      service.onDestroy();
    }
  });
});
