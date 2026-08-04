import { mkdtemp, rm, writeFile } from 'fs/promises';
import { tmpdir } from 'os';
import path from 'path';
import { afterEach, describe, expect, it } from 'vitest';
import { IsolatedExecutorService } from '@enfyra/kernel';

let tempDir: string | null = null;

afterEach(async () => {
  if (tempDir) {
    await rm(tempDir, { recursive: true, force: true });
    tempDir = null;
  }
});

function makeService(modulePath: string) {
  return new IsolatedExecutorService({
    packageCacheService: {
      getPackages: async () => ['sse-package'],
    } as any,
    packageCdnLoaderService: {
      getPackageSources: () => [
        {
          name: 'sse-package',
          safeName: 'sse_package',
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
});
