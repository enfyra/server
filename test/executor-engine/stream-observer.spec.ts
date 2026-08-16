import { mkdtemp, rm, writeFile } from 'fs/promises';
import { tmpdir } from 'os';
import path from 'path';
import { EventEmitter } from 'events';
import { createServer, type Server } from 'http';
import { afterEach, describe, expect, it } from 'vitest';
import { IsolatedExecutorService } from '@enfyra/kernel';

let tempDir: string | null = null;

afterEach(async () => {
  if (tempDir) {
    await rm(tempDir, { recursive: true, force: true });
    tempDir = null;
  }
});

function makeService(modulePath?: string, packageName = 'sse-package') {
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
          ...(modulePath ? { filePath: modulePath, fileUrl: modulePath } : {}),
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

  it('relays transformed package-backed stream chunks and terminal output', async () => {
    tempDir = await mkdtemp(path.join(tmpdir(), 'enfyra-stream-transform-'));
    const modulePath = path.join(tempDir, 'sse-package.mjs');
    await writeFile(
      modulePath,
      `
        class SseStream {
          async *[Symbol.asyncIterator]() {
            yield 'data: one\\n\\n';
            yield 'data: two\\n\\n';
          }
        }
        export default { createSseStream() { return new SseStream(); } };
      `,
      'utf8',
    );

    const service = makeService(modulePath);
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
            expect(options.transform).toBeUndefined();
            stream.on('data', (chunk: Buffer) => received.push(chunk));
            stream.on('end', () => resolve());
            stream.on('error', (error: Error) => reject(error));
          }),
      },
    };

    try {
      await service.run(
        `
          const stream = $ctx.$pkgs['sse-package'].createSseStream();
          await $ctx.$res.stream(stream, {
            transform: (text, kind) => {
              if (kind === 'end') return 'data: [DONE]\\n\\n';
              return text.replace('data:', 'event: converted\\ndata:');
            },
          });
        `,
        ctx,
        5000,
      );

      expect(Buffer.concat(received).toString('utf8')).toBe(
        'event: converted\ndata: one\n\n' +
          'event: converted\ndata: two\n\n' +
          'data: [DONE]\n\n',
      );
    } finally {
      service.onDestroy();
    }
  });

  it('preserves UTF-8 code points split across transformed stream chunks', async () => {
    tempDir = await mkdtemp(path.join(tmpdir(), 'enfyra-stream-transform-utf8-'));
    const modulePath = path.join(tempDir, 'utf8-package.mjs');
    await writeFile(
      modulePath,
      `
        export default {
          async *createStream() {
            const bytes = Buffer.from('chào 👋', 'utf8');
            yield bytes.subarray(0, 3);
            yield bytes.subarray(3);
          },
        };
      `,
      'utf8',
    );

    const runStream = async (script: string, module?: string) => {
      const service = module ? makeService(module, 'utf8-package') : new IsolatedExecutorService({
        packageCacheService: { getPackages: async () => [] } as any,
        packageCdnLoaderService: { getPackageSources: () => [] } as any,
      });
      const received: Buffer[] = [];
      const ctx: any = {
        $body: {}, $query: {}, $params: {}, $share: { $logs: [] }, $helpers: {}, $cache: {}, $repos: {}, $user: null,
        $res: { stream: (stream: any) => new Promise<void>((resolve, reject) => {
          stream.on('data', (chunk: Buffer) => received.push(chunk));
          stream.on('end', resolve);
          stream.on('error', reject);
        }) },
      };
      try {
        await service.run(script, ctx, 5000);
        return Buffer.concat(received).toString('utf8');
      } finally {
        service.onDestroy();
      }
    };

    const transform = "transform: (text, kind) => kind === 'end' ? undefined : text.toUpperCase()";
    await expect(runStream(`
      await $ctx.$res.stream($ctx.$pkgs['utf8-package'].createStream(), { ${transform} });
    `, modulePath)).resolves.toBe('CHÀO 👋');
    await expect(runStream(`
      const { Readable } = require('stream');
      await $ctx.$res.stream(Readable.from(['ch', 'ào 👋']), { ${transform} });
    `)).resolves.toBe('CHÀO 👋');
    await expect(runStream(`
      const { Readable } = require('stream');
      const bytes = new TextEncoder().encode('chào 👋');
      await $ctx.$res.stream(Readable.from(Array.from(bytes, (byte) => new Uint8Array([byte]))), { ${transform} });
    `)).resolves.toBe('CHÀO 👋');
  });

  it('preserves every UTF-8 byte boundary through the proxy package stream bridge', async () => {
    tempDir = await mkdtemp(path.join(tmpdir(), 'enfyra-stream-proxy-utf8-'));
    const modulePath = path.join(tempDir, 'proxy-utf8-package.mjs');
    const expected = 'ASCII café Ελληνικά Русский العربية हिन्दी বাংলা ไทย ქართული հայերեն עברית 中文 日本語 한국어 𐐷 👩🏽‍💻 é 👋🌍';
    await writeFile(
      modulePath,
      `
        import { Buffer } from 'node:buffer';
        const bytes = Buffer.from(${JSON.stringify(expected)}, 'utf8');
        export default {
          async *createStream() {
            for (let index = 0; index < bytes.length; index++) yield bytes.subarray(index, index + 1);
          },
        };
      `,
      'utf8',
    );

    const service = makeService(modulePath, 'proxy-utf8-package');
    const received: Buffer[] = [];
    const ctx: any = {
      $body: {}, $query: {}, $params: {}, $share: { $logs: [] }, $helpers: {}, $cache: {}, $repos: {}, $user: null,
      $res: { stream: (stream: any) => new Promise<void>((resolve, reject) => {
        stream.on('data', (chunk: Buffer) => received.push(chunk));
        stream.on('end', resolve);
        stream.on('error', reject);
      }) },
    };

    try {
      await service.run(
        `
          await $ctx.$res.stream($ctx.$pkgs['proxy-utf8-package'].createStream(), {
            transform: (text, kind) => kind === 'end' ? undefined : text,
          });
        `,
        ctx,
        5000,
      );
      expect(Buffer.concat(received).toString('utf8')).toBe(expected);
    } finally {
      service.onDestroy();
    }
  });

  it('preserves 320 multilingual SSE responses across randomized byte fragmentation in parallel', async () => {
    tempDir = await mkdtemp(path.join(tmpdir(), 'enfyra-stream-sse-fragments-'));
    const modulePath = path.join(tempDir, 'fragmenting-sse-package.mjs');
    await writeFile(
      modulePath,
      `
        import { Buffer } from 'node:buffer';
        export default {
          async *createStream(base64, fragmentSizes) {
            const bytes = Buffer.from(base64, 'base64');
            let offset = 0;
            for (const size of fragmentSizes) {
              if (offset >= bytes.length) return;
              const next = Math.min(bytes.length, offset + size);
              yield bytes.subarray(offset, next);
              offset = next;
            }
            if (offset < bytes.length) yield bytes.subarray(offset);
          },
        };
      `,
      'utf8',
    );

    const tokens = ['a', 'é', 'Ω', 'Ж', 'ع', 'ह', 'ব', 'ก', 'ა', 'Ա', 'א', '中', '日', '한', '𐐷', '👩', '🏽', '‍', '💻', 'é', '👋', '🌍'];
    const cases = Array.from({ length: 320 }, (_, index) => {
      let state = index + 1;
      const next = () => {
        state = (state * 1664525 + 1013904223) >>> 0;
        return state;
      };
      const expected = Array.from({ length: 48 + (next() % 32) }, () => tokens[next() % tokens.length]).join('');
      const source = Buffer.from(`data: ${JSON.stringify({ choices: [{ delta: { content: expected } }] })}\n\ndata: [DONE]\n\n`).toString('base64');
      const fragmentSizes: number[] = [];
      let remaining = Buffer.from(source, 'base64').length;
      while (remaining > 0) {
        const size = 1 + (next() % 23);
        fragmentSizes.push(size);
        remaining -= size;
      }
      return { expected, source, fragmentSizes };
    });

    let nextCase = 0;
    const runCase = async (
      service: IsolatedExecutorService,
      { expected, source, fragmentSizes }: typeof cases[number],
    ) => {
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
          stream: (stream: any) => new Promise<void>((resolve, reject) => {
            stream.on('data', (chunk: Buffer) => received.push(chunk));
            stream.on('end', resolve);
            stream.on('error', reject);
          }),
        },
      };
      await service.run(
          `
            const upstream = $ctx.$pkgs['fragmenting-sse-package'].createStream(${JSON.stringify(source)}, ${JSON.stringify(fragmentSizes)});
            let sseBuffer = '';
            let content = '';
            const consume = (packet) => {
              const data = packet.split(/\\r?\\n/).filter((line) => line.startsWith('data:')).map((line) => line.slice(5).trim()).join('\\n');
              if (!data || data === '[DONE]') return;
              content += JSON.parse(data).choices[0].delta.content;
            };
            await $ctx.$res.stream(upstream, {
              transform: (text, kind) => {
                if (kind === 'chunk') {
                  sseBuffer += text;
                  const packets = sseBuffer.split(/\\r?\\n\\r?\\n/);
                  sseBuffer = packets.pop() || '';
                  for (const packet of packets) consume(packet);
                  return null;
                }
                if (sseBuffer.trim()) consume(sseBuffer);
                return content;
              },
            });
          `,
          ctx,
          10000,
      );
      expect(Buffer.concat(received).toString('utf8')).toBe(expected);
    };

    const workers = Array.from({ length: 12 }, async () => {
      const service = makeService(modulePath, 'fragmenting-sse-package');
      try {
        while (true) {
          const index = nextCase++;
          if (index >= cases.length) return;
          await runCase(service, cases[index]);
        }
      } finally {
        service.onDestroy();
      }
    });
    await Promise.all(workers);
  });

  it('preserves every UTF-8 byte boundary from an undici response through a gateway-style SSE transform', async () => {
    const expected = 'ASCII café Ελληνικά Русский العربية हिन्दी বাংলা ไทย ქართული հայերեն עברית 中文 日本語 한국어 𐐷 👩🏽‍💻 é 👋🌍';
    const source = Buffer.from(`data: ${JSON.stringify({ choices: [{ delta: { content: expected } }] })}\n\ndata: [DONE]\n\n`);
    let server: Server | null = createServer(async (_request, response) => {
      response.writeHead(200, { 'content-type': 'text/event-stream; charset=utf-8' });
      for (const byte of source) {
        response.write(Buffer.from([byte]));
        await new Promise<void>((resolve) => setImmediate(resolve));
      }
      response.end();
    });
    await new Promise<void>((resolve) => server!.listen(0, '127.0.0.1', resolve));
    const address = server.address();
    if (!address || typeof address === 'string') throw new Error('Test server did not bind to a TCP port');
    const service = makeService(undefined, 'undici');
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
        stream: (stream: any) => new Promise<void>((resolve, reject) => {
          stream.on('data', (chunk: Buffer) => received.push(chunk));
          stream.on('end', resolve);
          stream.on('error', reject);
        }),
      },
    };

    try {
      await service.run(
        `
          const upstream = await $ctx.$pkgs.undici.request('http://127.0.0.1:${address.port}', { method: 'GET' });
          let packetBuffer = '';
          await $ctx.$res.stream(upstream.body, {
            transform: (text, kind) => {
              if (kind === 'chunk') {
                packetBuffer += text;
                const packets = packetBuffer.split(/\\n\\n/);
                packetBuffer = packets.pop() || '';
                const output = packets.map((packet) => {
                  const data = packet.split(/\\r?\\n/).filter((line) => line.startsWith('data:')).map((line) => line.slice(5).trim()).join('\\n');
                  if (!data || data === '[DONE]') return data ? 'data: [DONE]\\n\\n' : '';
                  return 'data: ' + JSON.stringify(JSON.parse(data)) + '\\n\\n';
                }).join('');
                return output || null;
              }
              return packetBuffer ? packetBuffer + '\\n\\n' : '';
            },
          });
        `,
        ctx,
        10000,
      );
      expect(Buffer.concat(received).toString('utf8')).toBe(source.toString('utf8'));
    } finally {
      service.onDestroy();
      await new Promise<void>((resolve, reject) => server!.close((error) => error ? reject(error) : resolve()));
      server = null;
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
