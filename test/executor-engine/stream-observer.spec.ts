import { mkdtemp, readFile, rm, writeFile } from 'fs/promises';
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

  it('treats byte views as one Readable.from chunk and rejects duplicate consumers', async () => {
    const service = makeService();

    try {
      const result = await service.run(
        `
          const { Readable } = require('stream');
          const readable = Readable.from(Uint8Array.from([1, 2, 3]));
          const first = [];
          for await (const chunk of readable) {
            first.push(Array.from(chunk));
          }
          let duplicateCode = null;
          try {
            readable[Symbol.asyncIterator]();
          } catch (error) {
            duplicateCode = error.code;
          }
          return { first, duplicateCode };
        `,
        {
          $body: {}, $query: {}, $params: {}, $share: { $logs: [] },
          $helpers: {}, $cache: {}, $repos: {}, $user: null,
        } as any,
        5_000,
      );

      expect(result).toEqual({
        first: [[1, 2, 3]],
        duplicateCode: 'ERR_PACKAGE_STREAM_ALREADY_CONSUMED',
      });
    } finally {
      service.onDestroy();
    }
  });

  it('cancels Readable.from before consumption without opening the source', async () => {
    const service = makeService();

    try {
      const result = await service.run(
        `
          const { Readable } = require('stream');
          let opened = 0;
          let nextCalls = 0;
          let returnCalls = 0;
          const source = {
            [Symbol.asyncIterator]() {
              opened += 1;
              return {
                next() {
                  nextCalls += 1;
                  return Promise.resolve({ done: false, value: 'unexpected' });
                },
                return() {
                  returnCalls += 1;
                  return Promise.resolve({ done: true, value: undefined });
                },
              };
            },
          };
          const readable = Readable.from(source);
          await $ctx.$streams.cancel(readable);
          const iterator = readable[Symbol.asyncIterator]();
          const next = await iterator.next();

          const bytes = Readable.from(Uint8Array.from([1, 2, 3]));
          const byteIterator = bytes[Symbol.asyncIterator]();
          const returned = await byteIterator.return();
          const afterReturn = await byteIterator.next();

          return {
            opened,
            nextCalls,
            returnCalls,
            next,
            returned,
            afterReturn,
          };
        `,
        {
          $body: {}, $query: {}, $params: {}, $share: { $logs: [] },
          $helpers: {}, $cache: {}, $repos: {}, $user: null,
        } as any,
        5_000,
      );

      expect(result).toEqual({
        opened: 0,
        nextCalls: 0,
        returnCalls: 0,
        next: { done: true, value: undefined },
        returned: { done: true, value: undefined },
        afterReturn: { done: true, value: undefined },
      });
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

  it('preserves split UTF-8 bytes when a transform passes chunks through', async () => {
    const service = makeService();
    const received: Buffer[] = [];
    try {
      await service.run(
        `
          const { Readable } = require('stream');
          const bytes = new TextEncoder().encode('A👋B');
          const source = Readable.from([
            bytes.slice(0, 2),
            bytes.slice(2, 4),
            bytes.slice(4),
          ]);
          await $ctx.$res.stream(source, {
            transform: (text) => text === 'A' ? 'a' : undefined,
          });
        `,
        {
          $body: {}, $query: {}, $params: {}, $share: { $logs: [] },
          $helpers: {}, $cache: {}, $repos: {}, $user: null,
          $res: {
            stream: (stream: any) => new Promise<void>((resolve, reject) => {
              stream.on('data', (chunk: Buffer) => received.push(chunk));
              stream.on('end', resolve);
              stream.on('error', reject);
            }),
          },
        } as any,
        5000,
      );

      expect(Buffer.concat(received)).toEqual(Buffer.from('a👋B', 'utf8'));
    } finally {
      service.onDestroy();
    }
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

  it('iterates a package-backed readable before starting a response', async () => {
    tempDir = await mkdtemp(path.join(tmpdir(), 'enfyra-stream-iterator-'));
    const modulePath = path.join(tempDir, 'iterator-package.mjs');
    await writeFile(
      modulePath,
      `
        import { Buffer } from 'node:buffer';
        export default {
          async *createStream() {
            const bytes = Buffer.from('reasoning: chào 👋', 'utf8');
            for (let index = 0; index < bytes.length; index++) {
              yield bytes.subarray(index, index + 1);
            }
          },
        };
      `,
      'utf8',
    );

    const service = makeService(modulePath, 'iterator-package');
    try {
      const result = await service.run(
        `
          const decoder = new TextDecoder();
          let text = '';
          for await (const chunk of $ctx.$pkgs['iterator-package'].createStream()) {
            text += decoder.decode(chunk, { stream: true });
          }
          text += decoder.decode();
          const repeated = $ctx.$pkgs['iterator-package'].createStream();
          const firstIterator = repeated[Symbol.asyncIterator]();
          await firstIterator.next();
          let duplicateCode = null;
          try {
            await repeated[Symbol.asyncIterator]().next();
          } catch (error) {
            duplicateCode = error.code;
          }
          await firstIterator.return();
          const concurrentIterator = $ctx.$pkgs['iterator-package'].createStream()[Symbol.asyncIterator]();
          const pendingNext = concurrentIterator.next();
          let concurrentCode = null;
          try {
            await concurrentIterator.next();
          } catch (error) {
            concurrentCode = error.code;
          }
          await pendingNext;
          await concurrentIterator.return();
          return { text, duplicateCode, concurrentCode };
        `,
        {
          $body: {}, $query: {}, $params: {}, $share: { $logs: [] },
          $helpers: {}, $cache: {}, $repos: {}, $user: null,
        } as any,
        5000,
      );

      expect(result).toEqual({
        text: 'reasoning: chào 👋',
        duplicateCode: 'ERR_PACKAGE_STREAM_ALREADY_CONSUMED',
        concurrentCode: 'ERR_PACKAGE_STREAM_CONCURRENT_NEXT',
      });
    } finally {
      service.onDestroy();
    }
  });

  it('preflights the first raw package chunk before response commit and replays it once', async () => {
    tempDir = await mkdtemp(path.join(tmpdir(), 'enfyra-stream-preflight-'));
    const modulePath = path.join(tempDir, 'preflight-package.mjs');
    const chunks = [
      'data: {"reasoning":"đang kiểm tra 👋"}\n\n',
      'data: {"tool":"get_weather","arguments":"{\\"city\\":\\"Hanoi\\"}"}\n\n',
      'data: [DONE]\n\n',
    ];
    await writeFile(
      modulePath,
      `
        export default {
          async *createStream() {
            yield new Uint8Array();
            const bytes = new TextEncoder().encode(${JSON.stringify(chunks.join(''))});
            for (const byte of bytes) yield new Uint8Array([byte]);
          },
        };
      `,
      'utf8',
    );

    const service = makeService(modulePath, 'preflight-package');
    const received: Buffer[] = [];
    let preflightCompleted = false;
    const ctx: any = {
      $body: {}, $query: {}, $params: {}, $share: { $logs: [] },
      $helpers: {
        markPreflightCompleted: () => {
          preflightCompleted = true;
        },
      },
      $cache: {}, $repos: {}, $user: null,
      $res: {
        stream: (stream: any) => new Promise<void>((resolve, reject) => {
          expect(preflightCompleted).toBe(true);
          stream.on('data', (chunk: Buffer) => received.push(chunk));
          stream.on('end', resolve);
          stream.on('error', reject);
        }),
      },
    };

    try {
      await service.run(
        `
          const upstream = $ctx.$pkgs['preflight-package'].createStream();
          const guarded = await $ctx.$streams.preflight(upstream, { timeoutMs: 1000 });
          await $ctx.$helpers.markPreflightCompleted();
          await $ctx.$res.stream(guarded.stream);
        `,
        ctx,
        5000,
      );

      const output = Buffer.concat(received).toString('utf8');
      expect(output).toBe(chunks.join(''));
      const packets = output.split('\n\n').filter(Boolean);
      const toolPayload = JSON.parse(packets[1].slice('data: '.length));
      expect(toolPayload.tool).toBe('get_weather');
      expect(JSON.parse(toolPayload.arguments)).toEqual({ city: 'Hanoi' });
      expect(packets.filter((packet) => packet === 'data: [DONE]')).toHaveLength(1);
    } finally {
      service.onDestroy();
    }
  });

  it('times out only the silent package stream and retries in the same task', async () => {
    tempDir = await mkdtemp(path.join(tmpdir(), 'enfyra-stream-retry-'));
    const modulePath = path.join(tempDir, 'retry-package.mjs');
    await writeFile(
      modulePath,
      `
        let attempts = 0;
        let cancelled = 0;
        const silentBody = () => {
          let release;
          return {
            [Symbol.asyncIterator]() { return this; },
            next() { return new Promise((resolve) => { release = resolve; }); },
            return() {
              cancelled += 1;
              release?.({ done: true, value: undefined });
              return Promise.resolve({ done: true, value: undefined });
            },
          };
        };
        export default {
          request() {
            attempts += 1;
            return {
              body: attempts === 1 ? silentBody() : (async function* () { yield new TextEncoder().encode('retry-ok'); })(),
            };
          },
          stats() { return { attempts, cancelled }; },
          healthy() { return 'healthy'; },
        };
      `,
      'utf8',
    );

    const service = makeService(modulePath, 'retry-package');
    const received: Buffer[] = [];
    const ctx: any = {
      $body: {}, $query: {}, $params: {}, $share: { $logs: [] },
      $helpers: {}, $cache: {}, $repos: {}, $user: null,
      $res: { stream: (stream: any) => new Promise<void>((resolve, reject) => {
        stream.on('data', (chunk: Buffer) => received.push(chunk));
        stream.on('end', resolve);
        stream.on('error', reject);
      }) },
    };

    try {
      const result = await service.run(
        `
          const pkg = $ctx.$pkgs['retry-package'];
          let timeoutCode = null;
          try {
            const first = await pkg.request();
            await $ctx.$streams.preflight(first.body, { timeoutMs: 40 });
          } catch (error) {
            timeoutCode = error.code;
          }
          const second = await pkg.request();
          const guarded = await $ctx.$streams.preflight(second.body, { timeoutMs: 1000 });
          await $ctx.$res.stream(guarded.stream);
          return { timeoutCode, stats: await pkg.stats(), healthy: await pkg.healthy() };
        `,
        ctx,
        5000,
      );

      expect(result).toEqual({
        timeoutCode: 'ERR_PACKAGE_STREAM_TIMEOUT',
        stats: { attempts: 2, cancelled: 1 },
        healthy: 'healthy',
      });
      expect(Buffer.concat(received).toString('utf8')).toBe('retry-ok');
    } finally {
      service.onDestroy();
    }
  });

  it('reports empty and failed streams before commit without aborting the task', async () => {
    tempDir = await mkdtemp(path.join(tmpdir(), 'enfyra-stream-errors-'));
    const modulePath = path.join(tempDir, 'stream-errors-package.mjs');
    await writeFile(
      modulePath,
      `
        export default {
          async *empty() {},
          async *failed() { throw new Error('upstream failed before bytes'); },
          healthy() { return 'healthy'; },
        };
      `,
      'utf8',
    );

    const service = makeService(modulePath, 'stream-errors-package');
    try {
      const result = await service.run(
        `
          const pkg = $ctx.$pkgs['stream-errors-package'];
          const errors = [];
          for (const stream of [pkg.empty(), pkg.failed()]) {
            try {
              await $ctx.$streams.preflight(stream, { timeoutMs: 1000 });
            } catch (error) {
              errors.push({ code: error.code || null, message: error.message });
            }
          }
          return { errors, healthy: await pkg.healthy() };
        `,
        {
          $body: {}, $query: {}, $params: {}, $share: { $logs: [] },
          $helpers: {}, $cache: {}, $repos: {}, $user: null,
        } as any,
        5000,
      );

      expect(result).toEqual({
        errors: [
          { code: 'ERR_PACKAGE_STREAM_EMPTY', message: 'Package stream ended before yielding bytes' },
          { code: null, message: expect.stringContaining('upstream failed before bytes') },
        ],
        healthy: 'healthy',
      });
    } finally {
      service.onDestroy();
    }
  });

  it('relays the preflight chunk once and propagates a later source error', async () => {
    tempDir = await mkdtemp(path.join(tmpdir(), 'enfyra-stream-late-error-'));
    const modulePath = path.join(tempDir, 'late-error-package.mjs');
    await writeFile(
      modulePath,
      `
        export default {
          async *stream() {
            yield new TextEncoder().encode('first');
            throw new Error('upstream failed after bytes');
          },
        };
      `,
      'utf8',
    );

    const service = makeService(modulePath, 'late-error-package');
    const received: Buffer[] = [];
    try {
      await expect(
        service.run(
          `
            const guarded = await $ctx.$streams.preflight(
              $ctx.$pkgs['late-error-package'].stream(),
              { timeoutMs: 1000 },
            );
            await $ctx.$res.stream(guarded.stream);
          `,
          {
            $body: {}, $query: {}, $params: {}, $share: { $logs: [] },
            $helpers: {}, $cache: {}, $repos: {}, $user: null,
            $res: { stream: (stream: any) => new Promise<void>((resolve, reject) => {
              stream.on('data', (chunk: Buffer) => received.push(chunk));
              stream.on('end', resolve);
              stream.on('error', reject);
            }) },
          } as any,
          5000,
        ),
      ).rejects.toThrow(/upstream failed after bytes/);
      expect(Buffer.concat(received).toString('utf8')).toBe('first');
    } finally {
      service.onDestroy();
    }
  });

  it('collects package streams without starting a response and enforces raw byte limits', async () => {
    tempDir = await mkdtemp(path.join(tmpdir(), 'enfyra-stream-collect-'));
    const modulePath = path.join(tempDir, 'collect-package.mjs');
    await writeFile(
      modulePath,
      `
        import { Buffer } from 'node:buffer';
        let cancelled = 0;
        const bytes = Buffer.from('chào 👋', 'utf8');
        const limitedBytes = Buffer.from('👋', 'utf8');
        export default {
          async *text() {
            for (let index = 0; index < bytes.length; index++) yield bytes.subarray(index, index + 1);
          },
          limited() {
            let index = 0;
            return {
              [Symbol.asyncIterator]() { return this; },
              next() {
                if (index >= limitedBytes.length) return Promise.resolve({ done: true });
                return Promise.resolve({ done: false, value: limitedBytes.subarray(index, ++index) });
              },
              return() { cancelled += 1; return Promise.resolve({ done: true }); },
            };
          },
          stats() { return { cancelled }; },
        };
      `,
      'utf8',
    );

    const service = makeService(modulePath, 'collect-package');
    let responseStarted = false;
    try {
      const result = await service.run(
        `
          const pkg = $ctx.$pkgs['collect-package'];
          const text = await $ctx.$streams.readText(pkg.text(), { timeoutMs: 1000, maxBytes: 1024 });
          let limitCode = null;
          try {
            await $ctx.$streams.readBytes(pkg.limited(), { timeoutMs: 1000, maxBytes: 3 });
          } catch (error) {
            limitCode = error.code;
          }
          return { text, limitCode, stats: await pkg.stats() };
        `,
        {
          $body: {}, $query: {}, $params: {}, $share: { $logs: [] },
          $helpers: {}, $cache: {}, $repos: {}, $user: null,
          $res: { stream: () => { responseStarted = true; } },
        } as any,
        5000,
      );

      expect(responseStarted).toBe(false);
      expect(result).toEqual({
        text: 'chào 👋',
        limitCode: 'ERR_PACKAGE_STREAM_MAX_BYTES',
        stats: { cancelled: 1 },
      });
    } finally {
      service.onDestroy();
    }
  });

  it('enforces timeout and cleanup after preflight composition', async () => {
    tempDir = await mkdtemp(path.join(tmpdir(), 'enfyra-stream-preflight-timeout-'));
    const modulePath = path.join(tempDir, 'preflight-timeout-package.mjs');
    await writeFile(
      modulePath,
      `
        let cancelled = 0;
        export default {
          stream() {
            let step = 0;
            let release;
            return {
              [Symbol.asyncIterator]() { return this; },
              next() {
                if (step++ === 0) return Promise.resolve({ done: false, value: new TextEncoder().encode('first') });
                return new Promise((resolve) => { release = resolve; });
              },
              return() {
                cancelled += 1;
                release?.({ done: true, value: undefined });
                return Promise.resolve({ done: true, value: undefined });
              },
            };
          },
          stats() { return { cancelled }; },
        };
      `,
      'utf8',
    );

    const service = makeService(modulePath, 'preflight-timeout-package');
    try {
      const result = await service.run(
        `
          const pkg = $ctx.$pkgs['preflight-timeout-package'];
          const guarded = await $ctx.$streams.preflight(pkg.stream(), { timeoutMs: 1000 });
          let timeoutCode = null;
          try {
            await $ctx.$streams.readText(guarded.stream, { timeoutMs: 40, maxBytes: 1024 });
          } catch (error) {
            timeoutCode = error.code;
          }
          return { timeoutCode, stats: await pkg.stats() };
        `,
        {
          $body: {}, $query: {}, $params: {}, $share: { $logs: [] },
          $helpers: {}, $cache: {}, $repos: {}, $user: null,
        } as any,
        2000,
      );

      expect(result).toEqual({
        timeoutCode: 'ERR_PACKAGE_STREAM_TIMEOUT',
        stats: { cancelled: 1 },
      });
    } finally {
      service.onDestroy();
    }
  });

  it('propagates maxBytes cancellation through a preflight replay stream', async () => {
    tempDir = await mkdtemp(path.join(tmpdir(), 'enfyra-stream-preflight-limit-'));
    const modulePath = path.join(tempDir, 'preflight-limit-package.mjs');
    await writeFile(
      modulePath,
      `
        let cancelled = 0;
        export default {
          stream() {
            let index = 0;
            const chunks = [new Uint8Array([1, 2]), new Uint8Array([3, 4])];
            return {
              [Symbol.asyncIterator]() { return this; },
              next() {
                if (index >= chunks.length) return Promise.resolve({ done: true, value: undefined });
                return Promise.resolve({ done: false, value: chunks[index++] });
              },
              return() {
                cancelled += 1;
                return Promise.resolve({ done: true, value: undefined });
              },
            };
          },
          stats() { return { cancelled }; },
        };
      `,
      'utf8',
    );

    const service = makeService(modulePath, 'preflight-limit-package');
    try {
      const result = await service.run(
        `
          const pkg = $ctx.$pkgs['preflight-limit-package'];
          const guarded = await $ctx.$streams.preflight(pkg.stream(), { timeoutMs: 1000 });
          let limitCode = null;
          try {
            await $ctx.$streams.readBytes(guarded.stream, { timeoutMs: 1000, maxBytes: 3 });
          } catch (error) {
            limitCode = error.code;
          }
          return { limitCode, stats: await pkg.stats() };
        `,
        {
          $body: {}, $query: {}, $params: {}, $share: { $logs: [] },
          $helpers: {}, $cache: {}, $repos: {}, $user: null,
        } as any,
        2000,
      );

      expect(result).toEqual({
        limitCode: 'ERR_PACKAGE_STREAM_MAX_BYTES',
        stats: { cancelled: 1 },
      });
    } finally {
      service.onDestroy();
    }
  });

  it('times out a silent local readable without waiting for the task deadline', async () => {
    const service = makeService();
    try {
      const startedAt = Date.now();
      const result = await service.run(
        `
          const readable = {
            [Symbol.asyncIterator]() { return this; },
            next() { return new Promise(() => {}); },
            return() { return new Promise(() => {}); },
          };
          let timeoutCode = null;
          try {
            await $ctx.$streams.readBytes(readable, { timeoutMs: 40, maxBytes: 1024 });
          } catch (error) {
            timeoutCode = error.code;
          }
          return { timeoutCode };
        `,
        {
          $body: {}, $query: {}, $params: {}, $share: { $logs: [] },
          $helpers: {}, $cache: {}, $repos: {}, $user: null,
        } as any,
        2000,
      );

      expect(result).toEqual({ timeoutCode: 'ERR_PACKAGE_STREAM_TIMEOUT' });
      expect(Date.now() - startedAt).toBeLessThan(500);
    } finally {
      service.onDestroy();
    }
  });

  it('uses a captured clock for local stream deadlines', async () => {
    const service = makeService();
    try {
      const startedAt = Date.now();
      const result = await service.run(
        `
          Date.now = () => 0;
          const readable = {
            [Symbol.asyncIterator]() { return this; },
            next() { return new Promise(() => {}); },
            return() { return new Promise(() => {}); },
          };
          let code = null;
          try {
            await $ctx.$streams.preflight(readable, {
              timeoutMs: 40,
              idleTimeoutMs: 40,
            });
          } catch (error) {
            code = error.code;
          }
          return { code };
        `,
        {
          $body: {}, $query: {}, $params: {}, $share: { $logs: [] },
          $helpers: {}, $cache: {}, $repos: {}, $user: null,
        } as any,
        2000,
      );

      expect(result).toEqual({ code: 'ERR_PACKAGE_STREAM_TIMEOUT' });
      expect(Date.now() - startedAt).toBeLessThan(500);
    } finally {
      service.onDestroy();
    }
  });

  it('bounds a silent local response stream by its total timeout', async () => {
    const service = makeService();
    try {
      const startedAt = Date.now();
      await expect(
        service.run(
          `
            const readable = {
              __enfyraLocalReadable: true,
              [Symbol.asyncIterator]() { return this; },
              next() { return new Promise(() => {}); },
              return() { return new Promise(() => {}); },
            };
            await $ctx.$res.stream(readable, { timeoutMs: 40 });
          `,
          {
            $body: {}, $query: {}, $params: {}, $share: { $logs: [] },
            $helpers: {}, $cache: {}, $repos: {}, $user: null,
            $res: {
              stream: () => Promise.resolve(),
            },
          } as any,
          2000,
        ),
      ).rejects.toThrow('Package stream timed out');
      expect(Date.now() - startedAt).toBeLessThan(500);
    } finally {
      service.onDestroy();
    }
  });

  it('honors idle timeout while preflighting a local stream', async () => {
    const service = makeService();
    try {
      const startedAt = Date.now();
      const result = await service.run(
        `
          const readable = {
            [Symbol.asyncIterator]() { return this; },
            next() { return new Promise(() => {}); },
            return() { return new Promise(() => {}); },
          };
          let code = null;
          try {
            await $ctx.$streams.preflight(readable, {
              timeoutMs: 1000,
              idleTimeoutMs: 40,
            });
          } catch (error) {
            code = error.code;
          }
          return { code };
        `,
        {
          $body: {}, $query: {}, $params: {}, $share: { $logs: [] },
          $helpers: {}, $cache: {}, $repos: {}, $user: null,
        } as any,
        2000,
      );

      expect(result).toEqual({ code: 'ERR_PACKAGE_STREAM_TIMEOUT' });
      expect(Date.now() - startedAt).toBeLessThan(500);
    } finally {
      service.onDestroy();
    }
  });

  it('guards an incremental package stream with idle timeout and maxBytes', async () => {
    tempDir = await mkdtemp(path.join(tmpdir(), 'enfyra-stream-guard-'));
    const modulePath = path.join(tempDir, 'guard-package.mjs');
    await writeFile(
      modulePath,
      `
        let cancelled = 0;
        export default {
          oversized() {
            let index = 0;
            const chunks = [new Uint8Array([1, 2]), new Uint8Array([3, 4])];
            return {
              [Symbol.asyncIterator]() { return this; },
              next() {
                if (index >= chunks.length) return Promise.resolve({ done: true, value: undefined });
                return Promise.resolve({ done: false, value: chunks[index++] });
              },
              return() { cancelled += 1; return Promise.resolve({ done: true, value: undefined }); },
            };
          },
          silentAfterFirst() {
            let step = 0;
            let release;
            return {
              [Symbol.asyncIterator]() { return this; },
              next() {
                if (step++ === 0) return Promise.resolve({ done: false, value: new Uint8Array([1]) });
                return new Promise((resolve) => { release = resolve; });
              },
              return() {
                cancelled += 1;
                release?.({ done: true, value: undefined });
                return Promise.resolve({ done: true, value: undefined });
              },
            };
          },
          continuous() {
            return {
              [Symbol.asyncIterator]() { return this; },
              next() { return Promise.resolve({ done: false, value: new Uint8Array([1]) }); },
              return() { cancelled += 1; return Promise.resolve({ done: true, value: undefined }); },
            };
          },
          stats() { return { cancelled }; },
        };
      `,
      'utf8',
    );

    const service = makeService(modulePath, 'guard-package');
    try {
      const result = await service.run(
        `
          const pkg = $ctx.$pkgs['guard-package'];
          let limitCode = null;
          try {
            for await (const _chunk of $ctx.$streams.guard(pkg.oversized(), {
              timeoutMs: 1000,
              idleTimeoutMs: 1000,
              maxBytes: 3,
            })) {}
          } catch (error) {
            limitCode = error.code;
          }
          let idleCode = null;
          try {
            for await (const _chunk of $ctx.$streams.guard(pkg.silentAfterFirst(), {
              timeoutMs: 1000,
              idleTimeoutMs: 40,
              maxBytes: 1024,
            })) {}
          } catch (error) {
            idleCode = error.code;
          }
          let totalCode = null;
          try {
            for await (const _chunk of $ctx.$streams.guard(pkg.continuous(), {
              timeoutMs: 40,
              idleTimeoutMs: 1000,
              maxBytes: 1000000000,
            })) {}
          } catch (error) {
            totalCode = error.code;
          }
          const statsBeforeUntouchedCancel = await pkg.stats();
          const cancelledBeforeConsume = $ctx.$streams.guard(pkg.silentAfterFirst(), {
            timeoutMs: 1000,
            maxBytes: 1024,
          });
          await $ctx.$streams.cancel(cancelledBeforeConsume);
          return {
            limitCode,
            idleCode,
            totalCode,
            statsBeforeUntouchedCancel,
            stats: await pkg.stats(),
          };
        `,
        {
          $body: {}, $query: {}, $params: {}, $share: { $logs: [] },
          $helpers: {}, $cache: {}, $repos: {}, $user: null,
        } as any,
        2000,
      );

      expect(result).toEqual({
        limitCode: 'ERR_PACKAGE_STREAM_MAX_BYTES',
        idleCode: 'ERR_PACKAGE_STREAM_TIMEOUT',
        totalCode: 'ERR_PACKAGE_STREAM_TIMEOUT',
        statsBeforeUntouchedCancel: { cancelled: 3 },
        stats: { cancelled: 3 },
      });
    } finally {
      service.onDestroy();
    }
  });

  it('keeps preflight replay reads under the original idle deadline', async () => {
    const service = makeService();

    try {
      const result = await service.run(
        `
          let step = 0;
          let cancelled = false;
          const readable = {
            [Symbol.asyncIterator]() { return this; },
            next() {
              if (step++ === 0) {
                return Promise.resolve({ done: false, value: Uint8Array.from([1]) });
              }
              return new Promise(() => {});
            },
            return() {
              cancelled = true;
              return Promise.resolve({ done: true, value: undefined });
            },
          };
          const preflight = await $ctx.$streams.preflight(readable, {
            timeoutMs: 1000,
            idleTimeoutMs: 40,
          });
          const iterator = preflight.stream[Symbol.asyncIterator]();
          const first = await iterator.next();
          let code = null;
          try {
            await iterator.next();
          } catch (error) {
            code = error.code;
          }
          return { first: Array.from(first.value), code, cancelled };
        `,
        {
          $body: {}, $query: {}, $params: {}, $share: { $logs: [] },
          $helpers: {}, $cache: {}, $repos: {}, $user: null,
        } as any,
        2_000,
      );

      expect(result).toEqual({
        first: [1],
        code: 'ERR_PACKAGE_STREAM_TIMEOUT',
        cancelled: true,
      });
    } finally {
      service.onDestroy();
    }
  });

  it('bounds tap callbacks by the stream total timeout', async () => {
    const service = makeService();

    try {
      const result = await service.run(
        `
          let cancelled = false;
          const readable = {
            [Symbol.asyncIterator]() { return this; },
            next() {
              return Promise.resolve({ done: false, value: Uint8Array.from([1]) });
            },
            return() {
              cancelled = true;
              return Promise.resolve({ done: true, value: undefined });
            },
          };
          const tapped = $ctx.$streams.tap(readable, {
            timeoutMs: 40,
            onChunk: () => new Promise(() => {}),
          });
          let code = null;
          try {
            await tapped[Symbol.asyncIterator]().next();
          } catch (error) {
            code = error.code;
          }
          return { code, cancelled };
        `,
        {
          $body: {}, $query: {}, $params: {}, $share: { $logs: [] },
          $helpers: {}, $cache: {}, $repos: {}, $user: null,
        } as any,
        2_000,
      );

      expect(result).toEqual({
        code: 'ERR_PACKAGE_STREAM_TIMEOUT',
        cancelled: true,
      });
    } finally {
      service.onDestroy();
    }
  });

  it('snapshots mutable binary chunks while reading bytes', async () => {
    const service = makeService();

    try {
      const result = await service.run(
        `
          const buffer = new Uint8Array([1]);
          let step = 0;
          const readable = {
            [Symbol.asyncIterator]() { return this; },
            next() {
              if (step === 0) {
                step++;
                return Promise.resolve({ done: false, value: buffer });
              }
              if (step === 1) {
                step++;
                buffer[0] = 2;
                return Promise.resolve({ done: false, value: buffer });
              }
              return Promise.resolve({ done: true, value: undefined });
            },
          };
          return Array.from(await $ctx.$streams.readBytes(readable));
        `,
        {
          $body: {}, $query: {}, $params: {}, $share: { $logs: [] },
          $helpers: {}, $cache: {}, $repos: {}, $user: null,
        } as any,
        2_000,
      );

      expect(result).toEqual([1, 2]);
    } finally {
      service.onDestroy();
    }
  });

  it('enforces a bounded first preflight chunk', async () => {
    tempDir = await mkdtemp(path.join(tmpdir(), 'enfyra-stream-first-chunk-limit-'));
    const modulePath = path.join(tempDir, 'first-chunk-limit-package.mjs');
    await writeFile(
      modulePath,
      `
        let cancelled = 0;
        export default {
          stream() {
            return {
              [Symbol.asyncIterator]() { return this; },
              next() { return Promise.resolve({ done: false, value: new Uint8Array([1, 2, 3, 4]) }); },
              return() { cancelled += 1; return Promise.resolve({ done: true, value: undefined }); },
            };
          },
          stats() { return { cancelled }; },
        };
      `,
      'utf8',
    );

    const service = makeService(modulePath, 'first-chunk-limit-package');
    try {
      const result = await service.run(
        `
          const pkg = $ctx.$pkgs['first-chunk-limit-package'];
          let code = null;
          try {
            await $ctx.$streams.preflight(pkg.stream(), {
              timeoutMs: 1000,
              maxFirstChunkBytes: 3,
            });
          } catch (error) {
            code = error.code;
          }
          return { code, stats: await pkg.stats() };
        `,
        {
          $body: {}, $query: {}, $params: {}, $share: { $logs: [] },
          $helpers: {}, $cache: {}, $repos: {}, $user: null,
        } as any,
        2000,
      );

      expect(result).toEqual({
        code: 'ERR_PACKAGE_STREAM_MAX_BYTES',
        stats: { cancelled: 1 },
      });
    } finally {
      service.onDestroy();
    }
  });

  it('taps raw binary chunks without changing the relayed bytes', async () => {
    const service = makeService();
    try {
      const result = await service.run(
        `
          const observed = [];
          const source = {
            async *[Symbol.asyncIterator]() {
              yield new Uint8Array([0, 255]);
              yield new Uint8Array([1, 2, 3]);
            },
          };
          const tapped = $ctx.$streams.tap(source, {
            timeoutMs: 1000,
            maxBytes: 1024,
            onChunk(bytes, totalBytes) {
              observed.push({ bytes: Array.from(bytes), totalBytes });
            },
          });
          const output = [];
          for await (const chunk of tapped) output.push(...chunk);
          return { observed, output };
        `,
        {
          $body: {}, $query: {}, $params: {}, $share: { $logs: [] },
          $helpers: {}, $cache: {}, $repos: {}, $user: null,
        } as any,
        2000,
      );

      expect(result).toEqual({
        observed: [
          { bytes: [0, 255], totalBytes: 2 },
          { bytes: [1, 2, 3], totalBytes: 5 },
        ],
        output: [0, 255, 1, 2, 3],
      });
    } finally {
      service.onDestroy();
    }
  });

  it('starts tap lazily and serializes concurrent reads through its observer', async () => {
    const service = makeService();
    try {
      const result = await service.run(
        `
          let opened = 0;
          let sourceReads = 0;
          let activeObservers = 0;
          let maxActiveObservers = 0;
          const source = {
            [Symbol.asyncIterator]() {
              opened += 1;
              let value = 0;
              return {
                async next() {
                  sourceReads += 1;
                  value += 1;
                  if (value > 2) return { done: true, value: undefined };
                  return { done: false, value: Uint8Array.from([value]) };
                },
              };
            },
          };
          const tapped = $ctx.$streams.tap(source, {
            timeoutMs: 1000,
            onChunk: async () => {
              activeObservers += 1;
              maxActiveObservers = Math.max(maxActiveObservers, activeObservers);
              await Promise.resolve();
              activeObservers -= 1;
            },
          });
          const openedBeforeConsume = opened;
          const iterator = tapped[Symbol.asyncIterator]();
          const [first, second] = await Promise.all([
            iterator.next(),
            iterator.next(),
          ]);
          await iterator.return();
          return {
            openedBeforeConsume,
            opened,
            sourceReads,
            maxActiveObservers,
            first: Array.from(first.value),
            second: Array.from(second.value),
          };
        `,
        {
          $body: {}, $query: {}, $params: {}, $share: { $logs: [] },
          $helpers: {}, $cache: {}, $repos: {}, $user: null,
        } as any,
        2000,
      );

      expect(result).toEqual({
        openedBeforeConsume: 0,
        opened: 1,
        sourceReads: 2,
        maxActiveObservers: 1,
        first: [1],
        second: [2],
      });
    } finally {
      service.onDestroy();
    }
  });

  it('cleans up tap sources when the observer fails and rejects duplicate consumers', async () => {
    const service = makeService();
    try {
      const result = await service.run(
        `
          let tapCancelled = 0;
          const tapSource = {
            [Symbol.asyncIterator]() { return this; },
            next() { return Promise.resolve({ done: false, value: new Uint8Array([1]) }); },
            return() {
              tapCancelled += 1;
              return Promise.resolve({ done: true, value: undefined });
            },
          };
          let tapMessage = null;
          try {
            for await (const _chunk of $ctx.$streams.tap(tapSource, {
              timeoutMs: 1000,
              maxBytes: 1024,
              onChunk() { throw new Error('tap observer failed'); },
            })) {}
          } catch (error) {
            tapMessage = error.message;
          }

          const duplicateSource = {
            [Symbol.asyncIterator]() { return this; },
            next() { return Promise.resolve({ done: true, value: undefined }); },
            return() { return Promise.resolve({ done: true, value: undefined }); },
          };
          const guarded = $ctx.$streams.guard(duplicateSource, { timeoutMs: 1000 });
          guarded[Symbol.asyncIterator]();
          let duplicateCode = null;
          try {
            guarded[Symbol.asyncIterator]();
          } catch (error) {
            duplicateCode = error.code;
          }
          await $ctx.$streams.cancel(guarded);
          return { tapMessage, tapCancelled, duplicateCode };
        `,
        {
          $body: {}, $query: {}, $params: {}, $share: { $logs: [] },
          $helpers: {}, $cache: {}, $repos: {}, $user: null,
        } as any,
        2000,
      );

      expect(result).toEqual({
        tapMessage: 'tap observer failed',
        tapCancelled: 1,
        duplicateCode: 'ERR_PACKAGE_STREAM_ALREADY_CONSUMED',
      });
    } finally {
      service.onDestroy();
    }
  });

  it('cancels a preflight stream through the native stream API', async () => {
    tempDir = await mkdtemp(path.join(tmpdir(), 'enfyra-stream-explicit-cancel-'));
    const modulePath = path.join(tempDir, 'cancel-package.mjs');
    await writeFile(
      modulePath,
      `
        let cancelled = 0;
        export default {
          stream() {
            let step = 0;
            return {
              [Symbol.asyncIterator]() { return this; },
              next() {
                if (step++ === 0) return Promise.resolve({ done: false, value: new Uint8Array([1]) });
                return new Promise(() => {});
              },
              return() { cancelled += 1; return Promise.resolve({ done: true, value: undefined }); },
            };
          },
          stats() { return { cancelled }; },
        };
      `,
      'utf8',
    );

    const service = makeService(modulePath, 'cancel-package');
    try {
      const result = await service.run(
        `
          const pkg = $ctx.$pkgs['cancel-package'];
          const guarded = await $ctx.$streams.preflight(pkg.stream(), { timeoutMs: 1000 });
          await $ctx.$streams.cancel(guarded.stream);
          return await pkg.stats();
        `,
        {
          $body: {}, $query: {}, $params: {}, $share: { $logs: [] },
          $helpers: {}, $cache: {}, $repos: {}, $user: null,
        } as any,
        2000,
      );

      expect(result).toEqual({ cancelled: 1 });
    } finally {
      service.onDestroy();
    }
  });

  it('sends JSON and binary payloads through native response boundaries', async () => {
    const service = makeService();
    const calls: Array<{ kind: string; value: unknown; options: unknown }> = [];
    const response = {
      __enfyraJson: async (jsonText: string, options: unknown) => {
        calls.push({ kind: 'json', value: jsonText, options });
      },
      __enfyraBytes: async (bytes: Uint8Array, options: unknown) => {
        calls.push({ kind: 'bytes', value: Array.from(bytes), options });
      },
    };

    try {
      const result = await service.run(
        `
          __taskId = 'forged-task-id';
          let jsonError = null;
          try {
            await $ctx.$res.json(undefined);
          } catch (error) {
            jsonError = error.name;
          }
          await $ctx.$res.json({ ok: true }, { statusCode: 201 });
          await $ctx.$res.bytes(new Uint8Array([1, 2, 3]), { mimetype: 'image/png' });
          await $ctx.$res.bytes('A');
          const viewBytes = new Uint8Array([4, 5, 6]);
          await $ctx.$res.bytes(new DataView(viewBytes.buffer, 1, 2));
          let bytesError = null;
          try {
            await $ctx.$res.bytes({ invalid: true });
          } catch (error) {
            bytesError = error.name;
          }
          return { jsonError, bytesError };
        `,
        {
          $body: {}, $query: {}, $params: {}, $share: { $logs: [] },
          $helpers: {}, $cache: {}, $repos: {}, $user: null,
          $res: response,
        } as any,
        2000,
      );

      expect(result).toEqual({ jsonError: 'TypeError', bytesError: 'TypeError' });
      expect(calls).toEqual([
        { kind: 'json', value: '{"ok":true}', options: { statusCode: 201 } },
        { kind: 'bytes', value: [1, 2, 3], options: { mimetype: 'image/png' } },
        { kind: 'bytes', value: [65], options: {} },
        { kind: 'bytes', value: [5, 6], options: {} },
      ]);
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

  it('cleans up a pending package stream iterator when the task is cancelled', async () => {
    tempDir = await mkdtemp(path.join(tmpdir(), 'enfyra-stream-task-cancel-'));
    const modulePath = path.join(tempDir, 'task-cancel-package.mjs');
    const openedPath = path.join(tempDir, 'opened.txt');
    const markerPath = path.join(tempDir, 'cancelled.txt');
    await writeFile(
      modulePath,
      `
        import { writeFileSync } from 'node:fs';
        export default {
          stream() {
            let release;
            return {
              [Symbol.asyncIterator]() {
                writeFileSync(${JSON.stringify(openedPath)}, 'opened', 'utf8');
                return this;
              },
              next() { return new Promise((resolve) => { release = resolve; }); },
              return() {
                writeFileSync(${JSON.stringify(markerPath)}, 'cancelled', 'utf8');
                release?.({ done: true, value: undefined });
                return Promise.resolve({ done: true, value: undefined });
              },
            };
          },
        };
      `,
      'utf8',
    );

    const service = makeService(modulePath, 'task-cancel-package');
    const controller = new AbortController();
    try {
      const result = service.runBatch(
        [
          {
            type: 'handler',
            code: `await $ctx.$streams.preflight($ctx.$pkgs['task-cancel-package'].stream(), { timeoutMs: 4000 });`,
          },
        ],
        {
          $body: {}, $query: {}, $params: {}, $share: { $logs: [] },
          $helpers: {}, $cache: {}, $repos: {}, $user: null,
        } as any,
        5000,
        { signal: controller.signal },
      );
      let opened = false;
      for (let attempt = 0; attempt < 100 && !opened; attempt++) {
        try {
          opened = (await readFile(openedPath, 'utf8')) === 'opened';
        } catch {
          await new Promise<void>((resolve) => setTimeout(resolve, 10));
        }
      }
      expect(opened).toBe(true);
      controller.abort();

      await expect(result).rejects.toMatchObject({ code: 'ERR_EXECUTION_ABORTED' });
      let marker = '';
      for (let attempt = 0; attempt < 50 && !marker; attempt++) {
        try {
          marker = await readFile(markerPath, 'utf8');
        } catch {
          await new Promise<void>((resolve) => setTimeout(resolve, 10));
        }
      }
      expect(marker).toBe('cancelled');
    } finally {
      service.onDestroy();
    }
  });
});
