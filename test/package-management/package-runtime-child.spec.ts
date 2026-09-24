import { fork } from 'child_process';
import { mkdtemp, rm, writeFile } from 'fs/promises';
import { createRequire } from 'module';
import { tmpdir } from 'os';
import path from 'path';
import { afterEach, describe, expect, it } from 'vitest';
import { IsolatedExecutorService } from '@enfyra/kernel';

const require = createRequire(import.meta.url);
const workerPath = require.resolve(
  '@enfyra/kernel/execution/package-runtime.child.js',
);
const bridgePath = path.resolve(
  process.cwd(),
  '../kernel/src/execution/executor-engine/workers/package-runtime-bridge.js',
);
const { createPackageRuntimeBridge } = require(bridgePath);

let tempDir: string | null = null;

afterEach(async () => {
  if (tempDir) {
    await rm(tempDir, { recursive: true, force: true });
    tempDir = null;
  }
});

function callRuntime(child: ReturnType<typeof fork>, message: any) {
  return new Promise<any>((resolve, reject) => {
    const timeout = setTimeout(() => {
      cleanup();
      reject(new Error('package runtime test timed out'));
    }, 5000);
    const cleanup = () => {
      clearTimeout(timeout);
      child.off('message', onMessage);
      child.off('error', onError);
      child.off('exit', onExit);
    };
    const onMessage = (response: any) => {
      if (response.id !== message.id) return;
      cleanup();
      resolve(response);
    };
    const onError = (error: Error) => {
      cleanup();
      reject(error);
    };
    const onExit = (code: number | null, signal: NodeJS.Signals | null) => {
      cleanup();
      reject(
        new Error(
          `package runtime exited before response${
            signal ? ` with signal ${signal}` : ` with code ${code}`
          }`,
        ),
      );
    };
    child.on('message', onMessage);
    child.on('error', onError);
    child.on('exit', onExit);
    child.send(message);
  });
}

describe('package runtime child', () => {
  it('uses the remaining task timeout for proxied package calls', async () => {
    tempDir = await mkdtemp(path.join(tmpdir(), 'enfyra-package-runtime-'));
    const modulePath = path.join(tempDir, 'slow-package.mjs');
    await writeFile(
      modulePath,
      `
        export default {
          async slow() {
            await new Promise((resolve) => setTimeout(resolve, 180));
            return 'done';
          }
        };
      `,
      'utf8',
    );

    const bridge = createPackageRuntimeBridge({
      workerDir: path.dirname(bridgePath),
      activeTaskContexts: new Map(),
    });
    bridge.setTaskPackages(
      'task-timeout',
      new Map([
        [
          'slow-package',
          {
            name: 'slow-package',
            fileUrl: modulePath,
          },
        ],
      ]),
    );

    try {
      const startedAt = Date.now();
      await expect(
        bridge.executePackageRuntimeCall('task-timeout', {
          packageName: 'slow-package',
          kind: 'call',
          path: ['slow'],
          argsJson: '[]',
          timeoutMs: 40,
        }),
      ).rejects.toThrow(/Package runtime call timed out after 40ms/);
      expect(Date.now() - startedAt).toBeLessThan(140);
    } finally {
      bridge.shutdownPackageRuntime();
    }
  });

  it('normalizes overflowing timeouts and non-Error package throws', async () => {
    tempDir = await mkdtemp(path.join(tmpdir(), 'enfyra-package-runtime-'));
    const modulePath = path.join(tempDir, 'error-package.mjs');
    await writeFile(
      modulePath,
      `
        export default {
          async delayed() {
            await new Promise((resolve) => setTimeout(resolve, 20));
            return 'done';
          },
          throwNull() {
            throw null;
          }
        };
      `,
      'utf8',
    );

    const child = fork(workerPath, [], { stdio: ['ignore', 'ignore', 'ignore', 'ipc'] });

    try {
      const delayed = await callRuntime(child, {
        id: 'infinite-timeout',
        op: 'call',
        taskId: 'task-timeout',
        packageName: 'error-package',
        package: { name: 'error-package', fileUrl: modulePath },
        path: ['delayed'],
        argsJson: '[]',
        timeoutMs: Infinity,
      });
      expect(delayed).toMatchObject({ ok: true, value: 'done' });

      const thrown = await callRuntime(child, {
        id: 'throw-null',
        op: 'call',
        taskId: 'task-error',
        packageName: 'error-package',
        package: { name: 'error-package', fileUrl: modulePath },
        path: ['throwNull'],
        argsJson: '[]',
      });
      expect(thrown.ok).toBe(false);
      expect(thrown.error.message).toContain('Package threw null');
    } finally {
      child.kill();
    }
  });

  it('rejects deeply nested package arguments without terminating the child', async () => {
    tempDir = await mkdtemp(path.join(tmpdir(), 'enfyra-package-runtime-'));
    const modulePath = path.join(tempDir, 'depth-package.mjs');
    await writeFile(
      modulePath,
      'export default { value(input) { return input; } };',
      'utf8',
    );
    const child = fork(workerPath, [], { stdio: ['ignore', 'ignore', 'ignore', 'ipc'] });
    let nested: any = 'leaf';
    for (let depth = 0; depth < 140; depth++) nested = { nested };

    try {
      const rejected = await callRuntime(child, {
        id: 'deep-argument',
        op: 'call',
        taskId: 'task-depth',
        packageName: 'depth-package',
        package: { name: 'depth-package', fileUrl: modulePath },
        path: ['value'],
        argsJson: JSON.stringify([nested]),
      });
      expect(rejected.ok).toBe(false);
      expect(rejected.error.message).toContain(
        'Package argument exceeded maximum deserialization depth',
      );

      const control = await callRuntime(child, {
        id: 'depth-control',
        op: 'call',
        taskId: 'task-depth-control',
        packageName: 'depth-package',
        package: { name: 'depth-package', fileUrl: modulePath },
        path: ['value'],
        argsJson: '["ok"]',
      });
      expect(control).toMatchObject({ ok: true, value: 'ok' });
    } finally {
      child.kill();
    }
  });

  it('keeps error replies IPC-safe for circular and BigInt details', async () => {
    tempDir = await mkdtemp(path.join(tmpdir(), 'enfyra-package-runtime-'));
    const modulePath = path.join(tempDir, 'unsafe-error-package.mjs');
    await writeFile(
      modulePath,
      `
        export default {
          fail() {
            const error = new Error('unsafe details');
            error.code = 'ERR_UNSAFE_DETAILS';
            error.details = { amount: 1n };
            error.details.self = error.details;
            throw error;
          }
        };
      `,
      'utf8',
    );
    const child = fork(workerPath, [], { stdio: ['ignore', 'ignore', 'ignore', 'ipc'] });

    try {
      const result = await callRuntime(child, {
        id: 'unsafe-error',
        op: 'call',
        taskId: 'task-unsafe-error',
        packageName: 'unsafe-error-package',
        package: { name: 'unsafe-error-package', fileUrl: modulePath },
        path: ['fail'],
        argsJson: '[]',
      });
      expect(result).toMatchObject({
        ok: false,
        error: {
          code: 'ERR_UNSAFE_DETAILS',
          details: { amount: '1', self: '[Circular]' },
        },
      });
    } finally {
      child.kill();
    }
  });

  it('keeps cancellation pending when a released task receives a late call', async () => {
    tempDir = await mkdtemp(path.join(tmpdir(), 'enfyra-package-runtime-'));
    const modulePath = path.join(tempDir, 'release-package.mjs');
    await writeFile(
      modulePath,
      `
        export default {
          async delay(ms) {
            await new Promise((resolve) => setTimeout(resolve, ms));
            return 'done';
          }
        };
      `,
      'utf8',
    );
    const child = fork(workerPath, [], { stdio: ['ignore', 'ignore', 'ignore', 'ipc'] });

    try {
      const active = callRuntime(child, {
        id: 'release-active',
        op: 'call',
        taskId: 'task-release',
        packageName: 'release-package',
        package: { name: 'release-package', fileUrl: modulePath },
        path: ['delay'],
        argsJson: '[150]',
      });
      await new Promise((resolve) => setTimeout(resolve, 20));
      await callRuntime(child, {
        id: 'release-task',
        op: 'releaseTask',
        taskId: 'task-release',
      });
      const startedAt = Date.now();
      const cancelled = callRuntime(child, {
        id: 'release-cancel',
        op: 'cancelTask',
        taskId: 'task-release',
      });
      const late = callRuntime(child, {
        id: 'release-late',
        op: 'call',
        taskId: 'task-release',
        packageName: 'release-package',
        package: { name: 'release-package', fileUrl: modulePath },
        path: ['delay'],
        argsJson: '[1]',
      });

      const lateResult = await late;
      expect(lateResult.ok).toBe(false);
      expect(lateResult.error.message).toContain(
        'Package task has already been released',
      );
      await active;
      await cancelled;
      expect(Date.now() - startedAt).toBeGreaterThanOrEqual(100);
    } finally {
      child.kill();
    }
  });

  it('keeps package class instances as handles so prototype methods can be called', async () => {
    tempDir = await mkdtemp(path.join(tmpdir(), 'enfyra-package-runtime-'));
    const modulePath = path.join(tempDir, 'instance-package.mjs');
    await writeFile(
      modulePath,
      `
        class Mailer {
          constructor(prefix) {
            this.prefix = prefix;
          }
          getPrefix() {
            return this.prefix;
          }
          async sendMail(message) {
            return { accepted: [message.to], subject: this.prefix + message.subject };
          }
        }
        export default {
          prefix: 'Welcome: ',
          createTransport(options) {
            return new Mailer(this.prefix + options.suffix);
          }
        };
      `,
      'utf8',
    );

    const child = fork(workerPath, [], { stdio: ['ignore', 'ignore', 'ignore', 'ipc'] });

    try {
      const createResult = await callRuntime(child, {
        id: 'create',
        op: 'call',
        taskId: 'task-1',
        packageName: 'instance-package',
        package: { name: 'instance-package', fileUrl: modulePath },
        path: ['createTransport'],
        argsJson: JSON.stringify([{ suffix: 'User: ' }]),
      });

      expect(createResult.ok).toBe(true);
      expect(createResult.value).toMatchObject({ __pkgHandle: expect.any(String) });

      const crossTaskResult = await callRuntime(child, {
        id: 'cross-task',
        op: 'handleCall',
        taskId: 'task-2',
        handleId: createResult.value.__pkgHandle,
        path: ['getPrefix'],
        argsJson: '[]',
      });
      expect(crossTaskResult.ok).toBe(false);
      expect(crossTaskResult.error.message).toContain('Package handle not found');

      const sendResult = await callRuntime(child, {
        id: 'send',
        op: 'handleCall',
        taskId: 'task-1',
        handleId: createResult.value.__pkgHandle,
        path: ['sendMail'],
        argsJson: JSON.stringify([{ to: 'user@test.com', subject: 'Hello' }]),
      });

      expect(sendResult.ok).toBe(true);
      expect(sendResult.value).toEqual({
        accepted: ['user@test.com'],
        subject: 'Welcome: User: Hello',
      });
    } finally {
      child.kill();
    }
  });

  it('awaits async iterator cleanup before acknowledging return', async () => {
    tempDir = await mkdtemp(path.join(tmpdir(), 'enfyra-package-runtime-'));
    const modulePath = path.join(tempDir, 'cleanup-stream-package.mjs');
    await writeFile(
      modulePath,
      `
        export default {
          async *stream() {
            try {
              yield new Uint8Array([1]);
            } finally {
              await new Promise((resolve) => setTimeout(resolve, 100));
            }
          }
        };
      `,
      'utf8',
    );
    const child = fork(workerPath, [], { stdio: ['ignore', 'ignore', 'ignore', 'ipc'] });

    try {
      const handle = await callRuntime(child, {
        id: 'cleanup-stream-handle',
        op: 'call',
        taskId: 'task-cleanup-stream',
        packageName: 'cleanup-stream-package',
        package: { name: 'cleanup-stream-package', fileUrl: modulePath },
        path: ['stream'],
        argsJson: '[]',
      });
      const opened = await callRuntime(child, {
        id: 'cleanup-stream-open',
        op: 'streamIteratorOpen',
        taskId: 'task-cleanup-stream',
        handleId: handle.value.__pkgHandle,
        path: [],
      });
      await callRuntime(child, {
        id: 'cleanup-stream-next',
        op: 'streamIteratorNext',
        taskId: 'task-cleanup-stream',
        streamId: opened.value.streamId,
      });
      const startedAt = Date.now();
      const returned = await callRuntime(child, {
        id: 'cleanup-stream-return',
        op: 'streamIteratorReturn',
        taskId: 'task-cleanup-stream',
        streamId: opened.value.streamId,
      });

      expect(returned).toMatchObject({ ok: true, value: { done: true } });
      expect(Date.now() - startedAt).toBeGreaterThanOrEqual(90);
    } finally {
      child.kill();
    }
  });

  it('times out a package response stream at the bridge boundary', async () => {
    tempDir = await mkdtemp(path.join(tmpdir(), 'enfyra-package-runtime-'));
    const modulePath = path.join(tempDir, 'slow-stream-package.mjs');
    await writeFile(
      modulePath,
      `
        export default {
          async *stream() {
            await new Promise((resolve) => setTimeout(resolve, 5000));
            yield new Uint8Array([111, 107]);
          }
        };
      `,
      'utf8',
    );
    const bridge = createPackageRuntimeBridge({
      workerDir: path.dirname(bridgePath),
      activeTaskContexts: new Map(),
    });
    bridge.setTaskPackages(
      'stream-timeout',
      new Map([
        [
          'slow-stream-package',
          { name: 'slow-stream-package', fileUrl: modulePath },
        ],
      ]),
    );
    try {
      const handle = await bridge.executePackageRuntimeCall('stream-timeout', {
        packageName: 'slow-stream-package',
        kind: 'call',
        path: ['stream'],
        argsJson: '[]',
        timeoutMs: 500,
      });
      await expect(
        bridge.streamPackageHandle(
          'stream-timeout',
          handle.__pkgHandle,
          async () => {},
          [],
          500,
        ),
      ).rejects.toThrow(/Package runtime/);
    } finally {
      bridge.shutdownPackageRuntime();
    }
  });

  it('preserves nested Date and Map values in package arguments and results', async () => {
    tempDir = await mkdtemp(path.join(tmpdir(), 'enfyra-package-runtime-'));
    const modulePath = path.join(tempDir, 'special-values-package.mjs');
    await writeFile(
      modulePath,
      `
        export default {
          echoSpecial(input) {
            const blob = new Blob(['hello'], { type: 'text/plain' });
            return {
              at: input.at,
              map: input.map,
              set: input.set,
              regexp: input.regexp,
              error: input.error,
              url: input.url,
              search: input.search,
              blob,
              formDataValue: input.formData.get('name'),
              nested: {
                at: new Date('2020-01-02T00:00:00.000Z'),
                map: new Map([['b', 2]]),
                set: new Set(['x', 'y']),
                regexp: /hello/gi,
                error: new TypeError('bad input'),
                url: new URL('https://example.com/path?a=1'),
                search: new URLSearchParams('b=2')
              }
            };
          }
        };
      `,
      'utf8',
    );

    const child = fork(workerPath, [], { stdio: ['ignore', 'ignore', 'ignore', 'ipc'] });

    try {
      const result = await callRuntime(child, {
        id: 'special',
        op: 'call',
        taskId: 'task-1',
        packageName: 'special-values-package',
        package: { name: 'special-values-package', fileUrl: modulePath },
        path: ['echoSpecial'],
        argsJson: JSON.stringify([
          {
            at: { __date: '2021-02-03T00:00:00.000Z' },
            map: { __map: [['a', 1]] },
            set: { __set: ['a', 'b'] },
            regexp: { __regexp: { source: 'abc', flags: 'i' } },
            error: { __error: { name: 'RangeError', message: 'too far' } },
            url: { __url: 'https://enfyra.test/users?id=1' },
            search: { __urlSearchParams: 'q=test&page=1' },
            formData: { __formData: [['name', 'Enfyra']] },
          },
        ]),
      });

      expect(result.ok).toBe(true);
      expect(result.value).toEqual({
        at: { __date: '2021-02-03T00:00:00.000Z' },
        map: { __map: [['a', 1]] },
        set: { __set: ['a', 'b'] },
        regexp: { __regexp: { source: 'abc', flags: 'i' } },
        error: expect.objectContaining({
          __error: expect.objectContaining({
            name: 'RangeError',
            message: 'too far',
          }),
        }),
        url: { __url: 'https://enfyra.test/users?id=1' },
        search: { __urlSearchParams: 'q=test&page=1' },
        blob: expect.objectContaining({ __pkgHandle: expect.any(String) }),
        formDataValue: 'Enfyra',
        nested: {
          at: { __date: '2020-01-02T00:00:00.000Z' },
          map: { __map: [['b', 2]] },
          set: { __set: ['x', 'y'] },
          regexp: { __regexp: { source: 'hello', flags: 'gi' } },
          error: expect.objectContaining({
            __error: expect.objectContaining({
              name: 'TypeError',
              message: 'bad input',
            }),
          }),
          url: { __url: 'https://example.com/path?a=1' },
          search: { __urlSearchParams: 'b=2' },
        },
      });
    } finally {
      child.kill();
    }
  });
});

describe('isolated executor package proxy', () => {
  it('auto-awaits package runtime results before calling instance methods', async () => {
    tempDir = await mkdtemp(path.join(tmpdir(), 'enfyra-package-proxy-'));
    const modulePath = path.join(tempDir, 'instance-package.mjs');
    await writeFile(
      modulePath,
      `
        class Mailer {
          constructor(prefix) {
            this.prefix = prefix;
          }
          getPrefix() {
            return this.prefix;
          }
          async sendMail(message) {
            return { accepted: [message.to], subject: this.prefix + message.subject };
          }
        }
        export default {
          prefix: 'Welcome: ',
          createTransport(options) {
            return new Mailer(this.prefix + options.suffix);
          }
        };
      `,
      'utf8',
    );

    const service = new IsolatedExecutorService({
      packageCacheService: {
        getPackages: async () => ['instance-package'],
      } as any,
      packageCdnLoaderService: {
        getPackageSources: () => [
          {
            name: 'instance-package',
            safeName: 'instance_package',
            version: '1.0.0',
            sourceCode: '',
            filePath: modulePath,
            fileUrl: modulePath,
          },
        ],
      } as any,
    });
    const ctx: any = {
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
      const result = await service.run(
        `
          const pkg = $ctx.$pkgs['instance-package'];
          const transporter = pkg.createTransport({ suffix: 'User: ' });
          const info = await transporter.sendMail({ to: 'user@test.com', subject: 'Hello' });
          return {
            info,
            prefix: await transporter.prefix,
            prefixMethod: await transporter.getPrefix()
          };
        `,
        ctx,
        5000,
      );

      expect(result).toEqual({
        info: {
          accepted: ['user@test.com'],
          subject: 'Welcome: User: Hello',
        },
        prefix: 'Welcome: User: ',
        prefixMethod: 'Welcome: User: ',
      });
    } finally {
      service.onDestroy();
    }
  });

  it('restores nested Date and Map values from proxied package calls', async () => {
    tempDir = await mkdtemp(path.join(tmpdir(), 'enfyra-package-proxy-'));
    const modulePath = path.join(tempDir, 'special-values-package.mjs');
    await writeFile(
      modulePath,
      `
        import { EventEmitter } from 'node:events';
        void EventEmitter;
        export default {
          echoSpecial(input) {
            const blob = new Blob(['hello'], { type: 'text/plain' });
            return {
              at: input.at,
              map: input.map,
              set: input.set,
              regexp: input.regexp,
              error: input.error,
              url: input.url,
              search: input.search,
              blob,
              formDataValue: input.formData.get('name'),
              nested: {
                at: new Date('2020-01-02T00:00:00.000Z'),
                map: new Map([['b', 2]]),
                set: new Set(['x', 'y']),
                regexp: /hello/gi,
                error: new TypeError('bad input'),
                url: new URL('https://example.com/path?a=1'),
                search: new URLSearchParams('b=2')
              }
            };
          }
        };
      `,
      'utf8',
    );

    const service = new IsolatedExecutorService({
      packageCacheService: {
        getPackages: async () => ['special-values-package'],
      } as any,
      packageCdnLoaderService: {
        getPackageSources: () => [
          {
            name: 'special-values-package',
            safeName: 'special_values_package',
            version: '1.0.0',
            sourceCode: '',
            filePath: modulePath,
            fileUrl: modulePath,
          },
        ],
      } as any,
    });
    const ctx: any = {
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
      const result = await service.run(
        `
          const pkg = $ctx.$pkgs['special-values-package'];
          const formData = new FormData();
          formData.append('name', 'Enfyra');
          const value = await pkg.echoSpecial({
            at: new Date('2021-02-03T00:00:00.000Z'),
            map: new Map([['a', 1]]),
            set: new Set(['a', 'b']),
            regexp: /abc/i,
            error: new RangeError('too far'),
            url: new URL('https://enfyra.test/users?id=1'),
            search: new URLSearchParams('q=test&page=1'),
            formData
          });
          return {
            atYear: value.at.getUTCFullYear(),
            mapValue: value.map.get('a'),
            setHas: value.set.has('b'),
            regexpMatches: value.regexp.test('ABC'),
            errorName: value.error.name,
            errorMessage: value.error.message,
            urlHost: value.url.host,
            searchPage: value.search.get('page'),
            blobText: await value.blob.text(),
            formDataValue: value.formDataValue,
            nestedYear: value.nested.at.getUTCFullYear(),
            nestedMapValue: value.nested.map.get('b'),
            nestedSetHas: value.nested.set.has('y'),
            nestedRegexpMatches: value.nested.regexp.test('HELLO'),
            nestedErrorName: value.nested.error.name,
            nestedUrlHost: value.nested.url.host,
            nestedSearch: value.nested.search.get('b')
          };
        `,
        ctx,
        5000,
      );

      expect(result).toEqual({
        atYear: 2021,
        mapValue: 1,
        setHas: true,
        regexpMatches: true,
        errorName: 'RangeError',
        errorMessage: 'too far',
        urlHost: 'enfyra.test',
        searchPage: '1',
        blobText: 'hello',
        formDataValue: 'Enfyra',
        nestedYear: 2020,
        nestedMapValue: 2,
        nestedSetHas: true,
        nestedRegexpMatches: true,
        nestedErrorName: 'TypeError',
        nestedUrlHost: 'example.com',
        nestedSearch: '2',
      });
    } finally {
      service.onDestroy();
    }
  });

  it('bounds a package response stream by the remaining task timeout', async () => {
    tempDir = await mkdtemp(path.join(tmpdir(), 'enfyra-package-proxy-'));
    const modulePath = path.join(tempDir, 'slow-stream-package.mjs');
    await writeFile(
      modulePath,
      `
        export default {
          async delay(ms) {
            await new Promise((resolve) => setTimeout(resolve, ms));
          },
          async *stream() {
            await new Promise((resolve) => setTimeout(resolve, 5000));
            yield new Uint8Array([111, 107]);
          }
        };
      `,
      'utf8',
    );

    const service = new IsolatedExecutorService({
      packageCacheService: {
        getPackages: async () => ['slow-stream-package'],
      } as any,
      packageCdnLoaderService: {
        getPackageSources: () => [
          {
            name: 'slow-stream-package',
            safeName: 'slow_stream_package',
            version: '1.0.0',
            sourceCode: '',
            filePath: modulePath,
            fileUrl: modulePath,
          },
        ],
      } as any,
    });
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
        stream: (stream: NodeJS.ReadableStream) => {
          stream.on('error', () => {});
          stream.resume();
          return Promise.resolve();
        },
      },
    };

    try {
      await expect(
        service.run(
          `
            const pkg = $ctx.$pkgs['slow-stream-package'];
            await pkg.delay(500);
            const stream = await pkg.stream();
            await $ctx.$res.stream(stream);
          `,
          ctx,
          2000,
        ),
      ).rejects.toThrow(/Script execution timed out after 2000ms/);
    } finally {
      service.onDestroy();
    }
  });
});
