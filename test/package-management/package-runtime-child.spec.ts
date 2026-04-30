import { fork } from 'child_process';
import { mkdtemp, rm, writeFile } from 'fs/promises';
import { tmpdir } from 'os';
import path from 'path';
import { afterEach, describe, expect, it } from 'vitest';
import { IsolatedExecutorService } from '@enfyra/kernel';

const workerPath = path.resolve(
  __dirname,
  '@enfyra/kernel/execution/package-runtime.child.js',
);

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
    child.on('message', onMessage);
    child.on('error', onError);
    child.send(message);
  });
}

describe('package runtime child', () => {
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
});
