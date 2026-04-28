import { fork } from 'child_process';
import { mkdtemp, rm, writeFile } from 'fs/promises';
import { tmpdir } from 'os';
import path from 'path';
import { afterEach, describe, expect, it } from 'vitest';
import { IsolatedExecutorService } from '../../src/kernel/execution';

const workerPath = path.resolve(
  __dirname,
  '../../src/kernel/execution/executor-engine/workers/package-runtime.child.js',
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
          return info;
        `,
        ctx,
        5000,
      );

      expect(result).toEqual({
        accepted: ['user@test.com'],
        subject: 'Welcome: User: Hello',
      });
    } finally {
      service.onDestroy();
    }
  });
});
