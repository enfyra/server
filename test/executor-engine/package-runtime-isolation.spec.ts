import { mkdtemp, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { IsolatedExecutorService } from '@enfyra/kernel';

function context() {
  return {
    $body: {},
    $query: {},
    $params: {},
    $share: {},
    $api: { request: {} },
  };
}

function makeService(modulePath: string) {
  return new IsolatedExecutorService({
    packageCacheService: {
      getPackages: async () => ['executor-runtime-fixture'],
    },
    packageCdnLoaderService: {
      getPackageSources: () => [
        {
          name: 'executor-runtime-fixture',
          safeName: 'executor_runtime_fixture',
          version: '1.0.0',
          sourceCode: '',
          filePath: modulePath,
          fileUrl: modulePath,
        },
      ],
    },
  });
}

async function createPackageFixture(): Promise<{
  directory: string;
  modulePath: string;
}> {
  const directory = await mkdtemp(
    path.join(tmpdir(), 'enfyra-package-runtime-isolation-'),
  );
  const modulePath = path.join(directory, 'fixture.mjs');
  await writeFile(
    modulePath,
    `
      import { createRequire } from 'node:module';
      createRequire(import.meta.url);

      export default {
        hang() {
          return new Promise(() => {});
        },
        delay(value, delayMs) {
          return new Promise((resolve) => setTimeout(() => resolve(value), delayMs));
        },
        spin() {
          while (true) {}
        },
        value(value) {
          return value;
        },
      };
    `,
    'utf8',
  );
  return { directory, modulePath };
}

describe('package runtime task isolation', () => {
  it('escalates an uncooperative parked package promise without terminating its runner', async () => {
    const fixture = await createPackageFixture();
    const executor = makeService(fixture.modulePath);

    try {
      const timedOut = executor.run(
        `return await $ctx.$pkgs['executor-runtime-fixture'].hang();`,
        context(),
        150,
      );
      const control = executor.run('return 2;', context(), 5_000);
      const collateral = executor.run(
        `return await $ctx.$pkgs['executor-runtime-fixture'].delay(3, 3000);`,
        context(),
        5_000,
      );

      await expect(timedOut).rejects.toMatchObject({
        errorCode: 'SCRIPT_TIMEOUT',
      });
      await expect(control).resolves.toBe(2);
      await expect(collateral).rejects.toMatchObject({
        errorCode: 'SCRIPT_EXECUTION_ERROR',
        details: {
          executorCode: 'ERR_PACKAGE_RUNTIME_LOST',
          outcome: 'unknown_outcome',
          retryable: false,
        },
      });
      expect(executor.getMetrics().crashesTotal).toBe(0);
    } finally {
      executor.onDestroy();
      await rm(fixture.directory, { recursive: true, force: true });
    }
  }, 15_000);

  it('replaces an unresponsive package child without terminating its runner', async () => {
    const fixture = await createPackageFixture();
    const executor = makeService(fixture.modulePath);
    let signalStarted: (() => void) | undefined;
    const started = new Promise<void>((resolve) => {
      signalStarted = resolve;
    });

    try {
      const timedOut = executor.run(
        `
          await $ctx.$helpers.started();
          return await $ctx.$pkgs['executor-runtime-fixture'].spin();
        `,
        {
          ...context(),
          $helpers: {
            started: () => signalStarted?.(),
          },
        },
        500,
      );
      await started;
      await new Promise((resolve) => setTimeout(resolve, 50));

      const control = executor.run('return 2;', context(), 5_000);
      const collateral = Array.from({ length: 20 }, (_, index) =>
        executor.run(
          `return await $ctx.$pkgs['executor-runtime-fixture'].delay(${index}, 3000);`,
          context(),
          5_000,
        ),
      );

      await expect(timedOut).rejects.toMatchObject({
        errorCode: 'SCRIPT_TIMEOUT',
      });
      await expect(control).resolves.toBe(2);
      const results = await Promise.allSettled(collateral);
      const rejected = results.filter(
        (result) =>
          result.status === 'rejected' &&
          result.reason?.details?.executorCode === 'ERR_PACKAGE_RUNTIME_LOST' &&
          result.reason?.details?.outcome === 'unknown_outcome' &&
          result.reason?.details?.retryable === false,
      );
      const fulfilled = results.filter((result) => result.status === 'fulfilled');
      expect(rejected.length).toBeGreaterThan(0);
      expect(fulfilled.length).toBeGreaterThan(0);
      expect(rejected.length + fulfilled.length).toBe(collateral.length);
      expect(executor.getMetrics().crashesTotal).toBe(0);
      await expect(
        executor.run(
          `return await $ctx.$pkgs['executor-runtime-fixture'].value(4);`,
          context(),
          5_000,
        ),
      ).resolves.toBe(4);
    } finally {
      executor.onDestroy();
      await rm(fixture.directory, { recursive: true, force: true });
    }
  }, 15_000);
});
