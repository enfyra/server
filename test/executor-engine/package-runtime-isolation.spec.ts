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

function makeService(
  modulePath: string,
  tuning?: ConstructorParameters<typeof IsolatedExecutorService>[0]['tuning'],
) {
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
    tuning,
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
      import { runInNewContext } from 'node:vm';
      createRequire(import.meta.url);

      export default {
        hang() {
          return new Promise(() => {});
        },
        async delay(value, delayMs, onStarted) {
          await onStarted?.();
          return new Promise((resolve) => setTimeout(() => resolve(value), delayMs));
        },
        spin() {
          while (true) {}
        },
        value(value) {
          return value;
        },
        crossRealmView() {
          return runInNewContext(
            'new DataView(Uint8Array.from([4, 5, 6]).buffer, 1, 2)',
          );
        },
        nullPrototypeValue() {
          const value = Object.create(null);
          value.marker = 9;
          return value;
        },
        taggedView() {
          const value = new Uint8Array([7, 8]);
          Object.defineProperty(value, Symbol.toStringTag, {
            value: 'Float64Array',
          });
          return value;
        },
        objectWithDefault() {
          return { default: 'business-default', marker: 10 };
        },
        async rememberCallback(callback, onReady, waitForRelease) {
          globalThis.__enfyraRememberedCallback = callback;
          await onReady();
          await waitForRelease();
          return true;
        },
        invokeRememberedCallback(value) {
          return globalThis.__enfyraRememberedCallback(value);
        },
      };
    `,
    'utf8',
  );
  return { directory, modulePath };
}

describe('package runtime task isolation', () => {
  it('round-trips supported package values without alias loss or prototype mutation', async () => {
    const fixture = await createPackageFixture();
    const executor = makeService(fixture.modulePath);

    try {
      const result = await executor.run(
        `
          const pkg = $ctx.$pkgs['executor-runtime-fixture'];
          const shared = { marker: 7 };
          const aliases = await pkg.value([shared, shared]);
          const promised = await pkg.value(Promise.resolve(42));
          const thenable = await pkg.value({ then(resolve) { resolve(43); } });

          const protoInput = {};
          Object.defineProperty(protoInput, '__proto__', {
            value: { safe: true },
            enumerable: true,
          });
          const protoOutput = await pkg.value(protoInput);
          const markerObject = await pkg.value({
            __date: '2024-01-01T00:00:00.000Z',
            other: 1,
          });
          const exactMarkerObject = await pkg.value({ __date: 'business-value' });
          const undefinedMarkerObject = await pkg.value({ __e: 'u' });
          const wrappedMarkerObject = await pkg.value({ __e: 'v', d: 'business-value' });
          const numbers = await pkg.value([NaN, Infinity, -Infinity, -0]);
          const scalarBigInt = await pkg.value(9007199254740993n);
          const undefinedValues = await pkg.value({
            own: undefined,
            array: [undefined],
          });

          const bytes = new Uint8Array([9, 8, 7, 6]);
          const view = await pkg.value(new DataView(bytes.buffer, 1, 2));
          const viewBytes = Array.from(
            new Uint8Array(view.buffer, view.byteOffset, view.byteLength),
          );
          const crossRealmView = await pkg.crossRealmView();
          const crossRealmViewBytes = Array.from(
            new Uint8Array(
              crossRealmView.buffer,
              crossRealmView.byteOffset,
              crossRealmView.byteLength,
            ),
          );

          const nullPrototype = await pkg.nullPrototypeValue();
          const taggedView = await pkg.taggedView();
          const taggedViewBytes = Array.from(taggedView);
          const objectWithDefault = await pkg.objectWithDefault();

          let bigInts = null;
          if (typeof BigInt64Array !== 'undefined') {
            const echoed = await pkg.value(new BigInt64Array([1n, -2n]));
            bigInts = Array.from(echoed, (value) => String(value));
          }

          return {
            aliases,
            promised,
            thenable,
            protoIsPlain: Object.getPrototypeOf(protoOutput) === Object.prototype,
            protoIsOwn: Object.prototype.hasOwnProperty.call(protoOutput, '__proto__'),
            protoValue: protoOutput.__proto__.safe,
            markerObject,
            exactMarkerObject,
            undefinedMarkerObject,
            wrappedMarkerObject,
            scalarBigInt: String(scalarBigInt),
            undefinedChecks: [
              Object.prototype.hasOwnProperty.call(undefinedValues, 'own'),
              undefinedValues.own === undefined,
              undefinedValues.array.length === 1,
              undefinedValues.array[0] === undefined,
            ],
            numberChecks: [
              Number.isNaN(numbers[0]),
              numbers[1] === Infinity,
              numbers[2] === -Infinity,
              Object.is(numbers[3], -0),
            ],
            viewBytes,
            crossRealmViewBytes,
            nullPrototypeChecks: [
              Object.getPrototypeOf(nullPrototype) === null,
              nullPrototype.marker === 9,
              nullPrototype.toString === undefined,
            ],
            taggedViewBytes,
            objectWithDefault,
            bigInts,
          };
        `,
        context(),
        5_000,
      );

      expect(result).toEqual({
        aliases: [{ marker: 7 }, { marker: 7 }],
        promised: 42,
        thenable: 43,
        protoIsPlain: true,
        protoIsOwn: true,
        protoValue: true,
        markerObject: {
          __date: '2024-01-01T00:00:00.000Z',
          other: 1,
        },
        exactMarkerObject: { __date: 'business-value' },
        undefinedMarkerObject: { __e: 'u' },
        wrappedMarkerObject: { __e: 'v', d: 'business-value' },
        scalarBigInt: '9007199254740993',
        undefinedChecks: [true, true, true, true],
        numberChecks: [true, true, true, true],
        viewBytes: [8, 7],
        crossRealmViewBytes: [5, 6],
        nullPrototypeChecks: [true, true, true],
        taggedViewBytes: [7, 8],
        objectWithDefault: { default: 'business-default', marker: 10 },
        bigInts: ['1', '-2'],
      });
    } finally {
      executor.onDestroy();
      await rm(fixture.directory, { recursive: true, force: true });
    }
  });

  it('keeps a retained package callback bound to its owning active task', async () => {
    const fixture = await createPackageFixture();
    const executor = makeService(fixture.modulePath, {
      maxConcurrentWorkers: 1,
      isolateMemoryLimitMb: 128,
      tasksPerWorkerCap: 216,
      isolatePoolSize: 2,
      tasksPerIsolate: 6,
    });
    let callbackRuns = 0;
    let signalReady: (() => void) | undefined;
    let releaseOwner: (() => void) | undefined;
    const ready = new Promise<void>((resolve) => {
      signalReady = resolve;
    });
    const ownerRelease = new Promise<void>((resolve) => {
      releaseOwner = resolve;
    });

    try {
      const owner = executor.run(
        `
          return await $ctx.$pkgs['executor-runtime-fixture'].rememberCallback(
            (value) => $ctx.$helpers.record(value),
            () => $ctx.$helpers.ready(),
            () => $ctx.$helpers.waitForRelease(),
          );
        `,
        {
          ...context(),
          $helpers: {
            record: (value: unknown) => {
              callbackRuns += 1;
              return value;
            },
            ready: () => signalReady?.(),
            waitForRelease: () => ownerRelease,
          },
        },
        5_000,
      );
      await ready;

      await expect(executor.run(
        `
          return await $ctx.$pkgs['executor-runtime-fixture']
            .invokeRememberedCallback('forged');
        `,
        context(),
        5_000,
      )).rejects.toThrow('Package callback is not authorized for this task');

      expect(callbackRuns).toBe(0);
      releaseOwner?.();
      await expect(owner).resolves.toBe(true);
    } finally {
      releaseOwner?.();
      executor.onDestroy();
      await rm(fixture.directory, { recursive: true, force: true });
    }
  });

  it('escalates an uncooperative parked package promise without terminating its runner', async () => {
    const fixture = await createPackageFixture();
    const executor = makeService(fixture.modulePath, {
      maxConcurrentWorkers: 1,
      isolateMemoryLimitMb: 128,
      tasksPerWorkerCap: 216,
      isolatePoolSize: 36,
      tasksPerIsolate: 6,
    });
    let signalCollateralStarted: (() => void) | undefined;
    const collateralStarted = new Promise<void>((resolve) => {
      signalCollateralStarted = resolve;
    });

    try {
      const collateral = executor.run(
        `return await $ctx.$pkgs['executor-runtime-fixture'].delay(3, 3000, () => $ctx.$helpers.started());`,
        {
          ...context(),
          $helpers: {
            started: () => signalCollateralStarted?.(),
          },
        },
        5_000,
      );
      await collateralStarted;
      const timedOut = executor.run(
        `return await $ctx.$pkgs['executor-runtime-fixture'].hang();`,
        context(),
        150,
      );
      const control = executor.run('return 2;', context(), 5_000);

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
