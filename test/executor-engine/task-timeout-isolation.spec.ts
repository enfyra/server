import { IsolatedExecutorService } from '@enfyra/kernel';

function context(overrides: Record<string, unknown> = {}) {
  return {
    $body: {},
    $query: {},
    $params: {},
    $share: {},
    $api: { request: {} },
    ...overrides,
  };
}

async function waitFor(
  condition: () => boolean,
  timeoutMs = 5_000,
): Promise<void> {
  const startedAt = Date.now();
  while (!condition()) {
    if (Date.now() - startedAt >= timeoutMs) {
      throw new Error(`Condition not met within ${timeoutMs}ms`);
    }
    await new Promise((resolve) => setTimeout(resolve, 10));
  }
}

describe('task-scoped executor timeout isolation', () => {
  it('times out a parked task without terminating collateral tasks on the same worker', async () => {
    const executor = new IsolatedExecutorService({
      packageCacheService: { getPackages: async () => [] },
      packageCdnLoaderService: { getPackageSources: () => [] },
    });

    try {
      const collateral = Array.from({ length: 12 }, (_, index) =>
        executor.run(
          'return await $ctx.$helpers.delayedValue();',
          context({
            $helpers: {
              delayedValue: async () => {
                await new Promise((resolve) => setTimeout(resolve, 200));
                return index + 1;
              },
            },
          }),
          5_000,
        ),
      );
      const timedOut = executor.run(
        'await new Promise(() => {}); return 1;',
        context(),
        100,
      );

      await expect(timedOut).rejects.toMatchObject({
        errorCode: 'SCRIPT_TIMEOUT',
      });
      const collateralResults = await Promise.allSettled(collateral);
      expect(collateralResults).toEqual(
        Array.from({ length: 12 }, (_, index) => ({
          status: 'fulfilled',
          value: index + 1,
        })),
      );
      expect(executor.getMetrics().crashesTotal).toBe(0);
    } finally {
      executor.onDestroy();
    }
  });

  it('contains an isolate CPU timeout without terminating parked sibling tasks', async () => {
    const executor = new IsolatedExecutorService({
      packageCacheService: { getPackages: async () => [] },
      packageCdnLoaderService: { getPackageSources: () => [] },
    });

    try {
      const collateral = Array.from({ length: 8 }, (_, index) =>
        executor.run(
          'return await $ctx.$helpers.delayedValue();',
          context({
            $helpers: {
              delayedValue: async () => {
                await new Promise((resolve) => setTimeout(resolve, 250));
                return index + 1;
              },
            },
          }),
          5_000,
        ),
      );
      const timedOut = executor.run(
        'while (true) {}',
        context(),
        100,
      );

      await expect(timedOut).rejects.toMatchObject({
        errorCode: 'SCRIPT_TIMEOUT',
      });
      const collateralResults = await Promise.allSettled(collateral);
      expect(collateralResults).toEqual(
        Array.from({ length: 8 }, (_, index) => ({
          status: 'fulfilled',
          value: index + 1,
        })),
      );
      expect(executor.getMetrics().crashesTotal).toBe(0);
    } finally {
      executor.onDestroy();
    }
  });

  it('contains isolate memory exhaustion without terminating sibling tasks', async () => {
    const executor = new IsolatedExecutorService({
      packageCacheService: { getPackages: async () => [] },
      packageCdnLoaderService: { getPackageSources: () => [] },
    });

    try {
      const collateral = Array.from({ length: 8 }, (_, index) =>
        executor.run(
          'return await $ctx.$helpers.delayedValue();',
          context({
            $helpers: {
              delayedValue: async () => {
                await new Promise((resolve) => setTimeout(resolve, 500));
                return index + 1;
              },
            },
          }),
          5_000,
        ),
      );
      const exhausted = executor.run(
        `
          const retained = [];
          while (true) retained.push(new Array(250000).fill('xxxxxxxx'));
        `,
        context(),
        10_000,
      );

      await expect(exhausted).rejects.toMatchObject({
        errorCode: 'SCRIPT_EXECUTION_ERROR',
        details: {
          executorCode: 'ERR_ISOLATE_MEMORY_LIMIT',
          outcome: 'unknown_outcome',
          retryable: false,
        },
      });
      await expect(Promise.all(collateral)).resolves.toEqual(
        Array.from({ length: 8 }, (_, index) => index + 1),
      );
      expect(executor.getMetrics().crashesTotal).toBe(0);
    } finally {
      executor.onDestroy();
    }
  }, 15_000);

  it('classifies spontaneous runner loss without retrying interrupted tasks', async () => {
    const executor = new IsolatedExecutorService({
      packageCacheService: { getPackages: async () => [] },
      packageCdnLoaderService: { getPackageSources: () => [] },
    });
    let calls = 0;

    try {
      const initialWorkers = executor.getMetrics().pool.workers;
      const crashedPid = initialWorkers[0].pid;
      expect(crashedPid).toBeTypeOf('number');

      const crashing = executor.run(
        'await $ctx.$helpers.crashRunner(); return 1;',
        context({
          $helpers: {
            crashRunner: () => {
              calls++;
              process.kill(crashedPid!, 'SIGKILL');
            },
          },
        }),
        5_000,
      );
      const control = executor.run('return 2;', context(), 5_000);
      const collateral = executor.run(
        'return await $ctx.$helpers.parked();',
        context({
          $helpers: {
            parked: () =>
              new Promise((resolve) => setTimeout(() => resolve(3), 5_000)),
          },
        }),
        10_000,
      );

      for (const interrupted of [crashing, collateral]) {
        await expect(interrupted).rejects.toMatchObject({
          errorCode: 'SCRIPT_EXECUTION_ERROR',
          details: {
            executorCode: 'ERR_EXECUTOR_RUNNER_LOST',
            outcome: 'unknown_outcome',
            retryable: false,
          },
        });
      }
      await expect(control).resolves.toBe(2);
      expect(calls).toBe(1);
      await waitFor(() => {
        const workers = executor.getMetrics().pool.workers;
        return (
          workers.length === 2 &&
          workers.every((worker) => worker.pid !== crashedPid)
        );
      });
      expect(executor.getMetrics().crashesTotal).toBe(1);
    } finally {
      executor.onDestroy();
    }
  }, 15_000);

  it('kills and replaces a runner only when isolate cancellation is not acknowledged', async () => {
    const executor = new IsolatedExecutorService({
      packageCacheService: { getPackages: async () => [] },
      packageCdnLoaderService: { getPackageSources: () => [] },
    });
    let calls = 0;
    let signalStopped: (() => void) | undefined;
    const stopped = new Promise<void>((resolve) => {
      signalStopped = resolve;
    });

    try {
      const initialWorkers = executor.getMetrics().pool.workers;
      expect(initialWorkers).toHaveLength(2);
      const stalledPid = initialWorkers[0].pid;
      expect(stalledPid).toBeTypeOf('number');

      const timedOut = executor.run(
        'await $ctx.$helpers.started(); await new Promise(() => {});',
        context({
          $helpers: {
            started: async () => {
              calls++;
              process.kill(stalledPid!, 'SIGSTOP');
              signalStopped?.();
            },
          },
        }),
        500,
      );
      await stopped;
      const control = executor.run('return 3;', context(), 5_000);
      const collateral = executor.run(
        'return await $ctx.$helpers.parked();',
        context({
          $helpers: {
            parked: () =>
              new Promise((resolve) => setTimeout(() => resolve(2), 5_000)),
          },
        }),
        10_000,
      );

      await expect(timedOut).rejects.toMatchObject({
        errorCode: 'SCRIPT_TIMEOUT',
        details: {
          outcome: 'unknown_outcome',
          retryable: false,
          escalation: 'runner_terminated_after_cancel_grace',
        },
      });
      await expect(collateral).rejects.toMatchObject({
        errorCode: 'SCRIPT_EXECUTION_ERROR',
        details: {
          executorCode: 'ERR_EXECUTOR_RUNNER_LOST',
          outcome: 'unknown_outcome',
          retryable: false,
        },
      });
      await expect(control).resolves.toBe(3);
      expect(calls).toBe(1);

      await waitFor(() => {
        const workers = executor.getMetrics().pool.workers;
        return (
          workers.length === 2 &&
          workers.every((worker) => worker.pid !== stalledPid)
        );
      });
      expect(executor.getMetrics().crashesTotal).toBe(1);
      await expect(executor.run('return 4;', context(), 5_000)).resolves.toBe(4);
    } finally {
      executor.onDestroy();
    }
  }, 15_000);
});
