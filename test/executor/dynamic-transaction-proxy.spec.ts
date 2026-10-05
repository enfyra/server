import { describe, expect, it, vi } from 'vitest';
import { getIoAbortSignal, IsolatedExecutorService } from '@enfyra/kernel';
import { DynamicContextFactory } from '../../src/shared/services/dynamic-context.factory';
import { deferDynamicTransactionEffect } from '../../src/shared/utils/dynamic-transaction-effects.util';

function createService() {
  return new IsolatedExecutorService({
    packageCacheService: { getPackages: async () => [] } as any,
    packageCdnLoaderService: { getPackageSources: () => [] } as any,
  });
}

function createContext(events: string[]) {
  let active = false;
  return {
    $body: {},
    $query: {},
    $params: {},
    $share: { $logs: [] },
    $helpers: {},
    $cache: {},
    $user: null,
    $repos: {
      records: {
        create: async ({ data }: { data: { id: string } }) => {
          events.push(`create:${data.id}:${active}`);
          return data;
        },
      },
    },
    $transaction: {
      run: async <T>(callback: () => Promise<T>) => {
        events.push('begin');
        active = true;
        try {
          const result = await callback();
          events.push('commit');
          return result;
        } catch (error) {
          events.push('rollback');
          throw error;
        } finally {
          active = false;
        }
      },
    },
  };
}

describe('isolated executor transaction proxy', () => {
  it('discards repository RPC effects when the sandbox transaction rolls back', async () => {
    const service = createService();
    const effects: string[] = [];
    const factory = new DynamicContextFactory({
      userCacheService: {},
      envService: { get: () => 'test-secret' },
      databaseConfigService: { isMongoDb: () => false },
      knexService: {
        transaction: (work: (trx: object) => Promise<unknown>) => work({}),
        runWithTransaction: (_trx: object, work: () => Promise<unknown>) => work(),
      },
    } as unknown as ConstructorParameters<typeof DynamicContextFactory>[0]);
    const context = factory.createBase({});
    context.$repos = {
      records: { create: async () => {
        const effect = () => { effects.push('created'); };
        if (!deferDynamicTransactionEffect(context, effect)) effect();
        return { data: [{ id: 1 }] };
      } },
    } as unknown as typeof context.$repos;
    try {
      await expect(service.run(`return await $ctx.$transaction.run(async () => {
        await $ctx.$repos.records.create({ data: {} });
        throw new Error('rollback');
      });`, context, 5000)).rejects.toThrow('rollback');
      expect(effects).toEqual([]);
    } finally { service.onDestroy(); }
  });

  it.each(['trusted', 'secure'])('passes an updateLocked callback through the %s repository bridge', async (access) => {
    const service = createService();
    const repository = {
      updateLocked: async (options: { fields: string[]; data: (current: { credit: number }) => Promise<unknown> }) => {
        expect(typeof options.data).toBe('function');
        expect(options.fields).toEqual(['credit']);
        return { data: [await options.data({ credit: 100 })] };
      },
    };
    const context = { ...createContext([]), $repos: { accounts: repository, secure: { accounts: repository } } };
    try {
      const path = access === 'secure' ? 'secure.accounts' : 'accounts';
      const result = await service.run(`return await $ctx.$repos.${path}.updateLocked({ id: 1, fields: ['credit'], data: current => ({ credit: current.credit - 10 }) });`, context, 5000);
      expect(result).toEqual({ data: [{ credit: 90 }] });
    } finally {
      service.onDestroy();
    }
  });

  it('runs repository RPC calls from the callback through the host transaction', async () => {
    const service = createService();
    const events: string[] = [];
    try {
      const result = await service.run(
        `return await $ctx.$transaction.run(async () => {
          await $ctx.$repos.records.create({ data: { id: 'one' } });
          return 'done';
        });`,
        createContext(events),
        5000,
      );

      expect(result).toBe('done');
      expect(events).toEqual(['begin', 'create:one:true', 'commit']);
    } finally {
      service.onDestroy();
    }
  });

  it('propagates a script error through the host transaction callback', async () => {
    const service = createService();
    const events: string[] = [];
    try {
      await expect(
        service.run(
          `await $ctx.$transaction.run(async () => {
            await $ctx.$repos.records.create({ data: { id: 'one' } });
            throw new Error('stop');
          });`,
          createContext(events),
          5000,
        ),
      ).rejects.toThrow('stop');

      expect(events).toEqual(['begin', 'create:one:true', 'rollback']);
    } finally {
      service.onDestroy();
    }
  });

  it('preserves a structured user throw through the host transaction callback', async () => {
    const service = createService();
    const events: string[] = [];
    try {
      await expect(
        service.run(
          `await $ctx.$transaction.run(async () => {
            await $ctx.$repos.records.create({ data: { id: 'one' } });
            $ctx.$throw['422']('Trial credit is unavailable', { offer: 'trial' });
          });`,
          createContext(events),
          5000,
        ),
      ).rejects.toMatchObject({
        statusCode: 422,
        errorCode: 'HTTP_422',
        details: { offer: 'trial' },
        message: 'Trial credit is unavailable',
      });

      expect(events).toEqual(['begin', 'create:one:true', 'rollback']);
    } finally {
      service.onDestroy();
    }
  });

  it.each(['single', 'batch'] as const)(
    'aborts already-started transaction work on a %s timeout',
    async (mode) => {
      const service = createService();
      const events: string[] = [];
      let signal: AbortSignal | undefined;
      let finishWork: (() => void) | undefined;
      const factory = new DynamicContextFactory({
        bcryptService: {} as any,
        userCacheService: {} as any,
        envService: { get: () => 'test-secret' } as any,
        databaseConfigService: { isMongoDb: () => false } as any,
        knexService: {
          transaction: async (callback: () => Promise<unknown>) => {
            events.push('begin');
            try {
              const result = await callback();
              events.push('commit');
              return result;
            } catch (error) {
              events.push('rollback');
              throw error;
            }
          },
          runWithTransaction: async (
            _trx: object,
            callback: () => Promise<unknown>,
          ) => await callback(),
        } as any,
        mongoService: {} as any,
        websocketContextFactory: {} as any,
      });
      const transactionContext = factory.createBase({});
      const context = {
        $helpers: {
          ready: () => true,
          pendingMutation: () => {
            signal = getIoAbortSignal();
            return transactionContext.$transaction.run(
              () => new Promise<void>((resolve) => { finishWork = resolve; }),
            );
          },
        },
      };
      const prepare = `
        const mutation = $ctx.$helpers.pendingMutation();
        mutation.then(() => {}, () => {});
        await $ctx.$helpers.ready();
      `;
      try {
        const task = mode === 'batch'
          ? service.runBatch([
            { type: 'preHook', code: prepare },
            { type: 'handler', code: 'while (true) {}' },
          ], context, 500)
          : service.run(prepare + 'while (true) {}', context, 500);
        await expect(task).rejects.toMatchObject({
          errorCode: 'SCRIPT_TIMEOUT',
          statusCode: 408,
        });
        expect(signal?.aborted).toBe(true);
        await vi.waitFor(() => expect(events).toEqual(['begin', 'rollback']));
        expect(service.getMetrics().crashesTotal).toBe(0);
      } finally {
        finishWork?.();
        service.onDestroy();
      }
    },
  );

  it('rolls back the outer transaction when the isolate times out', async () => {
    const service = createService();
    const events: string[] = [];
    const factory = new DynamicContextFactory({
      bcryptService: {} as any,
      userCacheService: {} as any,
      envService: { get: () => 'test-secret' } as any,
      databaseConfigService: { isMongoDb: () => false } as any,
      knexService: {
        transaction: async (callback: () => Promise<unknown>) => {
          events.push('begin');
          try {
            const result = await callback();
            events.push('commit');
            return result;
          } catch (error) {
            events.push('rollback');
            throw error;
          }
        },
        runWithTransaction: async (
          _trx: object,
          callback: () => Promise<unknown>,
        ) => await callback(),
      } as any,
      mongoService: {} as any,
      websocketContextFactory: {} as any,
    });
    const ctx = factory.createBase({
      helpers: {
        waitForever: () => new Promise(() => {}),
      } as any,
    });

    try {
      await expect(
        service.runBatch(
          [
            {
              code: `await $ctx.$transaction.run(async () => {
                await $ctx.$helpers.waitForever();
              });`,
              type: 'handler',
            },
          ],
          ctx,
          1000,
        ),
      ).rejects.toMatchObject({
        errorCode: 'SCRIPT_TIMEOUT',
        statusCode: 408,
      });

      await vi.waitFor(() => expect(events).toEqual(['begin', 'rollback']));
    } finally {
      service.onDestroy();
    }
  });
});
