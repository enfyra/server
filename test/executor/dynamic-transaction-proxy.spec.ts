import { describe, expect, it, vi } from 'vitest';
import { IsolatedExecutorService } from '@enfyra/kernel';
import { DynamicContextFactory } from '../../src/shared/services/dynamic-context.factory';

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
          50,
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
