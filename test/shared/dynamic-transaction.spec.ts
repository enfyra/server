import { describe, expect, it } from 'vitest';
import { DynamicContextFactory } from '../../src/shared/services/dynamic-context.factory';
import { deferDynamicTransactionEffect } from '../../src/shared/utils/dynamic-transaction-effects.util';
import { DynamicRepository } from '../../src/modules/dynamic-api';

type TransactionHarness = {
  ctx: any;
  events: string[];
};

function createHarness(dbType: 'mysql' | 'mongodb'): TransactionHarness {
  const events: string[] = [];
  let sqlTransactionActive = false;
  let mongoTransactionActive = false;
  const repo = {
    create: async (input: { data: { id: string } }) => {
      events.push(
        `create:${input.data.id}:${sqlTransactionActive || mongoTransactionActive}`,
      );
      return input.data;
    },
  };
  const knexService = {
    transaction: async (callback: (trx: object) => Promise<unknown>) => {
      events.push('sql:begin');
      sqlTransactionActive = true;
      try {
        const result = await callback({});
        events.push('sql:commit');
        return result;
      } catch (error) {
        events.push('sql:rollback');
        throw error;
      } finally {
        sqlTransactionActive = false;
      }
    },
    runWithTransaction: async (
      _trx: object,
      callback: () => Promise<unknown>,
    ) => await callback(),
  };
  const mongoService = {
    runInSaga: async (callback: (scope: object) => Promise<unknown>) => {
      events.push('mongo:begin');
      mongoTransactionActive = true;
      try {
        const data = await callback({});
        events.push('mongo:commit');
        return { data };
      } finally {
        mongoTransactionActive = false;
      }
    },
    runWithTransactionScope: async (
      _scope: object,
      callback: () => Promise<unknown>,
    ) => await callback(),
  };
  const factory = new DynamicContextFactory({
    bcryptService: {} as any,
    userCacheService: {} as any,
    envService: { get: () => 'test-secret' } as any,
    databaseConfigService: {
      isMongoDb: () => dbType === 'mongodb',
    } as any,
    knexService: knexService as any,
    mongoService: mongoService as any,
    websocketContextFactory: {} as any,
  });
  const ctx = factory.createBase({ repos: { records: repo } });
  return { ctx, events };
}

describe('dynamic transaction context', () => {
  it('runs every repository call inside one SQL transaction', async () => {
    const { ctx, events } = createHarness('mysql');

    const result = await ctx.$transaction.run(async () => {
      await ctx.$repos.records.create({ data: { id: 'one' } });
      await ctx.$repos.records.create({ data: { id: 'two' } });
      return 'done';
    });

    expect(result).toBe('done');
    expect(events).toEqual([
      'sql:begin',
      'create:one:true',
      'create:two:true',
      'sql:commit',
    ]);
  });

  it('propagates failures so the SQL transaction rolls back', async () => {
    const { ctx, events } = createHarness('mysql');

    await expect(
      ctx.$transaction.run(async () => {
        await ctx.$repos.records.create({ data: { id: 'one' } });
        throw new Error('stop');
      }),
    ).rejects.toThrow('stop');

    expect(events).toEqual(['sql:begin', 'create:one:true', 'sql:rollback']);
  });

  it('selects the Mongo transaction wrapper without exposing its mode', async () => {
    const { ctx, events } = createHarness('mongodb');

    await ctx.$transaction.run(async () => {
      await ctx.$repos.records.create({ data: { id: 'one' } });
    });

    expect(events).toEqual(['mongo:begin', 'create:one:true', 'mongo:commit']);
  });

  it('joins a nested call to the active transaction', async () => {
    const { ctx, events } = createHarness('mysql');

    await ctx.$transaction.run(async () => {
      await ctx.$transaction.run(async () => {
        await ctx.$repos.records.create({ data: { id: 'one' } });
      });
    });

    expect(events).toEqual(['sql:begin', 'create:one:true', 'sql:commit']);
  });

  it('flushes deferred repository effects only after commit', async () => {
    const { ctx, events } = createHarness('mysql');

    await ctx.$transaction.run(async () => {
      expect(
        deferDynamicTransactionEffect(ctx, async () => {
          events.push('effect');
        }),
      ).toBe(true);
      events.push('work');
    });

    expect(events).toEqual(['sql:begin', 'work', 'sql:commit', 'effect']);
  });

  it('drops deferred repository effects when the transaction rolls back', async () => {
    const { ctx, events } = createHarness('mysql');

    await expect(
      ctx.$transaction.run(async () => {
        deferDynamicTransactionEffect(ctx, async () => {
          events.push('effect');
        });
        throw new Error('stop');
      }),
    ).rejects.toThrow('stop');

    expect(events).toEqual(['sql:begin', 'sql:rollback']);
  });

  it('defers DynamicRepository cache and mutation events until commit', async () => {
    const { ctx, events } = createHarness('mysql');
    const repository = Object.assign(
      Object.create(DynamicRepository.prototype),
      {
        context: ctx,
        tableName: 'records',
        eventEmitter: {
          emitAsync: async (event: string) => {
            events.push(`async:${event}`);
          },
          emit: (event: string) => {
            events.push(`sync:${event}`);
          },
        },
      },
    );

    await ctx.$transaction.run(async () => {
      await repository.reload({ ids: ['one'] });
      repository.emitTableMutation('create', ['one'], { id: 'one' });
      events.push('work');
    });

    expect(events).toEqual([
      'sql:begin',
      'work',
      'sql:commit',
      'async:cache:invalidate',
      'sync:data:table:mutation',
    ]);
  });
});
