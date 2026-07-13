import { describe, expect, it } from 'vitest';
import { DynamicContextFactory } from '../../src/shared/services/dynamic-context.factory';

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
});
