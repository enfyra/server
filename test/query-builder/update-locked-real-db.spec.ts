import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import { EventEmitter2 } from 'eventemitter2';
import { ObjectId } from 'mongodb';
import {
  ExecutorEngineService,
  IsolatedExecutorService,
  QueryBuilderService,
  runWithIoAbortSignal,
} from '@enfyra/kernel';
import { KnexService } from '../../src/engines/knex/knex.service';
import { KnexHookManagerService } from '../../src/engines/knex/services/knex-hook-manager.service';
import { MongoService } from '../../src/engines/mongo/services/mongo.service';
import { MongoRelationManagerService } from '../../src/engines/mongo/services/mongo-relation-manager.service';
import { DynamicRepository } from '../../src/modules/dynamic-api/repositories/dynamic.repository';
import {
  CACHE_EVENTS,
  DATA_EVENTS,
} from '../../src/shared/utils/cache-events.constants';
import { runWithDeferredDynamicTransactionEffects } from '../../src/shared/utils/dynamic-transaction-effects.util';
import { DynamicContextFactory } from '../../src/shared/services/dynamic-context.factory';
import { RuntimeScriptExecutorService } from '../../src/engines/cache/services/runtime-script-executor.service';
import { RepoRegistryService } from '../../src/engines/cache/services/repo-registry.service';

function barrier() {
  let resolve!: () => void;
  const promise = new Promise<void>((done) => {
    resolve = done;
  });
  return { promise, resolve };
}

const selected = process.env.LOCKED_UPDATE_DATABASE;
if (!['postgres', 'mysql', 'mongodb'].includes(selected ?? '')) {
  throw new Error(
    'LOCKED_UPDATE_DATABASE must select one required real database',
  );
}

describe(`updateLocked real database (${selected})`, () => {
  const isMongo = selected === 'mongodb';
  const uri = process.env.LOCKED_UPDATE_DATABASE_URI;
  if (!uri) throw new Error('LOCKED_UPDATE_DATABASE_URI is required');
  const table = `credit_accounts_ul_${Date.now()}`;
  const ledgerTable = `${table}_ledger`;
  const id = isMongo ? new ObjectId().toHexString() : 1;
  const pk = isMongo ? '_id' : 'id';
  const tableMetadata = {
    name: table,
    alias: 'accounts',
    columns: [
      {
        name: pk,
        type: isMongo ? 'objectId' : 'integer',
        isPrimary: true,
        isUpdatable: false,
        isPublished: true,
      },
      { name: 'credit', type: 'integer', isUpdatable: true, isPublished: true },
      { name: 'status', type: 'varchar', isUpdatable: true, isPublished: true },
      {
        name: 'charged',
        type: 'integer',
        isUpdatable: true,
        isPublished: true,
      },
      {
        name: 'updatedAt',
        type: 'datetime',
        isUpdatable: false,
        isPublished: true,
      },
    ],
    relations: [],
  };
  const ledgerMetadata = {
    ...tableMetadata,
    name: ledgerTable,
    alias: 'ledger',
    columns: [
      {
        name: pk,
        type: isMongo ? 'objectId' : 'integer',
        isPrimary: true,
        isUpdatable: false,
        isPublished: true,
      },
      { name: 'amount', type: 'integer', isUpdatable: true, isPublished: true },
      {
        name: 'operation',
        type: 'integer',
        isUpdatable: true,
        isPublished: true,
      },
      {
        name: 'creditAfter',
        type: 'integer',
        isUpdatable: true,
        isPublished: true,
      },
      {
        name: 'updatedAt',
        type: 'datetime',
        isUpdatable: false,
        isPublished: true,
      },
    ],
  };
  const metadata = {
    tables: new Map([
      [table, tableMetadata],
      [ledgerTable, ledgerMetadata],
    ]),
    tablesList: [tableMetadata, ledgerMetadata],
  };
  const registry = {
    getMetadata: () => metadata,
    requireMetadata: () => metadata,
    getTableMetadata: (name: string) => metadata.tables.get(name),
    lookupTableByName: (name: string) => metadata.tables.get(name),
    getMaxQueryDepth: () => 5,
  };
  const config = {
    getDbType: () => selected,
    isMongoDb: () => isMongo,
    isPostgres: () => selected === 'postgres',
  };
  const env = {
    get: (key: string) =>
      key === 'DB_URI'
        ? uri
        : key === 'SQL_POOL_MAX' && process.env.LOCKED_UPDATE_POOL_MAX
          ? Number(process.env.LOCKED_UPDATE_POOL_MAX)
          : undefined,
  };
  const engines: Array<KnexService | MongoService> = [];
  const repositories: DynamicRepository[] = [];
  const contexts: Array<DynamicRepository['context']> = [];
  const events = new EventEmitter2();
  const mutations: unknown[] = [];
  const reloads: unknown[] = [];
  events.on(DATA_EVENTS.TABLE_MUTATION, (event) => {
    mutations.push(event);
  });
  events.on(CACHE_EVENTS.INVALIDATE, (event) => {
    reloads.push(event);
  });

  beforeAll(async () => {
    for (let index = 0; index < 2; index++) {
      const common = {
        envService: env,
        databaseConfigService: config,
        runtimeRegistryService: registry,
      };
      let engine: KnexService | MongoService;
      if (isMongo) {
        const relations = new MongoRelationManagerService({
          runtimeRegistryService: registry,
        } as unknown as ConstructorParameters<
          typeof MongoRelationManagerService
        >[0]);
        engine = new MongoService({
          ...common,
          mongoRelationManagerService: relations,
        } as unknown as ConstructorParameters<typeof MongoService>[0]);
      } else {
        const hooks = new KnexHookManagerService({
          runtimeRegistryService: registry,
        } as unknown as ConstructorParameters<
          typeof KnexHookManagerService
        >[0]);
        engine = new KnexService({
          ...common,
          knexHookManagerService: hooks,
          lazyRef: {
            runtimeRegistryService: registry,
            mySqlRuntimeWriteBarrierService: {
              runWithWriteLease: (work: () => Promise<unknown>) => work(),
            },
          },
        } as unknown as ConstructorParameters<typeof KnexService>[0]);
      }
      engines.push(engine);
      await engine.init();
      if (!isMongo)
        Object.assign(engine, {
          columnTypesMap: new Map(
            metadata.tablesList.map((entry) => [
              entry.name,
              new Map(
                entry.columns.map((column) => [column.name, column.type]),
              ),
            ]),
          ),
        });
      const query = new QueryBuilderService({
        databaseConfigService: config,
        ...(isMongo ? { mongoService: engine } : { knexService: engine }),
        lazyRef: {
          metadataCacheService: {
            isLoaded: () => true,
            getMetadata: () => metadata,
          },
        },
      });
      const context = {
        $query: {},
        $user: { id: 1, isRootAdmin: true },
      } as unknown as DynamicRepository['context'];
      contexts.push(context);
      repositories.push(
        new DynamicRepository({
          context,
          tableName: table,
          queryBuilderService: query,
          eventEmitter: events,
          runtimeRegistryService: registry,
          runtimeMetadataSchemaRouterService: { handles: () => false },
          policyService: {
            checkMutationSafety: async () => ({ allowed: true }),
          },
          tableValidationService: { assertTableValid: async () => {} },
          guardValidationService: {},
          bcryptService: {},
        } as unknown as ConstructorParameters<typeof DynamicRepository>[0]),
      );
    }
    if (isMongo) {
      const engine = engines[0] as MongoService;
      expect(engine.supportsNativeMultiDocumentTransactions()).toBe(true);
      await engine.getRawDb().createCollection(table);
    } else {
      await (engines[0] as KnexService)
        .getUnscopedWriteKnex()
        .schema.createTable(table, (builder) => {
          builder.integer('id').primary();
          builder.integer('credit').notNullable();
          builder.string('status').notNullable();
          builder.integer('charged').notNullable().defaultTo(0);
          builder.timestamp('updatedAt').nullable();
        });
      await (engines[0] as KnexService)
        .getUnscopedWriteKnex()
        .schema.createTable(ledgerTable, (builder) => {
          builder.increments('id').primary();
          builder.integer('amount').notNullable();
          builder.integer('operation').notNullable().unique();
          builder.integer('creditAfter').notNullable();
          builder.timestamp('updatedAt').nullable();
        });
    }
  }, 30000);

  beforeEach(async () => {
    mutations.length = 0;
    reloads.length = 0;
    for (const context of contexts) context.$query = {};
    if (isMongo) {
      const collection = (engines[0] as MongoService)
        .getRawDb()
        .collection(table);
      await collection.deleteMany({});
      await collection.insertOne({
        _id: new ObjectId(String(id)),
        credit: 100,
        status: 'pending',
      });
    } else {
      const db = (engines[0] as KnexService).getUnscopedWriteKnex();
      await db(table).delete();
      await db(ledgerTable).delete();
      await db(table).insert({
        id,
        credit: 100,
        status: 'pending',
        charged: 0,
      });
    }
  });

  afterAll(async () => {
    try {
      if (engines.length) {
        if (isMongo)
          await (engines[0] as MongoService)
            .getRawDb()
            .collection(table)
            .drop();
        else {
          await (engines[0] as KnexService)
            .getUnscopedWriteKnex()
            .schema.dropTableIfExists(ledgerTable);
          await (engines[0] as KnexService)
            .getUnscopedWriteKnex()
            .schema.dropTableIfExists(table);
        }
      }
    } finally {
      for (const engine of engines) await engine.onDestroy();
    }
  });

  it('preserves concurrent decrements across independent database clients', async () => {
    const results = await Promise.allSettled(
      Array.from({ length: 12 }, (_, index) =>
        repositories[index % 2].updateLocked({
          id,
          data: (current) => ({ credit: Number(current.credit) - 1 }),
        }),
      ),
    );
    for (const result of results)
      expect(result.status === 'fulfilled' ? null : result.reason).toBeNull();
    const result = await repositories[0].find({
      filter: { [pk]: { _eq: id } },
      fields: ['credit'],
      limit: 1,
    });
    expect(result.data[0].credit).toBe(88);
    expect(mutations).toHaveLength(12);
    expect(reloads).toHaveLength(12);
  }, 30000);

  it('sets computed and literal fields together and respects the response projection', async () => {
    const result = (await repositories[0].updateLocked({
      id,
      fields: ['credit', 'status'],
      data: (current) => ({
        credit: Number(current.credit) - 10,
        status: 'active',
      }),
    })) as { data: Array<Record<string, unknown>> };
    expect(result.data[0]).toMatchObject({ credit: 90, status: 'active' });
    expect(result.data[0].updatedAt).toBeUndefined();
    expect(mutations).toHaveLength(1);
  });

  it('ignores unrelated request pagination and projection when loading callback state', async () => {
    contexts[0].$query = { page: 9, fields: 'status' };
    const result = await repositories[0].updateLocked({
      id,
      fields: ['credit'],
      data: (current) => {
        expect(current.credit).toBe(100);
        return { credit: Number(current.credit) - 10 };
      },
    });
    contexts[0].$query = {};
    expect(result).toMatchObject({ data: [{ credit: 90 }] });
    const persisted = await repositories[0].find({
      filter: { [pk]: { _eq: id } },
      fields: ['credit'],
      limit: 1,
    });
    expect(persisted.data[0].credit).toBe(90);
  });

  it('rolls back callback rejection and emits no effects', async () => {
    await expect(
      repositories[0].updateLocked({
        id,
        data: () => {
          throw new Error('Insufficient credit');
        },
      }),
    ).rejects.toThrow('Insufficient credit');
    const result = await repositories[0].find({
      filter: { [pk]: { _eq: id } },
      fields: ['credit'],
      limit: 1,
    });
    expect(result.data[0].credit).toBe(100);
    expect(mutations).toHaveLength(0);
    expect(reloads).toHaveLength(0);
  });

  it('rejects malformed patches and missing records without mutation effects', async () => {
    await expect(
      repositories[0].updateLocked({ id, data: () => null as never }),
    ).rejects.toThrow('record patch');
    await expect(
      repositories[0].updateLocked({ id, data: { credit: 0 } } as never),
    ).rejects.toThrow('data');
    await expect(repositories[0].updateLocked({ id } as never)).rejects.toThrow(
      'data',
    );
    await expect(
      repositories[0].updateLocked({
        id: isMongo ? new ObjectId().toHexString() : 999,
        data: () => ({ credit: 0 }),
      }),
    ).rejects.toThrow('record not found');
    expect(mutations).toHaveLength(0);
  });

  it('joins the outer unit of work and discards its update when the owner rolls back', async () => {
    await expect(
      runWithDeferredDynamicTransactionEffects(
        contexts[0],
        async () => {
          await repositories[0].updateLocked({
            id,
            data: (current) => ({ credit: Number(current.credit) - 10 }),
          });
          expect(mutations).toHaveLength(0);
          expect(reloads).toHaveLength(0);
          throw new Error('Outer rollback');
        },
        (attempt) => engines[0].runWithLockedRecord(table, id, attempt),
      ),
    ).rejects.toThrow('Outer rollback');
    const result = await repositories[0].find({
      filter: { [pk]: { _eq: id } },
      fields: ['credit'],
      limit: 1,
    });
    expect(result.data[0].credit).toBe(100);
    expect(mutations).toHaveLength(0);
    expect(reloads).toHaveLength(0);
  });

  it('aborts the write after cancellation inside the callback', async () => {
    const controller = new AbortController();
    await expect(
      runWithIoAbortSignal(controller.signal, () =>
        repositories[0].updateLocked({
          id,
          data: (current) => {
            controller.abort();
            return { credit: Number(current.credit) - 10 };
          },
        }),
      ),
    ).rejects.toThrow();
    const result = await repositories[0].find({
      filter: { [pk]: { _eq: id } },
      fields: ['credit'],
      limit: 1,
    });
    expect(result.data[0].credit).toBe(100);
    expect(mutations).toHaveLength(0);
  });

  it('executes the real repository callback through the isolated worker', async () => {
    const executor = new IsolatedExecutorService({
      packageCacheService: { getPackages: async () => [] },
      packageCdnLoaderService: { getPackageSources: () => [] },
    });
    try {
      const result = await executor.run(
        `return await $ctx.$repos.accounts.updateLocked({ id: ${JSON.stringify(id)}, fields: ['credit'], data: current => ({ credit: current.credit - 10 }) });`,
        {
          ...contexts[0],
          $body: {},
          $params: {},
          $share: {},
          $helpers: {},
          $cache: {},
          $repos: { accounts: repositories[0] },
        },
        10000,
      );
      expect(result.data[0].credit).toBe(90);
      expect(mutations).toHaveLength(1);
    } finally {
      executor.onDestroy();
    }
  }, 15000);

  if (selected === 'postgres') {
    const createStressContext = async (index: number) => {
      const engine = engines[index % 2] as KnexService;
      const factory = new DynamicContextFactory({
        userCacheService: {},
        envService: env,
        databaseConfigService: config,
        knexService: engine,
      } as unknown as ConstructorParameters<typeof DynamicContextFactory>[0]);
      const ctx = factory.createBase({ user: { id: 1, isRootAdmin: true } });
      const query = new QueryBuilderService({
        databaseConfigService: config,
        knexService: engine,
        lazyRef: {
          metadataCacheService: {
            isLoaded: () => true,
            getMetadata: () => metadata,
          },
        },
      });
      const makeRepo = (name: string) =>
        new DynamicRepository({
          context: ctx,
          tableName: name,
          queryBuilderService: query,
          eventEmitter: events,
          runtimeRegistryService: registry,
          runtimeMetadataSchemaRouterService: { handles: () => false },
          policyService: {
            checkMutationSafety: async () => ({ allowed: true }),
          },
          tableValidationService: { assertTableValid: async () => {} },
          guardValidationService: {},
          bcryptService: {},
        } as unknown as ConstructorParameters<typeof DynamicRepository>[0]);
      const repoRegistry = new RepoRegistryService({
        metadataCacheService: {
          getAllTablesMetadata: async () => metadata.tablesList,
        },
        dynamicRepositoryFactory: { create: (name: string) => makeRepo(name) },
        eventEmitter: new EventEmitter2(),
      } as unknown as ConstructorParameters<typeof RepoRegistryService>[0]);
      await repoRegistry.rebuildFromMetadata();
      ctx.$repos = repoRegistry.createReposProxy(ctx) as typeof ctx.$repos;
      const repo = ctx.$repos.accounts as DynamicRepository;
      return { index, ctx, repo };
    };
    it('keeps concurrent sandbox outer transactions on their owning SQL connections', async () => {
      const executor = new IsolatedExecutorService({
        packageCacheService: { getPackages: async () => [] },
        packageCdnLoaderService: { getPackageSources: () => [] },
      });
      const runner = new RuntimeScriptExecutorService({
        kernelExecutorEngineService: new ExecutorEngineService({
          isolatedExecutorService: executor,
        }),
      });
      const concurrency = Number(
        process.env.LOCKED_UPDATE_STRESS_CONCURRENCY || 16,
      );
      const trace: Array<{ task: number; phase: string; at: number }> = [];
      const started = Date.now();
      const jobs = await Promise.all(
        Array.from({ length: concurrency }, (_, index) =>
          createStressContext(index),
        ),
      );
      await Promise.all(
        jobs.map(({ repo }) =>
          repo.find({ filter: { id: { _eq: 1 } }, fields: ['id'], limit: 1 }),
        ),
      );
      const sampler = setInterval(() => {
        console.log(
          'native stress pools',
          engines.map((engine) => (engine as KnexService).getPoolStats()),
          'phases',
          trace.slice(-8),
        );
      }, 1000);
      try {
        const results = await Promise.allSettled(
          jobs.map(({ index, ctx }) =>
            runner
              .run(
                `
          $ctx.$logs('before transaction');
          return await $ctx.$transaction.run(async () => {
            $ctx.$logs('transaction entered');
            const result = await $ctx.$repos.accounts.updateLocked({ id: 1, fields: ['credit'], data: current => ({ credit: Number(current.credit) - 1 }) });
            $ctx.$logs('update returned');
            await $ctx.$helpers.$sleep(10);
            return result.data[0];
          });
        `,
                ctx,
                8000,
              )
              .then((result) => {
                trace.push({
                  task: index,
                  phase: 'committed',
                  at: Date.now() - started,
                });
                return result;
              }),
          ),
        );
        console.log('native stress outcomes', {
          concurrency,
          durationMs: Date.now() - started,
          fulfilled: results.filter((result) => result.status === 'fulfilled')
            .length,
          trace,
        });
        for (const result of results)
          expect(
            result.status === 'fulfilled' ? null : result.reason,
          ).toBeNull();
        const persisted = await repositories[0].find({
          filter: { id: { _eq: 1 } },
          fields: ['credit'],
          limit: 1,
        });
        expect(persisted.data[0].credit).toBe(100 - concurrency);
        expect(mutations).toHaveLength(concurrency);
      } finally {
        clearInterval(sampler);
        executor.onDestroy();
      }
    }, 15000);

    it('settles 256 competing guarded debits and rolls back ledger failures atomically', async () => {
      const executor = new IsolatedExecutorService({
        packageCacheService: { getPackages: async () => [] },
        packageCdnLoaderService: { getPackageSources: () => [] },
      });
      const runner = new RuntimeScriptExecutorService({
        kernelExecutorEngineService: new ExecutorEngineService({
          isolatedExecutorService: executor,
        }),
      });
      const outcomes: Array<{
        task: number;
        committed: boolean;
        error?: string;
      }> = [];
      const started = Date.now();
      try {
        for (let wave = 0; wave < 4; wave++) {
          const jobs = await Promise.all(
            Array.from({ length: 64 }, (_, offset) =>
              createStressContext(wave * 64 + offset),
            ),
          );
          await Promise.all(
            jobs.map(({ repo }) =>
              repo.find({
                filter: { id: { _eq: 1 } },
                fields: ['id'],
                limit: 1,
              }),
            ),
          );
          const settled = await Promise.allSettled(
            jobs.map(({ index, ctx }) =>
              runner.run(
                `
            return await $ctx.$transaction.run(async () => {
              const result = await $ctx.$repos.accounts.updateLocked({ id: 1, fields: ['credit', 'charged'], data: current => {
                if (Number(current.credit) < 1) throw new Error('INSUFFICIENT_CREDIT');
                return { credit: Number(current.credit) - 1, charged: Number(current.charged) + 1 };
              } });
              await $ctx.$repos.ledger.create({ data: { operation: ${index + 1}, amount: -1, creditAfter: result.data[0].credit }, fields: ['id'] });
              await $ctx.$helpers.$sleep(5);
              ${index % 5 === 0 ? "throw new Error('AFTER_LEDGER_ROLLBACK');" : 'return result.data[0];'}
            });
          `,
                ctx,
                10000,
              ),
            ),
          );
          settled.forEach((result, offset) =>
            outcomes.push({
              task: jobs[offset].index,
              committed: result.status === 'fulfilled',
              ...(result.status === 'rejected'
                ? { error: String(result.reason?.message || result.reason) }
                : {}),
            }),
          );
          console.log('guarded settlement wave', {
            wave,
            completed: settled.filter((result) => result.status === 'fulfilled')
              .length,
            pools: engines.map((engine) =>
              (engine as KnexService).getPoolStats(),
            ),
          });
        }
        const successes = outcomes.filter((outcome) => outcome.committed);
        const failures = outcomes.filter((outcome) => !outcome.committed);
        console.log('guarded settlement totals', {
          durationMs: Date.now() - started,
          committed: successes.length,
          rolledBack: failures.filter((outcome) =>
            outcome.error?.includes('AFTER_LEDGER_ROLLBACK'),
          ).length,
          insufficient: failures.filter((outcome) =>
            outcome.error?.includes('INSUFFICIENT_CREDIT'),
          ).length,
          unexpected: failures
            .filter(
              (outcome) =>
                !/AFTER_LEDGER_ROLLBACK|INSUFFICIENT_CREDIT/.test(
                  outcome.error || '',
                ),
            )
            .slice(0, 8),
        });
        for (const failure of failures)
          expect(failure.error).toMatch(
            /AFTER_LEDGER_ROLLBACK|INSUFFICIENT_CREDIT/,
          );
        expect(successes).toHaveLength(100);
        const db = (engines[0] as KnexService).getUnscopedWriteKnex();
        expect(await db(table).where({ id: 1 }).first()).toMatchObject({
          credit: 0,
          charged: 100,
        });
        const ledger = await db(ledgerTable).orderBy('id');
        expect(ledger).toHaveLength(100);
        expect(new Set(ledger.map((row) => Number(row.creditAfter))).size).toBe(
          100,
        );
        expect(ledger.reduce((sum, row) => sum + Number(row.amount), 0)).toBe(
          -100,
        );
        expect(new Set(ledger.map((row) => Number(row.operation)))).toEqual(
          new Set(successes.map((outcome) => outcome.task + 1)),
        );
        expect(mutations).toHaveLength(200);
      } finally {
        executor.onDestroy();
      }
    }, 60000);
  }

  if (isMongo) {
    it('rejects a real standalone instance before invoking the callback', async () => {
      const standaloneUri = process.env.LOCKED_UPDATE_STANDALONE_URI;
      if (!standaloneUri)
        throw new Error('LOCKED_UPDATE_STANDALONE_URI is required');
      const relations = new MongoRelationManagerService({
        runtimeRegistryService: registry,
      } as unknown as ConstructorParameters<
        typeof MongoRelationManagerService
      >[0]);
      const engine = new MongoService({
        envService: {
          get: (key: string) => (key === 'DB_URI' ? standaloneUri : undefined),
        },
        databaseConfigService: config,
        runtimeRegistryService: registry,
        mongoRelationManagerService: relations,
      } as unknown as ConstructorParameters<typeof MongoService>[0]);
      let called = false;
      try {
        await engine.init();
        expect(engine.supportsNativeMultiDocumentTransactions()).toBe(false);
        const query = new QueryBuilderService({
          databaseConfigService: config,
          mongoService: engine,
          lazyRef: {
            metadataCacheService: {
              isLoaded: () => true,
              getMetadata: () => metadata,
            },
          },
        });
        await expect(
          query.updateLocked(table, {
            id,
            data: () => {
              called = true;
              return { credit: 0 };
            },
          }),
        ).rejects.toThrow('native');
        expect(called).toBe(false);
      } finally {
        await engine.onDestroy();
      }
    }, 15000);

    it('reruns the callback with fresh state after a real write conflict', async () => {
      const entered = barrier();
      const release = barrier();
      const reads: number[] = [];
      const first = repositories[0]
        .updateLocked({
          id,
          fields: ['credit'],
          data: async (current) => {
            reads.push(Number(current.credit));
            if (reads.length === 1) {
              entered.resolve();
              await release.promise;
            }
            return { credit: Number(current.credit) - 20 };
          },
        })
        .then(
          (value) => ({ value, error: undefined }),
          (error: unknown) => ({ value: undefined, error }),
        );
      try {
        await entered.promise;
        await repositories[1].updateLocked({
          id,
          data: (current) => ({ credit: Number(current.credit) - 10 }),
        });
      } finally {
        release.resolve();
        const outcome = await first;
        if (outcome.error) throw outcome.error;
        expect(outcome.value).toMatchObject({ data: [{ credit: 70 }] });
      }
      expect(reads).toEqual([100, 90]);
      const result = await repositories[0].find({
        filter: { [pk]: { _eq: id } },
        fields: ['credit'],
        limit: 1,
      });
      expect(result.data[0].credit).toBe(70);
      expect(mutations).toHaveLength(2);
      expect(reloads).toHaveLength(2);
    }, 15000);
  }
});
