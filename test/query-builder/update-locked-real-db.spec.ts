import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import { EventEmitter2 } from 'eventemitter2';
import { ObjectId } from 'mongodb';
import { IsolatedExecutorService, QueryBuilderService, runWithIoAbortSignal } from '@enfyra/kernel';
import { KnexService } from '../../src/engines/knex/knex.service';
import { KnexHookManagerService } from '../../src/engines/knex/services/knex-hook-manager.service';
import { MongoService } from '../../src/engines/mongo/services/mongo.service';
import { MongoRelationManagerService } from '../../src/engines/mongo/services/mongo-relation-manager.service';
import { DynamicRepository } from '../../src/modules/dynamic-api/repositories/dynamic.repository';
import { CACHE_EVENTS, DATA_EVENTS } from '../../src/shared/utils/cache-events.constants';
import { runWithDeferredDynamicTransactionEffects } from '../../src/shared/utils/dynamic-transaction-effects.util';

function barrier() {
  let resolve!: () => void;
  const promise = new Promise<void>((done) => { resolve = done; });
  return { promise, resolve };
}

const selected = process.env.LOCKED_UPDATE_DATABASE;
if (!['postgres', 'mysql', 'mongodb'].includes(selected ?? '')) {
  throw new Error('LOCKED_UPDATE_DATABASE must select one required real database');
}

describe(`updateLocked real database (${selected})`, () => {
  const isMongo = selected === 'mongodb';
  const uri = process.env.LOCKED_UPDATE_DATABASE_URI;
  if (!uri) throw new Error('LOCKED_UPDATE_DATABASE_URI is required');
  const table = `credit_accounts_ul_${Date.now()}`;
  const id = isMongo ? new ObjectId().toHexString() : 1;
  const pk = isMongo ? '_id' : 'id';
  const tableMetadata = {
    name: table,
    columns: [
      { name: pk, type: isMongo ? 'objectId' : 'integer', isPrimary: true, isUpdatable: false, isPublished: true },
      { name: 'credit', type: 'integer', isUpdatable: true, isPublished: true },
      { name: 'status', type: 'varchar', isUpdatable: true, isPublished: true },
      { name: 'updatedAt', type: 'datetime', isUpdatable: false, isPublished: true },
    ],
    relations: [],
  };
  const metadata = { tables: new Map([[table, tableMetadata]]), tablesList: [tableMetadata] };
  const registry = {
    getMetadata: () => metadata,
    requireMetadata: () => metadata,
    getTableMetadata: (name: string) => metadata.tables.get(name),
    lookupTableByName: (name: string) => metadata.tables.get(name),
    getMaxQueryDepth: () => 5,
  };
  const config = { getDbType: () => selected, isMongoDb: () => isMongo, isPostgres: () => selected === 'postgres' };
  const env = { get: (key: string) => key === 'DB_URI' ? uri : undefined };
  const engines: Array<KnexService | MongoService> = [];
  const repositories: DynamicRepository[] = [];
  const contexts: Array<DynamicRepository['context']> = [];
  const events = new EventEmitter2();
  const mutations: unknown[] = [];
  const reloads: unknown[] = [];
  events.on(DATA_EVENTS.TABLE_MUTATION, (event) => { mutations.push(event); });
  events.on(CACHE_EVENTS.INVALIDATE, (event) => { reloads.push(event); });

  beforeAll(async () => {
    for (let index = 0; index < 2; index++) {
      const common = { envService: env, databaseConfigService: config, runtimeRegistryService: registry };
      let engine: KnexService | MongoService;
      if (isMongo) {
        const relations = new MongoRelationManagerService({ runtimeRegistryService: registry } as unknown as ConstructorParameters<typeof MongoRelationManagerService>[0]);
        engine = new MongoService({ ...common, mongoRelationManagerService: relations } as unknown as ConstructorParameters<typeof MongoService>[0]);
      } else {
        const hooks = new KnexHookManagerService({ runtimeRegistryService: registry } as unknown as ConstructorParameters<typeof KnexHookManagerService>[0]);
        engine = new KnexService({ ...common, knexHookManagerService: hooks, lazyRef: {
          runtimeRegistryService: registry,
          mySqlRuntimeWriteBarrierService: { runWithWriteLease: (work: () => Promise<unknown>) => work() },
        } } as unknown as ConstructorParameters<typeof KnexService>[0]);
      }
      engines.push(engine);
      await engine.init();
      if (!isMongo) Object.assign(engine, { columnTypesMap: new Map([[table, new Map(tableMetadata.columns.map((column) => [column.name, column.type]))]]) });
      const query = new QueryBuilderService({
        databaseConfigService: config,
        ...(isMongo ? { mongoService: engine } : { knexService: engine }),
        lazyRef: { metadataCacheService: { isLoaded: () => true, getMetadata: () => metadata } },
      });
      const context = { $query: {}, $user: { id: 1, isRootAdmin: true } } as unknown as DynamicRepository['context'];
      contexts.push(context);
      repositories.push(new DynamicRepository({
        context, tableName: table, queryBuilderService: query, eventEmitter: events,
        runtimeRegistryService: registry,
        runtimeMetadataSchemaRouterService: { handles: () => false },
        policyService: { checkMutationSafety: async () => ({ allowed: true }) },
        tableValidationService: { assertTableValid: async () => {} },
        guardValidationService: {}, bcryptService: {},
      } as unknown as ConstructorParameters<typeof DynamicRepository>[0]));
    }
    if (isMongo) {
      const engine = engines[0] as MongoService;
      expect(engine.supportsNativeMultiDocumentTransactions()).toBe(true);
      await engine.getRawDb().createCollection(table);
    } else {
      await (engines[0] as KnexService).getUnscopedWriteKnex().schema.createTable(table, (builder) => {
        builder.integer('id').primary();
        builder.integer('credit').notNullable();
        builder.string('status').notNullable();
        builder.timestamp('updatedAt').nullable();
      });
    }
  }, 30000);

  beforeEach(async () => {
    mutations.length = 0;
    reloads.length = 0;
    for (const context of contexts) context.$query = {};
    if (isMongo) {
      const collection = (engines[0] as MongoService).getRawDb().collection(table);
      await collection.deleteMany({});
      await collection.insertOne({ _id: new ObjectId(String(id)), credit: 100, status: 'pending' });
    } else {
      const db = (engines[0] as KnexService).getUnscopedWriteKnex();
      await db(table).delete();
      await db(table).insert({ id, credit: 100, status: 'pending' });
    }
  });

  afterAll(async () => {
    try {
      if (engines.length) {
        if (isMongo) await (engines[0] as MongoService).getRawDb().collection(table).drop();
        else await (engines[0] as KnexService).getUnscopedWriteKnex().schema.dropTableIfExists(table);
      }
    } finally {
      for (const engine of engines) await engine.onDestroy();
    }
  });

  it('preserves concurrent decrements across independent database clients', async () => {
    const results = await Promise.allSettled(Array.from({ length: 12 }, (_, index) =>
      repositories[index % 2].updateLocked({ id, data: (current) => ({ credit: Number(current.credit) - 1 }) }),
    ));
    for (const result of results) expect(result.status === 'fulfilled' ? null : result.reason).toBeNull();
    const result = await repositories[0].find({ filter: { [pk]: { _eq: id } }, fields: ['credit'], limit: 1 });
    expect(result.data[0].credit).toBe(88);
    expect(mutations).toHaveLength(12);
    expect(reloads).toHaveLength(12);
  }, 30000);

  it('sets computed and literal fields together and respects the response projection', async () => {
    const result = await repositories[0].updateLocked({ id, fields: ['credit', 'status'], data: (current) => ({ credit: Number(current.credit) - 10, status: 'active' }) }) as { data: Array<Record<string, unknown>> };
    expect(result.data[0]).toMatchObject({ credit: 90, status: 'active' });
    expect(result.data[0].updatedAt).toBeUndefined();
    expect(mutations).toHaveLength(1);
  });

  it('ignores unrelated request pagination and projection when loading callback state', async () => {
    contexts[0].$query = { page: 9, fields: 'status' };
    const result = await repositories[0].updateLocked({ id, fields: ['credit'], data: (current) => {
      expect(current.credit).toBe(100);
      return { credit: Number(current.credit) - 10 };
    } });
    contexts[0].$query = {};
    expect(result).toMatchObject({ data: [{ credit: 90 }] });
    const persisted = await repositories[0].find({ filter: { [pk]: { _eq: id } }, fields: ['credit'], limit: 1 });
    expect(persisted.data[0].credit).toBe(90);
  });

  it('rolls back callback rejection and emits no effects', async () => {
    await expect(repositories[0].updateLocked({ id, data: () => { throw new Error('Insufficient credit'); } })).rejects.toThrow('Insufficient credit');
    const result = await repositories[0].find({ filter: { [pk]: { _eq: id } }, fields: ['credit'], limit: 1 });
    expect(result.data[0].credit).toBe(100);
    expect(mutations).toHaveLength(0);
    expect(reloads).toHaveLength(0);
  });

  it('rejects malformed patches and missing records without mutation effects', async () => {
    await expect(repositories[0].updateLocked({ id, data: () => null as never })).rejects.toThrow('record patch');
    await expect(repositories[0].updateLocked({ id, data: { credit: 0 } } as never)).rejects.toThrow('data');
    await expect(repositories[0].updateLocked({ id } as never)).rejects.toThrow('data');
    await expect(repositories[0].updateLocked({ id: isMongo ? new ObjectId().toHexString() : 999, data: () => ({ credit: 0 }) })).rejects.toThrow('record not found');
    expect(mutations).toHaveLength(0);
  });

  it('joins the outer unit of work and discards its update when the owner rolls back', async () => {
    await expect(runWithDeferredDynamicTransactionEffects(contexts[0], async () => {
      await repositories[0].updateLocked({ id, data: (current) => ({ credit: Number(current.credit) - 10 }) });
      expect(mutations).toHaveLength(0);
      expect(reloads).toHaveLength(0);
      throw new Error('Outer rollback');
    }, (attempt) => engines[0].runWithLockedRecord(table, id, attempt))).rejects.toThrow('Outer rollback');
    const result = await repositories[0].find({ filter: { [pk]: { _eq: id } }, fields: ['credit'], limit: 1 });
    expect(result.data[0].credit).toBe(100);
    expect(mutations).toHaveLength(0);
    expect(reloads).toHaveLength(0);
  });

  it('aborts the write after cancellation inside the callback', async () => {
    const controller = new AbortController();
    await expect(runWithIoAbortSignal(controller.signal, () => repositories[0].updateLocked({ id, data: (current) => {
      controller.abort();
      return { credit: Number(current.credit) - 10 };
    } }))).rejects.toThrow();
    const result = await repositories[0].find({ filter: { [pk]: { _eq: id } }, fields: ['credit'], limit: 1 });
    expect(result.data[0].credit).toBe(100);
    expect(mutations).toHaveLength(0);
  });

  it('executes the real repository callback through the isolated worker', async () => {
    const executor = new IsolatedExecutorService({ packageCacheService: { getPackages: async () => [] }, packageCdnLoaderService: { getPackageSources: () => [] } });
    try {
      const result = await executor.run(`return await $ctx.$repos.accounts.updateLocked({ id: ${JSON.stringify(id)}, fields: ['credit'], data: current => ({ credit: current.credit - 10 }) });`, {
        ...contexts[0], $body: {}, $params: {}, $share: {}, $helpers: {}, $cache: {},
        $repos: { accounts: repositories[0] },
      }, 10000);
      expect(result.data[0].credit).toBe(90);
      expect(mutations).toHaveLength(1);
    } finally {
      executor.onDestroy();
    }
  }, 15000);

  if (isMongo) {
    it('rejects a real standalone instance before invoking the callback', async () => {
      const standaloneUri = process.env.LOCKED_UPDATE_STANDALONE_URI;
      if (!standaloneUri) throw new Error('LOCKED_UPDATE_STANDALONE_URI is required');
      const relations = new MongoRelationManagerService({ runtimeRegistryService: registry } as unknown as ConstructorParameters<typeof MongoRelationManagerService>[0]);
      const engine = new MongoService({
        envService: { get: (key: string) => key === 'DB_URI' ? standaloneUri : undefined },
        databaseConfigService: config,
        runtimeRegistryService: registry,
        mongoRelationManagerService: relations,
      } as unknown as ConstructorParameters<typeof MongoService>[0]);
      let called = false;
      try {
        await engine.init();
        expect(engine.supportsNativeMultiDocumentTransactions()).toBe(false);
        const query = new QueryBuilderService({
          databaseConfigService: config, mongoService: engine,
          lazyRef: { metadataCacheService: { isLoaded: () => true, getMetadata: () => metadata } },
        });
        await expect(query.updateLocked(table, { id, data: () => {
          called = true;
          return { credit: 0 };
        } })).rejects.toThrow('native');
        expect(called).toBe(false);
      } finally {
        await engine.onDestroy();
      }
    }, 15000);

    it('reruns the callback with fresh state after a real write conflict', async () => {
      const entered = barrier();
      const release = barrier();
      const reads: number[] = [];
      const first = repositories[0].updateLocked({ id, fields: ['credit'], data: async (current) => {
        reads.push(Number(current.credit));
        if (reads.length === 1) { entered.resolve(); await release.promise; }
        return { credit: Number(current.credit) - 20 };
      } }).then((value) => ({ value, error: undefined }), (error: unknown) => ({ value: undefined, error }));
      try {
        await entered.promise;
        await repositories[1].updateLocked({ id, data: (current) => ({ credit: Number(current.credit) - 10 }) });
      } finally {
        release.resolve();
        const outcome = await first;
        if (outcome.error) throw outcome.error;
        expect(outcome.value).toMatchObject({ data: [{ credit: 70 }] });
      }
      expect(reads).toEqual([100, 90]);
      const result = await repositories[0].find({ filter: { [pk]: { _eq: id } }, fields: ['credit'], limit: 1 });
      expect(result.data[0].credit).toBe(70);
      expect(mutations).toHaveLength(2);
      expect(reloads).toHaveLength(2);
    }, 15000);
  }
});
