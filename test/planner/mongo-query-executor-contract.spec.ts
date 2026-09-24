import { MongoQueryExecutor, QueryPlanner } from '@enfyra/kernel';

function queryPlan(overrides: Record<string, unknown> = {}) {
  return {
    rawFields: ['*'],
    joins: [],
    hasRelationFilters: false,
    hasRelationSort: false,
    sortItems: [],
    ...overrides,
  } as any;
}

function metadata() {
  return {
    tables: new Map([
      ['items', { columns: [], relations: [] }],
    ]),
  };
}

function createMongoRuntime() {
  const pipelines: any[][] = [];
  let toArrayCalls = 0;
  let explainCalls = 0;
  const collection = {
    aggregate(pipeline: any[]) {
      pipelines.push(pipeline);
      return {
        async toArray() {
          toArrayCalls += 1;
          return [];
        },
        async explain() {
          explainCalls += 1;
          return { ok: 1 };
        },
      };
    },
    async countDocuments() {
      return 0;
    },
  };
  const db = {
    collection() {
      return collection;
    },
  };
  const mongoService = {
    getDb() {
      return db;
    },
    collection() {
      return collection;
    },
  };
  return {
    mongoService,
    pipelines,
    get toArrayCalls() { return toArrayCalls; },
    get explainCalls() { return explainCalls; },
  };
}

describe('Mongo query executor contracts', () => {
  it('explains a custom debug pipeline without executing it first', async () => {
    const runtime = createMongoRuntime();
    const executor = new MongoQueryExecutor(runtime.mongoService);
    const pipeline = [{ $match: { active: true } }];

    const result = await executor.execute({
      tableName: 'items',
      pipeline,
      debugMode: true,
      metadata: metadata(),
      plan: queryPlan(),
    });

    expect(result.pipeline).toEqual(pipeline);
    expect(runtime.toArrayCalls).toBe(0);
    expect(runtime.explainCalls).toBe(1);
  });

  it('rejects malformed pagination before building a Mongo pipeline', async () => {
    const runtime = createMongoRuntime();
    const executor = new MongoQueryExecutor(runtime.mongoService);

    await expect(executor.execute({
      tableName: 'items',
      page: 'abc' as any,
      limit: 10,
      metadata: metadata(),
      plan: queryPlan(),
    })).rejects.toThrow('page must be a positive integer');

    expect(runtime.pipelines).toHaveLength(0);
  });

  it('preserves a real id column through planning and Mongo sort rendering', async () => {
    const runtime = createMongoRuntime();
    const executor = new MongoQueryExecutor(runtime.mongoService);

    await executor.execute({
      tableName: 'items',
      fields: ['id'],
      sort: 'id',
      metadata: {
        tables: new Map([
          ['items', {
            columns: [{ name: '_id', isPrimary: true }, { name: 'id' }],
            relations: [],
          }],
        ]),
      },
    });

    expect(runtime.pipelines[0]).toContainEqual({ $sort: { id: 1 } });
  });

  it('returns all zero-limit rows alongside unpaginated counts', async () => {
    const countDocuments = vi.fn().mockResolvedValueOnce(7).mockResolvedValueOnce(3);
    const aggregate = vi.fn(() => ({ toArray: async () => [{ _id: 'one' }, { _id: 'two' }] }));
    const collection = { countDocuments, aggregate };
    const executor = new MongoQueryExecutor({
      getDb: () => ({ collection: () => collection }),
      collection: () => collection,
    });

    const result = await executor.execute({
      tableName: 'items',
      limit: 0,
      meta: '*',
      metadata: metadata(),
      plan: queryPlan(),
    });

    expect(result).toEqual({ data: [{ _id: 'one' }, { _id: 'two' }], meta: { totalCount: 7, filterCount: 3 } });
    expect(countDocuments).toHaveBeenCalledTimes(2);
    expect(aggregate).toHaveBeenCalledOnce();
    expect(aggregate.mock.calls[0][0]).not.toEqual(expect.arrayContaining([expect.objectContaining({ $limit: expect.anything() })]));
  });

  it('counts relation-filter matches alongside an unbounded data page', async () => {
    const pipelines: any[][] = [];
    const ownerFind = vi.fn(() => ({ toArray: async () => [{ _id: 'u' }] }));
    const root = {
      countDocuments: vi.fn().mockResolvedValue(9),
      aggregate: vi.fn((pipeline: any[]) => {
        pipelines.push(pipeline);
        return { toArray: async () => pipeline.some(stage => stage.$count) ? [{ count: 2 }] : [{ _id: 'item' }] };
      }),
    };
    const db = { collection: (name: string) => name === 'users' ? { find: ownerFind } : root };
    const executor = new MongoQueryExecutor({ getDb: () => db, collection: () => root });
    const result = await executor.execute({
      tableName: 'items', fields: ['_id'], limit: 0, meta: '*',
      filter: { author: { name: { _eq: 'Ada' } } },
      metadata: { tables: new Map([
        ['items', { columns: [{ name: '_id', isPrimary: true }, { name: 'authorId' }], relations: [
          { propertyName: 'author', type: 'many-to-one', targetTableName: 'users', foreignKeyColumn: 'authorId' },
        ] }],
        ['users', { columns: [{ name: '_id', isPrimary: true }, { name: 'name' }], relations: [] }],
      ]) },
    });

    expect(result).toEqual({ data: [{ _id: 'item' }], meta: { totalCount: 9, filterCount: 2 } });
    expect(pipelines).toHaveLength(2);
    expect(pipelines[1]).toContainEqual({ $count: 'count' });
    expect(pipelines[0]).not.toEqual(expect.arrayContaining([expect.objectContaining({ $lookup: expect.anything() })]));
  });

  it('does not replay a custom pipeline when counting relation-filter matches', async () => {
    const custom = [{ $match: { title: 'custom' } }];
    const pipelines: any[][] = [];
    const root = {
      aggregate(pipeline: any[]) {
        pipelines.push(pipeline);
        return { toArray: async () => pipeline === custom ? [{ _id: 'p' }] : [{ count: 4 }] };
      },
    };
    const db = { collection: (table: string) => table === 'users'
      ? { find: () => ({ toArray: async () => [{ _id: 'u' }] }) }
      : root };
    const executor = new MongoQueryExecutor({ getDb: () => db, collection: () => root });
    const result = await executor.execute({
      tableName: 'items', fields: ['_id'], pipeline: custom, meta: 'filterCount',
      filter: { author: { name: { _eq: 'Ada' } } },
      metadata: { tables: new Map([
        ['items', { columns: [{ name: '_id', isPrimary: true }], relations: [{ propertyName: 'author', type: 'many-to-one', targetTableName: 'users', foreignKeyColumn: 'authorId' }] }],
        ['users', { columns: [{ name: '_id', isPrimary: true }, { name: 'name' }], relations: [] }],
      ]) },
    });
    expect(pipelines.filter((pipeline) => pipeline === custom)).toHaveLength(1);
    expect(pipelines[1]).toContainEqual({ $count: 'count' });
    expect(result.meta.filterCount).toBe(4);
  });

  it.each([0, 5])('validates deep before metadata counts and zero-limit handling (%s)', async (limit) => {
    const countDocuments = vi.fn();
    const aggregate = vi.fn();
    const collection = { countDocuments, aggregate };
    const executor = new MongoQueryExecutor({ getDb: () => ({ collection: () => collection }), collection: () => collection });
    await expect(executor.execute({
      tableName: 'items', limit, meta: 'totalCount', deep: { missing: { limit: -1 } }, metadata: metadata(), plan: queryPlan(),
    })).rejects.toThrow(/relation|limit/i);
    expect(countDocuments).not.toHaveBeenCalled();
    expect(aggregate).not.toHaveBeenCalled();
  });

  it('normalizes missing fields to null in field-count accumulators', async () => {
    const runtime = createMongoRuntime();
    await new MongoQueryExecutor(runtime.mongoService).aggregate({
      tableName: 'items',
      aggregate: { measures: { populated: { count: 'value' } } },
      metadata: { tables: new Map([['items', { columns: [{ name: 'value', type: 'integer' }], relations: [] }]]) },
    });
    const group = runtime.pipelines[0].find((stage) => stage.$group).$group;
    const accumulator = Object.entries(group).find(([key]) => key !== '_id')![1];
    expect(accumulator).toEqual({ $sum: { $cond: [{ $ne: [{ $ifNull: ['$value', null] }, null] }, 1, 0] } });
  });

  it('counts relation predicates from a supplied plan without requiring duplicate raw filter input', async () => {
    const metadata = { tables: new Map([
      ['items', { columns: [{ name: '_id', isPrimary: true }, { name: 'authorId' }], relations: [{ propertyName: 'author', type: 'many-to-one', targetTableName: 'users', foreignKeyColumn: 'authorId' }] }],
      ['users', { columns: [{ name: '_id', isPrimary: true }, { name: 'name' }], relations: [] }],
    ]) };
    const plan = new QueryPlanner().plan({ tableName: 'items', fields: ['_id'], filter: { author: { name: { _eq: 'Ada' } } }, metadata, dbType: 'mongodb' });
    const countDocuments = vi.fn();
    const pipelines: any[][] = [];
    const root = { countDocuments, aggregate(pipeline: any[]) { pipelines.push(pipeline); return { toArray: async () => [{ count: 2 }] }; } };
    const db = { collection: (table: string) => table === 'users' ? { find: () => ({ toArray: async () => [{ _id: 'u' }] }) } : root };
    const result = await new MongoQueryExecutor({ getDb: () => db, collection: () => root }).execute({ tableName: 'items', fields: ['_id'], limit: 0, meta: 'filterCount', metadata, plan });
    expect(result.meta.filterCount).toBe(2);
    expect(countDocuments).not.toHaveBeenCalled();
    expect(pipelines[0]).toContainEqual({ $match: { authorId: { $in: ['u'] } } });
  });

  it('executes zero limit without adding a limit stage', async () => {
    const runtime = createMongoRuntime();
    const executor = new MongoQueryExecutor(runtime.mongoService);

    const result = await executor.execute({
      tableName: 'items',
      limit: 0,
      metadata: metadata(),
      plan: queryPlan(),
    });

    expect(result.data).toEqual([]);
    expect(runtime.toArrayCalls).toBe(1);
    expect(runtime.pipelines[0]).not.toEqual(expect.arrayContaining([expect.objectContaining({ $limit: expect.anything() })]));
  });

  it('routes selected rows through mongoService.parseResult', async () => {
    const rows = [{ _id: 'a', secret_token: 'enc:v1:iv:tag:payload' }];
    const collection = {
      aggregate: vi.fn(() => ({
        toArray: async () => rows,
        explain: async () => ({ ok: 1 }),
      })),
      countDocuments: vi.fn(async () => 0),
    };
    const parseResult = vi.fn(async (input: any[]) =>
      input.map((row) => ({ ...row, secret_token: 'plaintext' })),
    );
    const executor = new MongoQueryExecutor({
      getDb: () => ({ collection: () => collection }),
      collection: () => collection,
      parseResult,
    });

    const result = await executor.execute({
      tableName: 'items',
      fields: ['_id'],
      metadata: metadata(),
      plan: queryPlan(),
    });

    expect(parseResult).toHaveBeenCalledWith(expect.any(Array), 'items');
    expect(result.data).toEqual([{ _id: 'a', secret_token: 'plaintext' }]);
  });

  it('skips result post-processing when the runtime exposes no parseResult', async () => {
    const rows = [{ _id: 'a' }];
    const collection = {
      aggregate: vi.fn(() => ({
        toArray: async () => rows,
        explain: async () => ({ ok: 1 }),
      })),
      countDocuments: vi.fn(async () => 0),
    };
    const executor = new MongoQueryExecutor({
      getDb: () => ({ collection: () => collection }),
      collection: () => collection,
    });

    const result = await executor.execute({
      tableName: 'items',
      fields: ['_id'],
      metadata: metadata(),
      plan: queryPlan(),
    });

    expect(result.data).toEqual([{ _id: 'a' }]);
  });
});
