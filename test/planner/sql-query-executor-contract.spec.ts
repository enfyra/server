import { SqlQueryExecutor } from '@enfyra/kernel';

type QueryState = {
  selects: unknown[];
  whereCalls: unknown[][];
  limit?: number;
  offset?: number;
  rows: Record<string, unknown>[];
};

function createQuery(state: QueryState) {
  const query: any = {
    select(...items: unknown[]) {
      state.selects.push(...items.flat());
      return query;
    },
    where(...args: unknown[]) {
      state.whereCalls.push(args);
      return query;
    },
    whereNot(...args: unknown[]) {
      state.whereCalls.push(['not', ...args]);
      return query;
    },
    whereIn(...args: unknown[]) {
      state.whereCalls.push(['in', ...args]);
      return query;
    },
    whereNotIn(...args: unknown[]) {
      state.whereCalls.push(['notIn', ...args]);
      return query;
    },
    whereNull(...args: unknown[]) {
      state.whereCalls.push(['null', ...args]);
      return query;
    },
    whereNotNull(...args: unknown[]) {
      state.whereCalls.push(['notNull', ...args]);
      return query;
    },
    whereBetween(...args: unknown[]) {
      state.whereCalls.push(['between', ...args]);
      return query;
    },
    whereRaw(...args: unknown[]) {
      state.whereCalls.push(['raw', ...args]);
      return query;
    },
    orderBy() { return query; },
    orderByRaw() { return query; },
    groupBy() { return query; },
    groupByRaw() { return query; },
    limit(value: number) {
      state.limit = value;
      return query;
    },
    offset(value: number) {
      state.offset = value;
      return query;
    },
    clone() { return query; },
    clearSelect() { return query; },
    clearOrder() { return query; },
    count() { return query; },
    first() { return Promise.resolve({ count: 0 }); },
    toSQL() {
      return { toNative: () => ({ sql: 'select', bindings: [] }) };
    },
    then(resolve: (rows: Record<string, unknown>[]) => unknown) {
      return Promise.resolve(state.rows.map((row) => ({ ...row }))).then(resolve);
    },
  };
  return query;
}

function createKnex(rows: Record<string, unknown>[] = []) {
  const states: QueryState[] = [];
  const knex: any = () => {
    const state: QueryState = { selects: [], whereCalls: [], rows };
    states.push(state);
    return createQuery(state);
  };
  knex.raw = (sql: string) => ({ sql });
  return { knex, states };
}

function metadata(columns: Array<{ name: string; type: string }> = []) {
  return {
    tables: new Map([
      ['items', { columns, relations: [] }],
    ]),
  };
}

function simplePlan(overrides: Record<string, unknown> = {}) {
  return {
    rawFields: ['id'],
    joins: [],
    hasRelationFilters: false,
    hasRelationSort: false,
    sortItems: [],
    ...overrides,
  } as any;
}

describe('SQL query executor contracts', () => {
  it('applies a filter tree supplied by the query plan without raw filter input', async () => {
    const { knex, states } = createKnex([]);
    const executor = new SqlQueryExecutor(knex, 'postgres');

    await executor.execute({
      tableName: 'items',
      metadata: metadata(),
      plan: simplePlan({
        rawFields: ['*'],
        filterTree: {
          kind: 'compare',
          field: {
            joinId: null,
            fieldName: 'id',
          },
          op: 'eq',
          value: 7,
        },
      }),
    });

    expect(states[0].whereCalls.length).toBeGreaterThan(0);
  });

  it('retains root columns when metadata counts are selected without fields', async () => {
    const { knex, states } = createKnex([
      { id: 1, __filter_count__: 1 },
    ]);
    const executor = new SqlQueryExecutor(knex, 'postgres');

    const result = await executor.execute({
      tableName: 'items',
      meta: 'filterCount',
      metadata: metadata(),
      plan: simplePlan({ rawFields: ['*'] }),
    });

    expect(states[0].selects).toContain('items.*');
    expect(result.data).toEqual([{ id: 1 }]);
  });

  it('treats zero limit as an unbounded read', async () => {
    const { knex, states } = createKnex([]);
    const executor = new SqlQueryExecutor(knex, 'postgres');

    await executor.execute({
      tableName: 'items',
      fields: 'id',
      limit: 0,
      metadata: metadata(),
      plan: simplePlan({ limit: 0 }),
    });

    expect(states[0].limit).toBeUndefined();
  });

  it('parses simple-json rows on the optimized path without knexService', async () => {
    const { knex } = createKnex([{ id: 1, payload: '{"safe":true}' }]);
    const executor = new SqlQueryExecutor(knex, 'postgres');

    const result = await executor.execute({
      tableName: 'items',
      fields: 'id,payload',
      metadata: metadata([{ name: 'payload', type: 'simple-json' }]),
      plan: simplePlan({ rawFields: ['id', 'payload'] }),
    });

    expect(result.data).toEqual([{ id: 1, payload: { safe: true } }]);
  });

  it('preserves unsafe aggregate numeric strings', async () => {
    const { knex } = createKnex([{ total: '9007199254740993' }]);
    const executor = new SqlQueryExecutor(knex, 'postgres');

    const result = await executor.aggregate({
      tableName: 'items',
      aggregate: {
        measures: { total: { sum: 'amount' } },
      } as any,
      metadata: {
        tables: new Map([
          ['items', {
            columns: [
              { name: 'amount', type: 'bigint' },
            ],
            relations: [],
          }],
        ]),
      },
    });

    expect(result.data[0].total).toBe('9007199254740993');
  });
});
