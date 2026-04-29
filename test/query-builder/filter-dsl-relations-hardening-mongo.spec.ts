/**
 * Mongo parity of filter-dsl-relations-hardening.spec.ts.
 *
 * The SQL file mixes:
 *   1. DB-agnostic tests for `separateFilters` / `hasLogicalOperators` (not duplicated here)
 *   2. SqlQueryExecutor integration against SQLite (duplicated here against MongoQueryExecutor).
 *
 * This file focuses on (2): relation + logical operator coverage on the Mongo
 * executor to confirm SQL/Mongo behavioral parity for the filter DSL.
 */

import { MongoClient, Db, ObjectId } from 'mongodb';
import {
  MongoQueryExecutor,
  QueryBuilderService,
  QueryPlanner,
} from '../../src/kernel/query';

const MONGO_URI =
  process.env.MONGO_TEST_URI ||
  'mongodb://enfyra_admin:enfyra_password_123@localhost:27017/?authSource=admin';
const DB_NAME = `test_filter_dsl_hardening_mongo_${Date.now()}`;

type RelStub = {
  propertyName: string;
  type: 'one-to-one' | 'many-to-one' | 'one-to-many' | 'many-to-many';
  targetTable: string;
  targetTableName: string;
  foreignKeyColumn?: string;
  mappedBy?: string;
  isInverse?: boolean;
};

function makeTableMeta(
  name: string,
  columnNames: string[],
  relations: RelStub[],
) {
  return {
    id: 1,
    name,
    isSystem: false,
    columns: columnNames.map((n, i) => ({
      id: i + 1,
      name: n,
      type: 'mixed',
      isPrimary: n === '_id',
      isGenerated: n === '_id',
      isNullable: true,
      isSystem: false,
      isUpdatable: true,
      tableId: 1,
    })),
    relations: relations as any[],
  };
}

async function probeMongo(): Promise<boolean> {
  try {
    const c = new MongoClient(MONGO_URI, { serverSelectionTimeoutMS: 2000 });
    await c.connect();
    await c.close();
    return true;
  } catch {
    return false;
  }
}

describe('filter DSL relations hardening (MongoQueryExecutor parity)', () => {
  let available = false;
  let client: MongoClient;
  let db: Db;
  let executor: MongoQueryExecutor;
  let queryBuilder: QueryBuilderService;
  let meta: any;

  const menuIds: ObjectId[] = [];
  const userIds: ObjectId[] = [];
  const extIds: ObjectId[] = [];

  const warnSpy = jest.spyOn(console, 'warn').mockImplementation(() => {});

  beforeAll(async () => {
    available = await probeMongo();
    if (!available) return;

    client = new MongoClient(MONGO_URI);
    await client.connect();
    db = client.db(DB_NAME);

    // Mirror the extension layout from the SQL suite: 6 rows with FK combinations.
    for (let i = 0; i < 3; i++) menuIds.push(new ObjectId()); // [m1, m88, m99]
    for (let i = 0; i < 3; i++) userIds.push(new ObjectId()); // [u1, u2, u3]
    for (let i = 0; i < 6; i++) extIds.push(new ObjectId());

    await db.collection('menu').insertMany([
      { _id: menuIds[0], label: 'm1' },
      { _id: menuIds[1], label: 'm88' },
      { _id: menuIds[2], label: 'm99' },
    ]);
    await db.collection('user').insertMany([
      { _id: userIds[0], name: 'alice' },
      { _id: userIds[1], name: 'bob' },
      { _id: userIds[2], name: 'carol' },
    ]);
    await db.collection('extension').insertMany([
      {
        _id: extIds[0],
        title: 'alpha',
        prio: 10,
        menu: menuIds[1],
        owner: userIds[0],
      },
      {
        _id: extIds[1],
        title: 'beta',
        prio: 20,
        menu: menuIds[1],
        owner: userIds[1],
      },
      {
        _id: extIds[2],
        title: 'gamma_chunk',
        prio: 5,
        menu: menuIds[2],
        owner: userIds[0],
      },
      { _id: extIds[3], title: 'delta', prio: 0, menu: null, owner: null },
      {
        _id: extIds[4],
        title: 'unicode_你好',
        prio: 7,
        menu: menuIds[2],
        owner: userIds[2],
      },
      {
        _id: extIds[5],
        title: 'Résumé',
        prio: 8,
        menu: menuIds[1],
        owner: userIds[1],
      },
    ]);

    const menuTable = makeTableMeta('menu', ['_id', 'label'], []);
    const userTable = makeTableMeta('user', ['_id', 'name'], []);
    const extTable = makeTableMeta(
      'extension',
      ['_id', 'title', 'prio', 'menu', 'owner'],
      [
        {
          propertyName: 'menu',
          type: 'many-to-one',
          targetTable: 'menu',
          targetTableName: 'menu',
          foreignKeyColumn: 'menu',
        },
        {
          propertyName: 'owner',
          type: 'many-to-one',
          targetTable: 'user',
          targetTableName: 'user',
          foreignKeyColumn: 'owner',
        },
      ],
    );

    const m = new Map<string, any>();
    m.set('extension', extTable);
    m.set('menu', menuTable);
    m.set('user', userTable);
    meta = { tables: m };

    executor = new MongoQueryExecutor({
      getDb: () => db,
      collection: (name: string) => db.collection(name),
    } as any);

    queryBuilder = new QueryBuilderService({
      mongoService: {
        getDb: () => db,
        collection: (name: string) => db.collection(name),
      },
      databaseConfigService: {
        getDbType: () => 'mongodb',
        isMongoDb: () => true,
      },
      lazyRef: {
        metadataCacheService: {
          isLoaded: () => true,
          getMetadata: async () => meta,
        },
      },
    } as any);
  });

  afterAll(async () => {
    warnSpy.mockRestore();
    if (!available) return;
    await db.dropDatabase();
    await client.close();
  });

  function idxs(indices: number[]): string[] {
    return indices.map((i) => String(extIds[i])).sort();
  }

  async function rowIds(filter: any, extra: Record<string, any> = {}) {
    const planner = new QueryPlanner();
    const base = {
      tableName: 'extension',
      filter,
      fields: ['_id'],
      sort: '_id',
      metadata: meta,
      dbType: 'mongodb' as any,
      ...extra,
    };
    const plan = planner.plan(base);
    const r = await executor.execute({ ...base, plan });
    return (r.data as any[]).map((x) => String(x._id)).sort();
  }

  async function queryBuilderRowIds(where: any) {
    const result = await queryBuilder.find({
      table: 'extension',
      where,
      fields: ['_id'],
      sort: '_id',
    });
    return (result.data as any[]).map((x) => String(x._id)).sort();
  }

  function runOrSkip(name: string, fn: () => Promise<void>) {
    test(name, async () => {
      if (!available) {
        console.warn('MongoDB not available, skipping');
        return;
      }
      await fn();
    });
  }

  // --- relation + logical operators ---

  runOrSkip('_and: menu eq m88 and id neq ext[0]', async () => {
    const ids = await rowIds({
      _and: [{ menu: { label: { _eq: 'm88' } } }, { _id: { _neq: extIds[0] } }],
    });
    // rows with menu m88: 0,1,5; minus 0 → 1, 5
    expect(ids).toEqual(idxs([1, 5]));
  });

  runOrSkip('_or: two relation shards', async () => {
    const ids = await rowIds({
      _or: [
        { menu: { label: { _eq: 'm88' } } },
        { owner: { name: { _eq: 'carol' } } },
      ],
    });
    expect(ids).toEqual(idxs([0, 1, 4, 5]));
  });

  runOrSkip('_not around field', async () => {
    const ids = await rowIds({ _not: { _id: { _eq: extIds[0] } } });
    expect(ids).toEqual(idxs([1, 2, 3, 4, 5]));
  });

  runOrSkip('flat AND: menu + id without _and wrapper', async () => {
    const ids = await rowIds({
      menu: { label: { _eq: 'm88' } },
      _id: { _eq: extIds[1] },
    });
    expect(ids).toEqual(idxs([1]));
  });

  runOrSkip('complex _and compiles without throw', async () => {
    await expect(
      rowIds({
        _and: [
          { menu: { label: { _eq: 'm88' } } },
          { _or: [{ prio: { _gte: 10 } }, { prio: { _lte: 1 } }] },
        ],
      }),
    ).resolves.toBeDefined();
  });

  runOrSkip('filterCount meta on simple predicate', async () => {
    const planner = new QueryPlanner();
    const base = {
      tableName: 'extension',
      filter: { prio: { _gt: 5 } },
      fields: ['_id'],
      meta: 'filterCount',
      metadata: meta,
      dbType: 'mongodb' as any,
    };
    const plan = planner.plan(base);
    const r = await executor.execute({ ...base, plan });
    expect(r.meta?.filterCount).toBeGreaterThan(0);
    expect(r.meta?.filterCount).toBe(4); // rows 0 (10), 1 (20), 4 (7), 5 (8)
  });

  // --- null FK semantics ---

  runOrSkip('null FK: menu _is_null → row with null FK', async () => {
    expect(await rowIds({ menu: { _is_null: true } })).toEqual(idxs([3]));
  });

  runOrSkip(
    'menu _in [m88,m99] returns all non-null-menu rows except ext with m1',
    async () => {
      const ids = await rowIds({ menu: { _in: [menuIds[1], menuIds[2]] } });
      expect(ids).toEqual(idxs([0, 1, 2, 4, 5]));
    },
  );

  runOrSkip('root id shorthand accepts string ObjectId', async () => {
    expect(await rowIds({ id: String(extIds[0]) })).toEqual(idxs([0]));
    expect(await rowIds({ _id: String(extIds[1]) })).toEqual(idxs([1]));
  });

  runOrSkip('relation shorthand accepts string ObjectId', async () => {
    expect(await rowIds({ menu: String(menuIds[1]) })).toEqual(idxs([0, 1, 5]));
  });

  runOrSkip('relation _eq accepts normalized string ObjectId', async () => {
    expect(await rowIds({ menu: { _eq: String(menuIds[2]) } })).toEqual(
      idxs([2, 4]),
    );
  });

  runOrSkip('QueryBuilder where accepts id operators and relation shorthand', async () => {
    expect(await queryBuilderRowIds({ id: { _eq: String(extIds[0]) } })).toEqual(
      idxs([0]),
    );
    expect(await queryBuilderRowIds({ _id: String(extIds[1]) })).toEqual(
      idxs([1]),
    );
    expect(await queryBuilderRowIds({ menu: String(menuIds[1]) })).toEqual(
      idxs([0, 1, 5]),
    );
    expect(
      await queryBuilderRowIds({ menu: { _eq: String(menuIds[2]) } }),
    ).toEqual(idxs([2, 4]));
  });

  runOrSkip(
    'menu._not_in [m88]: excludes NULL FK (Mongo parity with SQL NOT IN)',
    async () => {
      const ids = await rowIds({ menu: { _not_in: [menuIds[1]] } });
      // excludes [0,1,5] AND excludes [3] (null FK, same SQL NOT IN semantics)
      expect(ids).toEqual(idxs([2, 4]));
    },
  );

  runOrSkip('owner _is_null → row 3 only', async () => {
    expect(await rowIds({ owner: { _is_null: true } })).toEqual(idxs([3]));
  });

  runOrSkip('owner _is_not_null → all but row 3', async () => {
    expect(await rowIds({ owner: { _is_not_null: true } })).toEqual(
      idxs([0, 1, 2, 4, 5]),
    );
  });

  // --- dual relation ---

  runOrSkip('dual relation _and menu=m88 owner=bob → [1,5]', async () => {
    const ids = await rowIds({
      _and: [
        { menu: { label: { _eq: 'm88' } } },
        { owner: { name: { _eq: 'bob' } } },
      ],
    });
    expect(ids).toEqual(idxs([1, 5]));
  });

  runOrSkip('dual relation flat menu=m88 owner=alice → [0]', async () => {
    const ids = await rowIds({
      menu: { label: { _eq: 'm88' } },
      owner: { name: { _eq: 'alice' } },
    });
    expect(ids).toEqual(idxs([0]));
  });

  runOrSkip(
    '_or relation vs field: menu=m99 or title=delta → [2,3,4]',
    async () => {
      const ids = await rowIds({
        _or: [{ menu: { label: { _eq: 'm99' } } }, { title: { _eq: 'delta' } }],
      });
      expect(ids).toEqual(idxs([2, 3, 4]));
    },
  );

  // --- field operators ---

  runOrSkip('prio _between [5,10] → [0,2,4,5]', async () => {
    const ids = await rowIds({ prio: { _between: [5, 10] } });
    expect(ids).toEqual(idxs([0, 2, 4, 5]));
  });

  runOrSkip('title _contains chunk → [2]', async () => {
    expect(await rowIds({ title: { _contains: 'chunk' } })).toEqual(idxs([2]));
  });

  runOrSkip('title _starts_with beta → [1]', async () => {
    expect(await rowIds({ title: { _starts_with: 'beta' } })).toEqual(
      idxs([1]),
    );
  });

  runOrSkip('title _ends_with unicode suffix → [4]', async () => {
    expect(await rowIds({ title: { _ends_with: '你好' } })).toEqual(idxs([4]));
  });

  runOrSkip('title exact unicode', async () => {
    expect(await rowIds({ title: { _eq: 'unicode_你好' } })).toEqual(idxs([4]));
  });

  runOrSkip('menu nested filter by label on related table', async () => {
    expect(await rowIds({ menu: { label: { _eq: 'm99' } } })).toEqual(
      idxs([2, 4]),
    );
  });

  // --- _not combinations ---

  runOrSkip(
    '_not on single relation shorthand: not menu=m88 → rows not linked to m88',
    async () => {
      const ids = await rowIds({
        _not: { menu: { label: { _eq: 'm88' } } },
      });
      expect(ids).toEqual(idxs([2, 3, 4]));
    },
  );

  runOrSkip(
    '_not + _and field + relation: NOT (id=ext[1] ∧ menu=m88) → all but [1]',
    async () => {
      const ids = await rowIds({
        _not: {
          _and: [
            { _id: { _eq: extIds[1] } },
            { menu: { label: { _eq: 'm88' } } },
          ],
        },
      });
      expect(ids).toEqual(idxs([0, 2, 3, 4, 5]));
    },
  );

  runOrSkip(
    '_not + _or field only: NOT (id=[0] ∨ id=[1]) → [2,3,4,5]',
    async () => {
      const ids = await rowIds({
        _not: {
          _or: [{ _id: { _eq: extIds[0] } }, { _id: { _eq: extIds[1] } }],
        },
      });
      expect(ids).toEqual(idxs([2, 3, 4, 5]));
    },
  );

  runOrSkip(
    '_not + _or relations: NOT (menu=m88 ∨ menu=m99) → [3]',
    async () => {
      const ids = await rowIds({
        _not: {
          _or: [{ menu: { _eq: menuIds[1] } }, { menu: { _eq: menuIds[2] } }],
        },
      });
      expect(ids).toEqual(idxs([3]));
    },
  );

  // --- combined deep _or/_and ---

  runOrSkip('_or branch is _and (id ∧ menu): only ext[0]', async () => {
    const ids = await rowIds({
      _or: [
        {
          _and: [
            { _id: { _eq: extIds[0] } },
            { menu: { label: { _eq: 'm88' } } },
          ],
        },
      ],
    });
    expect(ids).toEqual(idxs([0]));
  });

  runOrSkip('_and wraps _or (id=[0] ∨ menu=m99) → [0,2,4]', async () => {
    const ids = await rowIds({
      _and: [
        {
          _or: [{ _id: { _eq: extIds[0] } }, { menu: { _eq: menuIds[2] } }],
        },
      ],
    });
    expect(ids).toEqual(idxs([0, 2, 4]));
  });

  runOrSkip('_and: (_or id=[1] ∨ menu=m99) ∧ owner=alice → [2]', async () => {
    const ids = await rowIds({
      _and: [
        {
          _or: [{ _id: { _eq: extIds[1] } }, { menu: { _eq: menuIds[2] } }],
        },
        { owner: { _eq: userIds[0] } },
      ],
    });
    expect(ids).toEqual(idxs([2]));
  });

  // --- pagination ---

  runOrSkip('pagination limit 2 page 1 → first 2 by sort', async () => {
    const planner = new QueryPlanner();
    const base = {
      tableName: 'extension',
      filter: {},
      fields: ['_id', 'prio'],
      sort: 'prio',
      limit: 2,
      page: 1,
      metadata: meta,
      dbType: 'mongodb' as any,
    };
    const plan = planner.plan(base);
    const r = await executor.execute({ ...base, plan });
    expect((r.data as any[]).length).toBe(2);
  });

  runOrSkip('pagination limit 2 page 3 → rows 5,6 of 6', async () => {
    const planner = new QueryPlanner();
    const base = {
      tableName: 'extension',
      filter: {},
      fields: ['_id'],
      sort: 'prio',
      limit: 2,
      page: 3,
      metadata: meta,
      dbType: 'mongodb' as any,
    };
    const plan = planner.plan(base);
    const r = await executor.execute({ ...base, plan });
    expect((r.data as any[]).length).toBe(2);
  });

  // --- stress ---

  runOrSkip('id._in huge array still works', async () => {
    const noise = Array.from({ length: 5000 }, () => new ObjectId());
    const ids = await rowIds({
      _id: { _in: [...noise, extIds[0], extIds[2]] },
    });
    expect(ids).toEqual(idxs([0, 2]));
  });

  runOrSkip('baseline: empty filter → all ids', async () => {
    expect(await rowIds({})).toEqual(idxs([0, 1, 2, 3, 4, 5]));
  });

  runOrSkip('contradiction: _and id=[0] and id=[1] → no rows', async () => {
    const ids = await rowIds({
      _and: [{ _id: { _eq: extIds[0] } }, { _id: { _eq: extIds[1] } }],
    });
    expect(ids).toEqual([]);
  });

  runOrSkip('contradiction: _and menu=m88 and menu=m99 → no rows', async () => {
    const ids = await rowIds({
      _and: [
        { menu: { label: { _eq: 'm88' } } },
        { menu: { label: { _eq: 'm99' } } },
      ],
    });
    expect(ids).toEqual([]);
  });

  runOrSkip('deep _and length 5 (avoid NOT IN on nullable FK)', async () => {
    const ids = await rowIds({
      _and: [
        { prio: { _gte: 0 } },
        { prio: { _lte: 30 } },
        { _or: [{ menu: { _eq: menuIds[1] } }, { menu: { _eq: menuIds[2] } }] },
        { _id: { _neq: extIds[0] } },
        { _id: { _neq: extIds[1] } },
      ],
    });
    expect(ids).toEqual(idxs([2, 4, 5]));
  });
});
