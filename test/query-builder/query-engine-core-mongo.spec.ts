/**
 * Mongo parity of query-engine-core.spec.ts.
 *
 * The SQL file exercises `buildWhereClause` directly with Knex/SQLite. On Mongo
 * the equivalent code path is `utils/mongo/render-filter.ts`, which is always
 * invoked through `MongoQueryExecutor.execute(...)` once the planner produced
 * a plan. So this suite fires the same operator matrix against MongoQueryExecutor
 * instead of against buildWhereClause directly.
 *
 * Skipped sections from the SQL file (not relevant here):
 *   - Cascade / CRUD / Transaction tests — those are Knex-specific; the Mongo
 *     equivalents live in test/mongo-saga/* and test/knex/*.
 *   - hasLogicalOperators — DB-agnostic util, covered in the SQL file already.
 */

import { MongoClient, Db, ObjectId } from 'mongodb';
import { MongoQueryExecutor } from 'src/infrastructure/query-builder/executors/mongo-query-executor';
import { QueryPlanner } from 'src/infrastructure/query-builder/planner/query-planner';

const MONGO_URI =
  process.env.MONGO_TEST_URI ||
  'mongodb://enfyra_admin:enfyra_password_123@localhost:27017/?authSource=admin';
const DB_NAME = `test_query_engine_core_mongo_${Date.now()}`;

function makeTableMeta(name: string, columnNames: string[]) {
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
    relations: [] as any[],
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

describe('query engine core (MongoQueryExecutor render-filter parity)', () => {
  let available = false;
  let client: MongoClient;
  let db: Db;
  let executor: MongoQueryExecutor;
  let meta: any;

  const userIds: ObjectId[] = [];

  beforeAll(async () => {
    available = await probeMongo();
    if (!available) return;

    client = new MongoClient(MONGO_URI);
    await client.connect();
    db = client.db(DB_NAME);

    for (let i = 0; i < 4; i++) userIds.push(new ObjectId());

    await db.collection('users').insertMany([
      {
        _id: userIds[0],
        name: 'Alice',
        email: 'alice@test.com',
        age: 30,
        isActive: true,
        role: 'admin',
      },
      {
        _id: userIds[1],
        name: 'Bob',
        email: 'bob@test.com',
        age: 25,
        isActive: true,
        role: 'user',
      },
      {
        _id: userIds[2],
        name: 'Charlie',
        email: 'charlie@test.com',
        age: 35,
        isActive: false,
        role: 'user',
      },
      {
        _id: userIds[3],
        name: 'Diana',
        email: 'diana@test.com',
        age: 28,
        isActive: true,
        role: null,
      },
    ]);

    const usersTable = makeTableMeta('users', [
      '_id',
      'name',
      'email',
      'age',
      'isActive',
      'role',
    ]);
    const m = new Map<string, any>();
    m.set('users', usersTable);
    meta = { tables: m };

    executor = new MongoQueryExecutor({
      getDb: () => db,
      collection: (name: string) => db.collection(name),
    } as any);
  });

  afterAll(async () => {
    if (!available) return;
    await db.dropDatabase();
    await client.close();
  });

  function runOrSkip(name: string, fn: () => Promise<void>) {
    test(name, async () => {
      if (!available) {
        console.warn('MongoDB not available, skipping');
        return;
      }
      await fn();
    });
  }

  async function runFilter(filter: any, extra: Record<string, any> = {}) {
    const planner = new QueryPlanner();
    const base = {
      tableName: 'users',
      filter,
      fields: ['_id', 'name', 'age', 'isActive', 'role'],
      metadata: meta,
      dbType: 'mongodb' as any,
      ...extra,
    };
    const plan = planner.plan(base);
    const r = await executor.execute({ ...base, plan });
    return r.data as any[];
  }

  async function names(filter: any, extra: Record<string, any> = {}) {
    const rows = await runFilter(filter, extra);
    return rows.map((r) => r.name).sort();
  }

  // -------- _eq --------
  describe('_eq operator', () => {
    runOrSkip('filters by exact match', async () => {
      expect(await names({ name: { _eq: 'Alice' } })).toEqual(['Alice']);
    });

    runOrSkip('returns empty for non-existent value', async () => {
      expect(await names({ name: { _eq: 'NonExistent' } })).toEqual([]);
    });
  });

  // -------- _neq --------
  describe('_neq operator', () => {
    runOrSkip('excludes matching records', async () => {
      expect(await names({ name: { _neq: 'Alice' } })).toEqual([
        'Bob',
        'Charlie',
        'Diana',
      ]);
    });
  });

  // -------- comparison --------
  describe('comparison operators', () => {
    runOrSkip('_gt filters greater than', async () => {
      expect(await names({ age: { _gt: 28 } })).toEqual(['Alice', 'Charlie']);
    });

    runOrSkip('_gte filters greater than or equal', async () => {
      expect(await names({ age: { _gte: 30 } })).toEqual(['Alice', 'Charlie']);
    });

    runOrSkip('_lt filters less than', async () => {
      expect(await names({ age: { _lt: 30 } })).toEqual(['Bob', 'Diana']);
    });

    runOrSkip('_lte filters less than or equal', async () => {
      expect(await names({ age: { _lte: 28 } })).toEqual(['Bob', 'Diana']);
    });
  });

  // -------- _in / _nin --------
  describe('_in / _nin operators', () => {
    runOrSkip('_in matches multiple values', async () => {
      expect(await names({ age: { _in: [25, 30] } })).toEqual(['Alice', 'Bob']);
    });

    runOrSkip('_nin excludes multiple values', async () => {
      expect(await names({ age: { _nin: [25, 30] } })).toEqual([
        'Charlie',
        'Diana',
      ]);
    });

    runOrSkip('_in with single value', async () => {
      expect(await names({ age: { _in: [30] } })).toEqual(['Alice']);
    });

    runOrSkip('_in with empty array matches nothing', async () => {
      expect(await names({ age: { _in: [] } })).toEqual([]);
    });
  });

  // -------- _between --------
  describe('_between', () => {
    runOrSkip('filters range inclusive', async () => {
      expect(await names({ age: { _between: [25, 30] } })).toEqual([
        'Alice',
        'Bob',
        'Diana',
      ]);
    });
  });

  // -------- null --------
  describe('_is_null / _is_not_null', () => {
    runOrSkip('_is_null true filters null values', async () => {
      expect(await names({ role: { _is_null: true } })).toEqual(['Diana']);
    });

    runOrSkip('_is_not_null true filters non-null values', async () => {
      expect(await names({ role: { _is_not_null: true } })).toEqual([
        'Alice',
        'Bob',
        'Charlie',
      ]);
    });
  });

  // -------- string operators --------
  describe('_contains / _starts_with / _ends_with', () => {
    runOrSkip('_contains finds substring', async () => {
      expect(await names({ email: { _contains: 'alice' } })).toEqual(['Alice']);
    });

    runOrSkip('_starts_with finds prefix', async () => {
      expect(await names({ name: { _starts_with: 'Al' } })).toEqual(['Alice']);
    });

    runOrSkip('_ends_with finds suffix', async () => {
      expect(await names({ name: { _ends_with: 'lie' } })).toEqual(['Charlie']);
    });

    runOrSkip('_contains is case-insensitive', async () => {
      expect(await names({ email: { _contains: 'ALICE' } })).toEqual(['Alice']);
    });
  });

  // -------- logical --------
  describe('logical operators', () => {
    runOrSkip('_and combines conditions', async () => {
      const rows = await names({
        _and: [{ age: { _gte: 25 } }, { isActive: { _eq: true } }],
      });
      expect(rows).toEqual(['Alice', 'Bob', 'Diana']);
    });

    runOrSkip('_or matches either condition', async () => {
      const rows = await names({
        _or: [{ name: { _eq: 'Alice' } }, { name: { _eq: 'Bob' } }],
      });
      expect(rows).toEqual(['Alice', 'Bob']);
    });

    runOrSkip('_not excludes matching records', async () => {
      expect(await names({ _not: { name: { _eq: 'Alice' } } })).toEqual([
        'Bob',
        'Charlie',
        'Diana',
      ]);
    });

    runOrSkip('nested _and inside _or', async () => {
      const rows = await names({
        _or: [
          {
            _and: [{ age: { _gte: 30 } }, { isActive: { _eq: true } }],
          },
          { name: { _eq: 'Bob' } },
        ],
      });
      expect(rows).toEqual(['Alice', 'Bob']);
    });

    runOrSkip('complex nested logical operators', async () => {
      const rows = await names({
        _and: [
          {
            _or: [{ role: { _eq: 'admin' } }, { age: { _gt: 30 } }],
          },
          { isActive: { _eq: true } },
        ],
      });
      // Alice (admin & active), Charlie (age>30 & !active → excluded)
      expect(rows).toEqual(['Alice']);
    });

    runOrSkip('triple nested _not', async () => {
      const rows = await names({
        _not: { _not: { _not: { name: { _eq: 'Alice' } } } },
      });
      expect(rows).toEqual(['Bob', 'Charlie', 'Diana']);
    });
  });

  // -------- implicit AND --------
  describe('implicit AND', () => {
    runOrSkip('multiple fields are AND-ed', async () => {
      const rows = await names({
        age: { _gte: 25 },
        isActive: { _eq: true },
      });
      expect(rows).toEqual(['Alice', 'Bob', 'Diana']);
    });
  });

  // -------- pagination --------
  describe('pagination', () => {
    runOrSkip('limit restricts results', async () => {
      const rows = await runFilter({}, { sort: 'age', limit: 2 });
      expect(rows.length).toBe(2);
    });

    runOrSkip('page calculation: (page-1)*limit = offset', async () => {
      const all = await runFilter({}, { sort: 'age' });
      const allNames = all.map((r) => r.name);
      const p1 = await runFilter({}, { sort: 'age', limit: 2, page: 1 });
      const p2 = await runFilter({}, { sort: 'age', limit: 2, page: 2 });
      expect(p1.map((r) => r.name)).toEqual(allNames.slice(0, 2));
      expect(p2.map((r) => r.name)).toEqual(allNames.slice(2, 4));
    });
  });

  // -------- sort --------
  describe('sort', () => {
    runOrSkip('sort asc by age', async () => {
      const rows = await runFilter({}, { sort: 'age' });
      const ages = rows.map((r) => r.age);
      expect(ages).toEqual([...ages].sort((a, b) => a - b));
    });

    runOrSkip('sort desc by age', async () => {
      const rows = await runFilter({}, { sort: '-age' });
      const ages = rows.map((r) => r.age);
      expect(ages).toEqual([...ages].sort((a, b) => b - a));
    });

    runOrSkip('multi-column sort', async () => {
      const rows = await runFilter({}, { sort: ['role', 'age'] as any });
      expect(rows.length).toBe(4);
    });
  });
});
