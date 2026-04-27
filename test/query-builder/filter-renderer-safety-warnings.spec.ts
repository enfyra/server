import knex, { Knex } from 'knex';
import {
  SqlQueryExecutor,
  MongoQueryExecutor,
  QueryPlanner,
} from 'src/kernel/query';
import { MongoClient, Db, ObjectId } from 'mongodb';

const MONGO_URI =
  process.env.MONGO_TEST_URI ||
  'mongodb://enfyra_admin:enfyra_password_123@localhost:27017/?authSource=admin';
const DB_NAME = `test_renderer_warn_${Date.now()}`;

function makeMeta(tableDef: any, others: Array<any> = []) {
  const m = new Map<string, any>();
  m.set(tableDef.name, tableDef);
  for (const o of others) m.set(o.name, o);
  return { tables: m };
}

function colSet(names: string[]) {
  return names.map((n, i) => ({
    id: i + 1,
    name: n,
    type: n === 'id' || n === 'ownerId' ? 'int' : 'varchar',
    isPrimary: n === 'id' || n === '_id',
    isGenerated: n === 'id',
    isNullable: n === 'ownerId',
    isSystem: false,
    isUpdatable: true,
    tableId: 1,
  }));
}

describe('Renderer safety warnings (regression tracers)', () => {
  let db: Knex;
  let sqlExec: SqlQueryExecutor;
  let sqlMeta: any;

  let mongoAvailable = false;
  let mongoClient: MongoClient;
  let mongoDb: Db;
  let mongoExec: MongoQueryExecutor;
  let mongoMeta: any;
  const userId = new ObjectId();
  const postId1 = new ObjectId();
  const postId2 = new ObjectId();

  const warns: string[] = [];
  const warnSpy = jest.spyOn(console, 'warn').mockImplementation((...args) => {
    warns.push(args.map(String).join(' '));
  });

  beforeAll(async () => {
    db = knex({
      client: 'sqlite3',
      connection: ':memory:',
      useNullAsDefault: true,
    });
    await db.schema.createTable('user', (t) => {
      t.increments('id').primary();
      t.string('name');
    });
    await db.schema.createTable('post', (t) => {
      t.increments('id').primary();
      t.string('title');
      t.integer('ownerId').nullable();
    });
    await db('user').insert([
      { id: 1, name: 'alice' },
      { id: 2, name: 'bob' },
    ]);
    await db('post').insert([
      { id: 1, title: 'a', ownerId: 1 },
      { id: 2, title: 'b', ownerId: 2 },
      { id: 3, title: 'c', ownerId: null },
    ]);

    const userMeta = {
      id: 1,
      name: 'user',
      isSystem: false,
      columns: colSet(['id', 'name']),
      relations: [],
    };
    const postMeta = {
      id: 2,
      name: 'post',
      isSystem: false,
      columns: colSet(['id', 'title', 'ownerId']),
      relations: [
        {
          propertyName: 'owner',
          type: 'many-to-one',
          targetTable: 'user',
          targetTableName: 'user',
          foreignKeyColumn: 'ownerId',
        },
      ],
    };
    sqlMeta = makeMeta(postMeta, [userMeta]);
    sqlExec = new SqlQueryExecutor(db, 'sqlite', undefined, 6);

    try {
      const probe = new MongoClient(MONGO_URI, {
        serverSelectionTimeoutMS: 2000,
      });
      await probe.connect();
      await probe.close();
      mongoAvailable = true;
    } catch {
      mongoAvailable = false;
    }

    if (mongoAvailable) {
      mongoClient = new MongoClient(MONGO_URI);
      await mongoClient.connect();
      mongoDb = mongoClient.db(DB_NAME);
      await mongoDb
        .collection('user')
        .insertOne({ _id: userId as any, name: 'alice' });
      await mongoDb.collection('post').insertMany([
        { _id: postId1 as any, title: 'a', owner: userId },
        { _id: postId2 as any, title: 'b', owner: null },
      ]);
      const userMetaMongo = {
        id: 1,
        name: 'user',
        isSystem: false,
        columns: colSet(['_id', 'name']),
        relations: [],
      };
      const postMetaMongo = {
        id: 2,
        name: 'post',
        isSystem: false,
        columns: colSet(['_id', 'title', 'owner']),
        relations: [
          {
            propertyName: 'owner',
            type: 'many-to-one',
            targetTable: 'user',
            targetTableName: 'user',
            foreignKeyColumn: 'owner',
          },
        ],
      };
      mongoMeta = makeMeta(postMetaMongo, [userMetaMongo]);
      mongoExec = new MongoQueryExecutor({
        getDb: () => mongoDb,
        collection: (n: string) => mongoDb.collection(n),
      });
    }
  });

  afterAll(async () => {
    warnSpy.mockRestore();
    await db.destroy();
    if (mongoAvailable) {
      await mongoDb.dropDatabase();
      await mongoClient.close();
    }
  });

  beforeEach(() => {
    warns.length = 0;
  });

  test('SQL: {owner: {name: {_contains: "alice"}}} — nested relation field filter', async () => {
    const r = await sqlExec.execute({
      tableName: 'post',
      filter: { owner: { name: { _contains: 'alice' } } },
      fields: ['id'],
      sort: 'id',
      metadata: sqlMeta,
    });
    const ids = (r.data as any[]).map((x) => x.id).sort();
    expect(ids).toEqual([1]);
    const rendererWarn = warns.filter((w) => w.includes('sql-render-filter'));
    expect(rendererWarn).toEqual([]);
  });

  test('SQL: {_and: [{owner: {name: {_eq: "alice"}}}]} — top-level _and wrapping relation', async () => {
    const r = await sqlExec.execute({
      tableName: 'post',
      filter: { _and: [{ owner: { name: { _eq: 'alice' } } }] },
      fields: ['id'],
      sort: 'id',
      metadata: sqlMeta,
    });
    const ids = (r.data as any[]).map((x) => x.id).sort();
    expect(ids).toEqual([1]);
    const rendererWarn = warns.filter((w) => w.includes('sql-render-filter'));
    expect(rendererWarn).toEqual([]);
  });

  test('Mongo: {_and: [{owner: {name: {_eq: "alice"}}}]} — top-level _and wrapping relation', async () => {
    if (!mongoAvailable) return;
    const planner = new QueryPlanner();
    const filter = { _and: [{ owner: { name: { _eq: 'alice' } } }] };
    const plan = planner.plan({
      tableName: 'post',
      filter,
      fields: ['_id'],
      metadata: mongoMeta,
      dbType: 'mongodb' as any,
    });
    const r = await mongoExec.execute({
      tableName: 'post',
      filter,
      fields: ['_id'],
      metadata: mongoMeta,
      dbType: 'mongodb',
      plan,
    });
    const ids = (r.data as any[]).map((x) => String(x._id)).sort();
    expect(ids).toEqual([String(postId1)]);
    const rendererWarn = warns.filter((w) => w.includes('mongo-render-filter'));
    expect(rendererWarn).toEqual([]);
  });

  test('Mongo: {owner: {name: {_contains: "alice"}}} — nested relation field filter', async () => {
    if (!mongoAvailable) return;
    const planner = new QueryPlanner();
    const filter = { owner: { name: { _contains: 'alice' } } };
    const plan = planner.plan({
      tableName: 'post',
      filter,
      fields: ['_id'],
      metadata: mongoMeta,
      dbType: 'mongodb' as any,
    });
    const r = await mongoExec.execute({
      tableName: 'post',
      filter,
      fields: ['_id'],
      metadata: mongoMeta,
      dbType: 'mongodb',
      plan,
    });
    const ids = (r.data as any[]).map((x) => String(x._id)).sort();
    expect(ids).toEqual([String(postId1)]);
    const rendererWarn = warns.filter((w) => w.includes('mongo-render-filter'));
    expect(rendererWarn).toEqual([]);
  });
});
