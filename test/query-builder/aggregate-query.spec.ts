import knex, { Knex } from 'knex';
import { MongoClient, Db, ObjectId } from 'mongodb';
import { describe, expect, test, beforeAll, afterAll } from 'vitest';
import {
  buildAggregateFilter,
  MongoQueryExecutor,
  normalizeAggregateQuery,
  QueryPlanner,
  SqlQueryExecutor,
} from '../../src/kernel/query';

const MONGO_URI =
  process.env.MONGO_TEST_URI ||
  'mongodb://enfyra_admin:enfyra_password_123@localhost:27017/?authSource=admin';
const DB_NAME = `test_aggregate_query_${Date.now()}`;

function makeSalesMeta(name = 'sales') {
  const columns = [
    ['id', 'int'],
    ['_id', 'mixed'],
    ['region', 'varchar'],
    ['status', 'varchar'],
    ['amount', 'int'],
    ['discount', 'int'],
    ['score', 'float'],
    ['channel', 'varchar'],
    ['ownerId', 'int'],
  ];
  return {
    id: 1,
    name,
    isSystem: false,
    columns: columns
      .filter(([column]) => name !== 'sales_mongo' || column !== 'id')
      .filter(([column]) => name === 'sales_mongo' || column !== '_id')
      .map(([column, type], index) => ({
        id: index + 1,
        name: column,
        type,
        isPrimary: column === 'id' || column === '_id',
        isGenerated: column === 'id' || column === '_id',
        isNullable: !['id', '_id', 'amount'].includes(column),
        isSystem: false,
        isUpdatable: true,
        tableId: 1,
      })),
    relations: [
      {
        propertyName: 'owner',
        type: 'many-to-one',
        targetTable: 'user',
        targetTableName: 'user',
        foreignKeyColumn: 'ownerId',
      },
    ] as any[],
  };
}

function makeAuthorPostMetadata(authorTable = 'authors', postTable = 'posts') {
  const author = {
    id: 10,
    name: authorTable,
    isSystem: false,
    columns: [
      {
        id: 1,
        name: authorTable === 'authors_mongo' ? '_id' : 'id',
        type: authorTable === 'authors_mongo' ? 'mixed' : 'int',
        isPrimary: true,
        isGenerated: true,
        isNullable: false,
        isSystem: false,
        isUpdatable: true,
        tableId: 10,
      },
      {
        id: 2,
        name: 'country',
        type: 'varchar',
        isPrimary: false,
        isGenerated: false,
        isNullable: true,
        isSystem: false,
        isUpdatable: true,
        tableId: 10,
      },
    ],
    relations: [
      {
        propertyName: 'posts',
        type: 'one-to-many',
        targetTable: postTable,
        targetTableName: postTable,
        foreignKeyColumn: 'authorId',
      },
      {
        propertyName: 'tags',
        type: 'many-to-many',
        targetTable: authorTable === 'authors_mongo' ? 'tags_mongo' : 'tags',
        targetTableName: authorTable === 'authors_mongo' ? 'tags_mongo' : 'tags',
        junctionTableName:
          authorTable === 'authors_mongo' ? 'authors_tags_tags_mongo' : 'authors_tags_tags',
        junctionSourceColumn: 'authorsId',
        junctionTargetColumn: authorTable === 'authors_mongo' ? 'tags_mongoId' : 'tagsId',
      },
    ],
  };
  const post = {
    id: 11,
    name: postTable,
    isSystem: false,
    columns: [
      {
        id: 1,
        name: postTable === 'posts_mongo' ? '_id' : 'id',
        type: postTable === 'posts_mongo' ? 'mixed' : 'int',
        isPrimary: true,
        isGenerated: true,
        isNullable: false,
        isSystem: false,
        isUpdatable: true,
        tableId: 11,
      },
      {
        id: 2,
        name: 'authorId',
        type: authorTable === 'authors_mongo' ? 'mixed' : 'int',
        isPrimary: false,
        isGenerated: false,
        isNullable: true,
        isSystem: false,
        isUpdatable: true,
        tableId: 11,
      },
      {
        id: 3,
        name: 'status',
        type: 'varchar',
        isPrimary: false,
        isGenerated: false,
        isNullable: true,
        isSystem: false,
        isUpdatable: true,
        tableId: 11,
      },
      {
        id: 4,
        name: 'views',
        type: 'int',
        isPrimary: false,
        isGenerated: false,
        isNullable: true,
        isSystem: false,
        isUpdatable: true,
        tableId: 11,
      },
    ],
    relations: [],
  };
  const tagTable = authorTable === 'authors_mongo' ? 'tags_mongo' : 'tags';
  const tag = {
    id: 12,
    name: tagTable,
    isSystem: false,
    columns: [
      {
        id: 1,
        name: tagTable === 'tags_mongo' ? '_id' : 'id',
        type: tagTable === 'tags_mongo' ? 'mixed' : 'int',
        isPrimary: true,
        isGenerated: true,
        isNullable: false,
        isSystem: false,
        isUpdatable: true,
        tableId: 12,
      },
      {
        id: 2,
        name: 'name',
        type: 'varchar',
        isPrimary: false,
        isGenerated: false,
        isNullable: true,
        isSystem: false,
        isUpdatable: true,
        tableId: 12,
      },
      {
        id: 3,
        name: 'kind',
        type: 'varchar',
        isPrimary: false,
        isGenerated: false,
        isNullable: true,
        isSystem: false,
        isUpdatable: true,
        tableId: 12,
      },
    ],
    relations: [],
  };
  return {
    tables: new Map([
      [authorTable, author],
      [postTable, post],
      [tagTable, tag],
    ]),
  };
}

function makeMetadata(table: any) {
  const user = {
    id: 2,
    name: 'user',
    isSystem: false,
    columns: [
      {
        id: 1,
        name: 'id',
        type: 'int',
        isPrimary: true,
        isGenerated: true,
        isNullable: false,
        isSystem: false,
        isUpdatable: true,
        tableId: 2,
      },
      {
        id: 2,
        name: 'name',
        type: 'varchar',
        isPrimary: false,
        isGenerated: false,
        isNullable: true,
        isSystem: false,
        isUpdatable: true,
        tableId: 2,
      },
    ],
    relations: [],
  };
  return { tables: new Map([[table.name, table], ['user', user]]) };
}

describe('aggregate query normalization', () => {
  const metadata = makeMetadata(makeSalesMeta());

  test('normalizes field-scoped operations without aliases', () => {
    const operations = normalizeAggregateQuery(
      {
        amount: {
          count: { _gt: 0 },
          sum: { _gte: 100 },
          avg: true,
          min: { _is_not_null: true },
          max: { _lt: 1000 },
        },
      },
      'sales',
      metadata,
    );

    expect(operations).toMatchObject([
      { field: 'amount', outputKey: 'amount', path: 'aggregate.amount', op: 'count' },
      { field: 'amount', outputKey: 'amount', path: 'aggregate.amount', op: 'sum' },
      { field: 'amount', outputKey: 'amount', path: 'aggregate.amount', op: 'avg' },
      { field: 'amount', outputKey: 'amount', path: 'aggregate.amount', op: 'min' },
      { field: 'amount', outputKey: 'amount', path: 'aggregate.amount', op: 'max' },
    ]);
  });

  test('normalizes countRecords for relation properties', () => {
    const metadata = makeAuthorPostMetadata();
    const operations = normalizeAggregateQuery(
      { posts: { countRecords: { status: { _eq: 'published' } } } },
      'authors',
      metadata,
    );

    expect(operations).toMatchObject([
      {
        field: 'posts',
        outputKey: 'posts',
        path: 'aggregate.posts',
        op: 'countRecords',
      },
    ]);
  });

  test('combines base filter and aggregate field condition with _and', () => {
    const [operation] = normalizeAggregateQuery(
      { amount: { sum: { _gte: 100 } } },
      'sales',
      metadata,
    );
    expect(
      buildAggregateFilter({ status: { _eq: 'paid' } }, operation),
    ).toEqual({
      _and: [{ status: { _eq: 'paid' } }, { amount: { _gte: 100 } }],
    });
  });

  test('uses true as no operation-specific condition', () => {
    const [operation] = normalizeAggregateQuery(
      { amount: { avg: true } },
      'sales',
      metadata,
    );

    expect(operation.condition).toEqual({});
    expect(buildAggregateFilter({ status: { _eq: 'paid' } }, operation)).toEqual({
      status: { _eq: 'paid' },
    });
  });

  test('ignores as without changing the aggregate output key', () => {
    const operations = normalizeAggregateQuery(
      { amount: { as: 'revenue', sum: true } as any },
      'sales',
      metadata,
    );

    expect(operations).toMatchObject([
      {
        field: 'amount',
        outputKey: 'amount',
        path: 'aggregate.amount',
        op: 'sum',
        condition: {},
      },
    ]);
  });

  test('rejects aggregate configs that only contain as', () => {
    expect(() =>
      normalizeAggregateQuery(
        { amount: { as: 'revenue' } as any },
        'sales',
        metadata,
      ),
    ).toThrow(/aggregate field must define at least one operation/);
  });

  test('rejects unsupported operations with aggregate.<fieldName> path', () => {
    expect(() =>
      normalizeAggregateQuery(
        { amount: { median: true } as any },
        'sales',
        metadata,
      ),
    ).toThrow(/Unsupported aggregate operation "median"/);
  });

  test('rejects sum on non-numeric fields', () => {
    expect(() =>
      normalizeAggregateQuery({ status: { sum: true } }, 'sales', metadata),
    ).toThrow(/requires a numeric field/);
  });

  test('rejects countRecords on scalar fields', () => {
    expect(() =>
      normalizeAggregateQuery({ amount: { countRecords: true } }, 'sales', metadata),
    ).toThrow(/requires a relation/);
  });

  test('rejects scalar aggregate operations on relations', () => {
    expect(() =>
      normalizeAggregateQuery(
        { posts: { count: true } },
        'authors',
        makeAuthorPostMetadata(),
      ),
    ).toThrow(/not supported on relation/);
  });

  test('rejects unknown fields inside relation countRecords filters', () => {
    expect(() =>
      normalizeAggregateQuery(
        { posts: { countRecords: { missingField: { _eq: 'published' } } } },
        'authors',
        makeAuthorPostMetadata(),
      ),
    ).toThrow(/Unknown aggregate\.countRecords field or relation "missingField"/);
  });

  test('rejects unsupported operators inside relation countRecords filters', () => {
    expect(() =>
      normalizeAggregateQuery(
        { posts: { countRecords: { status: { _regex: '^pub' } } } },
        'authors',
        makeAuthorPostMetadata(),
      ),
    ).toThrow(/only supports field operators/);
  });

  test('rejects operator-shaped relation countRecords filters at root', () => {
    expect(() =>
      normalizeAggregateQuery(
        { posts: { countRecords: { _eq: 'published' } as any } },
        'authors',
        makeAuthorPostMetadata(),
      ),
    ).toThrow(/Unsupported aggregate\.countRecords condition key "_eq"/);
  });

  test('accepts nested logical relation countRecords filters', () => {
    const operations = normalizeAggregateQuery(
      {
        posts: {
          countRecords: {
            _and: [
              { status: { _eq: 'published' } },
              { _or: [{ views: { _gte: 10 } }, { views: { _is_null: true } }] },
            ],
          },
        },
      },
      'authors',
      makeAuthorPostMetadata(),
    );

    expect(operations).toMatchObject([
      { field: 'posts', path: 'aggregate.posts', op: 'countRecords' },
    ]);
  });

  test('rejects non-object operation conditions', () => {
    expect(() =>
      normalizeAggregateQuery({ amount: { count: false as any } }, 'sales', metadata),
    ).toThrow(/condition must be true or an object/);
  });
});

describe('SqlQueryExecutor aggregate addition query', () => {
  let db: Knex;
  let executor: SqlQueryExecutor;
  let metadata: any;

  beforeAll(async () => {
    db = knex({
      client: 'sqlite3',
      connection: ':memory:',
      useNullAsDefault: true,
    });

    await db.schema.createTable('sales', (t) => {
      t.increments('id').primary();
      t.string('region');
      t.string('status');
      t.integer('amount');
      t.integer('discount');
      t.float('score');
      t.string('channel');
      t.integer('ownerId');
    });
    await db.schema.createTable('user', (t) => {
      t.increments('id').primary();
      t.string('name');
    });
    await db('user').insert([
      { id: 1, name: 'alice' },
      { id: 2, name: 'bob' },
      { id: 3, name: 'carol' },
    ]);
    await db('sales').insert([
      { id: 1, region: 'north', status: 'paid', amount: 100, discount: 10, score: 4.5, channel: 'web', ownerId: 1 },
      { id: 2, region: 'north', status: 'paid', amount: 200, discount: 0, score: 3.5, channel: 'app', ownerId: 1 },
      { id: 3, region: 'south', status: 'paid', amount: 300, discount: 30, score: 5, channel: 'web', ownerId: 2 },
      { id: 4, region: 'south', status: 'refunded', amount: 400, discount: 40, score: 2, channel: 'app', ownerId: 2 },
      { id: 5, region: 'east', status: 'draft', amount: 50, discount: null, score: null, channel: 'web', ownerId: null },
      { id: 6, region: 'north', status: 'paid', amount: 0, discount: 5, score: 4, channel: 'web', ownerId: 3 },
    ]);

    await db.schema.createTable('authors', (t) => {
      t.increments('id').primary();
      t.string('country');
    });
    await db.schema.createTable('posts', (t) => {
      t.increments('id').primary();
      t.integer('authorId');
      t.string('status');
      t.integer('views');
    });
    await db.schema.createTable('tags', (t) => {
      t.increments('id').primary();
      t.string('name');
      t.string('kind');
    });
    await db.schema.createTable('authors_tags_tags', (t) => {
      t.integer('authorsId');
      t.integer('tagsId');
    });
    await db('authors').insert([
      { id: 1, country: 'VN' },
      { id: 2, country: 'VN' },
      { id: 3, country: 'US' },
    ]);
    await db('posts').insert([
      { id: 1, authorId: 1, status: 'published', views: 10 },
      { id: 2, authorId: 1, status: 'draft', views: 5 },
      { id: 3, authorId: 2, status: 'published', views: 30 },
      { id: 4, authorId: 2, status: 'published', views: 3 },
      { id: 5, authorId: 3, status: 'published', views: 50 },
    ]);
    await db('tags').insert([
      { id: 1, name: 'tech', kind: 'topic' },
      { id: 2, name: 'draft-only', kind: 'topic' },
      { id: 3, name: 'vip', kind: 'audience' },
    ]);
    await db('authors_tags_tags').insert([
      { authorsId: 1, tagsId: 1 },
      { authorsId: 1, tagsId: 1 },
      { authorsId: 1, tagsId: 3 },
      { authorsId: 2, tagsId: 1 },
      { authorsId: 2, tagsId: 2 },
      { authorsId: 3, tagsId: 1 },
    ]);

    metadata = makeMetadata(makeSalesMeta());
    executor = new SqlQueryExecutor(db, 'sqlite');
  });

  afterAll(async () => {
    await db.destroy();
  });

  test('returns data and meta.aggregate together without changing pagination', async () => {
    const result = await executor.execute({
      tableName: 'sales',
      fields: ['id', 'amount'],
      filter: { status: { _eq: 'paid' } },
      sort: 'id',
      limit: 2,
      meta: 'filterCount',
      aggregate: {
        amount: {
          count: { _gt: 0 },
          sum: { _gt: 0 },
          avg: true,
          min: true,
          max: true,
        },
        discount: {
          sum: { _is_not_null: true },
          count: { _gt: 0 },
        },
      },
      metadata,
    });

    expect(result.data.map((row: any) => row.id)).toEqual([1, 2]);
    expect(result.meta.filterCount).toBe(4);
    expect(result.meta.aggregate).toEqual({
      amount: { count: 3, sum: 600, avg: 150, min: 0, max: 300 },
      discount: { sum: 45, count: 3 },
    });
  });

  test('uses base filter as default _and scope for every aggregate operation', async () => {
    const result = await executor.execute({
      tableName: 'sales',
      fields: ['id'],
      filter: { region: { _eq: 'north' } },
      aggregate: {
        amount: {
          count: { _gte: 100 },
          sum: { _gte: 100 },
        },
        score: {
          min: { _is_not_null: true },
          max: { _is_not_null: true },
        },
      },
      metadata,
    });

    expect(result.meta.aggregate).toEqual({
      amount: { count: 2, sum: 300 },
      score: { min: 3.5, max: 4.5 },
    });
  });

  test('blocks invalid aggregate with details path aggregate.<fieldName>', async () => {
    await expect(
      executor.execute({
        tableName: 'sales',
        fields: ['id'],
        aggregate: { status: { avg: true } },
        metadata,
      }),
    ).rejects.toMatchObject({
      details: {
        path: 'aggregate.status',
        aggregate: {
          status: expect.stringMatching(/numeric field/),
        },
      },
    });
  });

  test('respects relation filters from the base query scope', async () => {
    const result = await executor.execute({
      tableName: 'sales',
      fields: ['id'],
      filter: { owner: { name: { _eq: 'alice' } } },
      aggregate: {
        amount: {
          count: { _gt: 0 },
          sum: { _gt: 0 },
        },
      },
      metadata,
    });

    expect(result.data.map((row: any) => row.id).sort()).toEqual([1, 2]);
    expect(result.meta.aggregate).toEqual({
      amount: { count: 2, sum: 300 },
    });
  });

  test('blocks relation filters inside aggregate conditions', async () => {
    await expect(
      executor.execute({
        tableName: 'sales',
        fields: ['id'],
        aggregate: { amount: { sum: { _gt: 0, owner: { name: { _eq: 'alice' } } } } },
        metadata,
      }),
    ).rejects.toMatchObject({
      details: {
        path: 'aggregate.amount',
      },
    });
  });

  test('counts relation target records in the base parent scope', async () => {
    const result = await executor.execute({
      tableName: 'authors',
      fields: ['id'],
      filter: { country: { _eq: 'VN' } },
      aggregate: {
        posts: {
          countRecords: {
            status: { _eq: 'published' },
            views: { _gte: 10 },
          },
        },
      },
      metadata: makeAuthorPostMetadata(),
    });

    expect(result.data.map((row: any) => row.id).sort()).toEqual([1, 2]);
    expect(result.meta.aggregate).toEqual({
      posts: { countRecords: 2 },
    });
  });

  test('returns zero relation countRecords when the parent scope is empty', async () => {
    const result = await executor.execute({
      tableName: 'authors',
      fields: ['id'],
      filter: { country: { _eq: 'JP' } },
      aggregate: {
        posts: {
          countRecords: {
            status: { _eq: 'published' },
          },
        },
      },
      metadata: makeAuthorPostMetadata(),
    });

    expect(result.data).toEqual([]);
    expect(result.meta.aggregate).toEqual({
      posts: { countRecords: 0 },
    });
  });

  test('uses countRecords true as no target-table filter', async () => {
    const result = await executor.execute({
      tableName: 'authors',
      fields: ['id'],
      filter: { country: { _eq: 'VN' } },
      aggregate: {
        posts: {
          countRecords: true,
        },
      },
      metadata: makeAuthorPostMetadata(),
    });

    expect(result.data.map((row: any) => row.id).sort()).toEqual([1, 2]);
    expect(result.meta.aggregate).toEqual({
      posts: { countRecords: 4 },
    });
  });

  test('returns zero relation countRecords when target filter matches nothing', async () => {
    const result = await executor.execute({
      tableName: 'authors',
      fields: ['id'],
      filter: { country: { _eq: 'VN' } },
      aggregate: {
        posts: {
          countRecords: {
            status: { _eq: 'archived' },
          },
        },
      },
      metadata: makeAuthorPostMetadata(),
    });

    expect(result.data.map((row: any) => row.id).sort()).toEqual([1, 2]);
    expect(result.meta.aggregate).toEqual({
      posts: { countRecords: 0 },
    });
  });

  test('counts many-to-many relation target records in the base parent scope', async () => {
    const result = await executor.execute({
      tableName: 'authors',
      fields: ['id'],
      filter: { country: { _eq: 'VN' } },
      aggregate: {
        tags: {
          countRecords: {
            kind: { _eq: 'topic' },
          },
        },
      },
      metadata: makeAuthorPostMetadata(),
    });

    expect(result.data.map((row: any) => row.id).sort()).toEqual([1, 2]);
    expect(result.meta.aggregate).toEqual({
      tags: { countRecords: 2 },
    });
  });

  test('counts distinct many-to-many targets when junction rows are duplicated', async () => {
    const result = await executor.execute({
      tableName: 'authors',
      fields: ['id'],
      filter: { country: { _eq: 'VN' } },
      aggregate: {
        tags: {
          countRecords: {
            name: { _eq: 'tech' },
          },
        },
      },
      metadata: makeAuthorPostMetadata(),
    });

    expect(result.data.map((row: any) => row.id).sort()).toEqual([1, 2]);
    expect(result.meta.aggregate).toEqual({
      tags: { countRecords: 1 },
    });
  });

  test('counts distinct many-to-one relation targets in the base parent scope', async () => {
    const result = await executor.execute({
      tableName: 'sales',
      fields: ['id'],
      filter: { status: { _eq: 'paid' } },
      aggregate: {
        owner: {
          countRecords: {
            name: { _eq: 'alice' },
          },
        },
      },
      metadata,
    });

    expect(result.data.map((row: any) => row.id).sort()).toEqual([1, 2, 3, 6]);
    expect(result.meta.aggregate).toEqual({
      owner: { countRecords: 1 },
    });
  });
});

describe('MongoQueryExecutor aggregate addition query', () => {
  let available = false;
  let client: MongoClient;
  let db: Db;
  let executor: MongoQueryExecutor;
  let metadata: any;

  beforeAll(async () => {
    try {
      const probe = new MongoClient(MONGO_URI, { serverSelectionTimeoutMS: 2000 });
      await probe.connect();
      await probe.close();
      available = true;
    } catch {
      available = false;
      return;
    }

    client = new MongoClient(MONGO_URI);
    await client.connect();
    db = client.db(DB_NAME);
    await db.collection('user').insertMany([
      { _id: 1, name: 'alice' },
      { _id: 2, name: 'bob' },
      { _id: 3, name: 'carol' },
    ]);
    await db.collection('sales_mongo').insertMany([
      { _id: new ObjectId(), region: 'north', status: 'paid', amount: 100, discount: 10, score: 4.5, channel: 'web', ownerId: 1 },
      { _id: new ObjectId(), region: 'north', status: 'paid', amount: 200, discount: 0, score: 3.5, channel: 'app', ownerId: 1 },
      { _id: new ObjectId(), region: 'south', status: 'paid', amount: 300, discount: 30, score: 5, channel: 'web', ownerId: 2 },
      { _id: new ObjectId(), region: 'south', status: 'refunded', amount: 400, discount: 40, score: 2, channel: 'app', ownerId: 2 },
      { _id: new ObjectId(), region: 'east', status: 'draft', amount: 50, discount: null, score: null, channel: 'web', ownerId: null },
      { _id: new ObjectId(), region: 'north', status: 'paid', amount: 0, discount: 5, score: 4, channel: 'web', ownerId: 3 },
    ]);

    const authorIds = [new ObjectId(), new ObjectId(), new ObjectId()];
    await db.collection('authors_mongo').insertMany([
      { _id: authorIds[0], country: 'VN' },
      { _id: authorIds[1], country: 'VN' },
      { _id: authorIds[2], country: 'US' },
    ]);
    await db.collection('posts_mongo').insertMany([
      { _id: new ObjectId(), authorId: authorIds[0], status: 'published', views: 10 },
      { _id: new ObjectId(), authorId: authorIds[0], status: 'draft', views: 5 },
      { _id: new ObjectId(), authorId: authorIds[1], status: 'published', views: 30 },
      { _id: new ObjectId(), authorId: authorIds[1], status: 'published', views: 3 },
      { _id: new ObjectId(), authorId: authorIds[2], status: 'published', views: 50 },
    ]);
    const tagIds = [new ObjectId(), new ObjectId(), new ObjectId()];
    await db.collection('tags_mongo').insertMany([
      { _id: tagIds[0], name: 'tech', kind: 'topic' },
      { _id: tagIds[1], name: 'draft-only', kind: 'topic' },
      { _id: tagIds[2], name: 'vip', kind: 'audience' },
    ]);
    await db.collection('authors_tags_tags_mongo').insertMany([
      { authorsId: authorIds[0], tags_mongoId: tagIds[0] },
      { authorsId: authorIds[0], tags_mongoId: tagIds[0] },
      { authorsId: authorIds[0], tags_mongoId: tagIds[2] },
      { authorsId: authorIds[1], tags_mongoId: tagIds[0] },
      { authorsId: authorIds[1], tags_mongoId: tagIds[1] },
      { authorsId: authorIds[2], tags_mongoId: tagIds[0] },
    ]);

    metadata = makeMetadata(makeSalesMeta('sales_mongo'));
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
      if (!available) return;
      await fn();
    });
  }

  runOrSkip('matches SQL aggregate semantics with base filter and field conditions', async () => {
    const base = {
      tableName: 'sales_mongo',
      fields: ['_id', 'amount'],
      filter: { status: { _eq: 'paid' } },
      sort: '_id',
      limit: 2,
      meta: 'filterCount',
      aggregate: {
        amount: {
          count: { _gt: 0 },
          sum: { _gt: 0 },
          avg: true,
          min: true,
          max: true,
        },
        discount: {
          sum: { _is_not_null: true },
          count: { _gt: 0 },
        },
      },
      metadata,
      dbType: 'mongodb' as any,
    };
    const plan = new QueryPlanner().plan(base);
    const result = await executor.execute({ ...base, plan });

    expect(result.data).toHaveLength(2);
    expect(result.meta.filterCount).toBe(4);
    expect(result.meta.aggregate).toEqual({
      amount: { count: 3, sum: 600, avg: 150, min: 0, max: 300 },
      discount: { sum: 45, count: 3 },
    });
  });

  runOrSkip('ignores as without remapping the aggregate output key', async () => {
    const result = await executor.execute({
        tableName: 'sales_mongo',
        fields: ['_id'],
        filter: { status: { _eq: 'paid' } },
        aggregate: { amount: { as: 'revenue', sum: true } as any },
        metadata,
        dbType: 'mongodb',
      },
    );

    expect(result.meta.aggregate).toEqual({
      amount: { sum: 600 },
    });
  });

  runOrSkip('counts relation target records in the base parent scope', async () => {
    const base = {
      tableName: 'authors_mongo',
      fields: ['_id'],
      filter: { country: { _eq: 'VN' } },
      aggregate: {
        posts: {
          countRecords: {
            status: { _eq: 'published' },
            views: { _gte: 10 },
          },
        },
      },
      metadata: makeAuthorPostMetadata('authors_mongo', 'posts_mongo'),
      dbType: 'mongodb' as any,
    };
    const plan = new QueryPlanner().plan(base);
    const result = await executor.execute({ ...base, plan });

    expect(result.data).toHaveLength(2);
    expect(result.meta.aggregate).toEqual({
      posts: { countRecords: 2 },
    });
  });

  runOrSkip('returns zero relation countRecords when the parent scope is empty', async () => {
    const base = {
      tableName: 'authors_mongo',
      fields: ['_id'],
      filter: { country: { _eq: 'JP' } },
      aggregate: {
        posts: {
          countRecords: {
            status: { _eq: 'published' },
          },
        },
      },
      metadata: makeAuthorPostMetadata('authors_mongo', 'posts_mongo'),
      dbType: 'mongodb' as any,
    };
    const plan = new QueryPlanner().plan(base);
    const result = await executor.execute({ ...base, plan });

    expect(result.data).toEqual([]);
    expect(result.meta.aggregate).toEqual({
      posts: { countRecords: 0 },
    });
  });

  runOrSkip('uses countRecords true as no target-table filter', async () => {
    const base = {
      tableName: 'authors_mongo',
      fields: ['_id'],
      filter: { country: { _eq: 'VN' } },
      aggregate: {
        posts: {
          countRecords: true,
        },
      },
      metadata: makeAuthorPostMetadata('authors_mongo', 'posts_mongo'),
      dbType: 'mongodb' as any,
    };
    const plan = new QueryPlanner().plan(base);
    const result = await executor.execute({ ...base, plan });

    expect(result.data).toHaveLength(2);
    expect(result.meta.aggregate).toEqual({
      posts: { countRecords: 4 },
    });
  });

  runOrSkip('returns zero relation countRecords when target filter matches nothing', async () => {
    const base = {
      tableName: 'authors_mongo',
      fields: ['_id'],
      filter: { country: { _eq: 'VN' } },
      aggregate: {
        posts: {
          countRecords: {
            status: { _eq: 'archived' },
          },
        },
      },
      metadata: makeAuthorPostMetadata('authors_mongo', 'posts_mongo'),
      dbType: 'mongodb' as any,
    };
    const plan = new QueryPlanner().plan(base);
    const result = await executor.execute({ ...base, plan });

    expect(result.data).toHaveLength(2);
    expect(result.meta.aggregate).toEqual({
      posts: { countRecords: 0 },
    });
  });

  runOrSkip('counts many-to-many relation target records in the base parent scope', async () => {
    const base = {
      tableName: 'authors_mongo',
      fields: ['_id'],
      filter: { country: { _eq: 'VN' } },
      aggregate: {
        tags: {
          countRecords: {
            kind: { _eq: 'topic' },
          },
        },
      },
      metadata: makeAuthorPostMetadata('authors_mongo', 'posts_mongo'),
      dbType: 'mongodb' as any,
    };
    const plan = new QueryPlanner().plan(base);
    const result = await executor.execute({ ...base, plan });

    expect(result.data).toHaveLength(2);
    expect(result.meta.aggregate).toEqual({
      tags: { countRecords: 2 },
    });
  });

  runOrSkip('counts distinct many-to-many targets when junction rows are duplicated', async () => {
    const base = {
      tableName: 'authors_mongo',
      fields: ['_id'],
      filter: { country: { _eq: 'VN' } },
      aggregate: {
        tags: {
          countRecords: {
            name: { _eq: 'tech' },
          },
        },
      },
      metadata: makeAuthorPostMetadata('authors_mongo', 'posts_mongo'),
      dbType: 'mongodb' as any,
    };
    const plan = new QueryPlanner().plan(base);
    const result = await executor.execute({ ...base, plan });

    expect(result.data).toHaveLength(2);
    expect(result.meta.aggregate).toEqual({
      tags: { countRecords: 1 },
    });
  });

  runOrSkip('counts distinct many-to-one relation targets in the base parent scope', async () => {
    const base = {
      tableName: 'sales_mongo',
      fields: ['_id'],
      filter: { status: { _eq: 'paid' } },
      aggregate: {
        owner: {
          countRecords: {
            name: { _eq: 'alice' },
          },
        },
      },
      metadata,
      dbType: 'mongodb' as any,
    };
    const plan = new QueryPlanner().plan(base);
    const result = await executor.execute({ ...base, plan });

    expect(result.data).toHaveLength(4);
    expect(result.meta.aggregate).toEqual({
      owner: { countRecords: 1 },
    });
  });
});
