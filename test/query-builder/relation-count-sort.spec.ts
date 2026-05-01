import knex, { Knex } from 'knex';
import { MongoClient, Db, ObjectId } from 'mongodb';
import {
  MongoQueryExecutor,
  QueryPlanner,
  SqlQueryExecutor,
  validateDeepOptions,
} from '@enfyra/kernel';

const MONGO_URI =
  process.env.MONGO_TEST_URI ||
  'mongodb://enfyra_admin:enfyra_password_123@localhost:27017/?authSource=admin';
const DB_NAME = `test_relation_count_sort_${Date.now()}`;

const REAL_SQL_DBS = [
  {
    name: 'postgres',
    client: 'pg',
    connection:
      process.env.PG_TEST_URI ||
      'postgresql://root:1234@localhost:5432/postgres',
    dbType: 'postgres' as const,
  },
  {
    name: 'mysql',
    client: 'mysql2',
    connection:
      process.env.MYSQL_TEST_URI || 'mysql://root:1234@localhost:3306/mysql',
    dbType: 'mysql' as const,
  },
];

function makeMetadata(isMongo = false) {
  const idColumn = isMongo
    ? { name: '_id', type: 'objectid', isPrimary: true }
    : { name: 'id', type: 'integer', isPrimary: true };
  return {
    tables: new Map<string, any>([
      [
        'posts',
        {
          name: 'posts',
          columns: [
            idColumn,
            { name: 'title', type: 'varchar' },
            { name: 'authorId', type: isMongo ? 'objectid' : 'integer' },
          ],
          relations: [
            {
              propertyName: 'comments',
              type: 'one-to-many',
              targetTableName: 'comments',
              targetTable: 'comments',
              foreignKeyColumn: 'postId',
              mappedBy: 'postId',
              isInverse: true,
            },
            {
              propertyName: 'tags',
              type: 'many-to-many',
              targetTableName: 'tags',
              targetTable: 'tags',
              junctionTableName: 'posts_tags',
              junctionSourceColumn: 'postId',
              junctionTargetColumn: 'tagId',
              isInverse: false,
            },
            {
              propertyName: 'author',
              type: 'many-to-one',
              targetTableName: 'users',
              targetTable: 'users',
              foreignKeyColumn: 'authorId',
              isInverse: false,
            },
          ],
        },
      ],
      [
        'comments',
        {
          name: 'comments',
          columns: [
            idColumn,
            { name: 'body', type: 'varchar' },
            { name: 'postId', type: isMongo ? 'objectid' : 'integer' },
          ],
          relations: [],
        },
      ],
      [
        'tags',
        {
          name: 'tags',
          columns: [idColumn, { name: 'label', type: 'varchar' }],
          relations: [
            {
              propertyName: 'posts',
              type: 'many-to-many',
              targetTableName: 'posts',
              targetTable: 'posts',
              mappedBy: 'tags',
              isInverse: true,
            },
          ],
        },
      ],
      [
        'users',
        {
          name: 'users',
          columns: [idColumn, { name: 'name', type: 'varchar' }],
          relations: [],
        },
      ],
      [
        'categories',
        {
          name: 'categories',
          columns: [
            { name: 'code', type: 'varchar', isPrimary: true },
            { name: 'name', type: 'varchar' },
          ],
          relations: [
            {
              propertyName: 'items',
              type: 'one-to-many',
              targetTableName: 'items',
              targetTable: 'items',
              foreignKeyColumn: 'categoryCode',
              mappedBy: 'categoryCode',
              isInverse: true,
            },
          ],
        },
      ],
      [
        'items',
        {
          name: 'items',
          columns: [
            idColumn,
            { name: 'categoryCode', type: 'varchar' },
            { name: 'label', type: 'varchar' },
          ],
          relations: [],
        },
      ],
    ]),
  };
}

describe('relation _count() sort (SQL)', () => {
  let db: Knex;
  let executor: SqlQueryExecutor;
  const metadata = makeMetadata();

  beforeAll(async () => {
    db = knex({
      client: 'sqlite3',
      connection: { filename: ':memory:' },
      useNullAsDefault: true,
    });

    await db.schema.createTable('users', (t) => {
      t.increments('id').primary();
      t.string('name');
    });
    await db.schema.createTable('posts', (t) => {
      t.increments('id').primary();
      t.string('title');
      t.integer('authorId');
    });
    await db.schema.createTable('comments', (t) => {
      t.increments('id').primary();
      t.string('body');
      t.integer('postId');
    });
    await db.schema.createTable('tags', (t) => {
      t.increments('id').primary();
      t.string('label');
    });
    await db.schema.createTable('posts_tags', (t) => {
      t.integer('postId');
      t.integer('tagId');
    });
    await db.schema.createTable('posts_tags_tags', (t) => {
      t.integer('postsId');
      t.integer('tagsId');
    });
    await db.schema.createTable('categories', (t) => {
      t.string('code').primary();
      t.string('name');
    });
    await db.schema.createTable('items', (t) => {
      t.increments('id').primary();
      t.string('categoryCode');
      t.string('label');
    });

    await db('users').insert([{ id: 1, name: 'Alice' }]);
    await db('posts').insert([
      { id: 1, title: 'Post A', authorId: 1 },
      { id: 2, title: 'Post B', authorId: 1 },
      { id: 3, title: 'Post C', authorId: 1 },
    ]);
    await db('comments').insert([
      { id: 1, body: 'a1', postId: 1 },
      { id: 2, body: 'a2', postId: 1 },
      { id: 3, body: 'a3', postId: 1 },
      { id: 4, body: 'b1', postId: 2 },
    ]);
    await db('tags').insert([
      { id: 1, label: 'one' },
      { id: 2, label: 'two' },
      { id: 3, label: 'three' },
    ]);
    await db('posts_tags').insert([
      { postId: 1, tagId: 1 },
      { postId: 2, tagId: 1 },
      { postId: 2, tagId: 2 },
      { postId: 2, tagId: 3 },
      { postId: 3, tagId: 1 },
    ]);
    await db('posts_tags_tags').insert([
      { postsId: 1, tagsId: 1 },
      { postsId: 2, tagsId: 1 },
      { postsId: 2, tagsId: 2 },
      { postsId: 2, tagsId: 3 },
      { postsId: 3, tagsId: 1 },
    ]);
    await db('categories').insert([
      { code: 'alpha', name: 'Alpha' },
      { code: 'beta', name: 'Beta' },
      { code: 'gamma', name: 'Gamma' },
    ]);
    await db('items').insert([
      { id: 1, categoryCode: 'beta', label: 'b1' },
      { id: 2, categoryCode: 'beta', label: 'b2' },
      { id: 3, categoryCode: 'alpha', label: 'a1' },
    ]);

    executor = new SqlQueryExecutor(db, 'sqlite');
  });

  afterAll(async () => {
    await db.destroy();
  });

  test('sorts root rows by o2m count', async () => {
    const result = await executor.execute({
      tableName: 'posts',
      fields: ['id', 'title'],
      sort: ['-_count(comments)', 'id'],
      limit: 3,
      metadata,
    });

    expect(result.data.map((row: any) => row.id)).toEqual([1, 2, 3]);
  });

  test('sorts root rows by m2m count', async () => {
    const result = await executor.execute({
      tableName: 'posts',
      fields: ['id', 'title'],
      sort: ['-_count(tags)', 'id'],
      limit: 3,
      metadata,
    });

    expect(result.data.map((row: any) => row.id)).toEqual([2, 1, 3]);
  });

  test('sorts root rows by inverse m2m count with generated mappedBy columns', async () => {
    const result = await executor.execute({
      tableName: 'tags',
      fields: ['id', 'label'],
      sort: ['-_count(posts)', 'id'],
      limit: 0,
      metadata,
    });

    expect(result.data.map((row: any) => row.id)).toEqual([1, 2, 3]);
  });

  test('keeps local sort priority before _count sort', async () => {
    const result = await executor.execute({
      tableName: 'posts',
      fields: ['id', 'title'],
      sort: ['title', '-_count(comments)'],
      limit: 0,
      metadata,
    });

    expect(result.data.map((row: any) => row.id)).toEqual([1, 2, 3]);
  });

  test('sorts by o2m count with a non-id primary key', async () => {
    const result = await executor.execute({
      tableName: 'categories',
      fields: ['code', 'name'],
      sort: ['-_count(items)', 'code'],
      limit: 0,
      metadata,
    });

    expect(result.data.map((row: any) => row.code)).toEqual([
      'beta',
      'alpha',
      'gamma',
    ]);
  });

  test('rejects _count on scalar relations', () => {
    expect(() =>
      new QueryPlanner().plan({
        tableName: 'posts',
        sort: '_count(author)',
        metadata,
        dbType: 'sqlite',
      }),
    ).toThrow(/only supported for list relations/i);
  });

  test.each([
    ['_count', /Use _count\(relationName\)/i],
    ['_count()', /Use _count\(relationName\)/i],
    ['_count(comments.body)', /direct relation name/i],
    ['_count(unknown)', /does not exist/i],
  ])('rejects invalid sort function %s', (sort, error) => {
    expect(() =>
      new QueryPlanner().plan({
        tableName: 'posts',
        sort,
        metadata,
        dbType: 'sqlite',
      }),
    ).toThrow(error);
  });

  test('does not allow _count inside deep relation sort', () => {
    expect(() =>
      validateDeepOptions(
        'posts',
        { comments: { sort: '_count(replies)' } },
        metadata,
      ),
    ).toThrow(/does not exist|not supported/i);
  });
});

describe('relation _count() sort (Mongo)', () => {
  let client: MongoClient;
  let db: Db;
  let executor: MongoQueryExecutor;
  let available = true;
  const metadata = makeMetadata(true);
  const postIds = [new ObjectId(), new ObjectId(), new ObjectId()];
  const tagIds = [new ObjectId(), new ObjectId(), new ObjectId()];

  beforeAll(async () => {
    try {
      client = await MongoClient.connect(MONGO_URI, {
        serverSelectionTimeoutMS: 2000,
      });
    } catch {
      available = false;
      return;
    }

    db = client.db(DB_NAME);
    await db.collection('posts').insertMany([
      { _id: postIds[0], title: 'Post A' },
      { _id: postIds[1], title: 'Post B' },
      { _id: postIds[2], title: 'Post C' },
    ]);
    await db.collection('comments').insertMany([
      { _id: new ObjectId(), body: 'a1', postId: postIds[0] },
      { _id: new ObjectId(), body: 'a2', postId: postIds[0] },
      { _id: new ObjectId(), body: 'a3', postId: postIds[0] },
      { _id: new ObjectId(), body: 'b1', postId: postIds[1] },
    ]);
    await db.collection('tags').insertMany([
      { _id: tagIds[0], label: 'one' },
      { _id: tagIds[1], label: 'two' },
      { _id: tagIds[2], label: 'three' },
    ]);
    await db.collection('posts_tags').insertMany([
      { postId: postIds[0], tagId: tagIds[0] },
      { postId: postIds[1], tagId: tagIds[0] },
      { postId: postIds[1], tagId: tagIds[1] },
      { postId: postIds[1], tagId: tagIds[2] },
      { postId: postIds[2], tagId: tagIds[0] },
    ]);
    await db.collection('posts_tags_tags').insertMany([
      { postsId: postIds[0], tagsId: tagIds[0] },
      { postsId: postIds[1], tagsId: tagIds[0] },
      { postsId: postIds[1], tagsId: tagIds[1] },
      { postsId: postIds[1], tagsId: tagIds[2] },
      { postsId: postIds[2], tagsId: tagIds[0] },
    ]);
    await db.collection('categories').insertMany([
      { _id: new ObjectId(), code: 'alpha', name: 'Alpha' },
      { _id: new ObjectId(), code: 'beta', name: 'Beta' },
      { _id: new ObjectId(), code: 'gamma', name: 'Gamma' },
    ]);
    await db.collection('items').insertMany([
      { _id: new ObjectId(), categoryCode: 'beta', label: 'b1' },
      { _id: new ObjectId(), categoryCode: 'beta', label: 'b2' },
      { _id: new ObjectId(), categoryCode: 'alpha', label: 'a1' },
    ]);

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

  async function run(sort: string | string[]) {
    const base = {
      tableName: 'posts',
      fields: ['_id', 'title'],
      sort,
      limit: 3,
      metadata,
      dbType: 'mongodb' as any,
    };
    const plan = new QueryPlanner().plan(base);
    return executor.execute({ ...base, plan });
  }

  runOrSkip('sorts root rows by o2m count', async () => {
    const result = await run(['-_count(comments)', '_id']);
    expect(result.data.map((row: any) => row._id)).toEqual([
      String(postIds[0]),
      String(postIds[1]),
      String(postIds[2]),
    ]);
  });

  runOrSkip('sorts root rows by m2m count', async () => {
    const result = await run(['-_count(tags)', '_id']);
    expect(result.data.map((row: any) => row._id)).toEqual([
      String(postIds[1]),
      String(postIds[0]),
      String(postIds[2]),
    ]);
  });

  runOrSkip('sorts root rows by inverse m2m count with generated mappedBy columns', async () => {
    const base = {
      tableName: 'tags',
      fields: ['_id', 'label'],
      sort: ['-_count(posts)', '_id'],
      limit: 0,
      metadata,
      dbType: 'mongodb' as any,
    };
    const plan = new QueryPlanner().plan(base);
    const result = await executor.execute({ ...base, plan });

    expect(result.data.map((row: any) => row._id)).toEqual([
      String(tagIds[0]),
      String(tagIds[1]),
      String(tagIds[2]),
    ]);
  });

  runOrSkip('does not expose temporary count fields', async () => {
    const result = await run(['-_count(comments)', '_id']);
    expect(
      Object.keys(result.data[0]).some((key) =>
        key.startsWith('__sort_count_'),
      ),
    ).toBe(false);
  });

  runOrSkip('sorts by o2m count with a non-_id primary key', async () => {
    const base = {
      tableName: 'categories',
      fields: ['code', 'name'],
      sort: ['-_count(items)', 'code'],
      limit: 0,
      metadata,
      dbType: 'mongodb' as any,
    };
    const plan = new QueryPlanner().plan(base);
    const result = await executor.execute({ ...base, plan });

    expect(result.data.map((row: any) => row.code)).toEqual([
      'beta',
      'alpha',
      'gamma',
    ]);
  });
});

for (const cfg of REAL_SQL_DBS) {
  describe(`relation _count() sort (${cfg.name} real DB)`, () => {
    let db: Knex;
    let available = true;
    const prefix = `__rcs_${cfg.name}_${Date.now()}_`;
    const tables = {
      posts: `${prefix}posts`,
      comments: `${prefix}comments`,
      tags: `${prefix}tags`,
      junction: `${prefix}posts_tags`,
    };
    const metadata = {
      tables: new Map<string, any>([
        [
          tables.posts,
          {
            name: tables.posts,
            columns: [
              { name: 'id', type: 'integer', isPrimary: true },
              { name: 'title', type: 'varchar' },
            ],
            relations: [
              {
                propertyName: 'comments',
                type: 'one-to-many',
                targetTableName: tables.comments,
                targetTable: tables.comments,
                foreignKeyColumn: 'postId',
                mappedBy: 'postId',
                isInverse: true,
              },
              {
                propertyName: 'tags',
                type: 'many-to-many',
                targetTableName: tables.tags,
                targetTable: tables.tags,
                junctionTableName: tables.junction,
                junctionSourceColumn: 'postId',
                junctionTargetColumn: 'tagId',
                isInverse: false,
              },
            ],
          },
        ],
        [
          tables.comments,
          {
            name: tables.comments,
            columns: [
              { name: 'id', type: 'integer', isPrimary: true },
              { name: 'postId', type: 'integer' },
            ],
            relations: [],
          },
        ],
        [
          tables.tags,
          {
            name: tables.tags,
            columns: [
              { name: 'id', type: 'integer', isPrimary: true },
              { name: 'label', type: 'varchar' },
            ],
            relations: [],
          },
        ],
      ]),
    };

    beforeAll(async () => {
      db = knex({
        client: cfg.client,
        connection: cfg.connection,
        pool: { min: 0, max: 4 },
      });

      try {
        await db.raw('SELECT 1');
      } catch {
        available = false;
        return;
      }

      await db.schema.dropTableIfExists(tables.junction);
      await db.schema.dropTableIfExists(tables.comments);
      await db.schema.dropTableIfExists(tables.posts);
      await db.schema.dropTableIfExists(tables.tags);

      await db.schema.createTable(tables.posts, (t) => {
        t.integer('id').primary();
        t.string('title');
      });
      await db.schema.createTable(tables.comments, (t) => {
        t.integer('id').primary();
        t.integer('postId');
      });
      await db.schema.createTable(tables.tags, (t) => {
        t.integer('id').primary();
        t.string('label');
      });
      await db.schema.createTable(tables.junction, (t) => {
        t.integer('postId');
        t.integer('tagId');
      });

      await db(tables.posts).insert([
        { id: 1, title: 'A' },
        { id: 2, title: 'B' },
        { id: 3, title: 'C' },
      ]);
      await db(tables.comments).insert([
        { id: 1, postId: 1 },
        { id: 2, postId: 1 },
        { id: 3, postId: 1 },
        { id: 4, postId: 2 },
      ]);
      await db(tables.tags).insert([
        { id: 1, label: 'one' },
        { id: 2, label: 'two' },
        { id: 3, label: 'three' },
      ]);
      await db(tables.junction).insert([
        { postId: 1, tagId: 1 },
        { postId: 2, tagId: 1 },
        { postId: 2, tagId: 2 },
        { postId: 2, tagId: 3 },
        { postId: 3, tagId: 1 },
      ]);
    }, 30000);

    afterAll(async () => {
      if (!db) return;
      try {
        if (available) {
          await db.schema.dropTableIfExists(tables.junction);
          await db.schema.dropTableIfExists(tables.comments);
          await db.schema.dropTableIfExists(tables.posts);
          await db.schema.dropTableIfExists(tables.tags);
        }
      } finally {
        await db.destroy();
      }
    }, 30000);

    function runOrSkip(name: string, fn: () => Promise<void>) {
      test(name, async () => {
        if (!available) return;
        await fn();
      });
    }

    runOrSkip('executes _count() ordering with dialect quoting', async () => {
      const executor = new SqlQueryExecutor(db, cfg.dbType);
      const byComments = await executor.execute({
        tableName: tables.posts,
        fields: ['id', 'title'],
        sort: ['-_count(comments)', 'id'],
        limit: 0,
        metadata,
      });
      const byTags = await executor.execute({
        tableName: tables.posts,
        fields: ['id', 'title'],
        sort: ['-_count(tags)', 'id'],
        limit: 0,
        metadata,
      });

      expect(byComments.data.map((row: any) => Number(row.id))).toEqual([
        1, 2, 3,
      ]);
      expect(byTags.data.map((row: any) => Number(row.id))).toEqual([
        2, 1, 3,
      ]);
    }, 30000);
  });
}
