import knex, { Knex } from 'knex';
import {
  executeBatchFetches,
  BatchFetchDescriptor,
} from 'src/kernel/query';
import { BatchTrace } from 'src/kernel/query';

const PREFIX = `__dit_${Date.now()}_`;
const T = {
  users: `${PREFIX}users`,
  posts: `${PREFIX}posts`,
  comments: `${PREFIX}comments`,
  tags: `${PREFIX}tags`,
  junction: `${PREFIX}posts_tags`,
};

const DBS = [
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
      process.env.MYSQL_TEST_URI || 'mysql://root:1234@localhost:3306/enfyra',
    dbType: 'mysql' as const,
  },
];

const META: Record<string, any> = {
  [T.posts]: {
    name: T.posts,
    columns: [
      { name: 'id', type: 'integer', isPrimary: true },
      { name: 'title', type: 'varchar' },
      { name: 'authorId', type: 'integer' },
    ],
    relations: [
      {
        propertyName: 'author',
        type: 'many-to-one',
        targetTableName: T.users,
        targetTable: T.users,
        foreignKeyColumn: 'authorId',
        isInverse: false,
      },
      {
        propertyName: 'comments',
        type: 'one-to-many',
        targetTableName: T.comments,
        targetTable: T.comments,
        foreignKeyColumn: 'postId',
        isInverse: true,
        mappedBy: 'post',
      },
      {
        propertyName: 'tags',
        type: 'many-to-many',
        targetTableName: T.tags,
        targetTable: T.tags,
        isInverse: false,
        junctionTableName: T.junction,
        junctionSourceColumn: 'postId',
        junctionTargetColumn: 'tagId',
      },
    ],
  },
  [T.users]: {
    name: T.users,
    columns: [
      { name: 'id', type: 'integer', isPrimary: true },
      { name: 'name', type: 'varchar' },
      { name: 'active', type: 'boolean' },
    ],
    relations: [
      {
        propertyName: 'posts',
        type: 'one-to-many',
        targetTableName: T.posts,
        targetTable: T.posts,
        foreignKeyColumn: 'authorId',
        isInverse: true,
        mappedBy: 'author',
      },
    ],
  },
  [T.comments]: {
    name: T.comments,
    columns: [
      { name: 'id', type: 'integer', isPrimary: true },
      { name: 'body', type: 'varchar' },
      { name: 'isPublished', type: 'boolean' },
      { name: 'seq', type: 'integer' },
      { name: 'postId', type: 'integer' },
    ],
    relations: [
      {
        propertyName: 'post',
        type: 'many-to-one',
        targetTableName: T.posts,
        targetTable: T.posts,
        foreignKeyColumn: 'postId',
        isInverse: false,
      },
    ],
  },
  [T.tags]: {
    name: T.tags,
    columns: [
      { name: 'id', type: 'integer', isPrimary: true },
      { name: 'label', type: 'varchar' },
      { name: 'priority', type: 'integer' },
    ],
    relations: [],
  },
};

const metadataGetter = async (table: string) => META[table] || null;

class TestTrace implements BatchTrace {
  entries: Array<{
    stage: string;
    ms: number;
    meta?: Record<string, unknown>;
  }> = [];
  dur(stage: string, startTs: number, meta?: Record<string, unknown>): number {
    const ms = performance.now() - startTs;
    this.entries.push({ stage, ms, meta });
    return ms;
  }
  find(relation: string) {
    return this.entries.find(
      (e) => e.stage.includes('batch_fetch') && e.stage.includes(relation),
    );
  }
}

for (const cfg of DBS) {
  describe(`deep integration (${cfg.name})`, () => {
    let db: Knex;
    let available = true;

    beforeAll(async () => {
      db = knex({
        client: cfg.client,
        connection: cfg.connection,
        pool: { min: 0, max: 4 },
      });

      try {
        await db.raw('SELECT 1');
      } catch (e) {
        available = false;
        return;
      }

      await db.schema.dropTableIfExists(T.junction);
      await db.schema.dropTableIfExists(T.comments);
      await db.schema.dropTableIfExists(T.posts);
      await db.schema.dropTableIfExists(T.tags);
      await db.schema.dropTableIfExists(T.users);

      await db.schema.createTable(T.users, (t) => {
        t.increments('id').primary();
        t.string('name');
        t.boolean('active').defaultTo(true);
      });
      await db.schema.createTable(T.posts, (t) => {
        t.increments('id').primary();
        t.string('title');
        t.integer('authorId').nullable();
      });
      await db.schema.createTable(T.comments, (t) => {
        t.increments('id').primary();
        t.string('body');
        t.boolean('isPublished').defaultTo(true);
        t.integer('seq');
        t.integer('postId');
      });
      await db.schema.createTable(T.tags, (t) => {
        t.increments('id').primary();
        t.string('label');
        t.integer('priority').defaultTo(0);
      });
      await db.schema.createTable(T.junction, (t) => {
        t.integer('postId');
        t.integer('tagId');
      });

      await db(T.users).insert([
        { id: 1, name: 'Alice', active: true },
        { id: 2, name: 'Bob', active: false },
        { id: 3, name: 'Charlie', active: true },
      ]);
      await db(T.posts).insert([
        { id: 1, title: 'Post A', authorId: 1 },
        { id: 2, title: 'Post B', authorId: 2 },
        { id: 3, title: 'Post C', authorId: null },
        { id: 4, title: 'Post D', authorId: 1 },
      ]);
      await db(T.comments).insert([
        { id: 1, body: 'C1-P1', isPublished: true, seq: 1, postId: 1 },
        { id: 2, body: 'C2-P1', isPublished: false, seq: 2, postId: 1 },
        { id: 3, body: 'C3-P1', isPublished: true, seq: 3, postId: 1 },
        { id: 4, body: 'C4-P1', isPublished: true, seq: 4, postId: 1 },
        { id: 5, body: 'C1-P2', isPublished: true, seq: 1, postId: 2 },
        { id: 6, body: 'C2-P2', isPublished: true, seq: 2, postId: 2 },
      ]);
      await db(T.tags).insert([
        { id: 1, label: 'alpha', priority: 3 },
        { id: 2, label: 'beta', priority: 1 },
        { id: 3, label: 'gamma', priority: 2 },
      ]);
      await db(T.junction).insert([
        { postId: 1, tagId: 1 },
        { postId: 1, tagId: 2 },
        { postId: 1, tagId: 3 },
        { postId: 2, tagId: 1 },
      ]);
    }, 30000);

    afterAll(async () => {
      if (!available) {
        if (db) await db.destroy();
        return;
      }
      try {
        await db.schema.dropTableIfExists(T.junction);
        await db.schema.dropTableIfExists(T.comments);
        await db.schema.dropTableIfExists(T.posts);
        await db.schema.dropTableIfExists(T.tags);
        await db.schema.dropTableIfExists(T.users);
      } finally {
        await db.destroy();
      }
    }, 30000);

    async function fetchPosts(ids: number[]) {
      return (await db(T.posts)
        .whereIn('id', ids)
        .orderBy('id', 'asc')) as any[];
    }

    async function run(
      rows: any[],
      descriptors: BatchFetchDescriptor[],
      trace?: TestTrace,
    ) {
      await executeBatchFetches(
        db,
        rows,
        descriptors,
        metadataGetter,
        3,
        0,
        T.posts,
        cfg.dbType,
        META,
        trace,
      );
    }

    test('basic o2m filter + limit returns correct per-parent top-k', async () => {
      if (!available) return;
      const rows = await fetchPosts([1, 2]);
      const desc: BatchFetchDescriptor = {
        relationName: 'comments',
        type: 'one-to-many',
        targetTable: T.comments,
        fields: ['id', 'seq'],
        isInverse: true,
        mappedBy: 'post',
        fkColumn: 'postId',
        userFilter: { isPublished: { _eq: true } },
        userSort: '-seq',
        userLimit: 2,
      };
      await run(rows, [desc]);
      const p1 = rows.find((r) => r.id === 1).comments;
      const p2 = rows.find((r) => r.id === 2).comments;
      expect(p1.map((c: any) => c.seq)).toEqual([4, 3]);
      expect(p2.map((c: any) => c.seq)).toEqual([2, 1]);
    });

    test('filter + sort + limit + page composition (page 2)', async () => {
      if (!available) return;
      const rows = await fetchPosts([1]);
      const desc: BatchFetchDescriptor = {
        relationName: 'comments',
        type: 'one-to-many',
        targetTable: T.comments,
        fields: ['id', 'seq'],
        isInverse: true,
        mappedBy: 'post',
        fkColumn: 'postId',
        userFilter: { isPublished: { _eq: true } },
        userSort: '-seq',
        userLimit: 2,
        userPage: 2,
      };
      await run(rows, [desc]);
      expect(rows[0].comments.map((c: any) => c.seq)).toEqual([1]);
    });

    test('dotted sort on o2m via m2o join (posts → author.name)', async () => {
      if (!available) return;
      const users = await db(T.users).whereIn('id', [1, 2]);
      const parentRows = users.map((u) => ({ ...u }));
      const desc: BatchFetchDescriptor = {
        relationName: 'posts',
        type: 'one-to-many',
        targetTable: T.posts,
        fields: ['id', 'title'],
        isInverse: true,
        mappedBy: 'author',
        fkColumn: 'authorId',
        userSort: 'title',
      };
      await executeBatchFetches(
        db,
        parentRows,
        [desc],
        metadataGetter,
        3,
        0,
        T.users,
        cfg.dbType,
        META,
      );
      const alicePosts = parentRows.find((r) => r.id === 1).posts;
      const bobPosts = parentRows.find((r) => r.id === 2).posts;
      expect(alicePosts.map((p: any) => p.title)).toEqual(['Post A', 'Post D']);
      expect(bobPosts.map((p: any) => p.title)).toEqual(['Post B']);
    });

    test('m2m with filter + limit per parent', async () => {
      if (!available) return;
      const rows = await fetchPosts([1]);
      const desc: BatchFetchDescriptor = {
        relationName: 'tags',
        type: 'many-to-many',
        targetTable: T.tags,
        fields: ['id', 'label', 'priority'],
        isInverse: false,
        junctionTableName: T.junction,
        junctionSourceColumn: 'postId',
        junctionTargetColumn: 'tagId',
        userFilter: { priority: { _gte: 2 } },
        userSort: '-priority',
        userLimit: 1,
      };
      await run(rows, [desc]);
      expect(rows[0].tags.length).toBe(1);
      expect(rows[0].tags[0].label).toBe('alpha');
    });

    test('null FK on m2o leaves parent.author = null', async () => {
      if (!available) return;
      const raw = await fetchPosts([3]);
      const rows = raw.map((r) => ({ ...r, author: r.authorId }));
      expect(raw[0].authorId).toBeNull();
      const desc: BatchFetchDescriptor = {
        relationName: 'author',
        type: 'many-to-one',
        targetTable: T.users,
        fields: ['id', 'name'],
        isInverse: false,
        fkColumn: 'authorId',
      };
      await run(rows, [desc]);
      expect(rows[0].author).toBeNull();
    });

    test('limit > fanout returns all available children', async () => {
      if (!available) return;
      const rows = await fetchPosts([2]);
      const desc: BatchFetchDescriptor = {
        relationName: 'comments',
        type: 'one-to-many',
        targetTable: T.comments,
        fields: ['id'],
        isInverse: true,
        mappedBy: 'post',
        fkColumn: 'postId',
        userLimit: 99,
      };
      await run(rows, [desc]);
      expect(rows[0].comments.length).toBe(2);
    });

    test('duplicate parent FKs are de-duplicated at fetch layer', async () => {
      if (!available) return;
      const raw = await fetchPosts([1, 4]);
      const rows = raw.map((r) => ({ ...r, author: r.authorId }));
      const trace = new TestTrace();
      const desc: BatchFetchDescriptor = {
        relationName: 'author',
        type: 'many-to-one',
        targetTable: T.users,
        fields: ['id', 'name'],
        isInverse: false,
        fkColumn: 'authorId',
      };
      await run(rows, [desc], trace);
      expect(rows[0].author.name).toBe('Alice');
      expect(rows[1].author.name).toBe('Alice');
      const entry = trace.find('author');
      expect(entry!.meta?.rowsTransferred).toBe(1);
    });

    test('debug trace emits per-parent-c16 strategy with roundtrips = parent count', async () => {
      if (!available) return;
      const rows = await fetchPosts([1, 2]);
      const trace = new TestTrace();
      const desc: BatchFetchDescriptor = {
        relationName: 'comments',
        type: 'one-to-many',
        targetTable: T.comments,
        fields: ['id', 'seq'],
        isInverse: true,
        mappedBy: 'post',
        fkColumn: 'postId',
        userLimit: 2,
        userSort: '-seq',
      };
      await run(rows, [desc], trace);
      const entry = trace.find('comments');
      expect(entry).toBeDefined();
      expect(entry!.meta?.strategy).toBe('per-parent-c16');
      expect(entry!.meta?.roundtrips).toBe(2);
    });

    test('debug trace emits batch-in strategy when no limit', async () => {
      if (!available) return;
      const rows = await fetchPosts([1, 2]);
      const trace = new TestTrace();
      const desc: BatchFetchDescriptor = {
        relationName: 'comments',
        type: 'one-to-many',
        targetTable: T.comments,
        fields: ['id'],
        isInverse: true,
        mappedBy: 'post',
        fkColumn: 'postId',
      };
      await run(rows, [desc], trace);
      const entry = trace.find('comments');
      expect(entry!.meta?.strategy).toBe('batch-in');
      expect(entry!.meta?.roundtrips).toBe(1);
    });
  });
}
