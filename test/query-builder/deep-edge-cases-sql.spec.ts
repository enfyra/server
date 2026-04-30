import knex, { Knex } from 'knex';
import {
  executeBatchFetches,
  BatchFetchDescriptor,
  BatchTrace,
} from '@enfyra/kernel';
let db: Knex;

const META: Record<string, any> = {
  posts: {
    name: 'posts',
    columns: [
      { name: 'id', type: 'integer', isPrimary: true },
      { name: 'title', type: 'varchar' },
      { name: 'authorId', type: 'integer' },
    ],
    relations: [
      {
        propertyName: 'author',
        type: 'many-to-one',
        targetTableName: 'users',
        targetTable: 'users',
        foreignKeyColumn: 'authorId',
        isInverse: false,
      },
      {
        propertyName: 'comments',
        type: 'one-to-many',
        targetTableName: 'comments',
        targetTable: 'comments',
        foreignKeyColumn: 'postId',
        isInverse: true,
        mappedBy: 'post',
      },
      {
        propertyName: 'tags',
        type: 'many-to-many',
        targetTableName: 'tags',
        targetTable: 'tags',
        isInverse: false,
        junctionTableName: 'posts_tags',
        junctionSourceColumn: 'postId',
        junctionTargetColumn: 'tagId',
      },
    ],
  },
  users: {
    name: 'users',
    columns: [
      { name: 'id', type: 'integer', isPrimary: true },
      { name: 'name', type: 'varchar' },
      { name: 'active', type: 'boolean' },
    ],
    relations: [],
  },
  comments: {
    name: 'comments',
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
        targetTableName: 'posts',
        targetTable: 'posts',
        foreignKeyColumn: 'postId',
        isInverse: false,
      },
    ],
  },
  tags: {
    name: 'tags',
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
  findBatchFetch(relation: string) {
    return this.entries.find(
      (e) => e.stage.includes(`batch_fetch`) && e.stage.includes(relation),
    );
  }
}

beforeAll(async () => {
  db = knex({
    client: 'sqlite3',
    connection: { filename: ':memory:' },
    useNullAsDefault: true,
  });

  await db.schema.createTable('users', (t) => {
    t.increments('id').primary();
    t.string('name');
    t.boolean('active').defaultTo(true);
  });
  await db.schema.createTable('posts', (t) => {
    t.increments('id').primary();
    t.string('title');
    t.integer('authorId').nullable();
  });
  await db.schema.createTable('comments', (t) => {
    t.increments('id').primary();
    t.string('body');
    t.boolean('isPublished').defaultTo(true);
    t.integer('seq');
    t.integer('postId').references('id').inTable('posts');
  });
  await db.schema.createTable('tags', (t) => {
    t.increments('id').primary();
    t.string('label');
    t.integer('priority').defaultTo(0);
  });
  await db.schema.createTable('posts_tags', (t) => {
    t.integer('postId').references('id').inTable('posts');
    t.integer('tagId').references('id').inTable('tags');
  });

  await db('users').insert([
    { id: 1, name: 'Alice', active: 1 },
    { id: 2, name: 'Bob', active: 1 },
  ]);
  await db('posts').insert([
    { id: 1, title: 'Post A', authorId: 1 },
    { id: 2, title: 'Post B', authorId: 2 },
    { id: 3, title: 'Post C', authorId: null },
    { id: 4, title: 'Post D', authorId: 1 },
  ]);
  await db('comments').insert([
    { id: 1, body: 'C1-P1', isPublished: 1, seq: 1, postId: 1 },
    { id: 2, body: 'C2-P1', isPublished: 0, seq: 2, postId: 1 },
    { id: 3, body: 'C3-P1', isPublished: 1, seq: 3, postId: 1 },
    { id: 4, body: 'C4-P1', isPublished: 1, seq: 4, postId: 1 },
    { id: 5, body: 'C5-P1', isPublished: 1, seq: 5, postId: 1 },
    { id: 6, body: 'C1-P2', isPublished: 1, seq: 1, postId: 2 },
    { id: 7, body: 'C2-P2', isPublished: 1, seq: 2, postId: 2 },
  ]);
  await db('tags').insert([
    { id: 1, label: 'alpha', priority: 3 },
    { id: 2, label: 'beta', priority: 1 },
    { id: 3, label: 'gamma', priority: 2 },
  ]);
  await db('posts_tags').insert([
    { postId: 1, tagId: 1 },
    { postId: 1, tagId: 2 },
    { postId: 1, tagId: 3 },
    { postId: 2, tagId: 1 },
  ]);
});

afterAll(async () => {
  await db.destroy();
});

async function fetchPosts(ids: (number | null)[]) {
  if (ids.length === 0) return [];
  return db('posts')
    .whereIn('id', ids as number[])
    .orderBy('id', 'asc') as Promise<any[]>;
}

async function runBatchFetch(
  rows: any[],
  desc: BatchFetchDescriptor,
  trace?: BatchTrace,
) {
  await executeBatchFetches(
    db,
    rows,
    [desc],
    metadataGetter,
    3,
    0,
    'posts',
    'sqlite',
    META,
    trace,
  );
}

describe('deep edge cases — empty/null/zero (SQL)', () => {
  test('empty parent set does not fire any DB query and leaves rows unchanged', async () => {
    const rows: any[] = [];
    const trace = new TestTrace();
    const desc: BatchFetchDescriptor = {
      relationName: 'comments',
      type: 'one-to-many',
      targetTable: 'comments',
      fields: ['id'],
      isInverse: true,
      mappedBy: 'post',
      fkColumn: 'postId',
      userLimit: 3,
    };
    await runBatchFetch(rows, desc, trace);
    expect(rows).toEqual([]);
    const batchEvents = trace.entries.filter((e) =>
      e.stage.startsWith('batch_fetch'),
    );
    expect(batchEvents.length).toBe(0);
  });

  test('null FK on m2o leaves parent.author = null without crashing', async () => {
    const raw = await fetchPosts([3]);
    expect(raw[0].authorId).toBeNull();
    const rows = raw.map((r) => ({ ...r, author: r.authorId }));
    const desc: BatchFetchDescriptor = {
      relationName: 'author',
      type: 'many-to-one',
      targetTable: 'users',
      fields: ['id', 'name'],
      isInverse: false,
      fkColumn: 'authorId',
    };
    await runBatchFetch(rows, desc);
    expect(rows[0].author).toBeNull();
  });

  test('limit = 0 returns empty array per parent', async () => {
    const rows = await fetchPosts([1, 2]);
    const desc: BatchFetchDescriptor = {
      relationName: 'comments',
      type: 'one-to-many',
      targetTable: 'comments',
      fields: ['id'],
      isInverse: true,
      mappedBy: 'post',
      fkColumn: 'postId',
      userLimit: 0,
    };
    await runBatchFetch(rows, desc);
    for (const row of rows) {
      expect(Array.isArray(row.comments)).toBe(true);
      expect(row.comments.length).toBe(0);
    }
  });

  test('limit bigger than fanout returns all children (no padding)', async () => {
    const rows = await fetchPosts([2]);
    const desc: BatchFetchDescriptor = {
      relationName: 'comments',
      type: 'one-to-many',
      targetTable: 'comments',
      fields: ['id', 'seq'],
      isInverse: true,
      mappedBy: 'post',
      fkColumn: 'postId',
      userLimit: 100,
    };
    await runBatchFetch(rows, desc);
    expect(rows[0].comments.length).toBe(2);
  });

  test('parent with no children gets empty array, not missing key', async () => {
    await db('posts').insert({ id: 99, title: 'Lonely', authorId: null });
    try {
      const rows = await db('posts').where('id', 99);
      const desc: BatchFetchDescriptor = {
        relationName: 'comments',
        type: 'one-to-many',
        targetTable: 'comments',
        fields: ['id'],
        isInverse: true,
        mappedBy: 'post',
        fkColumn: 'postId',
        userLimit: 5,
      };
      await runBatchFetch(rows, desc);
      expect(Object.prototype.hasOwnProperty.call(rows[0], 'comments')).toBe(
        true,
      );
      expect(rows[0].comments).toEqual([]);
    } finally {
      await db('posts').where('id', 99).del();
    }
  });

  test('duplicate parent FKs are de-duplicated at fetch layer', async () => {
    const raw = await fetchPosts([1, 4]);
    const rows = raw.map((r) => ({ ...r, author: r.authorId }));
    const desc: BatchFetchDescriptor = {
      relationName: 'author',
      type: 'many-to-one',
      targetTable: 'users',
      fields: ['id', 'name'],
      isInverse: false,
      fkColumn: 'authorId',
    };
    const trace = new TestTrace();
    await runBatchFetch(rows, desc, trace);
    expect(rows[0].author.id).toBe(1);
    expect(rows[1].author.id).toBe(1);
    expect(rows[0].author.name).toBe('Alice');
    const entry = trace.findBatchFetch('author');
    expect(entry!.meta?.rowsTransferred).toBe(1);
  });
});

describe('deep composition — filter + sort + limit + page (SQL)', () => {
  test('filter + sort DESC + limit 2 + page 1', async () => {
    const rows = await fetchPosts([1]);
    const desc: BatchFetchDescriptor = {
      relationName: 'comments',
      type: 'one-to-many',
      targetTable: 'comments',
      fields: ['id', 'seq', 'isPublished'],
      isInverse: true,
      mappedBy: 'post',
      fkColumn: 'postId',
      userFilter: { isPublished: { _eq: true } },
      userSort: '-seq',
      userLimit: 2,
      userPage: 1,
    };
    await runBatchFetch(rows, desc);
    const seqs = rows[0].comments.map((c: any) => c.seq);
    expect(seqs).toEqual([5, 4]);
    for (const c of rows[0].comments) expect(c.isPublished).toBe(1);
  });

  test('filter + sort + limit + page 2 returns next window', async () => {
    const rows = await fetchPosts([1]);
    const desc: BatchFetchDescriptor = {
      relationName: 'comments',
      type: 'one-to-many',
      targetTable: 'comments',
      fields: ['id', 'seq', 'isPublished'],
      isInverse: true,
      mappedBy: 'post',
      fkColumn: 'postId',
      userFilter: { isPublished: { _eq: true } },
      userSort: '-seq',
      userLimit: 2,
      userPage: 2,
    };
    await runBatchFetch(rows, desc);
    const seqs = rows[0].comments.map((c: any) => c.seq);
    expect(seqs).toEqual([3, 1]);
  });

  test('two sibling relations with independent options in one pass', async () => {
    const raw = await fetchPosts([1, 2]);
    const rows = raw.map((r) => ({ ...r, author: r.authorId }));
    const descComments: BatchFetchDescriptor = {
      relationName: 'comments',
      type: 'one-to-many',
      targetTable: 'comments',
      fields: ['id', 'seq'],
      isInverse: true,
      mappedBy: 'post',
      fkColumn: 'postId',
      userLimit: 1,
      userSort: '-seq',
    };
    const descAuthor: BatchFetchDescriptor = {
      relationName: 'author',
      type: 'many-to-one',
      targetTable: 'users',
      fields: ['id', 'name'],
      isInverse: false,
      fkColumn: 'authorId',
    };
    await executeBatchFetches(
      db,
      rows,
      [descComments, descAuthor],
      metadataGetter,
      3,
      0,
      'posts',
      'sqlite',
      META,
    );
    expect(rows[0].comments.length).toBe(1);
    expect(rows[0].comments[0].seq).toBe(5);
    expect(rows[1].comments[0].seq).toBe(2);
    expect(rows[0].author.name).toBe('Alice');
    expect(rows[1].author.name).toBe('Bob');
  });

  test('m2m with filter + limit combined', async () => {
    const rows = await fetchPosts([1]);
    const desc: BatchFetchDescriptor = {
      relationName: 'tags',
      type: 'many-to-many',
      targetTable: 'tags',
      fields: ['id', 'label', 'priority'],
      isInverse: false,
      junctionTableName: 'posts_tags',
      junctionSourceColumn: 'postId',
      junctionTargetColumn: 'tagId',
      userFilter: { priority: { _gte: 2 } },
      userSort: '-priority',
      userLimit: 1,
    };
    await runBatchFetch(rows, desc);
    expect(rows[0].tags.length).toBe(1);
    expect(rows[0].tags[0].label).toBe('alpha');
    expect(rows[0].tags[0].priority).toBe(3);
  });
});

describe('debug trace — strategy + metrics (SQL)', () => {
  test('limit set → strategy = per-parent-c16 with roundtrips = parent count', async () => {
    const rows = await fetchPosts([1, 2]);
    const trace = new TestTrace();
    const desc: BatchFetchDescriptor = {
      relationName: 'comments',
      type: 'one-to-many',
      targetTable: 'comments',
      fields: ['id', 'seq'],
      isInverse: true,
      mappedBy: 'post',
      fkColumn: 'postId',
      userLimit: 2,
      userSort: '-seq',
    };
    await runBatchFetch(rows, desc, trace);
    const entry = trace.findBatchFetch('comments');
    expect(entry).toBeDefined();
    expect(entry!.meta?.strategy).toBe('per-parent-c16');
    expect(entry!.meta?.roundtrips).toBe(2);
    expect(entry!.meta?.rowsReturned).toBe(4);
  });

  test('no limit → strategy = batch-in with roundtrips = 1', async () => {
    const rows = await fetchPosts([1, 2]);
    const trace = new TestTrace();
    const desc: BatchFetchDescriptor = {
      relationName: 'comments',
      type: 'one-to-many',
      targetTable: 'comments',
      fields: ['id'],
      isInverse: true,
      mappedBy: 'post',
      fkColumn: 'postId',
    };
    await runBatchFetch(rows, desc, trace);
    const entry = trace.findBatchFetch('comments');
    expect(entry).toBeDefined();
    expect(entry!.meta?.strategy).toBe('batch-in');
    expect(entry!.meta?.roundtrips).toBe(1);
  });

  test('m2o always uses batch-in (no per-parent)', async () => {
    const raw = await fetchPosts([1, 2]);
    const rows = raw.map((r) => ({ ...r, author: r.authorId }));
    const trace = new TestTrace();
    const desc: BatchFetchDescriptor = {
      relationName: 'author',
      type: 'many-to-one',
      targetTable: 'users',
      fields: ['id', 'name'],
      isInverse: false,
      fkColumn: 'authorId',
    };
    await runBatchFetch(rows, desc, trace);
    const entry = trace.findBatchFetch('author');
    expect(entry).toBeDefined();
    expect(entry!.meta?.strategy).toBe('batch-in');
  });

  test('m2m with limit → strategy = m2m-per-parent-c16', async () => {
    const rows = await fetchPosts([1, 2]);
    const trace = new TestTrace();
    const desc: BatchFetchDescriptor = {
      relationName: 'tags',
      type: 'many-to-many',
      targetTable: 'tags',
      fields: ['id', 'label'],
      isInverse: false,
      junctionTableName: 'posts_tags',
      junctionSourceColumn: 'postId',
      junctionTargetColumn: 'tagId',
      userLimit: 2,
    };
    await runBatchFetch(rows, desc, trace);
    const entry = trace.findBatchFetch('tags');
    expect(entry).toBeDefined();
    expect(entry!.meta?.strategy).toBe('m2m-per-parent-c16');
    expect(entry!.meta?.roundtrips).toBe(2);
  });

  test('trace meta includes rowsTransferred reflecting per-parent totals', async () => {
    const rows = await fetchPosts([1]);
    const trace = new TestTrace();
    const desc: BatchFetchDescriptor = {
      relationName: 'comments',
      type: 'one-to-many',
      targetTable: 'comments',
      fields: ['id'],
      isInverse: true,
      mappedBy: 'post',
      fkColumn: 'postId',
      userLimit: 2,
      userSort: '-seq',
    };
    await runBatchFetch(rows, desc, trace);
    const entry = trace.findBatchFetch('comments');
    expect(entry!.meta?.rowsTransferred).toBe(2);
    expect(entry!.meta?.rowsReturned).toBe(2);
  });
});

describe('deep nested within deep — recursion (SQL)', () => {
  test('fields dot-notation creates nested desc and hydrates', async () => {
    const rows = await fetchPosts([1, 2]);
    const desc: BatchFetchDescriptor = {
      relationName: 'comments',
      type: 'one-to-many',
      targetTable: 'comments',
      fields: ['id', 'seq', 'post.id', 'post.title'],
      isInverse: true,
      mappedBy: 'post',
      fkColumn: 'postId',
      userLimit: 1,
      userSort: 'seq',
    };
    await runBatchFetch(rows, desc);
    expect(rows[0].comments[0]).toHaveProperty('post');
    expect(rows[0].comments[0].post.title).toBe('Post A');
    expect(rows[1].comments[0].post.title).toBe('Post B');
  });

  test('nestedDeep enriches existing dot-field desc with extra options', async () => {
    const rows = await fetchPosts([1]);
    const trace = new TestTrace();
    const desc: BatchFetchDescriptor = {
      relationName: 'comments',
      type: 'one-to-many',
      targetTable: 'comments',
      fields: ['id', 'seq', 'post.id'],
      isInverse: true,
      mappedBy: 'post',
      fkColumn: 'postId',
      userLimit: 1,
      userSort: '-seq',
      nestedDeep: {
        post: {
          fields: 'id,title',
        },
      },
    };
    await runBatchFetch(rows, desc, trace);
    expect(rows[0].comments[0].post).toHaveProperty('title');
    expect(rows[0].comments[0].post.title).toBe('Post A');
    const depth1 = trace.entries.find((e) =>
      e.stage.startsWith('batch_fetch_L1_post'),
    );
    expect(depth1).toBeDefined();
  });
});
