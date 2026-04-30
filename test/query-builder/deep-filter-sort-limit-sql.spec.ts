import knex, { Knex } from 'knex';
import {
  executeBatchFetches,
  BatchFetchDescriptor,
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
      { name: 'companyId', type: 'integer' },
    ],
    relations: [
      {
        propertyName: 'company',
        type: 'many-to-one',
        targetTableName: 'companies',
        targetTable: 'companies',
        foreignKeyColumn: 'companyId',
        isInverse: false,
      },
      {
        propertyName: 'posts',
        type: 'one-to-many',
        targetTableName: 'posts',
        targetTable: 'posts',
        foreignKeyColumn: 'authorId',
        isInverse: true,
        mappedBy: 'author',
      },
    ],
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
  companies: {
    name: 'companies',
    columns: [
      { name: 'id', type: 'integer', isPrimary: true },
      { name: 'name', type: 'varchar' },
    ],
    relations: [],
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

beforeAll(async () => {
  db = knex({
    client: 'sqlite3',
    connection: { filename: ':memory:' },
    useNullAsDefault: true,
  });

  await db.schema.createTable('companies', (t) => {
    t.increments('id').primary();
    t.string('name');
  });
  await db.schema.createTable('users', (t) => {
    t.increments('id').primary();
    t.string('name');
    t.boolean('active').defaultTo(true);
    t.integer('companyId').references('id').inTable('companies').nullable();
  });
  await db.schema.createTable('posts', (t) => {
    t.increments('id').primary();
    t.string('title');
    t.integer('authorId').references('id').inTable('users').nullable();
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

  await db('companies').insert([
    { id: 1, name: 'Acme' },
    { id: 2, name: 'Beta Corp' },
  ]);
  await db('users').insert([
    { id: 1, name: 'Alice', active: 1, companyId: 1 },
    { id: 2, name: 'Bob', active: 0, companyId: 2 },
    { id: 3, name: 'Charlie', active: 1, companyId: 1 },
  ]);
  await db('posts').insert([
    { id: 1, title: 'Post A', authorId: 1 },
    { id: 2, title: 'Post B', authorId: 2 },
    { id: 3, title: 'Post C', authorId: 3 },
  ]);
  await db('comments').insert([
    { id: 1, body: 'Comment 1', isPublished: 1, seq: 1, postId: 1 },
    { id: 2, body: 'Comment 2', isPublished: 0, seq: 2, postId: 1 },
    { id: 3, body: 'Comment 3', isPublished: 1, seq: 3, postId: 1 },
    { id: 4, body: 'Comment 4', isPublished: 1, seq: 1, postId: 2 },
    { id: 5, body: 'Comment 5', isPublished: 1, seq: 2, postId: 2 },
    { id: 6, body: 'Comment 6', isPublished: 0, seq: 3, postId: 2 },
    { id: 7, body: 'Comment 7', isPublished: 1, seq: 1, postId: 3 },
  ]);
  await db('tags').insert([
    { id: 1, label: 'alpha', priority: 3 },
    { id: 2, label: 'beta', priority: 1 },
    { id: 3, label: 'gamma', priority: 2 },
    { id: 4, label: 'delta', priority: 4 },
  ]);
  await db('posts_tags').insert([
    { postId: 1, tagId: 1 },
    { postId: 1, tagId: 2 },
    { postId: 1, tagId: 3 },
    { postId: 2, tagId: 2 },
    { postId: 2, tagId: 4 },
    { postId: 3, tagId: 1 },
  ]);
});

afterAll(async () => {
  await db.destroy();
});

async function fetchPosts(ids: number[]) {
  return db('posts').whereIn('id', ids).orderBy('id', 'asc') as Promise<any[]>;
}

describe('deep filter on o2m (SQL)', () => {
  test('filter isPublished=true on comments', async () => {
    const rows = await fetchPosts([1, 2]);
    const desc: BatchFetchDescriptor = {
      relationName: 'comments',
      type: 'one-to-many',
      targetTable: 'comments',
      fields: ['id', 'body', 'isPublished'],
      isInverse: true,
      mappedBy: 'post',
      fkColumn: 'postId',
      userFilter: { isPublished: { _eq: true } },
    };
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
    );

    const post1Comments = rows.find((r) => r.id === 1).comments;
    const post2Comments = rows.find((r) => r.id === 2).comments;
    expect(post1Comments.every((c: any) => c.isPublished === 1)).toBe(true);
    expect(post1Comments.length).toBe(2);
    expect(post2Comments.every((c: any) => c.isPublished === 1)).toBe(true);
    expect(post2Comments.length).toBe(2);
  });

  test('filter isPublished=false on comments', async () => {
    const rows = await fetchPosts([1]);
    const desc: BatchFetchDescriptor = {
      relationName: 'comments',
      type: 'one-to-many',
      targetTable: 'comments',
      fields: ['id', 'body'],
      isInverse: true,
      mappedBy: 'post',
      fkColumn: 'postId',
      userFilter: { isPublished: { _eq: false } },
    };
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
    );
    const comments = rows[0].comments;
    expect(comments.length).toBe(1);
    expect(comments[0].id).toBe(2);
  });
});

describe('deep filter on m2o (SQL)', () => {
  test('filter active=true on author', async () => {
    const rawRows = await fetchPosts([1, 2]);
    const rows = rawRows.map((r) => ({ ...r, author: r.authorId }));
    const desc: BatchFetchDescriptor = {
      relationName: 'author',
      type: 'many-to-one',
      targetTable: 'users',
      fields: ['id', 'name', 'active'],
      isInverse: false,
      fkColumn: 'authorId',
      userFilter: { active: { _eq: true } },
    };
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
    );

    const post1 = rows.find((r) => r.id === 1);
    const post2 = rows.find((r) => r.id === 2);
    expect(post1.author).not.toBeNull();
    expect(post1.author.active).toBe(1);
    expect(post2.author).toBeNull();
  });
});

describe('deep sort on o2m (SQL)', () => {
  test('sort by seq DESC', async () => {
    const rows = await fetchPosts([1]);
    const desc: BatchFetchDescriptor = {
      relationName: 'comments',
      type: 'one-to-many',
      targetTable: 'comments',
      fields: ['id', 'seq'],
      isInverse: true,
      mappedBy: 'post',
      fkColumn: 'postId',
      userSort: '-seq',
    };
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
    );
    const seqs = rows[0].comments.map((c: any) => c.seq);
    expect(seqs).toEqual([3, 2, 1]);
  });

  test('sort by seq ASC', async () => {
    const rows = await fetchPosts([2]);
    const desc: BatchFetchDescriptor = {
      relationName: 'comments',
      type: 'one-to-many',
      targetTable: 'comments',
      fields: ['id', 'seq'],
      isInverse: true,
      mappedBy: 'post',
      fkColumn: 'postId',
      userSort: 'seq',
    };
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
    );
    const seqs = rows[0].comments.map((c: any) => c.seq);
    expect(seqs).toEqual([1, 2, 3]);
  });
});

describe('deep limit on o2m (SQL)', () => {
  test('limit 2 returns top 2 per parent', async () => {
    const rows = await fetchPosts([1, 2, 3]);
    const desc: BatchFetchDescriptor = {
      relationName: 'comments',
      type: 'one-to-many',
      targetTable: 'comments',
      fields: ['id', 'seq'],
      isInverse: true,
      mappedBy: 'post',
      fkColumn: 'postId',
      userLimit: 2,
      userSort: 'seq',
    };
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
    );

    const post1Comments = rows.find((r) => r.id === 1).comments;
    const post2Comments = rows.find((r) => r.id === 2).comments;
    const post3Comments = rows.find((r) => r.id === 3).comments;
    expect(post1Comments.length).toBe(2);
    expect(post2Comments.length).toBe(2);
    expect(post3Comments.length).toBe(1);
    expect(post1Comments.map((c: any) => c.seq)).toEqual([1, 2]);
  });

  test('limit 1 with sort DESC picks last comment', async () => {
    const rows = await fetchPosts([1]);
    const desc: BatchFetchDescriptor = {
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
    );
    expect(rows[0].comments.length).toBe(1);
    expect(rows[0].comments[0].seq).toBe(3);
  });

  test('limit + page (page 2) returns correct window', async () => {
    const rows = await fetchPosts([1]);
    const desc: BatchFetchDescriptor = {
      relationName: 'comments',
      type: 'one-to-many',
      targetTable: 'comments',
      fields: ['id', 'seq'],
      isInverse: true,
      mappedBy: 'post',
      fkColumn: 'postId',
      userLimit: 2,
      userPage: 2,
      userSort: 'seq',
    };
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
    );
    expect(rows[0].comments.length).toBe(1);
    expect(rows[0].comments[0].seq).toBe(3);
  });

  test('filter + limit combined', async () => {
    const rows = await fetchPosts([1]);
    const desc: BatchFetchDescriptor = {
      relationName: 'comments',
      type: 'one-to-many',
      targetTable: 'comments',
      fields: ['id', 'isPublished'],
      isInverse: true,
      mappedBy: 'post',
      fkColumn: 'postId',
      userFilter: { isPublished: { _eq: true } },
      userLimit: 1,
      userSort: 'seq',
    };
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
    );
    expect(rows[0].comments.length).toBe(1);
    expect(rows[0].comments[0].isPublished).toBe(1);
  });
});

describe('deep limit on m2m (SQL)', () => {
  test('limit 2 on tags per post', async () => {
    const rows = await fetchPosts([1, 2]);
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
      userSort: 'priority',
    };
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
    );

    const post1Tags = rows.find((r) => r.id === 1).tags;
    const post2Tags = rows.find((r) => r.id === 2).tags;
    expect(post1Tags.length).toBe(2);
    expect(post2Tags.length).toBe(2);
  });

  test('filter on tags label', async () => {
    const rows = await fetchPosts([1]);
    const desc: BatchFetchDescriptor = {
      relationName: 'tags',
      type: 'many-to-many',
      targetTable: 'tags',
      fields: ['id', 'label'],
      isInverse: false,
      junctionTableName: 'posts_tags',
      junctionSourceColumn: 'postId',
      junctionTargetColumn: 'tagId',
      userFilter: { label: { _eq: 'alpha' } },
    };
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
    );
    const tags = rows[0].tags;
    expect(tags.length).toBe(1);
    expect(tags[0].label).toBe('alpha');
  });
});

describe('deep dotted sort (SQL)', () => {
  test('sort o2m posts by author.name', async () => {
    const desc: BatchFetchDescriptor = {
      relationName: 'posts',
      type: 'one-to-many',
      targetTable: 'posts',
      fields: ['id', 'title', 'authorId'],
      isInverse: true,
      mappedBy: 'author',
      fkColumn: 'authorId',
      userSort: 'title',
    };

    const userRows = [
      { id: 1, name: 'Alice', active: 1, companyId: 1 },
      { id: 2, name: 'Bob', active: 0, companyId: 2 },
    ];

    await executeBatchFetches(
      db,
      userRows,
      [desc],
      metadataGetter,
      3,
      0,
      'users',
      'sqlite',
      META,
    );
    const alice = userRows.find((u) => u.id === 1);
    const bob = userRows.find((u) => u.id === 2);
    expect(alice!.posts.length).toBe(1);
    expect(alice!.posts[0].title).toBe('Post A');
    expect(bob!.posts.length).toBe(1);
  });
});

describe('deep fields override (SQL)', () => {
  test('fields override only returns specified fields', async () => {
    const rows = await fetchPosts([1]);
    const desc: BatchFetchDescriptor = {
      relationName: 'comments',
      type: 'one-to-many',
      targetTable: 'comments',
      fields: ['id', 'body'],
      isInverse: true,
      mappedBy: 'post',
      fkColumn: 'postId',
    };
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
    );
    const c = rows[0].comments[0];
    expect(c.id).toBeDefined();
    expect(c.body).toBeDefined();
    expect(c.isPublished).toBeUndefined();
    expect(c.seq).toBeUndefined();
  });
});
