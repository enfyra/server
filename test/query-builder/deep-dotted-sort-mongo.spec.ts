import { MongoClient, Db, ObjectId } from 'mongodb';
import {
  executeMongoBatchFetches,
  MongoBatchFetchDescriptor,
} from '../../src/infrastructure/query-builder/utils/mongo/batch-relation-fetcher';
import { BatchTrace } from '../../src/infrastructure/query-builder/utils/shared/batch-fetch-engine';

const MONGO_URI =
  process.env.MONGO_TEST_URI ||
  'mongodb://enfyra_admin:enfyra_password_123@localhost:27017/?authSource=admin';
const DB_NAME = `test_deep_dotted_${Date.now()}`;

let client: MongoClient;
let db: Db;

const companyIds = [new ObjectId(), new ObjectId()];
const userIds = [new ObjectId(), new ObjectId(), new ObjectId()];
const postIds = [new ObjectId(), new ObjectId()];
const commentIds = Array.from({ length: 6 }, () => new ObjectId());

const META: Record<string, any> = {
  posts: {
    name: 'posts',
    columns: [
      { name: '_id', type: 'objectid' },
      { name: 'title', type: 'varchar' },
    ],
    relations: [
      {
        propertyName: 'comments',
        type: 'one-to-many',
        targetTableName: 'comments',
        mappedBy: 'post',
        isInverse: true,
      },
    ],
  },
  comments: {
    name: 'comments',
    columns: [
      { name: '_id', type: 'objectid' },
      { name: 'body', type: 'varchar' },
      { name: 'seq', type: 'integer' },
      { name: 'post', type: 'objectid' },
      { name: 'author', type: 'objectid' },
    ],
    relations: [
      {
        propertyName: 'post',
        type: 'many-to-one',
        targetTableName: 'posts',
        isInverse: false,
      },
      {
        propertyName: 'author',
        type: 'many-to-one',
        targetTableName: 'users',
        isInverse: false,
      },
    ],
  },
  users: {
    name: 'users',
    columns: [
      { name: '_id', type: 'objectid' },
      { name: 'name', type: 'varchar' },
      { name: 'company', type: 'objectid' },
    ],
    relations: [
      {
        propertyName: 'company',
        type: 'many-to-one',
        targetTableName: 'companies',
        isInverse: false,
      },
    ],
  },
  companies: {
    name: 'companies',
    columns: [
      { name: '_id', type: 'objectid' },
      { name: 'name', type: 'varchar' },
    ],
    relations: [],
  },
};

const metadata = { tables: new Map(Object.entries(META)) };
const metadataGetter = async (table: string) =>
  META[table] ? { ...META[table] } : null;

class TestTrace implements BatchTrace {
  entries: Array<{ stage: string; ms: number; meta?: Record<string, unknown> }> = [];
  dur(stage: string, startTs: number, meta?: Record<string, unknown>): number {
    const ms = performance.now() - startTs;
    this.entries.push({ stage, ms, meta });
    return ms;
  }
}

beforeAll(async () => {
  client = await MongoClient.connect(MONGO_URI);
  db = client.db(DB_NAME);

  await db.collection('companies').insertMany([
    { _id: companyIds[0], name: 'Acme' },
    { _id: companyIds[1], name: 'Beta' },
  ]);

  await db.collection('users').insertMany([
    { _id: userIds[0], name: 'Alice', company: companyIds[0] },
    { _id: userIds[1], name: 'Bob', company: companyIds[1] },
    { _id: userIds[2], name: 'Charlie', company: companyIds[0] },
  ]);

  await db.collection('posts').insertMany([
    { _id: postIds[0], title: 'Post A' },
    { _id: postIds[1], title: 'Post B' },
  ]);

  await db.collection('comments').insertMany([
    { _id: commentIds[0], body: 'c0', seq: 1, post: postIds[0], author: userIds[1] },
    { _id: commentIds[1], body: 'c1', seq: 2, post: postIds[0], author: userIds[0] },
    { _id: commentIds[2], body: 'c2', seq: 3, post: postIds[0], author: userIds[2] },
    { _id: commentIds[3], body: 'c3', seq: 1, post: postIds[1], author: userIds[2] },
    { _id: commentIds[4], body: 'c4', seq: 2, post: postIds[1], author: userIds[1] },
    { _id: commentIds[5], body: 'c5', seq: 3, post: postIds[1], author: userIds[0] },
  ]);
});

afterAll(async () => {
  await db.dropDatabase();
  await client.close();
});

function makePosts(ids: ObjectId[]) {
  return ids.map((id, idx) => ({
    _id: id,
    title: `Post ${idx}`,
  }));
}

describe('Mongo dotted sort on o2m (aggregate path)', () => {
  test('sort comments ASC by author.name within each post', async () => {
    const rows = makePosts(postIds);
    const desc: MongoBatchFetchDescriptor = {
      relationName: 'comments',
      type: 'one-to-many',
      targetTable: 'comments',
      fields: ['_id', 'body', 'seq'],
      isInverse: true,
      mappedBy: 'post',
      foreignField: 'post',
      userSort: 'author.name',
    };
    await executeMongoBatchFetches(
      db,
      rows,
      [desc],
      metadataGetter,
      3,
      0,
      'posts',
      metadata,
    );
    const post0Bodies = rows[0].comments.map((c: any) => c.body);
    const post1Bodies = rows[1].comments.map((c: any) => c.body);
    expect(post0Bodies).toEqual(['c1', 'c0', 'c2']);
    expect(post1Bodies).toEqual(['c5', 'c4', 'c3']);
  });

  test('sort comments DESC by author.name within each post', async () => {
    const rows = makePosts(postIds);
    const desc: MongoBatchFetchDescriptor = {
      relationName: 'comments',
      type: 'one-to-many',
      targetTable: 'comments',
      fields: ['_id', 'body'],
      isInverse: true,
      mappedBy: 'post',
      foreignField: 'post',
      userSort: '-author.name',
    };
    await executeMongoBatchFetches(
      db,
      rows,
      [desc],
      metadataGetter,
      3,
      0,
      'posts',
      metadata,
    );
    expect(rows[0].comments.map((c: any) => c.body)).toEqual(['c2', 'c0', 'c1']);
    expect(rows[1].comments.map((c: any) => c.body)).toEqual(['c3', 'c4', 'c5']);
  });

  test('dotted sort + limit per-parent returns top-k by sort key', async () => {
    const rows = makePosts(postIds);
    const desc: MongoBatchFetchDescriptor = {
      relationName: 'comments',
      type: 'one-to-many',
      targetTable: 'comments',
      fields: ['_id', 'body'],
      isInverse: true,
      mappedBy: 'post',
      foreignField: 'post',
      userSort: 'author.name',
      userLimit: 2,
    };
    await executeMongoBatchFetches(
      db,
      rows,
      [desc],
      metadataGetter,
      3,
      0,
      'posts',
      metadata,
    );
    expect(rows[0].comments.length).toBe(2);
    expect(rows[0].comments.map((c: any) => c.body)).toEqual(['c1', 'c0']);
    expect(rows[1].comments.length).toBe(2);
    expect(rows[1].comments.map((c: any) => c.body)).toEqual(['c5', 'c4']);
  });

  test('2-hop dotted sort: author.company.name', async () => {
    const rows = makePosts(postIds);
    const desc: MongoBatchFetchDescriptor = {
      relationName: 'comments',
      type: 'one-to-many',
      targetTable: 'comments',
      fields: ['_id', 'body'],
      isInverse: true,
      mappedBy: 'post',
      foreignField: 'post',
      userSort: 'author.company.name',
    };
    await executeMongoBatchFetches(
      db,
      rows,
      [desc],
      metadataGetter,
      3,
      0,
      'posts',
      metadata,
    );
    const post0Bodies = rows[0].comments.map((c: any) => c.body);
    expect(post0Bodies[0] === 'c1' || post0Bodies[0] === 'c2').toBe(true);
    expect(post0Bodies[post0Bodies.length - 1]).toBe('c0');
  });

  test('dotted sort + filter combined (filter applied after join)', async () => {
    const rows = makePosts([postIds[0]]);
    const desc: MongoBatchFetchDescriptor = {
      relationName: 'comments',
      type: 'one-to-many',
      targetTable: 'comments',
      fields: ['_id', 'body', 'seq'],
      isInverse: true,
      mappedBy: 'post',
      foreignField: 'post',
      userFilter: { seq: { _gte: 2 } },
      userSort: 'author.name',
    };
    await executeMongoBatchFetches(
      db,
      rows,
      [desc],
      metadataGetter,
      3,
      0,
      'posts',
      metadata,
    );
    const bodies = rows[0].comments.map((c: any) => c.body);
    expect(bodies).toEqual(['c1', 'c2']);
  });

  test('dotted sort emits trace with strategy reflecting aggregate path', async () => {
    const rows = makePosts(postIds);
    const trace = new TestTrace();
    const desc: MongoBatchFetchDescriptor = {
      relationName: 'comments',
      type: 'one-to-many',
      targetTable: 'comments',
      fields: ['_id', 'body'],
      isInverse: true,
      mappedBy: 'post',
      foreignField: 'post',
      userSort: 'author.name',
      userLimit: 2,
    };
    await executeMongoBatchFetches(
      db,
      rows,
      [desc],
      metadataGetter,
      3,
      0,
      'posts',
      metadata,
      trace,
    );
    const entry = trace.entries.find((e) => e.stage.includes('comments'));
    expect(entry).toBeDefined();
    expect(entry!.meta?.strategy).toBe('per-parent-c16');
    expect(entry!.meta?.userSort).toBe(true);
  });

  test('simple sort without dots stays on find() path (no aggregate)', async () => {
    const rows = makePosts([postIds[0]]);
    const desc: MongoBatchFetchDescriptor = {
      relationName: 'comments',
      type: 'one-to-many',
      targetTable: 'comments',
      fields: ['_id', 'body'],
      isInverse: true,
      mappedBy: 'post',
      foreignField: 'post',
      userSort: '-seq',
    };
    await executeMongoBatchFetches(
      db,
      rows,
      [desc],
      metadataGetter,
      3,
      0,
      'posts',
      metadata,
    );
    const bodies = rows[0].comments.map((c: any) => c.body);
    expect(bodies).toEqual(['c2', 'c1', 'c0']);
  });
});
