import { MongoClient, Db, ObjectId } from 'mongodb';
import {
  executeMongoBatchFetches,
  MongoBatchFetchDescriptor,
} from '../../src/infrastructure/query-builder/utils/mongo/batch-relation-fetcher';

const MONGO_URI =
  process.env.MONGO_TEST_URI ||
  'mongodb://enfyra_admin:enfyra_password_123@localhost:27017/?authSource=admin';
const DB_NAME = `test_deep_mongo_${Date.now()}`;

let client: MongoClient;
let db: Db;

const companyIds = [new ObjectId(), new ObjectId()];
const userIds = [new ObjectId(), new ObjectId(), new ObjectId()];
const postIds = [new ObjectId(), new ObjectId(), new ObjectId()];
const tagIds = [new ObjectId(), new ObjectId(), new ObjectId(), new ObjectId()];
const commentIds = [
  new ObjectId(),
  new ObjectId(),
  new ObjectId(),
  new ObjectId(),
  new ObjectId(),
  new ObjectId(),
  new ObjectId(),
];

const META: Record<string, any> = {
  posts: {
    name: 'posts',
    columns: [
      { name: '_id', type: 'objectid' },
      { name: 'title', type: 'varchar' },
      { name: 'author', type: 'objectid' },
    ],
    relations: [
      {
        propertyName: 'author',
        type: 'many-to-one',
        targetTableName: 'users',
        isInverse: false,
      },
      {
        propertyName: 'comments',
        type: 'one-to-many',
        targetTableName: 'comments',
        mappedBy: 'post',
        isInverse: true,
      },
      {
        propertyName: 'tags',
        type: 'many-to-many',
        targetTableName: 'tags',
        isInverse: false,
      },
    ],
  },
  users: {
    name: 'users',
    columns: [
      { name: '_id', type: 'objectid' },
      { name: 'name', type: 'varchar' },
      { name: 'active', type: 'boolean' },
    ],
    relations: [],
  },
  comments: {
    name: 'comments',
    columns: [
      { name: '_id', type: 'objectid' },
      { name: 'body', type: 'varchar' },
      { name: 'isPublished', type: 'boolean' },
      { name: 'seq', type: 'integer' },
      { name: 'post', type: 'objectid' },
    ],
    relations: [],
  },
  tags: {
    name: 'tags',
    columns: [
      { name: '_id', type: 'objectid' },
      { name: 'label', type: 'varchar' },
      { name: 'priority', type: 'integer' },
    ],
    relations: [],
  },
};

const junctionName = `tags_posts_tags`;

const metadata = { tables: new Map(Object.entries(META)) };
const metadataGetter = async (table: string) =>
  META[table] ? { ...META[table] } : null;

beforeAll(async () => {
  client = await MongoClient.connect(MONGO_URI);
  db = client.db(DB_NAME);

  await db.collection('users').insertMany([
    { _id: userIds[0], name: 'Alice', active: true },
    { _id: userIds[1], name: 'Bob', active: false },
    { _id: userIds[2], name: 'Charlie', active: true },
  ]);

  await db.collection('posts').insertMany([
    { _id: postIds[0], title: 'Post A', author: userIds[0] },
    { _id: postIds[1], title: 'Post B', author: userIds[1] },
    { _id: postIds[2], title: 'Post C', author: userIds[2] },
  ]);

  await db.collection('comments').insertMany([
    { _id: commentIds[0], body: 'Comment 1', isPublished: true, seq: 1, post: postIds[0] },
    { _id: commentIds[1], body: 'Comment 2', isPublished: false, seq: 2, post: postIds[0] },
    { _id: commentIds[2], body: 'Comment 3', isPublished: true, seq: 3, post: postIds[0] },
    { _id: commentIds[3], body: 'Comment 4', isPublished: true, seq: 1, post: postIds[1] },
    { _id: commentIds[4], body: 'Comment 5', isPublished: true, seq: 2, post: postIds[1] },
    { _id: commentIds[5], body: 'Comment 6', isPublished: false, seq: 3, post: postIds[1] },
    { _id: commentIds[6], body: 'Comment 7', isPublished: true, seq: 1, post: postIds[2] },
  ]);

  await db.collection('tags').insertMany([
    { _id: tagIds[0], label: 'alpha', priority: 3 },
    { _id: tagIds[1], label: 'beta', priority: 1 },
    { _id: tagIds[2], label: 'gamma', priority: 2 },
    { _id: tagIds[3], label: 'delta', priority: 4 },
  ]);

  await db.collection(junctionName).insertMany([
    { tags_id: tagIds[0], posts_id: postIds[0] },
    { tags_id: tagIds[1], posts_id: postIds[0] },
    { tags_id: tagIds[2], posts_id: postIds[0] },
    { tags_id: tagIds[1], posts_id: postIds[1] },
    { tags_id: tagIds[3], posts_id: postIds[1] },
    { tags_id: tagIds[0], posts_id: postIds[2] },
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
    author: userIds[idx] ?? userIds[0],
  }));
}

describe('deep filter on o2m (Mongo)', () => {
  test('filter isPublished=true on comments', async () => {
    const rows = makePosts([postIds[0], postIds[1]]);
    const desc: MongoBatchFetchDescriptor = {
      relationName: 'comments',
      type: 'one-to-many',
      targetTable: 'comments',
      fields: ['_id', 'body', 'isPublished'],
      isInverse: true,
      mappedBy: 'post',
      foreignField: 'post',
      userFilter: { isPublished: { _eq: true } },
    };
    await executeMongoBatchFetches(db, rows, [desc], metadataGetter, 3, 0, 'posts', metadata);

    const post0 = rows.find((r) => r._id === postIds[0]);
    const post1 = rows.find((r) => r._id === postIds[1]);
    expect(post0!.comments.every((c: any) => c.isPublished === true)).toBe(true);
    expect(post0!.comments.length).toBe(2);
    expect(post1!.comments.every((c: any) => c.isPublished === true)).toBe(true);
    expect(post1!.comments.length).toBe(2);
  });
});

describe('deep sort on o2m (Mongo)', () => {
  test('sort by seq DESC', async () => {
    const rows = makePosts([postIds[0]]);
    const desc: MongoBatchFetchDescriptor = {
      relationName: 'comments',
      type: 'one-to-many',
      targetTable: 'comments',
      fields: ['_id', 'seq'],
      isInverse: true,
      mappedBy: 'post',
      foreignField: 'post',
      userSort: '-seq',
    };
    await executeMongoBatchFetches(db, rows, [desc], metadataGetter, 3, 0, 'posts', metadata);
    const seqs = rows[0].comments.map((c: any) => c.seq);
    expect(seqs).toEqual([3, 2, 1]);
  });
});

describe('deep limit on o2m (Mongo)', () => {
  test('limit 2 returns top 2 per parent', async () => {
    const rows = makePosts([postIds[0], postIds[1], postIds[2]]);
    const desc: MongoBatchFetchDescriptor = {
      relationName: 'comments',
      type: 'one-to-many',
      targetTable: 'comments',
      fields: ['_id', 'seq'],
      isInverse: true,
      mappedBy: 'post',
      foreignField: 'post',
      userLimit: 2,
      userSort: 'seq',
    };
    await executeMongoBatchFetches(db, rows, [desc], metadataGetter, 3, 0, 'posts', metadata);

    expect(rows[0].comments.length).toBe(2);
    expect(rows[1].comments.length).toBe(2);
    expect(rows[2].comments.length).toBe(1);
    expect(rows[0].comments.map((c: any) => c.seq)).toEqual([1, 2]);
  });

  test('limit 1 with sort DESC picks last', async () => {
    const rows = makePosts([postIds[0]]);
    const desc: MongoBatchFetchDescriptor = {
      relationName: 'comments',
      type: 'one-to-many',
      targetTable: 'comments',
      fields: ['_id', 'seq'],
      isInverse: true,
      mappedBy: 'post',
      foreignField: 'post',
      userLimit: 1,
      userSort: '-seq',
    };
    await executeMongoBatchFetches(db, rows, [desc], metadataGetter, 3, 0, 'posts', metadata);
    expect(rows[0].comments.length).toBe(1);
    expect(rows[0].comments[0].seq).toBe(3);
  });

  test('limit + page 2 returns second page', async () => {
    const rows = makePosts([postIds[0]]);
    const desc: MongoBatchFetchDescriptor = {
      relationName: 'comments',
      type: 'one-to-many',
      targetTable: 'comments',
      fields: ['_id', 'seq'],
      isInverse: true,
      mappedBy: 'post',
      foreignField: 'post',
      userLimit: 2,
      userPage: 2,
      userSort: 'seq',
    };
    await executeMongoBatchFetches(db, rows, [desc], metadataGetter, 3, 0, 'posts', metadata);
    expect(rows[0].comments.length).toBe(1);
    expect(rows[0].comments[0].seq).toBe(3);
  });

  test('filter + limit combined', async () => {
    const rows = makePosts([postIds[0]]);
    const desc: MongoBatchFetchDescriptor = {
      relationName: 'comments',
      type: 'one-to-many',
      targetTable: 'comments',
      fields: ['_id', 'isPublished'],
      isInverse: true,
      mappedBy: 'post',
      foreignField: 'post',
      userFilter: { isPublished: { _eq: true } },
      userLimit: 1,
      userSort: 'seq',
    };
    await executeMongoBatchFetches(db, rows, [desc], metadataGetter, 3, 0, 'posts', metadata);
    expect(rows[0].comments.length).toBe(1);
    expect(rows[0].comments[0].isPublished).toBe(true);
  });
});

describe('deep filter on m2o (Mongo)', () => {
  test('filter active=true on author', async () => {
    const rows = [
      { _id: postIds[0], title: 'Post A', author: userIds[0] },
      { _id: postIds[1], title: 'Post B', author: userIds[1] },
    ];
    const desc: MongoBatchFetchDescriptor = {
      relationName: 'author',
      type: 'many-to-one',
      targetTable: 'users',
      fields: ['_id', 'name', 'active'],
      isInverse: false,
      userFilter: { active: { _eq: true } },
    };
    await executeMongoBatchFetches(db, rows, [desc], metadataGetter, 3, 0, 'posts', metadata);

    const post0 = rows.find((r) => r._id === postIds[0]);
    const post1 = rows.find((r) => r._id === postIds[1]);
    expect(post0!.author).not.toBeNull();
    expect(post0!.author.active).toBe(true);
    expect(post1!.author).toBeNull();
  });
});

describe('deep limit on m2m (Mongo)', () => {
  test('limit 2 on tags per post', async () => {
    const rows = makePosts([postIds[0], postIds[1]]);
    const tableMeta = META['posts'];
    const relMeta = tableMeta.relations.find((r: any) => r.propertyName === 'tags');
    const desc: MongoBatchFetchDescriptor = {
      relationName: 'tags',
      type: 'many-to-many',
      targetTable: 'tags',
      fields: ['_id', 'label'],
      isInverse: false,
      userLimit: 2,
      userSort: 'priority',
    };
    await executeMongoBatchFetches(db, rows, [desc], metadataGetter, 3, 0, 'posts', metadata);

    const post0Tags = rows[0].tags;
    const post1Tags = rows[1].tags;
    expect(post0Tags.length).toBeLessThanOrEqual(2);
    expect(post1Tags.length).toBeLessThanOrEqual(2);
  });
});

describe('debug trace (Mongo)', () => {
  test('trace emits batch_fetch entry', async () => {
    const { BatchFetchEngine, PER_PARENT_CONCURRENCY } = await import(
      '../../src/infrastructure/query-builder/utils/shared/batch-fetch-engine'
    );
    const { MongoBatchAdapter } = await import(
      '../../src/infrastructure/query-builder/utils/mongo/mongo-batch-adapter'
    );

    const traceEntries: any[] = [];
    const mockTrace = {
      dur(stage: string, startTs: number, meta?: any) {
        traceEntries.push({ stage, meta });
        return 0;
      },
    };

    const rows = makePosts([postIds[0]]);
    const desc: MongoBatchFetchDescriptor = {
      relationName: 'comments',
      type: 'one-to-many',
      targetTable: 'comments',
      fields: ['_id', 'seq'],
      isInverse: true,
      mappedBy: 'post',
      foreignField: 'post',
      userLimit: 2,
    };

    const adapter = new MongoBatchAdapter(db, metadata);
    const engine = new BatchFetchEngine(adapter, metadataGetter, mockTrace);
    await engine.execute(rows, [desc], 3, 0, 'posts');

    const traceEntry = traceEntries.find((e) =>
      e.stage.startsWith('batch_fetch_L0_comments'),
    );
    expect(traceEntry).toBeDefined();
    expect(traceEntry.meta.strategy).toBe('per-parent-c16');
    expect(traceEntry.meta.userLimit).toBe(2);
  });
});
