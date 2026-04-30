import { MongoClient, ObjectId, type Db } from 'mongodb';
import { afterAll, beforeAll, describe, expect, test, vi } from 'vitest';
import { MongoPhysicalMigrationService } from '../../src/engines/mongo';
import { MongoQueryExecutor, QueryPlanner } from '@enfyra/kernel';

const MONGO_URI =
  process.env.MONGO_TEST_URI ||
  'mongodb://enfyra_admin:enfyra_password_123@localhost:27017/?authSource=admin';
const DB_NAME = `test_mongo_physical_migration_${Date.now()}`;

async function probeMongo(): Promise<boolean> {
  try {
    const client = new MongoClient(MONGO_URI, { serverSelectionTimeoutMS: 2000 });
    await client.connect();
    await client.close();
    return true;
  } catch {
    return false;
  }
}

function makeService(db: Db, add = vi.fn()) {
  return new MongoPhysicalMigrationService({
    mongoService: {
      getDb: () => db,
    } as any,
    databaseConfigService: {
      isMongoDb: () => true,
    } as any,
    envService: {
      get: (key: string) =>
        key === 'NODE_NAME'
          ? 'test_node'
          : 'redis://localhost:6379',
    } as any,
    mongoPhysicalMigrationQueue: {
      add,
    } as any,
  });
}

function makeMetadata() {
  const postsTable = {
    id: 1,
    name: 'posts',
    columns: [
      { id: 1, name: '_id', type: 'mixed' },
      { id: 2, name: 'title', type: 'string' },
      { id: 3, name: 'author', type: 'mixed' },
    ],
    relations: [
      {
        propertyName: 'author',
        type: 'many-to-one',
        targetTable: 'authors',
        targetTableName: 'authors',
        foreignKeyColumn: 'author',
      },
    ],
  };
  const authorsTable = {
    id: 2,
    name: 'authors',
    columns: [
      { id: 1, name: '_id', type: 'mixed' },
      { id: 2, name: 'title', type: 'string' },
    ],
    relations: [],
  };
  const tables = new Map<string, any>();
  tables.set('posts', postsTable);
  tables.set('authors', authorsTable);
  return { tables };
}

describe('MongoPhysicalMigrationService', () => {
  let available = false;
  let client: MongoClient;
  let db: Db;

  beforeAll(async () => {
    available = await probeMongo();
    if (!available) return;
    client = new MongoClient(MONGO_URI);
    await client.connect();
    db = client.db(DB_NAME);
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

  runOrSkip('queues field rename records and processes physical rename in batches', async () => {
    await db.collection('posts').insertMany([
      { _id: new ObjectId(), old_title: 'one' },
      { _id: new ObjectId(), old_title: 'two' },
    ]);

    const add = vi.fn().mockResolvedValue({});
    const service = makeService(db, add);
    await service.enqueueFieldRenames('posts', [
      { oldName: 'old_title', newName: 'title' },
    ]);

    expect(add).toHaveBeenCalledWith(
      'mongo-field-rename',
      expect.objectContaining({ migrationId: expect.any(String) }),
      expect.objectContaining({
        jobId: expect.stringContaining('mongo-field-rename:'),
      }),
    );

    const migration = await db
      .collection('schema_physical_migration_definition')
      .findOne({ tableName: 'posts', oldName: 'old_title', newName: 'title' });
    expect(migration?.status).toBe('pending');

    const result = await (service as any).processFieldRename(
      migration?.migrationId,
    );
    expect(result.processed).toBe(2);

    const rows = await db.collection('posts').find().toArray();
    expect(rows.map((row) => row.title).sort()).toEqual(['one', 'two']);
    expect(rows.some((row) => row.old_title !== undefined)).toBe(false);

    const completed = await db
      .collection('schema_physical_migration_definition')
      .findOne({ migrationId: migration?.migrationId });
    expect(completed?.status).toBe('completed');
  });

  runOrSkip('query executor reads renamed field through pending migration fallback', async () => {
    await db.collection('posts').deleteMany({});
    await db.collection('schema_physical_migration_definition').deleteMany({});
    await db.collection('posts').insertOne({
      _id: new ObjectId(),
      old_title: 'visible through title',
    });
    await db.collection('schema_physical_migration_definition').insertOne({
      migrationId: 'pending-read-fallback',
      kind: 'field_rename',
      tableName: 'posts',
      oldName: 'old_title',
      newName: 'title',
      status: 'pending',
      processed: 0,
      conflictCount: 0,
      createdAt: new Date().toISOString(),
      updatedAt: new Date().toISOString(),
    });

    const service = makeService(db);
    const executor = new MongoQueryExecutor(
      {
        getDb: () => db,
        collection: (name: string) => db.collection(name),
      } as any,
      service,
    );
    const planner = new QueryPlanner();
    const base = {
      tableName: 'posts',
      fields: ['_id', 'title'],
      metadata: makeMetadata(),
      dbType: 'mongodb' as any,
    };
    const plan = planner.plan(base);
    const result = await executor.execute({ ...base, plan });

    expect(result.data).toHaveLength(1);
    expect(result.data[0].title).toBe('visible through title');
    expect(result.data[0].old_title).toBeUndefined();
  });

  runOrSkip('batch relation fetch reads renamed child fields through fallback', async () => {
    await db.collection('posts').deleteMany({});
    await db.collection('authors').deleteMany({});
    await db.collection('schema_physical_migration_definition').deleteMany({});
    const authorId = new ObjectId();
    await db.collection('authors').insertOne({
      _id: authorId,
      old_title: 'child title',
    });
    await db.collection('posts').insertOne({
      _id: new ObjectId(),
      author: authorId,
    });
    await db.collection('schema_physical_migration_definition').insertOne({
      migrationId: 'pending-child-read-fallback',
      kind: 'field_rename',
      tableName: 'authors',
      oldName: 'old_title',
      newName: 'title',
      status: 'pending',
      processed: 0,
      conflictCount: 0,
      createdAt: new Date().toISOString(),
      updatedAt: new Date().toISOString(),
    });

    const service = makeService(db);
    const executor = new MongoQueryExecutor(
      {
        getDb: () => db,
        collection: (name: string) => db.collection(name),
      } as any,
      service,
    );
    const planner = new QueryPlanner();
    const base = {
      tableName: 'posts',
      fields: ['_id', 'author.title'],
      metadata: makeMetadata(),
      dbType: 'mongodb' as any,
    };
    const plan = planner.plan(base);
    const result = await executor.execute({ ...base, plan });

    expect(result.data).toHaveLength(1);
    expect(result.data[0].author.title).toBe('child title');
    expect(result.data[0].author.old_title).toBeUndefined();
  });
});
