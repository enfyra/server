import { MongoClient, ObjectId } from 'mongodb';
import { BootstrapUnitOfWorkService } from '../../src/engines/bootstrap';
import {
  MongoSagaCoordinator,
  MongoSagaLockService,
  MongoSagaSnapshotService,
  MongoService,
} from '../../src/engines/mongo';
import { InstanceService } from '../../src/shared/services';

const MONGO_URI =
  process.env.MATRIX_MONGO_URI ??
  'mongodb://enfyra_admin:enfyra_password_123@localhost:27017/enfyra_test?authSource=admin';

describe('Mongo bootstrap unit of work', () => {
  let client: MongoClient;

  beforeAll(async () => {
    client = new MongoClient(MONGO_URI, {
      serverSelectionTimeoutMS: 2000,
      connectTimeoutMS: 2000,
    });
    await client.connect();
    await client.db('admin').command({ ping: 1 });
  });

  afterAll(async () => {
    await client.close();
  });

  it('rolls documents, collections, renames, and indexes back as one saga', async () => {
    const db = client.db(`bootstrap_uow_${Date.now()}`);
    const baseCollection = 'records';
    const droppedCollection = 'legacy_records';
    const createdCollection = 'new_records';
    const renamedCollection = 'renamed_records';
    const originalId = new ObjectId();
    const lazyRef: Record<string, any> = {};
    const mongoService = new MongoService({ lazyRef } as any);
    Object.defineProperty(mongoService, 'db', { value: db });
    Object.defineProperty(mongoService, 'client', { value: client });
    Object.defineProperty(mongoService, 'nativeMultiDocSupported', {
      value: false,
      writable: true,
    });
    const lockService = new MongoSagaLockService({ mongoService });
    const snapshotService = new MongoSagaSnapshotService({ mongoService });
    lazyRef.mongoSagaCoordinator = new MongoSagaCoordinator({
      mongoService,
      lockService,
      snapshotService,
      instanceService: new InstanceService(),
      cacheService: undefined,
    });
    const unitOfWork = new BootstrapUnitOfWorkService({
      databaseConfigService: { isMongoDb: () => true },
      knexService: {},
      mongoService,
    } as any);

    await db.collection(baseCollection).insertOne({
      _id: originalId,
      value: 'before',
      legacy: 'keep',
    });
    await db
      .collection(baseCollection)
      .createIndex({ legacy: 1 }, { name: 'legacy_1' });
    await db.collection(droppedCollection).insertOne({ value: 'restore-me' });

    await expect(
      unitOfWork.run(async () => {
        const scopedDb = mongoService.getDb();
        await scopedDb
          .collection(baseCollection)
          .updateOne({ _id: originalId }, { $set: { value: 'after' } });
        await scopedDb.collection(baseCollection).dropIndex('legacy_1');
        await scopedDb
          .collection(baseCollection)
          .createIndex({ value: 1 }, { name: 'value_1' });
        await scopedDb.createCollection(createdCollection);
        await scopedDb.collection(createdCollection).insertOne({ value: 1 });
        await scopedDb.dropCollection(droppedCollection);
        await scopedDb.collection(baseCollection).rename(renamedCollection);
        throw new Error('fail after structural migrations');
      }),
    ).rejects.toThrow('fail after structural migrations');

    expect(
      await db.collection(baseCollection).findOne({ _id: originalId }),
    ).toMatchObject({ value: 'before', legacy: 'keep' });
    expect(
      await db.listCollections({ name: renamedCollection }).hasNext(),
    ).toBe(false);
    expect(
      await db.listCollections({ name: createdCollection }).hasNext(),
    ).toBe(false);
    expect(await db.collection(droppedCollection).countDocuments({})).toBe(1);
    const indexNames = (
      await db.collection(baseCollection).listIndexes().toArray()
    ).map((index) => index.name);
    expect(indexNames).toContain('legacy_1');
    expect(indexNames).not.toContain('value_1');
    expect(
      await db.collection('system_saga_snapshots').countDocuments({}),
    ).toBe(0);

    await db.dropDatabase();
  });
});
