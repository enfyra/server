import { MongoClient, Db, ObjectId } from 'mongodb';
import {
  MongoService,
  MongoSagaLockService,
  MongoOperationLogService,
  MongoSagaCoordinator,
} from 'src/engine/mongo';
import { InstanceService } from 'src/shared/services';

const MONGO_URI =
  'mongodb://enfyra_admin:enfyra_password_123@localhost:27017/enfyra_test?authSource=admin';

interface IBenchmarkResult {
  name: string;
  durationMs: number;
  operationsCount: number;
  avgOpDurationMs: number;
  overheadVsBaseline?: number;
}

describe('MongoDB Saga System - Integration Tests', () => {
  let mongoClient: MongoClient;
  let db: Db;
  let mongoService: MongoService;
  let lockService: MongoSagaLockService;
  let logService: MongoOperationLogService;
  let coordinator: MongoSagaCoordinator;
  const benchmarkResults: IBenchmarkResult[] = [];

  const COLLECTIONS = {
    orders: 'test_orders',
    products: 'test_products',
    users: 'test_users',
    locks: 'system_transaction_locks',
    meta: 'system_transaction_metadata',
    logs: 'system_operation_logs',
    counters: 'system_operation_counters',
  };

  beforeAll(async () => {
    try {
      const probe = new MongoClient(MONGO_URI, {
        serverSelectionTimeoutMS: 2000,
        connectTimeoutMS: 2000,
      });
      await probe.connect();
      await probe.db('admin').command({ ping: 1 });
      await probe.close();
    } catch {
      throw new Error('MongoDB not available - skipping integration tests');
    }
    mongoClient = new MongoClient(MONGO_URI);
    await mongoClient.connect();
    db = mongoClient.db('enfyra_test');

    // Manual dependency injection
    const envService = {
      get: (key: string) => {
        if (key === 'DB_URI') return MONGO_URI;
        return process.env[key];
      },
    } as any;

    const instanceService = new InstanceService();
    mongoService = new MongoService({ envService });
    Object.defineProperty(mongoService, 'db', { value: db });
    Object.defineProperty(mongoService, 'client', { value: mongoClient });

    lockService = new MongoSagaLockService({ mongoService });
    logService = new MongoOperationLogService({ mongoService });

    coordinator = new MongoSagaCoordinator({
      mongoService,
      lockService,
      logService,
      instanceService,
      cacheService: undefined,
    });

    await cleanupAllCollections();
  });

  afterAll(async () => {
    printBenchmarkResults();
    await mongoClient.close();
  });

  async function cleanupAllCollections() {
    for (const name of Object.values(COLLECTIONS)) {
      try {
        await db.collection(name).deleteMany({});
      } catch {}
    }
  }

  function printBenchmarkResults() {
    console.log('\n========== BENCHMARK RESULTS ==========\n');
    benchmarkResults.forEach((result) => {
      console.log(`${result.name}:`);
      console.log(`  Total: ${result.durationMs.toFixed(2)}ms`);
      console.log(`  Ops: ${result.operationsCount}`);
      console.log(`  Avg/op: ${result.avgOpDurationMs.toFixed(2)}ms`);
      if (result.overheadVsBaseline) {
        console.log(`  Overhead: ${result.overheadVsBaseline.toFixed(2)}x`);
      }
      console.log('');
    });
  }

  // ====================================================================
  // CRUD: INSERT
  // ====================================================================
  describe('CRUD: Insert Operations', () => {
    beforeEach(async () => {
      await db.collection(COLLECTIONS.orders).deleteMany({});
    });

    it('should insert a single document and return it with _id', async () => {
      const result = await coordinator.execute(async (tx) => {
        return tx.insertOne(COLLECTIONS.orders, {
          customerId: 'cust-1',
          total: 100,
          status: 'pending',
        });
      });

      expect(result.success).toBe(true);
      expect(result.data._id).toBeInstanceOf(ObjectId);
      expect(result.data.id).toBeDefined();
      expect(result.data.customerId).toBe('cust-1');
      expect(result.data.total).toBe(100);
      expect(result.stats?.operationsCount).toBe(1);

      const doc = await db
        .collection(COLLECTIONS.orders)
        .findOne({ _id: result.data._id });
      expect(doc).toBeDefined();
      expect(doc!.customerId).toBe('cust-1');
      expect(doc!.__txId).toBeUndefined();
    });

    it('should insert multiple documents sequentially', async () => {
      const result = await coordinator.execute(async (tx) => {
        const orders = [];
        for (let i = 1; i <= 5; i++) {
          orders.push(
            await tx.insertOne(COLLECTIONS.orders, {
              customerId: `seq-${i}`,
              total: i * 100,
            }),
          );
        }
        return orders;
      });

      expect(result.success).toBe(true);
      expect(result.data).toHaveLength(5);

      const count = await db.collection(COLLECTIONS.orders).countDocuments();
      expect(count).toBe(5);
    });

    it('should insertMany in batch', async () => {
      const docs = Array.from({ length: 20 }, (_, i) => ({
        customerId: `batch-${i}`,
        total: i * 10,
        status: 'pending',
      }));

      const result = await coordinator.execute(async (tx) => {
        return tx.insertMany(COLLECTIONS.orders, docs);
      });

      expect(result.success).toBe(true);
      expect(result.data).toHaveLength(20);

      const dbDocs = await db.collection(COLLECTIONS.orders).find({}).toArray();
      expect(dbDocs).toHaveLength(20);
      expect(dbDocs.every((d) => d.__txId === undefined)).toBe(true);
    });

    it('should insertMany with empty array', async () => {
      const result = await coordinator.execute(async (tx) => {
        return tx.insertMany(COLLECTIONS.orders, []);
      });

      expect(result.success).toBe(true);
      expect(result.data).toHaveLength(0);
    });

    it('should rollback insert on error', async () => {
      const countBefore = await db
        .collection(COLLECTIONS.orders)
        .countDocuments();

      const result = await coordinator.execute(async (tx) => {
        await tx.insertOne(COLLECTIONS.orders, {
          customerId: 'will-rollback',
          total: 999,
        });
        throw new Error('Force rollback');
      });

      expect(result.success).toBe(false);
      const countAfter = await db
        .collection(COLLECTIONS.orders)
        .countDocuments();
      expect(countAfter).toBe(countBefore);
    });

    it('should rollback insertMany on error', async () => {
      const countBefore = await db
        .collection(COLLECTIONS.orders)
        .countDocuments();

      const result = await coordinator.execute(async (tx) => {
        await tx.insertMany(COLLECTIONS.orders, [
          { customerId: 'batch-rollback-1' },
          { customerId: 'batch-rollback-2' },
          { customerId: 'batch-rollback-3' },
        ]);
        throw new Error('Force rollback');
      });

      expect(result.success).toBe(false);
      const countAfter = await db
        .collection(COLLECTIONS.orders)
        .countDocuments();
      expect(countAfter).toBe(countBefore);
    });
  });

  // ====================================================================
  // CRUD: UPDATE
  // ====================================================================
  describe('CRUD: Update Operations', () => {
    let seedIds: ObjectId[];

    beforeEach(async () => {
      await db.collection(COLLECTIONS.orders).deleteMany({});
      const inserts = await db.collection(COLLECTIONS.orders).insertMany([
        { customerId: 'u-1', total: 100, status: 'pending' },
        { customerId: 'u-2', total: 200, status: 'pending' },
        { customerId: 'u-3', total: 300, status: 'pending' },
        { customerId: 'u-4', total: 400, status: 'pending' },
        { customerId: 'u-5', total: 500, status: 'pending' },
      ]);
      seedIds = Object.values(inserts.insertedIds);
    });

    it('should update a single document by id', async () => {
      const result = await coordinator.execute(async (tx) => {
        return tx.updateOne(COLLECTIONS.orders, seedIds[0], {
          status: 'completed',
          total: 150,
        });
      });

      expect(result.success).toBe(true);
      expect(result.data.status).toBe('completed');
      expect(result.data.total).toBe(150);

      const doc = await db
        .collection(COLLECTIONS.orders)
        .findOne({ _id: seedIds[0] });
      expect(doc!.__txId).toBeUndefined();
    });

    it('should update by filter (operator style)', async () => {
      const result = await coordinator.execute(async (tx) => {
        return tx.updateOneByFilter(
          COLLECTIONS.orders,
          { customerId: 'u-2' },
          { $set: { status: 'shipped' }, $inc: { total: 50 } },
        );
      });

      expect(result.success).toBe(true);
      const doc = await db
        .collection(COLLECTIONS.orders)
        .findOne({ customerId: 'u-2' });
      expect(doc!.status).toBe('shipped');
      expect(doc!.total).toBe(250);
    });

    it('should update by filter (plain object)', async () => {
      const result = await coordinator.execute(async (tx) => {
        return tx.updateOneByFilter(
          COLLECTIONS.orders,
          { customerId: 'u-3' },
          { status: 'cancelled' },
        );
      });

      expect(result.success).toBe(true);
      const doc = await db
        .collection(COLLECTIONS.orders)
        .findOne({ customerId: 'u-3' });
      expect(doc!.status).toBe('cancelled');
    });

    it('should updateOneByFilter return no-op for non-existent filter', async () => {
      const result = await coordinator.execute(async (tx) => {
        return tx.updateOneByFilter(
          COLLECTIONS.orders,
          { customerId: 'non-existent' },
          { status: 'x' },
        );
      });

      expect(result.success).toBe(true);
      expect(result.data.matchedCount).toBe(0);
    });

    it('should updateManyByFilter', async () => {
      const result = await coordinator.execute(async (tx) => {
        return tx.updateManyByFilter(
          COLLECTIONS.orders,
          { status: 'pending' },
          { status: 'processing' },
        );
      });

      expect(result.success).toBe(true);
      expect(result.data.matchedCount).toBe(5);

      const docs = await db.collection(COLLECTIONS.orders).find({}).toArray();
      expect(docs.every((d) => d.status === 'processing')).toBe(true);
    });

    it('should batch updateMany by id', async () => {
      const updates = seedIds.slice(0, 3).map((id, i) => ({
        id,
        data: { status: `batch-${i}` },
      }));

      const result = await coordinator.execute(async (tx) => {
        return tx.updateMany(COLLECTIONS.orders, updates);
      });

      expect(result.success).toBe(true);
      expect(result.data).toHaveLength(3);

      for (let i = 0; i < 3; i++) {
        const doc = await db
          .collection(COLLECTIONS.orders)
          .findOne({ _id: seedIds[i] });
        expect(doc!.status).toBe(`batch-${i}`);
        expect(doc!.__txId).toBeUndefined();
      }
    });

    it('should rollback update on error', async () => {
      const originalDoc = await db
        .collection(COLLECTIONS.orders)
        .findOne({ _id: seedIds[0] });

      const result = await coordinator.execute(async (tx) => {
        await tx.updateOne(COLLECTIONS.orders, seedIds[0], {
          status: 'will-rollback',
          total: 99999,
        });
        throw new Error('Force rollback');
      });

      expect(result.success).toBe(false);
      const doc = await db
        .collection(COLLECTIONS.orders)
        .findOne({ _id: seedIds[0] });
      expect(doc!.status).toBe(originalDoc!.status);
      expect(doc!.total).toBe(originalDoc!.total);
    });

    it('should rollback batch updateMany on error', async () => {
      const originals = await db
        .collection(COLLECTIONS.orders)
        .find({ _id: { $in: seedIds.slice(0, 3) } })
        .toArray();

      const result = await coordinator.execute(async (tx) => {
        await tx.updateMany(
          COLLECTIONS.orders,
          seedIds.slice(0, 3).map((id) => ({
            id,
            data: { status: 'will-rollback' },
          })),
        );
        throw new Error('Force rollback');
      });

      expect(result.success).toBe(false);
      for (const orig of originals) {
        const doc = await db
          .collection(COLLECTIONS.orders)
          .findOne({ _id: orig._id });
        expect(doc!.status).toBe(orig.status);
      }
    });

    it('should throw on update of non-existent document', async () => {
      const fakeId = new ObjectId();
      const result = await coordinator.execute(async (tx) => {
        return tx.updateOne(COLLECTIONS.orders, fakeId, { status: 'x' });
      });

      expect(result.success).toBe(false);
      expect(result.error?.message).toContain('Document not found');
    });
  });

  // ====================================================================
  // CRUD: DELETE
  // ====================================================================
  describe('CRUD: Delete Operations', () => {
    let seedIds: ObjectId[];

    beforeEach(async () => {
      await db.collection(COLLECTIONS.orders).deleteMany({});
      const inserts = await db.collection(COLLECTIONS.orders).insertMany([
        { customerId: 'd-1', total: 100 },
        { customerId: 'd-2', total: 200 },
        { customerId: 'd-3', total: 300 },
        { customerId: 'd-4', total: 400 },
        { customerId: 'd-5', total: 500 },
      ]);
      seedIds = Object.values(inserts.insertedIds);
    });

    it('should delete a single document', async () => {
      const result = await coordinator.execute(async (tx) => {
        return tx.deleteOne(COLLECTIONS.orders, seedIds[0]);
      });

      expect(result.success).toBe(true);
      expect(result.data).toBe(true);

      const doc = await db
        .collection(COLLECTIONS.orders)
        .findOne({ _id: seedIds[0] });
      expect(doc).toBeNull();
    });

    it('should return false for deleting non-existent document', async () => {
      const fakeId = new ObjectId();
      const result = await coordinator.execute(async (tx) => {
        return tx.deleteOne(COLLECTIONS.orders, fakeId);
      });

      expect(result.success).toBe(true);
      expect(result.data).toBe(false);
    });

    it('should deleteMany in batch', async () => {
      const idsToDelete = seedIds.slice(0, 3);

      const result = await coordinator.execute(async (tx) => {
        return tx.deleteMany(COLLECTIONS.orders, idsToDelete);
      });

      expect(result.success).toBe(true);
      expect(result.data.deletedCount).toBe(3);
      expect(result.data.deletedIds).toHaveLength(3);

      const remaining = await db
        .collection(COLLECTIONS.orders)
        .countDocuments();
      expect(remaining).toBe(2);
    });

    it('should deleteMany with empty array', async () => {
      const result = await coordinator.execute(async (tx) => {
        return tx.deleteMany(COLLECTIONS.orders, []);
      });

      expect(result.success).toBe(true);
      expect(result.data.deletedCount).toBe(0);
    });

    it('should rollback delete on error', async () => {
      const countBefore = await db
        .collection(COLLECTIONS.orders)
        .countDocuments();

      const result = await coordinator.execute(async (tx) => {
        await tx.deleteOne(COLLECTIONS.orders, seedIds[0]);
        const check = await db
          .collection(COLLECTIONS.orders)
          .findOne({ _id: seedIds[0] });
        expect(check).toBeNull();
        throw new Error('Force rollback');
      });

      expect(result.success).toBe(false);
      const countAfter = await db
        .collection(COLLECTIONS.orders)
        .countDocuments();
      expect(countAfter).toBe(countBefore);

      const restoredDoc = await db
        .collection(COLLECTIONS.orders)
        .findOne({ _id: seedIds[0] });
      expect(restoredDoc).not.toBeNull();
      expect(restoredDoc!.customerId).toBe('d-1');
    });

    it('should rollback deleteMany on error', async () => {
      const countBefore = await db
        .collection(COLLECTIONS.orders)
        .countDocuments();

      const result = await coordinator.execute(async (tx) => {
        await tx.deleteMany(COLLECTIONS.orders, seedIds.slice(0, 3));
        throw new Error('Force rollback');
      });

      expect(result.success).toBe(false);
      const countAfter = await db
        .collection(COLLECTIONS.orders)
        .countDocuments();
      expect(countAfter).toBe(countBefore);
    });
  });

  // ====================================================================
  // CRUD: READ
  // ====================================================================
  describe('CRUD: Read Operations', () => {
    let seedIds: ObjectId[];

    beforeEach(async () => {
      await db.collection(COLLECTIONS.orders).deleteMany({});
      const inserts = await db.collection(COLLECTIONS.orders).insertMany(
        Array.from({ length: 10 }, (_, i) => ({
          customerId: `r-${i}`,
          total: (i + 1) * 100,
          status: i < 5 ? 'active' : 'archived',
        })),
      );
      seedIds = Object.values(inserts.insertedIds);
    });

    it('should findOne by _id', async () => {
      const result = await coordinator.execute(async (tx) => {
        return tx.findOne(COLLECTIONS.orders, { _id: seedIds[0] });
      });

      expect(result.success).toBe(true);
      expect(result.data.customerId).toBe('r-0');
    });

    it('should findOne by filter', async () => {
      const result = await coordinator.execute(async (tx) => {
        return tx.findOne(COLLECTIONS.orders, { customerId: 'r-5' });
      });

      expect(result.success).toBe(true);
      expect(result.data.total).toBe(600);
    });

    it('should findOne return null for non-existent', async () => {
      const result = await coordinator.execute(async (tx) => {
        return tx.findOne(COLLECTIONS.orders, { customerId: 'non-existent' });
      });

      expect(result.success).toBe(true);
      expect(result.data).toBeNull();
    });

    it('should findOne with consistent read (acquires read lock)', async () => {
      const result = await coordinator.execute(async (tx) => {
        return tx.findOne(
          COLLECTIONS.orders,
          { _id: seedIds[0] },
          { useConsistentRead: true },
        );
      });

      expect(result.success).toBe(true);
      expect(result.data.customerId).toBe('r-0');
    });

    it('should find multiple documents', async () => {
      const result = await coordinator.execute(async (tx) => {
        return tx.find(COLLECTIONS.orders, { status: 'active' });
      });

      expect(result.success).toBe(true);
      expect(result.data).toHaveLength(5);
    });

    it('should find with limit and skip', async () => {
      const result = await coordinator.execute(async (tx) => {
        return tx.find(COLLECTIONS.orders, {}, { limit: 3, skip: 2 });
      });

      expect(result.success).toBe(true);
      expect(result.data).toHaveLength(3);
    });

    it('should find with consistent read (re-reads after locking)', async () => {
      const result = await coordinator.execute(async (tx) => {
        return tx.find(
          COLLECTIONS.orders,
          { status: 'active' },
          { useConsistentRead: true },
        );
      });

      expect(result.success).toBe(true);
      expect(result.data).toHaveLength(5);
    });

    it('should parallelRead multiple collections', async () => {
      await db.collection(COLLECTIONS.products).deleteMany({});
      await db
        .collection(COLLECTIONS.products)
        .insertOne({ name: 'Widget', price: 25 });

      const result = await coordinator.execute(async (tx) => {
        return tx.parallelRead([
          { collection: COLLECTIONS.orders, filter: { customerId: 'r-0' } },
          { collection: COLLECTIONS.products, filter: { name: 'Widget' } },
        ]);
      });

      expect(result.success).toBe(true);
      expect(result.data).toHaveLength(2);
      expect(result.data[0].customerId).toBe('r-0');
      expect(result.data[1].name).toBe('Widget');
    });
  });

  // ====================================================================
  // MIXED OPERATIONS
  // ====================================================================
  describe('Mixed Operations in Single Transaction', () => {
    beforeEach(async () => {
      await db.collection(COLLECTIONS.orders).deleteMany({});
      await db.collection(COLLECTIONS.products).deleteMany({});
    });

    it('should handle insert + update + delete in one transaction', async () => {
      const existing = await db.collection(COLLECTIONS.orders).insertOne({
        customerId: 'existing',
        total: 1000,
        status: 'pending',
      });

      const result = await coordinator.execute(async (tx) => {
        const newOrder = await tx.insertOne(COLLECTIONS.orders, {
          customerId: 'new-order',
          total: 500,
        });

        await tx.updateOne(COLLECTIONS.orders, existing.insertedId, {
          status: 'processing',
        });

        await tx.deleteOne(COLLECTIONS.orders, existing.insertedId);

        return newOrder;
      });

      expect(result.success).toBe(true);
      expect(result.data.customerId).toBe('new-order');

      const deleted = await db
        .collection(COLLECTIONS.orders)
        .findOne({ _id: existing.insertedId });
      expect(deleted).toBeNull();

      const count = await db.collection(COLLECTIONS.orders).countDocuments();
      expect(count).toBe(1);
    });

    it('should handle cross-collection operations', async () => {
      const result = await coordinator.execute(async (tx) => {
        const order = await tx.insertOne(COLLECTIONS.orders, {
          customerId: 'cross-coll',
          total: 100,
        });

        const product = await tx.insertOne(COLLECTIONS.products, {
          name: 'Widget',
          orderId: order._id.toString(),
        });

        return { order, product };
      });

      expect(result.success).toBe(true);
      expect(result.data.order.customerId).toBe('cross-coll');
      expect(result.data.product.name).toBe('Widget');

      const orderCount = await db
        .collection(COLLECTIONS.orders)
        .countDocuments();
      const productCount = await db
        .collection(COLLECTIONS.products)
        .countDocuments();
      expect(orderCount).toBe(1);
      expect(productCount).toBe(1);
    });

    it('should rollback cross-collection operations on error', async () => {
      const ordersBefore = await db
        .collection(COLLECTIONS.orders)
        .countDocuments();
      const productsBefore = await db
        .collection(COLLECTIONS.products)
        .countDocuments();

      const result = await coordinator.execute(async (tx) => {
        await tx.insertOne(COLLECTIONS.orders, {
          customerId: 'rollback-cross',
        });
        await tx.insertOne(COLLECTIONS.products, { name: 'rollback-product' });
        throw new Error('Force rollback');
      });

      expect(result.success).toBe(false);
      expect(await db.collection(COLLECTIONS.orders).countDocuments()).toBe(
        ordersBefore,
      );
      expect(await db.collection(COLLECTIONS.products).countDocuments()).toBe(
        productsBefore,
      );
    });
  });

  // ====================================================================
  // ROLLBACK & CHECKPOINTS
  // ====================================================================
  describe('Rollback and Checkpoints', () => {
    beforeEach(async () => {
      await db.collection(COLLECTIONS.orders).deleteMany({});
    });

    it('should auto-rollback on error with correct stats', async () => {
      const result = await coordinator.execute(async (tx) => {
        await tx.insertOne(COLLECTIONS.orders, { customerId: 'rb-1' });
        await tx.insertOne(COLLECTIONS.orders, { customerId: 'rb-2' });
        await tx.insertOne(COLLECTIONS.orders, { customerId: 'rb-3' });
        throw new Error('Intentional error');
      });

      expect(result.success).toBe(false);
      expect(result.rollbackResult).toBeDefined();
      expect(result.rollbackResult!.success).toBe(true);
      expect(result.rollbackResult!.rolledBackOperations.length).toBe(3);
      expect(result.rollbackResult!.failedOperations).toHaveLength(0);

      const count = await db.collection(COLLECTIONS.orders).countDocuments();
      expect(count).toBe(0);
    });

    it('should support checkpoint and partial rollback', async () => {
      const result = await coordinator.execute(async (tx) => {
        await tx.insertOne(COLLECTIONS.orders, { customerId: 'cp-1' });

        const cp = await tx.createCheckpoint('after-first');

        await tx.insertOne(COLLECTIONS.orders, { customerId: 'cp-2' });
        await tx.insertOne(COLLECTIONS.orders, { customerId: 'cp-3' });

        await tx.rollbackToCheckpoint(cp);

        return 'done';
      });

      expect(result.success).toBe(true);

      const docs = await db.collection(COLLECTIONS.orders).find({}).toArray();
      expect(docs).toHaveLength(1);
      expect(docs[0].customerId).toBe('cp-1');
    });

    it('should support multiple checkpoints', async () => {
      const result = await coordinator.execute(async (tx) => {
        await tx.insertOne(COLLECTIONS.orders, { customerId: 'mcp-1' });
        await tx.createCheckpoint('first');

        await tx.insertOne(COLLECTIONS.orders, { customerId: 'mcp-2' });
        const cp2 = await tx.createCheckpoint('second');

        await tx.insertOne(COLLECTIONS.orders, { customerId: 'mcp-3' });

        await tx.rollbackToCheckpoint(cp2);
        return 'done';
      });

      expect(result.success).toBe(true);
      const docs = await db
        .collection(COLLECTIONS.orders)
        .find({ customerId: { $regex: /^mcp-/ } })
        .sort({ customerId: 1 })
        .toArray();
      expect(docs).toHaveLength(2);
      expect(docs[0].customerId).toBe('mcp-1');
      expect(docs[1].customerId).toBe('mcp-2');
    });

    it('should rollback update restoring original data', async () => {
      const inserted = await db.collection(COLLECTIONS.orders).insertOne({
        customerId: 'rb-update',
        total: 100,
        status: 'original',
        tags: ['a', 'b'],
      });

      const result = await coordinator.execute(async (tx) => {
        await tx.updateOne(COLLECTIONS.orders, inserted.insertedId, {
          status: 'modified',
          total: 999,
          tags: ['x'],
        });
        throw new Error('Force rollback');
      });

      expect(result.success).toBe(false);
      const doc = await db
        .collection(COLLECTIONS.orders)
        .findOne({ _id: inserted.insertedId });
      expect(doc!.status).toBe('original');
      expect(doc!.total).toBe(100);
      expect(doc!.tags).toEqual(['a', 'b']);
    });

    it('should rollback delete restoring full document', async () => {
      const inserted = await db.collection(COLLECTIONS.orders).insertOne({
        customerId: 'rb-delete',
        total: 500,
        nested: { key: 'value' },
      });

      const result = await coordinator.execute(async (tx) => {
        await tx.deleteOne(COLLECTIONS.orders, inserted.insertedId);
        throw new Error('Force rollback');
      });

      expect(result.success).toBe(false);
      const doc = await db
        .collection(COLLECTIONS.orders)
        .findOne({ _id: inserted.insertedId });
      expect(doc).not.toBeNull();
      expect(doc!.customerId).toBe('rb-delete');
      expect(doc!.nested).toEqual({ key: 'value' });
    });

    it('should get transaction stats', async () => {
      const result = await coordinator.execute(async (tx) => {
        await tx.insertOne(COLLECTIONS.orders, { customerId: 'stats-1' });
        await tx.insertOne(COLLECTIONS.orders, { customerId: 'stats-2' });

        const stats = await tx.getStats();
        expect(stats.total).toBeGreaterThanOrEqual(2);
        return stats;
      });

      expect(result.success).toBe(true);
    });
  });

  // ====================================================================
  // __txId CLEANUP
  // ====================================================================
  describe('__txId Cleanup on Commit', () => {
    beforeEach(async () => {
      await db.collection(COLLECTIONS.orders).deleteMany({});
    });

    it('should remove __txId from inserted documents after commit', async () => {
      const result = await coordinator.execute(async (tx) => {
        return tx.insertOne(COLLECTIONS.orders, { customerId: 'txid-cleanup' });
      });

      expect(result.success).toBe(true);
      const doc = await db
        .collection(COLLECTIONS.orders)
        .findOne({ _id: result.data._id });
      expect(doc!.__txId).toBeUndefined();
    });

    it('should remove __txId from updated documents after commit', async () => {
      const inserted = await db
        .collection(COLLECTIONS.orders)
        .insertOne({ customerId: 'before-update' });

      await coordinator.execute(async (tx) => {
        return tx.updateOne(COLLECTIONS.orders, inserted.insertedId, {
          customerId: 'after-update',
        });
      });

      const doc = await db
        .collection(COLLECTIONS.orders)
        .findOne({ _id: inserted.insertedId });
      expect(doc!.__txId).toBeUndefined();
      expect(doc!.customerId).toBe('after-update');
    });

    it('should remove __txId from batch operations after commit', async () => {
      const result = await coordinator.execute(async (tx) => {
        return tx.insertMany(COLLECTIONS.orders, [
          { customerId: 'batch-txid-1' },
          { customerId: 'batch-txid-2' },
          { customerId: 'batch-txid-3' },
        ]);
      });

      expect(result.success).toBe(true);
      const docs = await db.collection(COLLECTIONS.orders).find({}).toArray();
      expect(docs.every((d) => d.__txId === undefined)).toBe(true);
    });

    it('should not leave __txId on cross-collection commit', async () => {
      await db.collection(COLLECTIONS.products).deleteMany({});

      await coordinator.execute(async (tx) => {
        await tx.insertOne(COLLECTIONS.orders, { customerId: 'cross-txid' });
        await tx.insertOne(COLLECTIONS.products, { name: 'product-txid' });
      });

      const order = await db
        .collection(COLLECTIONS.orders)
        .findOne({ customerId: 'cross-txid' });
      const product = await db
        .collection(COLLECTIONS.products)
        .findOne({ name: 'product-txid' });
      expect(order!.__txId).toBeUndefined();
      expect(product!.__txId).toBeUndefined();
    });
  });

  // ====================================================================
  // CONCURRENCY & LOCKING
  // ====================================================================
  describe('Concurrency and Locking', () => {
    beforeEach(async () => {
      await db.collection(COLLECTIONS.orders).deleteMany({});
      await db.collection(COLLECTIONS.locks).deleteMany({});
      await db.collection(COLLECTIONS.meta).deleteMany({});
    });

    it('should handle concurrent transactions on different resources', async () => {
      const promises = Array.from({ length: 5 }, (_, i) =>
        coordinator.execute(async (tx) => {
          await tx.insertOne(COLLECTIONS.orders, {
            customerId: `concurrent-${i}`,
            total: i * 100,
          });
          return { index: i };
        }),
      );

      const results = await Promise.all(promises);
      expect(results.every((r) => r.success)).toBe(true);

      const count = await db.collection(COLLECTIONS.orders).countDocuments();
      expect(count).toBe(5);
    });

    it('should allow shared read locks from multiple transactions', async () => {
      const doc = await db.collection(COLLECTIONS.orders).insertOne({
        customerId: 'shared-read',
        total: 100,
      });

      const results = await Promise.all([
        coordinator.execute(async (tx) => {
          return tx.findOne(
            COLLECTIONS.orders,
            { _id: doc.insertedId },
            { useConsistentRead: true },
          );
        }),
        coordinator.execute(async (tx) => {
          return tx.findOne(
            COLLECTIONS.orders,
            { _id: doc.insertedId },
            { useConsistentRead: true },
          );
        }),
        coordinator.execute(async (tx) => {
          return tx.findOne(
            COLLECTIONS.orders,
            { _id: doc.insertedId },
            { useConsistentRead: true },
          );
        }),
      ]);

      expect(results.every((r) => r.success)).toBe(true);
      expect(results.every((r) => r.data?.customerId === 'shared-read')).toBe(
        true,
      );
    });

    it('should detect and prevent deadlock scenarios', async () => {
      const order1 = await db
        .collection(COLLECTIONS.orders)
        .insertOne({ customerId: 'deadlock-1' });
      const order2 = await db
        .collection(COLLECTIONS.orders)
        .insertOne({ customerId: 'deadlock-2' });

      const [result1, result2] = await Promise.all([
        coordinator.execute(
          async (tx) => {
            await tx.updateOne(COLLECTIONS.orders, order1.insertedId, {
              status: 'tx1-held',
            });
            await new Promise((r) => setTimeout(r, 100));
            await tx.updateOne(COLLECTIONS.orders, order2.insertedId, {
              status: 'tx1-wants',
            });
          },
          { maxRetries: 2, waitTimeout: 500 },
        ),
        coordinator.execute(
          async (tx) => {
            await new Promise((r) => setTimeout(r, 50));
            await tx.updateOne(COLLECTIONS.orders, order2.insertedId, {
              status: 'tx2-held',
            });
            await tx.updateOne(COLLECTIONS.orders, order1.insertedId, {
              status: 'tx2-wants',
            });
          },
          { maxRetries: 2, waitTimeout: 500 },
        ),
      ]);

      const oneSucceeded = result1.success || result2.success;
      expect(oneSucceeded).toBe(true);
    });

    it('should serialize concurrent writes to same document', async () => {
      const doc = await db.collection(COLLECTIONS.orders).insertOne({
        customerId: 'serialize-test',
        counter: 0,
      });

      const results = [];
      for (let i = 0; i < 5; i++) {
        const result = await coordinator.execute(async (tx) => {
          const current = await tx.findOne(COLLECTIONS.orders, {
            _id: doc.insertedId,
          });
          await tx.updateOne(COLLECTIONS.orders, doc.insertedId, {
            counter: current.counter + 1,
          });
          return current.counter + 1;
        });
        results.push(result);
      }

      expect(results.every((r) => r.success)).toBe(true);
      const final = await db
        .collection(COLLECTIONS.orders)
        .findOne({ _id: doc.insertedId });
      expect(final!.counter).toBe(5);
    });
  });

  // ====================================================================
  // EDGE CASES
  // ====================================================================
  describe('Edge Cases', () => {
    beforeEach(async () => {
      await db.collection(COLLECTIONS.orders).deleteMany({});
    });

    it('should handle empty transaction (no operations)', async () => {
      const result = await coordinator.execute(async () => {
        return 'empty-tx';
      });

      expect(result.success).toBe(true);
      expect(result.data).toBe('empty-tx');
    });

    it('should handle large batch insert', async () => {
      const docs = Array.from({ length: 100 }, (_, i) => ({
        customerId: `large-batch-${i}`,
        total: i,
      }));

      const result = await coordinator.execute(async (tx) => {
        return tx.insertMany(COLLECTIONS.orders, docs);
      });

      expect(result.success).toBe(true);
      expect(result.data).toHaveLength(100);

      const count = await db.collection(COLLECTIONS.orders).countDocuments();
      expect(count).toBe(100);
    });

    it('should handle documents with nested objects', async () => {
      const result = await coordinator.execute(async (tx) => {
        return tx.insertOne(COLLECTIONS.orders, {
          customerId: 'nested-test',
          shipping: {
            address: { street: '123 Main', city: 'Anytown' },
            tracking: [{ carrier: 'UPS', code: '1Z999' }],
          },
          metadata: { createdBy: 'test', tags: ['urgent', 'express'] },
        });
      });

      expect(result.success).toBe(true);
      const doc = await db
        .collection(COLLECTIONS.orders)
        .findOne({ _id: result.data._id });
      expect(doc!.shipping.address.city).toBe('Anytown');
      expect(doc!.shipping.tracking).toHaveLength(1);
    });

    it('should handle insert-then-update-then-delete of same doc in one tx', async () => {
      const result = await coordinator.execute(async (tx) => {
        const doc = await tx.insertOne(COLLECTIONS.orders, {
          customerId: 'lifecycle',
          status: 'created',
        });
        await tx.updateOne(COLLECTIONS.orders, doc._id, { status: 'updated' });
        await tx.deleteOne(COLLECTIONS.orders, doc._id);
        return doc._id;
      });

      expect(result.success).toBe(true);
      const doc = await db
        .collection(COLLECTIONS.orders)
        .findOne({ _id: result.data });
      expect(doc).toBeNull();
    });

    it('should rollback insert-then-update-then-delete correctly', async () => {
      const countBefore = await db
        .collection(COLLECTIONS.orders)
        .countDocuments();

      const result = await coordinator.execute(async (tx) => {
        const doc = await tx.insertOne(COLLECTIONS.orders, {
          customerId: 'lifecycle-rb',
        });
        await tx.updateOne(COLLECTIONS.orders, doc._id, { status: 'updated' });
        await tx.deleteOne(COLLECTIONS.orders, doc._id);
        throw new Error('Force rollback');
      });

      expect(result.success).toBe(false);
      const countAfter = await db
        .collection(COLLECTIONS.orders)
        .countDocuments();
      expect(countAfter).toBe(countBefore);
    });

    it('should handle string ids correctly', async () => {
      const inserted = await db
        .collection(COLLECTIONS.orders)
        .insertOne({ customerId: 'string-id-test' });
      const idString = inserted.insertedId.toString();

      const result = await coordinator.execute(async (tx) => {
        return tx.updateOne(COLLECTIONS.orders, idString, {
          status: 'updated-via-string',
        });
      });

      expect(result.success).toBe(true);
      expect(result.data.status).toBe('updated-via-string');
    });
  });

  // ====================================================================
  // __txId NEVER LEAKS TO RESPONSES
  // ====================================================================
  describe('__txId Never Leaks to Responses', () => {
    beforeEach(async () => {
      await db.collection(COLLECTIONS.orders).deleteMany({});
    });

    it('should strip __txId from insertOne return value', async () => {
      const result = await coordinator.execute(async (tx) => {
        return tx.insertOne(COLLECTIONS.orders, { customerId: 'leak-test' });
      });

      expect(result.success).toBe(true);
      expect(result.data.__txId).toBeUndefined();
    });

    it('should strip __txId from updateOne return value', async () => {
      const ins = await db
        .collection(COLLECTIONS.orders)
        .insertOne({ customerId: 'leak-upd' });
      const result = await coordinator.execute(async (tx) => {
        return tx.updateOne(COLLECTIONS.orders, ins.insertedId, {
          status: 'updated',
        });
      });

      expect(result.success).toBe(true);
      expect(result.data.__txId).toBeUndefined();
    });

    it('should strip __txId from findOne return value during transaction', async () => {
      const result = await coordinator.execute(async (tx) => {
        const doc = await tx.insertOne(COLLECTIONS.orders, {
          customerId: 'leak-find',
        });
        return tx.findOne(COLLECTIONS.orders, { _id: doc._id });
      });

      expect(result.success).toBe(true);
      expect(result.data.__txId).toBeUndefined();
    });

    it('should strip __txId from find return values during transaction', async () => {
      const result = await coordinator.execute(async (tx) => {
        await tx.insertOne(COLLECTIONS.orders, { customerId: 'leak-find-1' });
        await tx.insertOne(COLLECTIONS.orders, { customerId: 'leak-find-2' });
        return tx.find(COLLECTIONS.orders, {});
      });

      expect(result.success).toBe(true);
      expect(result.data.every((d: any) => d.__txId === undefined)).toBe(true);
    });

    it('should strip __txId from insertMany return values', async () => {
      const result = await coordinator.execute(async (tx) => {
        return tx.insertMany(COLLECTIONS.orders, [
          { customerId: 'leak-batch-1' },
          { customerId: 'leak-batch-2' },
        ]);
      });

      expect(result.success).toBe(true);
      expect(result.data.every((d: any) => d.__txId === undefined)).toBe(true);
    });

    it('should strip __txId from updateMany return values', async () => {
      const ins = await db
        .collection(COLLECTIONS.orders)
        .insertMany([{ customerId: 'leak-um-1' }, { customerId: 'leak-um-2' }]);
      const ids = Object.values(ins.insertedIds);

      const result = await coordinator.execute(async (tx) => {
        return tx.updateMany(
          COLLECTIONS.orders,
          ids.map((id) => ({ id, data: { status: 'x' } })),
        );
      });

      expect(result.success).toBe(true);
      expect(result.data.every((d: any) => d.__txId === undefined)).toBe(true);
    });

    it('should strip __txId from parallelRead return values', async () => {
      await db
        .collection(COLLECTIONS.orders)
        .insertOne({ customerId: 'leak-pr', __txId: 'stale-tx' });

      const result = await coordinator.execute(async (tx) => {
        return tx.parallelRead([
          { collection: COLLECTIONS.orders, filter: { customerId: 'leak-pr' } },
        ]);
      });

      expect(result.success).toBe(true);
      expect(result.data[0].__txId).toBeUndefined();
    });
  });

  // ====================================================================
  // NESTED TRANSACTION GUARD
  // ====================================================================
  describe('Nested Saga Guard', () => {
    it('should reject nested saga calls', async () => {
      const result = await coordinator.execute(async (_tx) => {
        const innerResult = await coordinator.execute(async (_innerTx) => {
          return 'should not reach here';
        });
        return innerResult;
      });

      expect(result.success).toBe(false);
      expect(result.error?.message).toContain(
        'Nested saga executions are not supported',
      );
    });
  });

  // ====================================================================
  // ORPHAN RECOVERY
  // ====================================================================
  describe('Orphan Recovery', () => {
    it('should recover stale __txId markers', async () => {
      await db.collection(COLLECTIONS.orders).deleteMany({});
      await db.collection(COLLECTIONS.orders).insertMany([
        { customerId: 'orphan-1', __txId: 'tx-dead-1' },
        { customerId: 'orphan-2', __txId: 'tx-dead-2' },
        { customerId: 'clean', total: 100 },
      ]);

      const result = await coordinator.recoverOrphanedSagas();
      expect(result.recovered).toBeGreaterThanOrEqual(2);

      const docs = await db.collection(COLLECTIONS.orders).find({}).toArray();
      expect(docs.every((d) => d.__txId === undefined)).toBe(true);
    });

    it('should not unset __txId while transaction metadata is active and fresh', async () => {
      await db.collection(COLLECTIONS.orders).deleteMany({});
      const liveTxId = await lockService.beginTransaction();
      await db.collection(COLLECTIONS.orders).insertOne({
        customerId: 'live-1',
        __txId: liveTxId,
      });

      const result = await coordinator.recoverOrphanedSagas();
      expect(result.recovered).toBe(0);

      const doc = await db
        .collection(COLLECTIONS.orders)
        .findOne({ customerId: 'live-1' });
      expect(doc?.__txId).toBe(liveTxId);

      await lockService.abortTransaction(liveTxId, 'test teardown');
      await db.collection(COLLECTIONS.orders).deleteMany({});
    });
  });

  // ====================================================================
  // SYSTEM COLLECTIONS
  // ====================================================================
  describe('System Collections Management', () => {
    it('should auto-create system collections on first use', async () => {
      const collections = await db.listCollections().toArray();
      const names = collections.map((c) => c.name);

      expect(names).toContain(COLLECTIONS.locks);
      expect(names).toContain(COLLECTIONS.meta);
      expect(names).toContain(COLLECTIONS.logs);
    });

    it('should cleanup expired locks and logs', async () => {
      const cleanedLocks = await lockService.cleanupOrphanedLocks();
      const oldLogs = await logService.cleanupOldLogs(0);

      expect(typeof cleanedLocks).toBe('number');
      expect(typeof oldLogs).toBe('number');
    });

    it('should detect deadlocks in the system', async () => {
      const deadlocks = await lockService.detectDeadlocks();
      expect(Array.isArray(deadlocks)).toBe(true);
    });

    it('should report resource lock status', async () => {
      const isLocked = await lockService.isResourceLocked(
        'test_orders',
        'some-id',
      );
      expect(typeof isLocked).toBe('boolean');
    });
  });

  // ====================================================================
  // TRANSACTION PLAN (batch lock + log + execute + complete)
  // ====================================================================
  describe('SagaPlan: Batched Operations', () => {
    beforeEach(async () => {
      await db.collection(COLLECTIONS.orders).deleteMany({});
      await db.collection(COLLECTIONS.products).deleteMany({});
    });

    it('should batch insert multiple documents', async () => {
      const result = await coordinator.execute(async (tx) => {
        const plan = tx.createSagaPlan();
        plan.insert(COLLECTIONS.orders, { customerId: 'plan-1', total: 100 });
        plan.insert(COLLECTIONS.orders, { customerId: 'plan-2', total: 200 });
        plan.insert(COLLECTIONS.orders, { customerId: 'plan-3', total: 300 });
        return plan.execute();
      });

      expect(result.success).toBe(true);
      const inserts = result.data.inserts.get(COLLECTIONS.orders);
      expect(inserts).toHaveLength(3);

      const count = await db.collection(COLLECTIONS.orders).countDocuments();
      expect(count).toBe(3);

      const docs = await db.collection(COLLECTIONS.orders).find({}).toArray();
      expect(docs.every((d) => d.__txId === undefined)).toBe(true);
    });

    it('should batch update multiple documents', async () => {
      const ins = await db.collection(COLLECTIONS.orders).insertMany([
        { customerId: 'pu-1', status: 'pending' },
        { customerId: 'pu-2', status: 'pending' },
        { customerId: 'pu-3', status: 'pending' },
      ]);
      const ids = Object.values(ins.insertedIds);

      const result = await coordinator.execute(async (tx) => {
        const plan = tx.createSagaPlan();
        plan.update(COLLECTIONS.orders, ids[0], { status: 'completed' });
        plan.update(COLLECTIONS.orders, ids[1], { status: 'shipped' });
        plan.update(COLLECTIONS.orders, ids[2], { status: 'cancelled' });
        return plan.execute();
      });

      expect(result.success).toBe(true);
      const docs = await db
        .collection(COLLECTIONS.orders)
        .find({ _id: { $in: ids } })
        .sort({ customerId: 1 })
        .toArray();
      expect(docs[0].status).toBe('completed');
      expect(docs[1].status).toBe('shipped');
      expect(docs[2].status).toBe('cancelled');
      expect(docs.every((d) => d.__txId === undefined)).toBe(true);
    });

    it('should batch delete multiple documents', async () => {
      const ins = await db
        .collection(COLLECTIONS.orders)
        .insertMany([
          { customerId: 'pd-1' },
          { customerId: 'pd-2' },
          { customerId: 'pd-3' },
        ]);
      const ids = Object.values(ins.insertedIds);

      const result = await coordinator.execute(async (tx) => {
        const plan = tx.createSagaPlan();
        plan.delete(COLLECTIONS.orders, ids[0]);
        plan.delete(COLLECTIONS.orders, ids[1]);
        return plan.execute();
      });

      expect(result.success).toBe(true);
      const remaining = await db
        .collection(COLLECTIONS.orders)
        .countDocuments();
      expect(remaining).toBe(1);
    });

    it('should handle mixed insert + update + delete in one plan', async () => {
      const ins = await db.collection(COLLECTIONS.orders).insertMany([
        { customerId: 'mix-1', status: 'pending' },
        { customerId: 'mix-2', status: 'pending' },
      ]);
      const ids = Object.values(ins.insertedIds);

      const result = await coordinator.execute(async (tx) => {
        const plan = tx.createSagaPlan();
        plan.insert(COLLECTIONS.orders, {
          customerId: 'mix-new',
          status: 'created',
        });
        plan.update(COLLECTIONS.orders, ids[0], { status: 'completed' });
        plan.delete(COLLECTIONS.orders, ids[1]);
        return plan.execute();
      });

      expect(result.success).toBe(true);
      const docs = await db
        .collection(COLLECTIONS.orders)
        .find({})
        .sort({ customerId: 1 })
        .toArray();
      expect(docs).toHaveLength(2);
      expect(docs.find((d) => d.customerId === 'mix-1')!.status).toBe(
        'completed',
      );
      expect(docs.find((d) => d.customerId === 'mix-new')!.status).toBe(
        'created',
      );
      expect(docs.find((d) => d.customerId === 'mix-2')).toBeUndefined();
    });

    it('should handle cross-collection plan', async () => {
      const result = await coordinator.execute(async (tx) => {
        const plan = tx.createSagaPlan();
        plan.insert(COLLECTIONS.orders, { customerId: 'cross-plan-order' });
        plan.insert(COLLECTIONS.products, { name: 'cross-plan-product' });
        return plan.execute();
      });

      expect(result.success).toBe(true);
      expect(await db.collection(COLLECTIONS.orders).countDocuments()).toBe(1);
      expect(await db.collection(COLLECTIONS.products).countDocuments()).toBe(
        1,
      );
    });

    it('should rollback plan on error', async () => {
      const ins = await db
        .collection(COLLECTIONS.orders)
        .insertOne({ customerId: 'plan-rb', status: 'original' });

      const result = await coordinator.execute(async (tx) => {
        const plan = tx.createSagaPlan();
        plan.insert(COLLECTIONS.orders, { customerId: 'plan-rb-new' });
        plan.update(COLLECTIONS.orders, ins.insertedId, { status: 'changed' });
        await plan.execute();
        throw new Error('Force plan rollback');
      });

      expect(result.success).toBe(false);
      expect(result.rollbackResult?.success).toBe(true);

      const doc = await db
        .collection(COLLECTIONS.orders)
        .findOne({ _id: ins.insertedId });
      expect(doc!.status).toBe('original');
      const count = await db.collection(COLLECTIONS.orders).countDocuments();
      expect(count).toBe(1);
    });

    it('should handle empty plan', async () => {
      const result = await coordinator.execute(async (tx) => {
        const plan = tx.createSagaPlan();
        return plan.execute();
      });

      expect(result.success).toBe(true);
    });

    it('should throw on update of non-existent document in plan', async () => {
      const fakeId = new ObjectId();
      const result = await coordinator.execute(async (tx) => {
        const plan = tx.createSagaPlan();
        plan.update(COLLECTIONS.orders, fakeId, { status: 'x' });
        return plan.execute();
      });

      expect(result.success).toBe(false);
      expect(result.error?.message).toContain('Document not found');
    });

    it('should mix plan with regular operations', async () => {
      const result = await coordinator.execute(async (tx) => {
        const doc = await tx.insertOne(COLLECTIONS.orders, {
          customerId: 'before-plan',
          total: 50,
        });

        const plan = tx.createSagaPlan();
        plan.insert(COLLECTIONS.orders, {
          customerId: 'plan-after',
          total: 100,
        });
        plan.update(COLLECTIONS.orders, doc._id, { total: 75 });
        await plan.execute();

        return doc._id;
      });

      expect(result.success).toBe(true);
      const docs = await db
        .collection(COLLECTIONS.orders)
        .find({})
        .sort({ customerId: 1 })
        .toArray();
      expect(docs).toHaveLength(2);
      expect(docs.find((d) => d.customerId === 'before-plan')!.total).toBe(75);
    });
  });

  // ====================================================================
  // BATCH ROLLBACK
  // ====================================================================
  describe('Batch Rollback', () => {
    beforeEach(async () => {
      await db.collection(COLLECTIONS.orders).deleteMany({});
      await db.collection(COLLECTIONS.products).deleteMany({});
    });

    it('should batch rollback multiple inserts', async () => {
      const result = await coordinator.execute(async (tx) => {
        for (let i = 0; i < 10; i++) {
          await tx.insertOne(COLLECTIONS.orders, {
            customerId: `batch-rb-${i}`,
          });
        }
        throw new Error('Rollback 10 inserts');
      });

      expect(result.success).toBe(false);
      expect(result.rollbackResult?.success).toBe(true);
      expect(result.rollbackResult!.rolledBackOperations.length).toBe(10);
      expect(await db.collection(COLLECTIONS.orders).countDocuments()).toBe(0);
    });

    it('should batch rollback mixed operations across collections', async () => {
      const existing = await db.collection(COLLECTIONS.orders).insertMany([
        { customerId: 'brb-1', status: 'original' },
        { customerId: 'brb-2', status: 'original' },
      ]);
      const ids = Object.values(existing.insertedIds);

      const result = await coordinator.execute(async (tx) => {
        await tx.insertOne(COLLECTIONS.orders, { customerId: 'brb-new' });
        await tx.insertOne(COLLECTIONS.products, { name: 'brb-product' });
        await tx.updateOne(COLLECTIONS.orders, ids[0], { status: 'changed' });
        await tx.deleteOne(COLLECTIONS.orders, ids[1]);
        throw new Error('Rollback mixed');
      });

      expect(result.success).toBe(false);
      expect(result.rollbackResult?.success).toBe(true);

      const orders = await db
        .collection(COLLECTIONS.orders)
        .find({})
        .sort({ customerId: 1 })
        .toArray();
      expect(orders).toHaveLength(2);
      expect(orders[0].status).toBe('original');
      expect(orders[1].status).toBe('original');
      expect(await db.collection(COLLECTIONS.products).countDocuments()).toBe(
        0,
      );
    });

    it('should batch rollback plan operations', async () => {
      const existing = await db
        .collection(COLLECTIONS.orders)
        .insertOne({ customerId: 'plan-brb', status: 'original' });

      const result = await coordinator.execute(async (tx) => {
        const plan = tx.createSagaPlan();
        plan.insert(COLLECTIONS.orders, { customerId: 'plan-brb-new-1' });
        plan.insert(COLLECTIONS.orders, { customerId: 'plan-brb-new-2' });
        plan.update(COLLECTIONS.orders, existing.insertedId, {
          status: 'changed',
        });
        await plan.execute();
        throw new Error('Rollback plan');
      });

      expect(result.success).toBe(false);
      expect(result.rollbackResult?.success).toBe(true);

      const doc = await db
        .collection(COLLECTIONS.orders)
        .findOne({ _id: existing.insertedId });
      expect(doc!.status).toBe('original');
      expect(await db.collection(COLLECTIONS.orders).countDocuments()).toBe(1);
    });
  });

  // ====================================================================
  // BENCHMARKS
  // ====================================================================
  describe('Performance Benchmarks', () => {
    const ITERATIONS = 10;

    beforeAll(async () => {
      await db.collection(COLLECTIONS.orders).deleteMany({});
    });

    it('BASELINE: raw insert (no transaction)', async () => {
      const start = Date.now();
      for (let i = 0; i < ITERATIONS; i++) {
        await db.collection(COLLECTIONS.orders).insertOne({
          customerId: `baseline-${i}`,
          total: i * 10,
        });
      }
      const duration = Date.now() - start;

      benchmarkResults.push({
        name: 'BASELINE: Raw Insert',
        durationMs: duration,
        operationsCount: ITERATIONS,
        avgOpDurationMs: duration / ITERATIONS,
      });
    });

    it('TX: single insert per transaction', async () => {
      const start = Date.now();
      for (let i = 0; i < ITERATIONS; i++) {
        await coordinator.execute(async (tx) => {
          await tx.insertOne(COLLECTIONS.orders, {
            customerId: `tx-single-${i}`,
            total: i * 10,
          });
        });
      }
      const duration = Date.now() - start;
      const baseline = benchmarkResults.find(
        (r) => r.name === 'BASELINE: Raw Insert',
      );

      benchmarkResults.push({
        name: 'TX: Single Insert',
        durationMs: duration,
        operationsCount: ITERATIONS,
        avgOpDurationMs: duration / ITERATIONS,
        overheadVsBaseline: baseline
          ? duration / baseline.durationMs
          : undefined,
      });
    });

    it('TX: batch insertMany vs individual inserts', async () => {
      const docCount = 20;

      const individualStart = Date.now();
      await coordinator.execute(async (tx) => {
        for (let i = 0; i < docCount; i++) {
          await tx.insertOne(COLLECTIONS.orders, { customerId: `indiv-${i}` });
        }
      });
      const individualDuration = Date.now() - individualStart;

      const batchStart = Date.now();
      await coordinator.execute(async (tx) => {
        await tx.insertMany(
          COLLECTIONS.orders,
          Array.from({ length: docCount }, (_, i) => ({
            customerId: `batch-bm-${i}`,
          })),
        );
      });
      const batchDuration = Date.now() - batchStart;

      benchmarkResults.push({
        name: `Individual inserts (${docCount})`,
        durationMs: individualDuration,
        operationsCount: docCount,
        avgOpDurationMs: individualDuration / docCount,
      });
      benchmarkResults.push({
        name: `Batch insertMany (${docCount})`,
        durationMs: batchDuration,
        operationsCount: docCount,
        avgOpDurationMs: batchDuration / docCount,
        overheadVsBaseline:
          batchDuration > 0 ? individualDuration / batchDuration : undefined,
      });
    });

    it('BASELINE + TX: read operations comparison', async () => {
      const ids: ObjectId[] = [];
      for (let i = 0; i < ITERATIONS; i++) {
        const r = await db
          .collection(COLLECTIONS.orders)
          .insertOne({ customerId: `read-bm-${i}` });
        ids.push(r.insertedId);
      }

      const rawStart = Date.now();
      for (const id of ids) {
        await db.collection(COLLECTIONS.orders).findOne({ _id: id });
      }
      const rawDuration = Date.now() - rawStart;

      const txFastStart = Date.now();
      await coordinator.execute(async (tx) => {
        for (const id of ids) {
          await tx.findOne(COLLECTIONS.orders, { _id: id });
        }
      });
      const txFastDuration = Date.now() - txFastStart;

      const txConsistentStart = Date.now();
      await coordinator.execute(async (tx) => {
        for (const id of ids) {
          await tx.findOne(
            COLLECTIONS.orders,
            { _id: id },
            { useConsistentRead: true },
          );
        }
      });
      const txConsistentDuration = Date.now() - txConsistentStart;

      const parallelStart = Date.now();
      await coordinator.execute(async (tx) => {
        await tx.parallelRead(
          ids.map((id) => ({
            collection: COLLECTIONS.orders,
            filter: { _id: id },
          })),
        );
      });
      const parallelDuration = Date.now() - parallelStart;

      benchmarkResults.push(
        {
          name: 'BASELINE: Raw Reads',
          durationMs: rawDuration,
          operationsCount: ITERATIONS,
          avgOpDurationMs: rawDuration / ITERATIONS,
        },
        {
          name: 'TX: Fast Reads (no lock)',
          durationMs: txFastDuration,
          operationsCount: ITERATIONS,
          avgOpDurationMs: txFastDuration / ITERATIONS,
          overheadVsBaseline:
            rawDuration > 0 ? txFastDuration / rawDuration : undefined,
        },
        {
          name: 'TX: Consistent Reads (read lock)',
          durationMs: txConsistentDuration,
          operationsCount: ITERATIONS,
          avgOpDurationMs: txConsistentDuration / ITERATIONS,
          overheadVsBaseline:
            rawDuration > 0 ? txConsistentDuration / rawDuration : undefined,
        },
        {
          name: 'TX: Parallel Reads',
          durationMs: parallelDuration,
          operationsCount: ITERATIONS,
          avgOpDurationMs: parallelDuration / ITERATIONS,
          overheadVsBaseline:
            rawDuration > 0 ? parallelDuration / rawDuration : undefined,
        },
      );
    });

    it('TX: update benchmark', async () => {
      const ids: ObjectId[] = [];
      for (let i = 0; i < ITERATIONS; i++) {
        const r = await db
          .collection(COLLECTIONS.orders)
          .insertOne({ customerId: `upd-bm-${i}`, counter: 0 });
        ids.push(r.insertedId);
      }

      const rawStart = Date.now();
      for (const id of ids) {
        await db
          .collection(COLLECTIONS.orders)
          .updateOne({ _id: id }, { $inc: { counter: 1 } });
      }
      const rawDuration = Date.now() - rawStart;

      const txStart = Date.now();
      await coordinator.execute(async (tx) => {
        for (const id of ids) {
          await tx.updateOne(COLLECTIONS.orders, id, { counter: 2 });
        }
      });
      const txDuration = Date.now() - txStart;

      benchmarkResults.push(
        {
          name: 'BASELINE: Raw Updates',
          durationMs: rawDuration,
          operationsCount: ITERATIONS,
          avgOpDurationMs: rawDuration / ITERATIONS,
        },
        {
          name: 'TX: Updates',
          durationMs: txDuration,
          operationsCount: ITERATIONS,
          avgOpDurationMs: txDuration / ITERATIONS,
          overheadVsBaseline:
            rawDuration > 0 ? txDuration / rawDuration : undefined,
        },
      );
    });

    it('TX: delete benchmark', async () => {
      const rawIds: ObjectId[] = [];
      const txIds: ObjectId[] = [];
      for (let i = 0; i < ITERATIONS; i++) {
        const r1 = await db
          .collection(COLLECTIONS.orders)
          .insertOne({ customerId: `del-raw-${i}` });
        rawIds.push(r1.insertedId);
        const r2 = await db
          .collection(COLLECTIONS.orders)
          .insertOne({ customerId: `del-tx-${i}` });
        txIds.push(r2.insertedId);
      }

      const rawStart = Date.now();
      for (const id of rawIds) {
        await db.collection(COLLECTIONS.orders).deleteOne({ _id: id });
      }
      const rawDuration = Date.now() - rawStart;

      const txStart = Date.now();
      await coordinator.execute(async (tx) => {
        for (const id of txIds) {
          await tx.deleteOne(COLLECTIONS.orders, id);
        }
      });
      const txDuration = Date.now() - txStart;

      benchmarkResults.push(
        {
          name: 'BASELINE: Raw Deletes',
          durationMs: rawDuration,
          operationsCount: ITERATIONS,
          avgOpDurationMs: rawDuration / ITERATIONS,
        },
        {
          name: 'TX: Deletes',
          durationMs: txDuration,
          operationsCount: ITERATIONS,
          avgOpDurationMs: txDuration / ITERATIONS,
          overheadVsBaseline:
            rawDuration > 0 ? txDuration / rawDuration : undefined,
        },
      );
    });

    it('TX: concurrent transactions benchmark', async () => {
      const concurrency = 10;
      const start = Date.now();

      const results = await Promise.all(
        Array.from({ length: concurrency }, (_, i) =>
          coordinator.execute(async (tx) => {
            await tx.insertOne(COLLECTIONS.orders, {
              customerId: `conc-bm-${i}`,
            });
            return i;
          }),
        ),
      );
      const duration = Date.now() - start;

      expect(results.every((r) => r.success)).toBe(true);

      benchmarkResults.push({
        name: `TX: Concurrent (${concurrency} parallel)`,
        durationMs: duration,
        operationsCount: concurrency,
        avgOpDurationMs: duration / concurrency,
      });
    });

    it('PLAN vs IMPERATIVE: insert benchmark', async () => {
      const docCount = 20;

      const imperativeStart = Date.now();
      await coordinator.execute(async (tx) => {
        for (let i = 0; i < docCount; i++) {
          await tx.insertOne(COLLECTIONS.orders, {
            customerId: `imp-plan-${i}`,
          });
        }
      });
      const imperativeDuration = Date.now() - imperativeStart;

      const planStart = Date.now();
      await coordinator.execute(async (tx) => {
        const plan = tx.createSagaPlan();
        for (let i = 0; i < docCount; i++) {
          plan.insert(COLLECTIONS.orders, { customerId: `plan-bm-${i}` });
        }
        await plan.execute();
      });
      const planDuration = Date.now() - planStart;

      benchmarkResults.push({
        name: `IMPERATIVE: ${docCount} inserts`,
        durationMs: imperativeDuration,
        operationsCount: docCount,
        avgOpDurationMs: imperativeDuration / docCount,
      });
      benchmarkResults.push({
        name: `PLAN: ${docCount} inserts`,
        durationMs: planDuration,
        operationsCount: docCount,
        avgOpDurationMs: planDuration / docCount,
        overheadVsBaseline:
          planDuration > 0 ? imperativeDuration / planDuration : undefined,
      });
    });

    it('PLAN vs IMPERATIVE: mixed operations benchmark', async () => {
      const opCount = 10;

      const setupIds1: ObjectId[] = [];
      const setupIds2: ObjectId[] = [];
      for (let i = 0; i < opCount; i++) {
        const r1 = await db
          .collection(COLLECTIONS.orders)
          .insertOne({ customerId: `mix-imp-${i}`, counter: 0 });
        setupIds1.push(r1.insertedId);
        const r2 = await db
          .collection(COLLECTIONS.orders)
          .insertOne({ customerId: `mix-plan-${i}`, counter: 0 });
        setupIds2.push(r2.insertedId);
      }

      const imperativeStart = Date.now();
      await coordinator.execute(async (tx) => {
        for (let i = 0; i < opCount; i++) {
          await tx.insertOne(COLLECTIONS.orders, {
            customerId: `mix-imp-new-${i}`,
          });
          await tx.updateOne(COLLECTIONS.orders, setupIds1[i], {
            counter: i + 1,
          });
        }
      });
      const imperativeDuration = Date.now() - imperativeStart;

      const planStart = Date.now();
      await coordinator.execute(async (tx) => {
        const plan = tx.createSagaPlan();
        for (let i = 0; i < opCount; i++) {
          plan.insert(COLLECTIONS.orders, { customerId: `mix-plan-new-${i}` });
          plan.update(COLLECTIONS.orders, setupIds2[i], { counter: i + 1 });
        }
        await plan.execute();
      });
      const planDuration = Date.now() - planStart;

      benchmarkResults.push({
        name: `IMPERATIVE: ${opCount} inserts + ${opCount} updates`,
        durationMs: imperativeDuration,
        operationsCount: opCount * 2,
        avgOpDurationMs: imperativeDuration / (opCount * 2),
      });
      benchmarkResults.push({
        name: `PLAN: ${opCount} inserts + ${opCount} updates`,
        durationMs: planDuration,
        operationsCount: opCount * 2,
        avgOpDurationMs: planDuration / (opCount * 2),
        overheadVsBaseline:
          planDuration > 0 ? imperativeDuration / planDuration : undefined,
      });
    });

    it('ROLLBACK benchmark: batch vs sequential', async () => {
      const opCount = 20;

      const rollbackStart = Date.now();
      const result = await coordinator.execute(async (tx) => {
        for (let i = 0; i < opCount; i++) {
          await tx.insertOne(COLLECTIONS.orders, { customerId: `rb-bm-${i}` });
        }
        throw new Error('Force rollback benchmark');
      });
      const rollbackDuration = Date.now() - rollbackStart;

      expect(result.success).toBe(false);
      expect(result.rollbackResult?.success).toBe(true);
      expect(
        await db
          .collection(COLLECTIONS.orders)
          .countDocuments({ customerId: { $regex: /^rb-bm-/ } }),
      ).toBe(0);

      benchmarkResults.push({
        name: `BATCH ROLLBACK: ${opCount} inserts`,
        durationMs: rollbackDuration,
        operationsCount: opCount,
        avgOpDurationMs: rollbackDuration / opCount,
      });
    });
  });

  describe('Saga: lease renewal & recovery metrics', () => {
    beforeEach(async () => {
      await db.collection(COLLECTIONS.locks).deleteMany({});
      await db.collection(COLLECTIONS.meta).deleteMany({});
    });

    it('renewTransactionLease extends metadata and lock expiresAt for active saga', async () => {
      const txId = await lockService.beginTransaction();
      const acq = await lockService.acquireLocks(txId, [
        { type: 'order', id: '1', mode: 'write' },
      ]);
      expect(acq.success).toBe(true);

      const past = new Date(Date.now() - 120_000);
      await db
        .collection(COLLECTIONS.meta)
        .updateOne(
          { txId },
          { $set: { expiresAt: past, lastActivityAt: past } },
        );
      await db
        .collection(COLLECTIONS.locks)
        .updateMany({ txId }, { $set: { expiresAt: past } });

      await lockService.renewTransactionLease(txId);

      const metaAfter = await db.collection(COLLECTIONS.meta).findOne({ txId });
      const lockAfter = await db
        .collection(COLLECTIONS.locks)
        .findOne({ txId });

      expect(metaAfter!.expiresAt!.getTime()).toBeGreaterThan(past.getTime());
      expect(lockAfter!.expiresAt!.getTime()).toBeGreaterThan(past.getTime());

      await lockService.commitTransaction(txId);
    });

    it('renewTransactionLease is no-op when transaction metadata is missing', async () => {
      await expect(
        lockService.renewTransactionLease('tx-missing-' + Date.now()),
      ).resolves.toBeUndefined();
    });

    it('recoverOrphanedSagas(boot) updates recovery metrics', async () => {
      const before = coordinator.getSagaRecoveryMetrics();
      await coordinator.recoverOrphanedSagas('boot');
      const after = coordinator.getSagaRecoveryMetrics();
      expect(after.totalRuns).toBe(before.totalRuns + 1);
      expect(after.bootRuns).toBe(before.bootRuns + 1);
      expect(after.periodicRuns).toBe(before.periodicRuns);
      expect(after.lastError).toBeNull();
    });

    it('recoverOrphanedSagas(periodic) increments periodicRuns', async () => {
      const before = coordinator.getSagaRecoveryMetrics();
      await coordinator.recoverOrphanedSagas('periodic');
      const after = coordinator.getSagaRecoveryMetrics();
      expect(after.totalRuns).toBe(before.totalRuns + 1);
      expect(after.periodicRuns).toBe(before.periodicRuns + 1);
    });

    it('execute invokes renewTransactionLease during long callback (heartbeat)', async () => {
      const spy = jest.spyOn(lockService, 'renewTransactionLease');
      await db.collection(COLLECTIONS.orders).deleteMany({
        customerId: 'heartbeat-probe',
      });
      await coordinator.execute(
        async (tx) => {
          await new Promise((r) => setTimeout(r, 4500));
          return tx.insertOne(COLLECTIONS.orders, {
            customerId: 'heartbeat-probe',
            total: 1,
          });
        },
        { maxDurationMs: 20_000 },
      );
      expect(spy).toHaveBeenCalled();
      spy.mockRestore();
      const doc = await db.collection(COLLECTIONS.orders).findOne({
        customerId: 'heartbeat-probe',
      });
      expect(doc).toBeTruthy();
    }, 20_000);
  });
});
