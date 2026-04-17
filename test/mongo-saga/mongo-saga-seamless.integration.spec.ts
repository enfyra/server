import { Test, TestingModule } from '@nestjs/testing';
import { MongoClient, Db } from 'mongodb';
import { MongoService } from '../../src/infrastructure/mongo/services/mongo.service';
import { MongoSagaLockService } from '../../src/infrastructure/mongo/services/mongo-saga-lock.service';
import { MongoOperationLogService } from '../../src/infrastructure/mongo/services/mongo-operation-log.service';
import { MongoSagaCoordinator } from '../../src/infrastructure/mongo/services/mongo-saga-coordinator.service';
import { MetadataCacheService } from '../../src/infrastructure/cache/services/metadata-cache.service';
import { InstanceService } from '../../src/shared/services/instance.service';

const MONGO_URI =
  'mongodb://enfyra_admin:enfyra_password_123@localhost:27017/enfyra_seamless_test?authSource=admin';

const TX_RETURN_ENVELOPE = { throwOnFailure: false } as const;

class MockMetadataCacheService {
  async lookupTableByName() {
    return null;
  }
  async getTableMetadata() {
    return null;
  }
}

describe('MongoDB Saga Seamless Integration', () => {
  let mongoClient: MongoClient;
  let db: Db;
  let mongoService: MongoService;

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
    db = mongoClient.db('enfyra_seamless_test');

    const module: TestingModule = await Test.createTestingModule({
      providers: [
        MongoService,
        MongoSagaLockService,
        MongoOperationLogService,
        InstanceService,
        MongoSagaCoordinator,
        { provide: MetadataCacheService, useClass: MockMetadataCacheService },
      ],
    }).compile();

    mongoService = module.get<MongoService>(MongoService);
    Object.defineProperty(mongoService, 'db', { value: db });
    Object.defineProperty(mongoService, 'client', { value: mongoClient });
    Object.defineProperty(mongoService, 'nativeMultiDocSupported', {
      value: false,
      writable: true,
    });

    await db.collection('seamless_orders').deleteMany({});
    await db.collection('seamless_inventory').deleteMany({});
    await db.collection('system_transaction_locks').deleteMany({});
    await db.collection('system_transaction_metadata').deleteMany({});
    await db.collection('system_operation_logs').deleteMany({});
  });

  afterAll(async () => {
    await mongoClient.close();
  });

  describe('Seamless Transaction API', () => {
    it('should return native collection when not in transaction', () => {
      const collection = mongoService.collection('seamless_orders');

      expect(mongoService.isInTransaction()).toBe(false);
      expect(collection.constructor.name).not.toBe('SagaCollection');
    });

    it('should execute transaction with automatic enrollment', async () => {
      const result = await mongoService.runInSaga(async () => {
        const orders = mongoService.collection('seamless_orders');

        expect(mongoService.isInTransaction()).toBe(true);

        const insertResult = await orders.insertOne({
          customerId: 'seamless-1',
          total: 100,
          status: 'pending',
        } as any);

        return insertResult;
      }, TX_RETURN_ENVELOPE);

      expect(result.success).toBe(true);
      expect(result.data).toBeDefined();
      expect(result.txId).toBeDefined();

      const order = await db
        .collection('seamless_orders')
        .findOne({ customerId: 'seamless-1' });
      expect(order).toBeDefined();
      expect(order?.total).toBe(100);
    });

    it('should rollback on error automatically', async () => {
      const startCount = await db
        .collection('seamless_orders')
        .countDocuments();

      const result = await mongoService.runInSaga(async () => {
        const orders = mongoService.collection('seamless_orders');

        await orders.insertOne({
          customerId: 'rollback-test',
          total: 200,
        } as any);

        throw new Error('Intentional error for rollback');
      }, TX_RETURN_ENVELOPE);

      expect(result.success).toBe(false);
      expect(result.error).toBeDefined();

      const endCount = await db.collection('seamless_orders').countDocuments();
      expect(endCount).toBe(startCount);
    });

    it('should handle update within transaction', async () => {
      const initial = await db.collection('seamless_orders').insertOne({
        customerId: 'update-test',
        total: 300,
        status: 'pending',
      });

      const result = await mongoService.runInSaga(async () => {
        const orders = mongoService.collection('seamless_orders');

        await orders.updateOne(
          { _id: initial.insertedId } as any,
          { $set: { status: 'completed' } } as any,
        );

        return { updated: true };
      }, TX_RETURN_ENVELOPE);

      expect(result.success).toBe(true);

      const updated = await db
        .collection('seamless_orders')
        .findOne({ _id: initial.insertedId });
      expect(updated?.status).toBe('completed');
    });

    it('should handle delete within transaction', async () => {
      const initial = await db.collection('seamless_orders').insertOne({
        customerId: 'delete-test',
        total: 400,
      });

      const result = await mongoService.runInSaga(async () => {
        const orders = mongoService.collection('seamless_orders');

        await orders.deleteOne({ _id: initial.insertedId } as any);

        return { deleted: true };
      }, TX_RETURN_ENVELOPE);

      expect(result.success).toBe(true);

      const deleted = await db
        .collection('seamless_orders')
        .findOne({ _id: initial.insertedId });
      expect(deleted).toBeNull();
    });

    it('should support multiple operations in single transaction', async () => {
      const result = await mongoService.runInSaga(async () => {
        const orders = mongoService.collection('seamless_orders');
        const inventory = mongoService.collection('seamless_inventory');

        const order = await orders.insertOne({
          customerId: 'multi-op',
          total: 500,
          status: 'pending',
        } as any);

        await inventory.insertOne({
          productId: 'prod-1',
          stock: 100,
        } as any);

        await orders.updateOne(
          { _id: (order as any).insertedId } as any,
          { $set: { status: 'confirmed' } } as any,
        );

        await inventory.updateOne(
          { productId: 'prod-1' } as any,
          { $inc: { stock: -1 } } as any,
        );

        return order;
      }, TX_RETURN_ENVELOPE);

      expect(result.success).toBe(true);

      const order = await db
        .collection('seamless_orders')
        .findOne({ customerId: 'multi-op' });
      expect(order?.status).toBe('confirmed');

      const inventory = await db
        .collection('seamless_inventory')
        .findOne({ productId: 'prod-1' });
      expect(inventory?.stock).toBe(99);
    });

    it('should rollback all operations on failure', async () => {
      const result = await mongoService.runInSaga(async () => {
        const orders = mongoService.collection('seamless_orders');
        const inventory = mongoService.collection('seamless_inventory');

        await orders.insertOne({
          customerId: 'rollback-multi-1',
          total: 600,
        } as any);

        await inventory.insertOne({
          productId: 'rollback-prod',
          stock: 50,
        } as any);

        await orders.insertOne({
          customerId: 'rollback-multi-2',
          total: 700,
        } as any);

        throw new Error('Fail after multiple operations');
      }, TX_RETURN_ENVELOPE);

      expect(result.success).toBe(false);

      const order1 = await db
        .collection('seamless_orders')
        .findOne({ customerId: 'rollback-multi-1' });
      const order2 = await db
        .collection('seamless_orders')
        .findOne({ customerId: 'rollback-multi-2' });
      const inv = await db
        .collection('seamless_inventory')
        .findOne({ productId: 'rollback-prod' });

      expect(order1).toBeNull();
      expect(order2).toBeNull();
      expect(inv).toBeNull();
    });

    it('should support read operations without locking overhead', async () => {
      await db.collection('seamless_orders').insertOne({
        customerId: 'read-test',
        total: 800,
      });

      const result = await mongoService.runInSaga(async () => {
        const orders = mongoService.collection('seamless_orders');

        const found = await orders.findOne({ customerId: 'read-test' } as any);
        const all = await orders.find({} as any);
        const count = await orders.countDocuments();

        return { found, count: all.length, totalCount: count };
      }, TX_RETURN_ENVELOPE);

      expect(result.success).toBe(true);
      expect(result.data?.found?.customerId).toBe('read-test');
      expect(result.data?.count).toBeGreaterThan(0);
    });

    it('should handle concurrent transactions correctly', async () => {
      const promises = Array.from({ length: 3 }, (_, i) =>
        mongoService.runInSaga(async () => {
          const orders = mongoService.collection('seamless_orders');

          await orders.insertOne({
            customerId: `concurrent-${i}`,
            total: i * 100,
          } as any);

          await new Promise((resolve) => setTimeout(resolve, 20));

          return { index: i };
        }, TX_RETURN_ENVELOPE),
      );

      const results = await Promise.all(promises);

      const allSuccess = results.every((r) => r.success);
      expect(allSuccess).toBe(true);

      for (let i = 0; i < 3; i++) {
        const order = await db
          .collection('seamless_orders')
          .findOne({ customerId: `concurrent-${i}` });
        expect(order).toBeDefined();
        expect(order?.total).toBe(i * 100);
      }
    });
  });

  describe('Transaction Context', () => {
    it('should track transaction ID correctly', async () => {
      let capturedTxId: string | undefined;

      await mongoService.runInSaga(async () => {
        capturedTxId = mongoService.getCurrentTransactionId();
        return {};
      }, TX_RETURN_ENVELOPE);

      expect(capturedTxId).toBeDefined();
      expect(capturedTxId).toMatch(/^tx-/);
    });

    it('should not have transaction ID outside transaction', () => {
      expect(mongoService.getCurrentTransactionId()).toBeUndefined();
      expect(mongoService.isInTransaction()).toBe(false);
    });
  });

  describe('Performance Comparison', () => {
    it('should compare native vs transactional operations', async () => {
      const nativeStart = Date.now();
      for (let i = 0; i < 5; i++) {
        await db.collection('seamless_orders').insertOne({
          customerId: `native-perf-${i}`,
          total: i,
        });
      }
      const nativeDuration = Date.now() - nativeStart;

      const txStart = Date.now();
      await mongoService.runInSaga(async () => {
        const orders = mongoService.collection('seamless_orders');
        for (let i = 0; i < 5; i++) {
          await orders.insertOne({
            customerId: `tx-perf-${i}`,
            total: i,
          } as any);
        }
      }, TX_RETURN_ENVELOPE);
      const txDuration = Date.now() - txStart;

      console.log(`\nPerformance Comparison (5 inserts):`);
      console.log(`  Native: ${nativeDuration}ms (${nativeDuration / 5}ms/op)`);
      console.log(`  Transactional: ${txDuration}ms (${txDuration / 5}ms/op)`);
      console.log(`  Overhead: ${(txDuration / nativeDuration).toFixed(2)}x`);

      expect(txDuration).toBeGreaterThan(nativeDuration);
    });
  });
});
