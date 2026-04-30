import { Collection, ObjectId } from 'mongodb';
import { Logger } from '../../../shared/logger';
import { getErrorMessage } from '../../../shared/utils/error.util';
import { MongoService } from './mongo.service';

export type TSagaSnapshotOp =
  | 'insert'
  | 'update'
  | 'delete'
  | 'update_inverse'
  | 'nested_insert'
  | 'nested_update'
  | 'checkpoint';

export interface ISagaSnapshot {
  _id?: ObjectId;
  sessionId: string;
  snapshotId: string;
  seq: number;
  op: TSagaSnapshotOp;
  collection: string;
  documentId: string | ObjectId;
  before: any;
  afterPatch: any;
  metadata: {
    hasInverseRelations?: boolean;
    nestedSnapshots?: string[];
    timestamps?: {
      started?: Date;
      completed?: Date;
    };
  };
  status: 'pending' | 'completed' | 'failed' | 'rolled_back' | 'aborted';
  error?: string;
  createdAt: Date;
}

export interface IRollbackResult {
  success: boolean;
  rolledBackSnapshots: string[];
  failedSnapshots: Array<{ snapshotId: string; error: string }>;
  txId: string;
}

export class MongoSagaSnapshotService {
  private readonly logger = new Logger(MongoSagaSnapshotService.name);
  private readonly snapshotCollectionName = 'system_saga_snapshots';
  private readonly counterCollectionName = 'system_saga_counters';
  private collectionReady = false;
  private readonly mongoService: MongoService;

  constructor(deps: { mongoService: MongoService }) {
    this.mongoService = deps.mongoService;
  }

  private async ensureCollection(): Promise<void> {
    if (this.collectionReady) return;

    const db = this.mongoService.getDb();
    const collections = await db.listCollections().toArray();
    const collectionNames = new Set(collections.map((c) => c.name));

    if (!collectionNames.has(this.snapshotCollectionName)) {
      try {
        await db.createCollection(this.snapshotCollectionName);
      } catch (error: any) {
        if (error.code !== 48) throw error;
      }
    }

    const collection = db.collection(this.snapshotCollectionName);
    const existingIndexes = await collection.indexes();
    const indexNames = new Set(existingIndexes.map((i: any) => i.name));
    const legacyIndexNames = [
      'operationId_1',
      'txId_1_sequence_1',
      'txId_1_status_1',
    ];

    for (const indexName of legacyIndexNames) {
      if (indexNames.has(indexName)) {
        try {
          await collection.dropIndex(indexName);
          indexNames.delete(indexName);
        } catch {}
      }
    }

    if (!indexNames.has('sessionId_1_seq_1')) {
      try {
        await collection.createIndex({ sessionId: 1, seq: 1 });
      } catch {}
    }
    if (!indexNames.has('sessionId_1_status_1')) {
      try {
        await collection.createIndex({ sessionId: 1, status: 1 });
      } catch {}
    }
    if (!indexNames.has('snapshotId_1')) {
      try {
        await collection.createIndex({ snapshotId: 1 }, { unique: true });
      } catch {}
    }
    if (!indexNames.has('collection_1_documentId_1')) {
      try {
        await collection.createIndex({ collection: 1, documentId: 1 });
      } catch {}
    }
    if (!indexNames.has('createdAt_1')) {
      try {
        await collection.createIndex(
          { createdAt: 1 },
          { expireAfterSeconds: 86400 * 7 },
        );
      } catch {}
    }

    this.collectionReady = true;
  }

  getSnapshotCollection(): Collection<ISagaSnapshot> {
    return this.mongoService
      .getDb()
      .collection<ISagaSnapshot>(this.snapshotCollectionName);
  }

  async createSnapshot(
    sessionId: string,
    op: TSagaSnapshotOp,
    collection: string,
    documentId: string | ObjectId,
    before: any,
    afterPatch: any,
    metadata?: ISagaSnapshot['metadata'],
  ): Promise<ISagaSnapshot> {
    await this.ensureCollection();

    const seq = await this.getNextSequence(sessionId);
    const snapshotId = `${sessionId}-${seq}`;
    const snapshot: ISagaSnapshot = {
      sessionId,
      snapshotId,
      seq,
      op,
      collection,
      documentId,
      before,
      afterPatch,
      metadata: metadata || {},
      status: 'pending',
      createdAt: new Date(),
    };

    const result = await this.getSnapshotCollection().insertOne(snapshot);
    snapshot._id = result.insertedId;
    return snapshot;
  }

  async createSnapshotsBatch(
    sessionId: string,
    entries: Array<{
      op: TSagaSnapshotOp;
      collection: string;
      documentId: string | ObjectId;
      before: any;
      afterPatch: any;
    }>,
  ): Promise<ISagaSnapshot[]> {
    await this.ensureCollection();
    if (entries.length === 0) return [];

    const counterCollection = this.mongoService
      .getDb()
      .collection(this.counterCollectionName);
    const counterResult = await counterCollection.findOneAndUpdate(
      { _id: sessionId as any },
      { $inc: { seq: entries.length } },
      { upsert: true, returnDocument: 'after' },
    );
    const endSeq = (counterResult as any)!.seq;
    const startSeq = endSeq - entries.length + 1;
    const now = new Date();

    const snapshots: ISagaSnapshot[] = entries.map((entry, i) => {
      const seq = startSeq + i;
      return {
        sessionId,
        snapshotId: `${sessionId}-${seq}`,
        seq,
        op: entry.op,
        collection: entry.collection,
        documentId: entry.documentId,
        before: entry.before,
        afterPatch: entry.afterPatch,
        metadata: {},
        status: 'pending',
        createdAt: now,
      };
    });

    const result = await this.getSnapshotCollection().insertMany(snapshots);
    for (let i = 0; i < snapshots.length; i++) {
      snapshots[i]._id = result.insertedIds[i];
    }
    return snapshots;
  }

  async markSnapshotCompleted(snapshotId: string): Promise<void> {
    await this.getSnapshotCollection().updateOne(
      { snapshotId },
      {
        $set: {
          status: 'completed',
          'metadata.timestamps.completed': new Date(),
        },
      },
    );
  }

  async markSnapshotFailed(snapshotId: string, error: string): Promise<void> {
    await this.getSnapshotCollection().updateOne(
      { snapshotId },
      { $set: { status: 'failed', error } },
    );
  }

  async markSnapshotsBatchCompleted(snapshotIds: string[]): Promise<void> {
    if (snapshotIds.length === 0) return;
    await this.getSnapshotCollection().updateMany(
      { snapshotId: { $in: snapshotIds } },
      {
        $set: {
          status: 'completed',
          'metadata.timestamps.completed': new Date(),
        },
      },
    );
  }

  async markSnapshotsBatchFailed(
    snapshotIds: string[],
    error: string,
  ): Promise<void> {
    if (snapshotIds.length === 0) return;
    await this.getSnapshotCollection().updateMany(
      { snapshotId: { $in: snapshotIds } },
      { $set: { status: 'failed', error } },
    );
  }

  async rollbackTransaction(sessionId: string): Promise<IRollbackResult> {
    await this.ensureCollection();

    const snapshots = await this.getSnapshotCollection()
      .find({
        sessionId,
        status: { $in: ['completed', 'pending'] },
      })
      .sort({ seq: -1 })
      .toArray();

    if (snapshots.length === 0) {
      return {
        success: true,
        rolledBackSnapshots: [],
        failedSnapshots: [],
        txId: sessionId,
      };
    }

    return this.rollbackBatch(sessionId, snapshots);
  }

  private async rollbackBatch(
    sessionId: string,
    snapshots: ISagaSnapshot[],
  ): Promise<IRollbackResult> {
    const rolledBackSnapshots: string[] = [];
    const failedSnapshots: Array<{ snapshotId: string; error: string }> = [];
    const snapshotsByDocKey = new Map<string, ISagaSnapshot[]>();
    const batchableByCollection = new Map<
      string,
      {
        deleteIds: Array<string | ObjectId>;
        deleteSnapshots: ISagaSnapshot[];
        restoreOps: Array<{
          id: string | ObjectId;
          before: any;
          snapshots: ISagaSnapshot[];
        }>;
      }
    >();

    for (const snapshot of snapshots) {
      if (snapshot.op === 'checkpoint') {
        rolledBackSnapshots.push(snapshot.snapshotId);
        continue;
      }
      const docKey = `${snapshot.collection}:${snapshot.documentId}`;
      const arr = snapshotsByDocKey.get(docKey) || [];
      arr.push(snapshot);
      snapshotsByDocKey.set(docKey, arr);
    }

    for (const [, docSnapshots] of snapshotsByDocKey) {
      const ordered = docSnapshots.sort((a, b) => a.seq - b.seq);
      const first = ordered[0];
      if (!batchableByCollection.has(first.collection)) {
        batchableByCollection.set(first.collection, {
          deleteIds: [],
          deleteSnapshots: [],
          restoreOps: [],
        });
      }
      const group = batchableByCollection.get(first.collection)!;
      if (
        first.op === 'insert' ||
        first.op === 'nested_insert' ||
        !first.before
      ) {
        group.deleteIds.push(first.documentId);
        group.deleteSnapshots.push(...ordered);
      } else {
        group.restoreOps.push({
          id: first.documentId,
          before: first.before,
          snapshots: ordered,
        });
      }
    }

    const batchPromises: Promise<void>[] = [];

    for (const [collectionName, group] of batchableByCollection) {
      const collection = this.mongoService.getDb().collection(collectionName);

      if (group.deleteIds.length > 0) {
        batchPromises.push(
          (async () => {
            try {
              const idsToDelete = group.deleteIds.map((id) =>
                this.toMongoDocumentId(id),
              );
              await collection.deleteMany({ _id: { $in: idsToDelete } } as any);
              rolledBackSnapshots.push(
                ...group.deleteSnapshots.map((snapshot) => snapshot.snapshotId),
              );
            } catch (error) {
              for (const snapshot of group.deleteSnapshots) {
                failedSnapshots.push({
                  snapshotId: snapshot.snapshotId,
                  error: getErrorMessage(error),
                });
              }
            }
          })(),
        );
      }

      if (group.restoreOps.length > 0) {
        batchPromises.push(
          (async () => {
            const bulkOps = group.restoreOps.map((op) => ({
              replaceOne: {
                filter: { _id: this.toMongoDocumentId(op.id) },
                replacement: op.before,
                upsert: true,
              },
            }));
            try {
              await collection.bulkWrite(bulkOps as any[], { ordered: false });
              rolledBackSnapshots.push(
                ...group.restoreOps.flatMap((op) =>
                  op.snapshots.map((snapshot) => snapshot.snapshotId),
                ),
              );
            } catch (error) {
              for (const op of group.restoreOps) {
                for (const snapshot of op.snapshots) {
                  failedSnapshots.push({
                    snapshotId: snapshot.snapshotId,
                    error: getErrorMessage(error),
                  });
                }
              }
            }
          })(),
        );
      }
    }

    await Promise.allSettled(batchPromises);

    if (rolledBackSnapshots.length > 0) {
      await this.getSnapshotCollection().updateMany(
        { snapshotId: { $in: rolledBackSnapshots } },
        { $set: { status: 'rolled_back' } },
      );
    }

    return {
      success: failedSnapshots.length === 0,
      rolledBackSnapshots,
      failedSnapshots,
      txId: sessionId,
    };
  }

  private toMongoDocumentId(id: string | ObjectId): string | ObjectId {
    if (typeof id !== 'string') return id;
    return ObjectId.isValid(id) ? new ObjectId(id) : id;
  }

  async getSnapshots(sessionId: string): Promise<ISagaSnapshot[]> {
    return this.getSnapshotCollection()
      .find({ sessionId })
      .sort({ seq: 1 })
      .toArray();
  }

  async cleanupOldSnapshots(olderThanDays: number = 7): Promise<number> {
    const cutoffDate = new Date(
      Date.now() - olderThanDays * 24 * 60 * 60 * 1000,
    );
    const result = await this.getSnapshotCollection().deleteMany({
      createdAt: { $lt: cutoffDate },
      status: { $in: ['completed', 'rolled_back', 'aborted'] },
    });

    const staleSessionIds = await this.getSnapshotCollection().distinct(
      'sessionId',
      {
        createdAt: { $lt: cutoffDate },
      },
    );
    const activeSessionIds =
      await this.getSnapshotCollection().distinct('sessionId');
    const orphanedCounterIds = staleSessionIds.filter(
      (id: any) => !activeSessionIds.includes(id),
    );
    if (orphanedCounterIds.length > 0) {
      await this.mongoService
        .getDb()
        .collection(this.counterCollectionName)
        .deleteMany({ _id: { $in: orphanedCounterIds } as any });
    }

    return result.deletedCount || 0;
  }

  async getSnapshotsForDocument(
    collection: string,
    documentId: string | ObjectId,
    sessionId?: string,
  ): Promise<ISagaSnapshot[]> {
    const query: any = { collection, documentId };
    if (sessionId) query.sessionId = sessionId;
    return this.getSnapshotCollection()
      .find(query)
      .sort({ createdAt: -1 })
      .toArray();
  }

  private async getNextSequence(sessionId: string): Promise<number> {
    const counterCollection = this.mongoService
      .getDb()
      .collection(this.counterCollectionName);
    const result = await counterCollection.findOneAndUpdate(
      { _id: sessionId as any },
      { $inc: { seq: 1 } },
      { upsert: true, returnDocument: 'after' },
    );
    return (result as any)!.seq;
  }

  async createCheckpoint(sessionId: string, description?: string): Promise<string> {
    await this.ensureCollection();
    const checkpointId = `checkpoint-${Date.now()}`;
    const seq = await this.getNextSequence(sessionId);
    const timestamp = new Date();

    await this.getSnapshotCollection().insertOne({
      sessionId,
      snapshotId: checkpointId,
      seq,
      op: 'checkpoint',
      collection: '_checkpoint',
      documentId: checkpointId,
      before: null,
      afterPatch: { description, timestamp },
      metadata: {},
      status: 'completed',
      createdAt: timestamp,
    });

    return checkpointId;
  }

  async rollbackToCheckpoint(
    sessionId: string,
    checkpointId: string,
  ): Promise<IRollbackResult> {
    const [checkpoint, snapshotsToRollback] = await Promise.all([
      this.getSnapshotCollection().findOne({ snapshotId: checkpointId }),
      this.getSnapshotCollection()
        .find({
          sessionId,
          status: { $in: ['completed', 'pending'] },
        })
        .sort({ seq: -1 })
        .toArray(),
    ]);

    if (!checkpoint) {
      throw new Error(`Checkpoint ${checkpointId} not found`);
    }

    const filteredSnapshots = snapshotsToRollback.filter(
      (snapshot) => snapshot.seq > checkpoint.seq,
    );

    return this.rollbackBatch(sessionId, filteredSnapshots);
  }

  async getTransactionStats(sessionId: string): Promise<{
    total: number;
    completed: number;
    failed: number;
    rolledBack: number;
    pending: number;
  }> {
    const [totalResult, statusResults] = await Promise.all([
      this.getSnapshotCollection().countDocuments({ sessionId }),
      this.getSnapshotCollection()
        .aggregate([
          { $match: { sessionId } },
          {
            $group: {
              _id: '$status',
              count: { $sum: 1 },
            },
          },
        ])
        .toArray(),
    ]);

    const result = {
      total: totalResult,
      completed: 0,
      failed: 0,
      rolledBack: 0,
      pending: 0,
    };

    for (const stat of statusResults) {
      if (stat._id === 'completed') result.completed = stat.count;
      if (stat._id === 'failed') result.failed = stat.count;
      if (stat._id === 'rolled_back') result.rolledBack = stat.count;
      if (stat._id === 'pending') result.pending = stat.count;
    }

    return result;
  }
}
