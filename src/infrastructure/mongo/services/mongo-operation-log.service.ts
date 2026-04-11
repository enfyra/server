import { Injectable, Logger, Inject, forwardRef } from '@nestjs/common';
import { Collection, ObjectId } from 'mongodb';
import { MongoService } from './mongo.service';

export type TOperationType = 'insert' | 'update' | 'delete' | 'update_inverse' | 'nested_insert' | 'nested_update' | 'checkpoint';

export interface IOperationLog {
  _id?: ObjectId;
  txId: string;
  operationId: string;
  sequence: number;
  operationType: TOperationType;
  collection: string;
  documentId: string | ObjectId;
  oldData: any;
  newData: any;
  metadata: {
    hasInverseRelations?: boolean;
    nestedOperations?: string[];
    timestamps?: {
      started: Date;
      completed?: Date;
    };
  };
  status: 'pending' | 'completed' | 'failed' | 'rolled_back' | 'aborted';
  error?: string;
  createdAt: Date;
}

export interface IRollbackResult {
  success: boolean;
  rolledBackOperations: string[];
  failedOperations: Array<{ operationId: string; error: string }>;
  txId: string;
}

export interface IOperationContext {
  txId: string;
  sequence: number;
}

@Injectable()
export class MongoOperationLogService {
  private readonly logger = new Logger(MongoOperationLogService.name);
  private readonly logCollectionName = 'system_operation_logs';
  private collectionReady = false;

  constructor(
    @Inject(forwardRef(() => MongoService))
    private readonly mongoService: MongoService,
  ) {}

  private async ensureCollection(): Promise<void> {
    if (this.collectionReady) {
      return;
    }

    const db = this.mongoService.getDb();
    const collections = await db.listCollections().toArray();
    const collectionNames = new Set(collections.map((c) => c.name));

    if (!collectionNames.has(this.logCollectionName)) {
      try {
        await db.createCollection(this.logCollectionName);
      } catch (error: any) {
        if (error.code !== 48) {
          throw error;
        }
      }
    }

    const collection = db.collection(this.logCollectionName);
    const existingIndexes = await collection.indexes();
    const indexNames = new Set(existingIndexes.map((i: any) => i.name));

    if (!indexNames.has('txId_1_sequence_1')) {
      try {
        await collection.createIndex({ txId: 1, sequence: 1 });
      } catch {}
    }
    if (!indexNames.has('txId_1_status_1')) {
      try {
        await collection.createIndex({ txId: 1, status: 1 });
      } catch {}
    }
    if (!indexNames.has('operationId_1')) {
      try {
        await collection.createIndex({ operationId: 1 }, { unique: true });
      } catch {}
    }
    if (!indexNames.has('collection_1_documentId_1')) {
      try {
        await collection.createIndex({ collection: 1, documentId: 1 });
      } catch {}
    }
    if (!indexNames.has('createdAt_1')) {
      try {
        await collection.createIndex({ createdAt: 1 }, { expireAfterSeconds: 86400 * 7 });
      } catch {}
    }

    this.collectionReady = true;
  }

  getLogCollection(): Collection<IOperationLog> {
    return this.mongoService.getDb().collection<IOperationLog>(this.logCollectionName);
  }

  async logOperation(
    txId: string,
    operationType: TOperationType,
    collection: string,
    documentId: string | ObjectId,
    oldData: any,
    newData: any,
    metadata?: IOperationLog['metadata'],
  ): Promise<IOperationLog> {
    await this.ensureCollection();

    const sequence = await this.getNextSequence(txId);
    const operationId = `${txId}-${sequence}`;

    const logEntry: IOperationLog = {
      txId,
      operationId,
      sequence,
      operationType,
      collection,
      documentId,
      oldData,
      newData,
      metadata: metadata || {},
      status: 'pending',
      createdAt: new Date(),
    };

    const result = await this.getLogCollection().insertOne(logEntry);
    logEntry._id = result.insertedId;

    return logEntry;
  }

  async markOperationCompleted(operationId: string): Promise<void> {
    await this.getLogCollection().updateOne(
      { operationId },
      {
        $set: {
          status: 'completed',
          'metadata.timestamps.completed': new Date(),
        },
      },
    );
  }

  async markOperationFailed(operationId: string, error: string): Promise<void> {
    await this.getLogCollection().updateOne(
      { operationId },
      {
        $set: {
          status: 'failed',
          error,
        },
      },
    );
  }

  async logOperationsBatch(
    txId: string,
    entries: Array<{
      operationType: TOperationType;
      collection: string;
      documentId: string | ObjectId;
      oldData: any;
      newData: any;
    }>,
  ): Promise<IOperationLog[]> {
    await this.ensureCollection();

    if (entries.length === 0) return [];

    const counterCollection = this.mongoService.getDb().collection('system_operation_counters');
    const counterResult = await counterCollection.findOneAndUpdate(
      { _id: txId as any },
      { $inc: { seq: entries.length } },
      { upsert: true, returnDocument: 'after' },
    );
    const endSeq = (counterResult as any)!.seq;
    const startSeq = endSeq - entries.length + 1;

    const now = new Date();
    const logDocs: IOperationLog[] = entries.map((entry, i) => {
      const seq = startSeq + i;
      return {
        txId,
        operationId: `${txId}-${seq}`,
        sequence: seq,
        operationType: entry.operationType,
        collection: entry.collection,
        documentId: entry.documentId,
        oldData: entry.oldData,
        newData: entry.newData,
        metadata: {},
        status: 'pending' as const,
        createdAt: now,
      };
    });

    const result = await this.getLogCollection().insertMany(logDocs);
    for (let i = 0; i < logDocs.length; i++) {
      logDocs[i]._id = result.insertedIds[i];
    }

    return logDocs;
  }

  async markOperationsBatchCompleted(operationIds: string[]): Promise<void> {
    if (operationIds.length === 0) return;
    await this.getLogCollection().updateMany(
      { operationId: { $in: operationIds } },
      { $set: { status: 'completed', 'metadata.timestamps.completed': new Date() } },
    );
  }

  async markOperationsBatchFailed(operationIds: string[], error: string): Promise<void> {
    if (operationIds.length === 0) return;
    await this.getLogCollection().updateMany(
      { operationId: { $in: operationIds } },
      { $set: { status: 'failed', error } },
    );
  }

  async rollbackTransaction(txId: string): Promise<IRollbackResult> {
    await this.ensureCollection();

    const operations = await this.getLogCollection()
      .find({ txId, status: { $in: ['completed', 'pending'] } })
      .sort({ sequence: -1 })
      .toArray();

    if (operations.length === 0) {
      return { success: true, rolledBackOperations: [], failedOperations: [], txId };
    }

    return this.rollbackBatch(txId, operations);
  }

  private async rollbackBatch(txId: string, operations: IOperationLog[]): Promise<IRollbackResult> {
    const rolledBackOperations: string[] = [];
    const failedOperations: Array<{ operationId: string; error: string }> = [];

    const opsByDocKey = new Map<string, IOperationLog[]>();
    const sequentialOps: IOperationLog[] = [];
    const batchableByCollection = new Map<string, {
      inserts: IOperationLog[];
      updates: IOperationLog[];
      deletes: IOperationLog[];
    }>();

    for (const op of operations) {
      if (op.operationType === 'checkpoint') {
        rolledBackOperations.push(op.operationId);
        continue;
      }
      if (op.operationType === 'update_inverse') {
        sequentialOps.push(op);
        continue;
      }
      const docKey = `${op.collection}:${op.documentId}`;
      const arr = opsByDocKey.get(docKey) || [];
      arr.push(op);
      opsByDocKey.set(docKey, arr);
    }

    for (const [, ops] of opsByDocKey) {
      if (ops.length > 1) {
        sequentialOps.push(...ops);
      } else {
        const op = ops[0];
        const key = op.collection;
        if (!batchableByCollection.has(key)) {
          batchableByCollection.set(key, { inserts: [], updates: [], deletes: [] });
        }
        const group = batchableByCollection.get(key)!;
        if (op.operationType === 'insert' || op.operationType === 'nested_insert') {
          group.inserts.push(op);
        } else if (op.operationType === 'update' || op.operationType === 'nested_update') {
          group.updates.push(op);
        } else if (op.operationType === 'delete') {
          group.deletes.push(op);
        } else {
          sequentialOps.push(op);
        }
      }
    }

    for (const op of sequentialOps) {
      try {
        await this.rollbackSingleOperation(op);
        rolledBackOperations.push(op.operationId);
      } catch (error) {
        failedOperations.push({ operationId: op.operationId, error: error.message });
        this.logger.error(`[${txId}] Failed to rollback ${op.operationId}: ${error.message}`);
      }
    }

    const batchPromises: Promise<void>[] = [];

    for (const [collName, group] of batchableByCollection) {
      const collection = this.mongoService.getDb().collection(collName);

      if (group.inserts.length > 0) {
        batchPromises.push(
          (async () => {
            try {
              const idsToDelete = group.inserts.map((op) =>
                typeof op.documentId === 'string' ? new ObjectId(op.documentId) : op.documentId,
              );
              await collection.deleteMany({ _id: { $in: idsToDelete } });
              rolledBackOperations.push(...group.inserts.map((op) => op.operationId));
            } catch (error) {
              for (const op of group.inserts) {
                failedOperations.push({ operationId: op.operationId, error: error.message });
              }
            }
          })(),
        );
      }

      if (group.updates.length > 0) {
        batchPromises.push(
          (async () => {
            const bulkOps = group.updates.map((op) => {
              const docId = typeof op.documentId === 'string' ? new ObjectId(op.documentId) : op.documentId;
              return op.oldData
                ? { replaceOne: { filter: { _id: docId }, replacement: op.oldData } }
                : { deleteOne: { filter: { _id: docId } } };
            });
            try {
              await collection.bulkWrite(bulkOps, { ordered: false });
              rolledBackOperations.push(...group.updates.map((op) => op.operationId));
            } catch (error) {
              for (const op of group.updates) {
                failedOperations.push({ operationId: op.operationId, error: error.message });
              }
            }
          })(),
        );
      }

      if (group.deletes.length > 0) {
        batchPromises.push(
          (async () => {
            const docsToRestore = group.deletes
              .filter((op) => op.oldData)
              .map((op) => {
                const docId = typeof op.documentId === 'string' ? new ObjectId(op.documentId) : op.documentId;
                return { ...op.oldData, _id: docId };
              });
            try {
              if (docsToRestore.length > 0) {
                await collection.insertMany(docsToRestore, { ordered: false });
              }
              rolledBackOperations.push(...group.deletes.map((op) => op.operationId));
            } catch (error) {
              for (const op of group.deletes) {
                failedOperations.push({ operationId: op.operationId, error: error.message });
              }
            }
          })(),
        );
      }
    }

    await Promise.allSettled(batchPromises);

    if (rolledBackOperations.length > 0) {
      await this.getLogCollection().updateMany(
        { operationId: { $in: rolledBackOperations } },
        { $set: { status: 'rolled_back' } },
      );
    }

    return {
      success: failedOperations.length === 0,
      rolledBackOperations,
      failedOperations,
      txId,
    };
  }

  private async rollbackSingleOperation(op: IOperationLog): Promise<void> {
    const collection = this.mongoService.getDb().collection(op.collection);
    const docId = typeof op.documentId === 'string' ? new ObjectId(op.documentId) : op.documentId;

    switch (op.operationType) {
      case 'insert':
      case 'nested_insert':
        await collection.deleteOne({ _id: docId });
        break;

      case 'update':
      case 'nested_update':
        if (op.oldData) {
          await collection.replaceOne({ _id: docId }, op.oldData);
        } else {
          await collection.deleteOne({ _id: docId });
        }
        break;

      case 'delete':
        if (op.oldData) {
          await collection.insertOne({ ...op.oldData, _id: docId });
        }
        break;

      case 'update_inverse':
        await this.rollbackInverseRelation(op);
        break;

      case 'checkpoint':
        break;

      default:
        throw new Error(`Unknown operation type: ${op.operationType}`);
    }
  }

  private async rollbackInverseRelation(op: IOperationLog): Promise<void> {
    if (!op.metadata?.nestedOperations || op.metadata.nestedOperations.length === 0) {
      return;
    }

    for (const nestedOpId of op.metadata.nestedOperations) {
      const nestedOp = await this.getLogCollection().findOne({ operationId: nestedOpId });
      if (nestedOp) {
        await this.rollbackSingleOperation(nestedOp);
      }
    }
  }

  async getOperationLogs(txId: string): Promise<IOperationLog[]> {
    return this.getLogCollection()
      .find({ txId })
      .sort({ sequence: 1 })
      .toArray();
  }

  async cleanupOldLogs(olderThanDays: number = 7): Promise<number> {
    const cutoffDate = new Date(Date.now() - olderThanDays * 24 * 60 * 60 * 1000);
    const result = await this.getLogCollection().deleteMany({
      createdAt: { $lt: cutoffDate },
      status: { $in: ['completed', 'rolled_back', 'aborted'] },
    });

    const staleTxIds = await this.getLogCollection().distinct('txId', {
      createdAt: { $lt: cutoffDate },
    });
    const activeTxIds = await this.getLogCollection().distinct('txId');
    const counterCollection = this.mongoService.getDb().collection('system_operation_counters');
    const orphanedCounterIds = staleTxIds.filter((id: any) => !activeTxIds.includes(id));
    if (orphanedCounterIds.length > 0) {
      await counterCollection.deleteMany({ _id: { $in: orphanedCounterIds } as any });
    }

    return result.deletedCount || 0;
  }

  async getOperationsForDocument(
    collection: string,
    documentId: string | ObjectId,
    txId?: string,
  ): Promise<IOperationLog[]> {
    const query: any = { collection, documentId };
    if (txId) {
      query.txId = txId;
    }
    return this.getLogCollection()
      .find(query)
      .sort({ createdAt: -1 })
      .toArray();
  }

  private async getNextSequence(txId: string): Promise<number> {
    const counterCollection = this.mongoService.getDb().collection('system_operation_counters');
    const result = await counterCollection.findOneAndUpdate(
      { _id: txId as any },
      { $inc: { seq: 1 } },
      { upsert: true, returnDocument: 'after' },
    );
    return (result as any)!.seq;
  }

  async createCheckpoint(txId: string, description?: string): Promise<string> {
    const checkpointId = `checkpoint-${Date.now()}`;

    await this.getLogCollection().insertOne({
      txId,
      operationId: checkpointId,
      sequence: await this.getNextSequence(txId),
      operationType: 'checkpoint' as TOperationType,
      collection: '_checkpoint',
      documentId: checkpointId,
      oldData: null,
      newData: { description, timestamp: new Date() },
      metadata: {},
      status: 'completed',
      createdAt: new Date(),
    });

    return checkpointId;
  }

  async rollbackToCheckpoint(txId: string, checkpointId: string): Promise<IRollbackResult> {
    const [checkpoint, operationsToRollback] = await Promise.all([
      this.getLogCollection().findOne({ operationId: checkpointId }),
      this.getLogCollection()
        .find({
          txId,
          sequence: { $gt: 0 },
          status: { $in: ['completed', 'pending'] },
        })
        .sort({ sequence: -1 })
        .toArray(),
    ]);

    if (!checkpoint) {
      throw new Error(`Checkpoint ${checkpointId} not found`);
    }

    const filteredOps = operationsToRollback.filter(
      (op) => op.sequence > checkpoint.sequence,
    );

    return this.rollbackBatch(txId, filteredOps);
  }

  async getTransactionStats(txId: string): Promise<{
    total: number;
    completed: number;
    failed: number;
    rolledBack: number;
    pending: number;
  }> {
    const [totalResult, statusResults] = await Promise.all([
      this.getLogCollection().countDocuments({ txId }),
      this.getLogCollection()
        .aggregate([
          { $match: { txId } },
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
