import { Logger } from '../../../shared/logger';
import { ObjectId } from 'mongodb';
import { MongoSagaLockService } from './mongo-saga-lock.service';
import {
  MongoOperationLogService,
  TOperationType,
} from './mongo-operation-log.service';
import { MongoService } from './mongo.service';
import { DatabaseException } from '../../../core/exceptions/custom-exceptions';
import { ISagaOptions, ISagaContext } from './mongo-saga.types';

interface IPlanInsert {
  type: 'insert';
  collection: string;
  data: any;
  predictedId: ObjectId;
}

interface IPlanUpdate {
  type: 'update';
  collection: string;
  id: ObjectId;
  data: any;
}

interface IPlanDelete {
  type: 'delete';
  collection: string;
  id: ObjectId;
}

type TPlanOperation = IPlanInsert | IPlanUpdate | IPlanDelete;

export interface IPlanExecuteResult {
  inserts: Map<string, any[]>;
  updates: Map<string, any[]>;
  deletes: Map<string, string[]>;
}

export class SagaPlan {
  private readonly operations: TPlanOperation[] = [];
  private readonly logger = new Logger(SagaPlan.name);

  constructor(
    private readonly txId: string,
    private readonly lockService: MongoSagaLockService,
    private readonly logService: MongoOperationLogService,
    private readonly mongoService: MongoService,
    private readonly options: Required<ISagaOptions>,
    private readonly context: ISagaContext,
  ) {}

  insert(collection: string, data: any): this {
    this.operations.push({
      type: 'insert',
      collection,
      data,
      predictedId: new ObjectId(),
    });
    return this;
  }

  update(collection: string, id: string | ObjectId, data: any): this {
    this.operations.push({
      type: 'update',
      collection,
      id: typeof id === 'string' ? new ObjectId(id) : id,
      data,
    });
    return this;
  }

  delete(collection: string, id: string | ObjectId): this {
    this.operations.push({
      type: 'delete',
      collection,
      id: typeof id === 'string' ? new ObjectId(id) : id,
    });
    return this;
  }

  async execute(): Promise<IPlanExecuteResult> {
    const elapsed = Date.now() - this.context.metadata.startedAt.getTime();
    if (elapsed > this.context.metadata.maxDurationMs) {
      throw new DatabaseException(
        `Transaction ${this.txId} exceeded max duration`,
        {
          txId: this.txId,
          elapsed,
        },
      );
    }

    if (this.operations.length === 0) {
      return { inserts: new Map(), updates: new Map(), deletes: new Map() };
    }

    const lockResources = this.operations.map((op) => {
      if (op.type === 'insert') {
        return {
          type: op.collection,
          id: op.predictedId.toString(),
          mode: 'write' as const,
        };
      }
      return {
        type: op.collection,
        id: op.id.toString(),
        mode: 'write' as const,
      };
    });

    const lockResult = await this.lockService.acquireLocks(
      this.txId,
      lockResources,
      {
        waitTimeout: this.options.waitTimeout,
        maxRetries: this.options.maxRetries,
      },
    );

    if (!lockResult.success) {
      throw new DatabaseException('Cannot acquire locks for plan execution', {
        txId: this.txId,
        failedLocks: lockResult.failedLocks,
      });
    }

    const updateOps = this.operations.filter(
      (op): op is IPlanUpdate => op.type === 'update',
    );
    const deleteOps = this.operations.filter(
      (op): op is IPlanDelete => op.type === 'delete',
    );
    const insertOps = this.operations.filter(
      (op): op is IPlanInsert => op.type === 'insert',
    );

    const oldDocMap = new Map<string, any>();

    const fetchPromises: Promise<void>[] = [];
    const fetchGroups = new Map<string, ObjectId[]>();

    for (const op of [...updateOps, ...deleteOps]) {
      const ids = fetchGroups.get(op.collection) || [];
      ids.push(op.id);
      fetchGroups.set(op.collection, ids);
    }

    for (const [collName, ids] of fetchGroups) {
      fetchPromises.push(
        (async () => {
          const coll = this.mongoService.getDb().collection(collName);
          const docs = await coll.find({ _id: { $in: ids } }).toArray();
          for (const doc of docs) {
            oldDocMap.set(`${collName}:${doc._id.toString()}`, doc);
          }
        })(),
      );
    }

    await Promise.all(fetchPromises);

    for (const op of updateOps) {
      const key = `${op.collection}:${op.id.toString()}`;
      if (!oldDocMap.has(key)) {
        throw new DatabaseException(`Document not found: ${key}`, {
          collection: op.collection,
          id: op.id.toString(),
        });
      }
    }

    const logEntries = await this.logService.logOperationsBatch(
      this.txId,
      this.operations.map((op) => {
        if (op.type === 'insert') {
          return {
            operationType: 'insert' as TOperationType,
            collection: op.collection,
            documentId: op.predictedId,
            oldData: null,
            newData: op.data,
          };
        }
        if (op.type === 'update') {
          const oldDoc = oldDocMap.get(`${op.collection}:${op.id.toString()}`);
          return {
            operationType: 'update' as TOperationType,
            collection: op.collection,
            documentId: op.id,
            oldData: oldDoc,
            newData: op.data,
          };
        }
        const oldDoc = oldDocMap.get(`${op.collection}:${op.id.toString()}`);
        return {
          operationType: 'delete' as TOperationType,
          collection: op.collection,
          documentId: op.id,
          oldData: oldDoc,
          newData: null,
        };
      }),
    );

    const result: IPlanExecuteResult = {
      inserts: new Map(),
      updates: new Map(),
      deletes: new Map(),
    };
    const executePromises: Promise<void>[] = [];

    const insertsByCollection = new Map<string, IPlanInsert[]>();
    for (const op of insertOps) {
      const arr = insertsByCollection.get(op.collection) || [];
      arr.push(op);
      insertsByCollection.set(op.collection, arr);
    }
    for (const [collName, ops] of insertsByCollection) {
      executePromises.push(
        (async () => {
          const coll = this.mongoService.getDb().collection(collName);
          const docs = ops.map((op) => ({
            ...op.data,
            _id: op.predictedId,
            __txId: this.txId,
          }));
          await coll.insertMany(docs, { ordered: false });
          for (const op of ops) {
            this.context.modifiedDocuments.push({
              collection: collName,
              id: op.predictedId.toString(),
            });
          }
          result.inserts.set(
            collName,
            docs.map((d) =>
              stripInternalFields({ ...d, id: d._id.toString() }),
            ),
          );
        })(),
      );
    }

    const updatesByCollection = new Map<string, IPlanUpdate[]>();
    for (const op of updateOps) {
      const arr = updatesByCollection.get(op.collection) || [];
      arr.push(op);
      updatesByCollection.set(op.collection, arr);
    }
    for (const [collName, ops] of updatesByCollection) {
      executePromises.push(
        (async () => {
          const coll = this.mongoService.getDb().collection(collName);
          const bulkOps = ops.map((op) => ({
            updateOne: {
              filter: { _id: op.id },
              update: { $set: { ...op.data, __txId: this.txId } },
            },
          }));
          await coll.bulkWrite(bulkOps, { ordered: false });
          for (const op of ops) {
            this.context.modifiedDocuments.push({
              collection: collName,
              id: op.id.toString(),
            });
          }
          const updatedDocs = await coll
            .find({ _id: { $in: ops.map((o) => o.id) } })
            .toArray();
          result.updates.set(collName, updatedDocs.map(stripInternalFields));
        })(),
      );
    }

    const deletesByCollection = new Map<string, IPlanDelete[]>();
    for (const op of deleteOps) {
      const arr = deletesByCollection.get(op.collection) || [];
      arr.push(op);
      deletesByCollection.set(op.collection, arr);
    }
    for (const [collName, ops] of deletesByCollection) {
      executePromises.push(
        (async () => {
          const coll = this.mongoService.getDb().collection(collName);
          const ids = ops.map((op) => op.id);
          await coll.deleteMany({ _id: { $in: ids } });
          result.deletes.set(
            collName,
            ids.map((id) => id.toString()),
          );
        })(),
      );
    }

    try {
      await Promise.all(executePromises);

      await this.logService.markOperationsBatchCompleted(
        logEntries.map((e) => e.operationId),
      );

      return result;
    } catch (error) {
      await this.logService.markOperationsBatchFailed(
        logEntries.map((e) => e.operationId),
        error.message,
      );
      throw error;
    }
  }
}

const TX_INTERNAL_FIELDS = ['__txId'];

function stripInternalFields(doc: any): any {
  if (!doc || typeof doc !== 'object') return doc;
  const cleaned = { ...doc };
  for (const field of TX_INTERNAL_FIELDS) {
    delete cleaned[field];
  }
  return cleaned;
}
