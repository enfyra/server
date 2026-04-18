import { Logger } from '../../../shared/logger';
import { ObjectId, AggregationCursor } from 'mongodb';
import {
  MongoSagaLockService,
  ILockAcquisitionResult,
} from './mongo-saga-lock.service';
import {
  MongoOperationLogService,
  IOperationLog,
  IRollbackResult,
} from './mongo-operation-log.service';
import { MongoService } from './mongo.service';
import { DatabaseException } from '../../../core/exceptions/custom-exceptions';
import { ISagaOptions, ISagaContext } from './mongo-saga.types';
import { SagaPlan } from './mongo-saga-plan';

const TX_INTERNAL_FIELDS = ['__txId'];

function stripInternalFields(doc: any): any {
  if (!doc || typeof doc !== 'object') return doc;
  const cleaned = { ...doc };
  for (const field of TX_INTERNAL_FIELDS) {
    delete cleaned[field];
  }
  return cleaned;
}

export class MongoSagaSession {
  private readonly logger = new Logger(MongoSagaSession.name);

  constructor(
    public readonly txId: string,
    private readonly lockService: MongoSagaLockService,
    private readonly logService: MongoOperationLogService,
    private readonly mongoService: MongoService,
    private readonly options: Required<ISagaOptions>,
    private readonly context: ISagaContext,
  ) {}

  private checkDuration(): void {
    const elapsed = Date.now() - this.context.metadata.startedAt.getTime();
    if (elapsed > this.context.metadata.maxDurationMs) {
      throw new DatabaseException(
        `Transaction ${this.txId} exceeded max duration of ${this.context.metadata.maxDurationMs}ms`,
        {
          txId: this.txId,
          elapsed,
          maxDuration: this.context.metadata.maxDurationMs,
        },
      );
    }
  }

  assertWithinMaxDuration(): void {
    this.checkDuration();
  }

  private buildSagaDocumentVisibilityFilter(): Record<string, unknown> {
    return {
      $or: [
        { __txId: { $exists: false } },
        { __txId: null },
        { __txId: this.txId },
      ],
    };
  }

  aggregate(
    collectionName: string,
    pipeline: any[],
    options?: Record<string, unknown>,
  ): AggregationCursor {
    this.checkDuration();
    const collection = this.mongoService.getDb().collection(collectionName);
    const vis = this.buildSagaDocumentVisibilityFilter();
    const fullPipeline = [{ $match: vis }, ...pipeline];
    return collection.aggregate(fullPipeline, options);
  }

  async countDocuments(collectionName: string, filter?: any): Promise<number> {
    this.checkDuration();
    const collection = this.mongoService.getDb().collection(collectionName);
    const vis = this.buildSagaDocumentVisibilityFilter();
    const merged =
      filter && typeof filter === 'object' && Object.keys(filter).length > 0
        ? { $and: [filter, vis] }
        : vis;
    return collection.countDocuments(merged);
  }

  private trackModifiedDocument(collection: string, id: string): void {
    this.context.modifiedDocuments.push({ collection, id });
  }

  async lockResources(
    resources: Array<{ type: string; id: string; mode: 'read' | 'write' }>,
  ): Promise<ILockAcquisitionResult> {
    return this.lockService.acquireLocks(this.txId, resources, {
      waitTimeout: this.options.waitTimeout,
      maxRetries: this.options.maxRetries,
    });
  }

  async insertOne(
    collectionName: string,
    data: any,
    options?: { skipLogging?: boolean },
  ): Promise<any> {
    this.checkDuration();

    const predictedId = new ObjectId();
    const resourceKey = `${collectionName}:${predictedId.toString()}`;

    const lockResult = await this.lockService.acquireLocks(this.txId, [
      { type: collectionName, id: predictedId.toString(), mode: 'write' },
    ]);

    if (!lockResult.success) {
      throw new DatabaseException(
        `Cannot acquire lock for insert on ${collectionName}`,
        {
          txId: this.txId,
          resource: resourceKey,
          failedLocks: lockResult.failedLocks,
        },
      );
    }

    let logEntry: IOperationLog | undefined;

    if (!options?.skipLogging) {
      logEntry = await this.logService.logOperation(
        this.txId,
        'insert',
        collectionName,
        predictedId,
        null,
        data,
      );
    }

    try {
      const collection = this.mongoService.getDb().collection(collectionName);
      await collection.insertOne({
        ...data,
        _id: predictedId,
        __txId: this.txId,
      });

      this.trackModifiedDocument(collectionName, predictedId.toString());

      if (logEntry) {
        await this.logService.markOperationCompleted(logEntry.operationId);
      }

      return {
        ...data,
        _id: predictedId,
        id: predictedId.toString(),
      };
    } catch (error) {
      if (logEntry) {
        await this.logService.markOperationFailed(
          logEntry.operationId,
          error.message,
        );
      }
      throw error;
    }
  }

  async updateOne(
    collectionName: string,
    id: string | ObjectId,
    data: any,
    options?: { skipLogging?: boolean; skipInverseRelations?: boolean },
  ): Promise<any> {
    this.checkDuration();

    const objectId = typeof id === 'string' ? new ObjectId(id) : id;
    const idString = objectId.toString();
    const resourceKey = `${collectionName}:${idString}`;

    const lockResult = await this.lockService.acquireLocks(this.txId, [
      { type: collectionName, id: idString, mode: 'write' },
    ]);

    if (!lockResult.success) {
      throw new DatabaseException(
        `Cannot acquire lock for update on ${collectionName}:${idString}`,
        {
          txId: this.txId,
          resource: resourceKey,
          failedLocks: lockResult.failedLocks,
        },
      );
    }

    const collection = this.mongoService.getDb().collection(collectionName);
    const oldDoc = await collection.findOne({ _id: objectId });

    if (!oldDoc) {
      throw new DatabaseException(
        `Document not found: ${collectionName}:${idString}`,
        {
          collection: collectionName,
          id: idString,
        },
      );
    }

    let logEntry: IOperationLog | undefined;

    if (!options?.skipLogging) {
      logEntry = await this.logService.logOperation(
        this.txId,
        'update',
        collectionName,
        objectId,
        oldDoc,
        data,
      );
    }

    try {
      const updateData = {
        ...data,
        __txId: this.txId,
      };

      await collection.updateOne({ _id: objectId }, { $set: updateData });

      this.trackModifiedDocument(collectionName, idString);

      if (logEntry) {
        await this.logService.markOperationCompleted(logEntry.operationId);
      }

      const updated = await collection.findOne({ _id: objectId });
      return stripInternalFields(updated);
    } catch (error) {
      if (logEntry) {
        await this.logService.markOperationFailed(
          logEntry.operationId,
          error.message,
        );
      }
      throw error;
    }
  }

  async updateOneByFilter(
    collectionName: string,
    filter: any,
    update: any,
    options?: { skipLogging?: boolean },
  ): Promise<any> {
    this.checkDuration();

    const collection = this.mongoService.getDb().collection(collectionName);
    const oldDoc = await collection.findOne(filter);
    if (!oldDoc) {
      return {
        acknowledged: true,
        matchedCount: 0,
        modifiedCount: 0,
        upsertedCount: 0,
      };
    }
    const idString = oldDoc._id.toString();
    const lockResult = await this.lockService.acquireLocks(this.txId, [
      { type: collectionName, id: idString, mode: 'write' },
    ]);
    if (!lockResult.success) {
      throw new DatabaseException(
        `Cannot acquire lock for update on ${collectionName}:${idString}`,
        {
          txId: this.txId,
          resource: `${collectionName}:${idString}`,
          failedLocks: lockResult.failedLocks,
        },
      );
    }
    let logEntry: IOperationLog | undefined;
    if (!options?.skipLogging) {
      logEntry = await this.logService.logOperation(
        this.txId,
        'update',
        collectionName,
        oldDoc._id,
        oldDoc,
        update,
      );
    }
    try {
      let payload: any;
      if (!update || typeof update !== 'object' || Array.isArray(update)) {
        payload = { $set: { __txId: this.txId } };
      } else {
        const keys = Object.keys(update);
        const operatorOnly =
          keys.length > 0 && keys.every((k) => k.startsWith('$'));
        if (operatorOnly) {
          payload = { ...update };
          payload.$set = { ...(payload.$set || {}), __txId: this.txId };
        } else {
          payload = { $set: { ...update, __txId: this.txId } };
        }
      }
      const result = await collection.updateOne(filter, payload);

      this.trackModifiedDocument(collectionName, idString);

      if (logEntry) {
        await this.logService.markOperationCompleted(logEntry.operationId);
      }
      return result;
    } catch (error) {
      if (logEntry) {
        await this.logService.markOperationFailed(
          logEntry.operationId,
          error.message,
        );
      }
      throw error;
    }
  }

  async updateManyByFilter(
    collectionName: string,
    filter: any,
    update: any,
    options?: { skipLogging?: boolean },
  ): Promise<any> {
    this.checkDuration();

    const collection = this.mongoService.getDb().collection(collectionName);
    const cursor = collection.find(filter || {});
    let modified = 0;
    let matched = 0;
    for await (const d of cursor) {
      matched++;
      if (matched % 200 === 0) {
        this.checkDuration();
      }
      const r = await this.updateOneByFilter(
        collectionName,
        { _id: d._id },
        update,
        options,
      );
      modified += r.modifiedCount || 0;
    }
    return {
      acknowledged: true,
      matchedCount: matched,
      modifiedCount: modified,
    };
  }

  async deleteOne(
    collectionName: string,
    id: string | ObjectId,
    options?: { skipLogging?: boolean },
  ): Promise<boolean> {
    this.checkDuration();

    const objectId = typeof id === 'string' ? new ObjectId(id) : id;
    const idString = objectId.toString();

    const lockResult = await this.lockService.acquireLocks(this.txId, [
      { type: collectionName, id: idString, mode: 'write' },
    ]);

    if (!lockResult.success) {
      throw new DatabaseException(
        `Cannot acquire lock for delete on ${collectionName}:${idString}`,
        {
          txId: this.txId,
          failedLocks: lockResult.failedLocks,
        },
      );
    }

    const collection = this.mongoService.getDb().collection(collectionName);
    const oldDoc = await collection.findOne({ _id: objectId });

    if (!oldDoc) {
      return false;
    }

    let logEntry: IOperationLog | undefined;

    if (!options?.skipLogging) {
      logEntry = await this.logService.logOperation(
        this.txId,
        'delete',
        collectionName,
        objectId,
        oldDoc,
        null,
      );
    }

    try {
      const result = await collection.deleteOne({ _id: objectId });

      if (logEntry) {
        await this.logService.markOperationCompleted(logEntry.operationId);
      }

      return result.deletedCount > 0;
    } catch (error) {
      if (logEntry) {
        await this.logService.markOperationFailed(
          logEntry.operationId,
          error.message,
        );
      }
      throw error;
    }
  }

  async findOne(
    collectionName: string,
    filter: any,
    options?: { useConsistentRead?: boolean },
  ): Promise<any> {
    const collection = this.mongoService.getDb().collection(collectionName);

    if (options?.useConsistentRead && filter._id) {
      const id =
        typeof filter._id === 'string' ? new ObjectId(filter._id) : filter._id;
      const lockResult = await this.lockService.acquireReadLocks(this.txId, [
        { type: collectionName, id: id.toString() },
      ]);
      if (!lockResult.success) {
        throw new DatabaseException(
          `Cannot acquire read lock on ${collectionName}:${id}`,
          {
            txId: this.txId,
            failedLocks: lockResult.failedLocks,
          },
        );
      }
    }

    const doc = await collection.findOne(filter);
    return stripInternalFields(doc);
  }

  async find(
    collectionName: string,
    filter?: any,
    options?: { limit?: number; skip?: number; useConsistentRead?: boolean },
  ): Promise<any[]> {
    const collection = this.mongoService.getDb().collection(collectionName);

    let cursor = collection.find(filter || {});
    if (options?.skip) cursor = cursor.skip(options.skip);
    if (options?.limit) cursor = cursor.limit(options.limit);

    const docs = await cursor.toArray();

    if (options?.useConsistentRead && docs.length > 0) {
      const ids = docs.map((d) => d._id.toString());
      const lockResult = await this.lockService.acquireReadLocks(
        this.txId,
        ids.map((id) => ({ type: collectionName, id })),
      );

      if (!lockResult.success) {
        throw new DatabaseException(
          `Cannot acquire read locks on ${collectionName}`,
          {
            txId: this.txId,
            failedLocks: lockResult.failedLocks,
          },
        );
      }

      const reDocs = await collection
        .find({ _id: { $in: docs.map((d) => d._id) } })
        .toArray();
      return reDocs.map(stripInternalFields);
    }

    return docs.map(stripInternalFields);
  }

  async createCheckpoint(description?: string): Promise<string> {
    return this.logService.createCheckpoint(this.txId, description);
  }

  async rollbackToCheckpoint(checkpointId: string): Promise<IRollbackResult> {
    return this.logService.rollbackToCheckpoint(this.txId, checkpointId);
  }

  async getStats() {
    return this.logService.getTransactionStats(this.txId);
  }

  async insertMany(
    collectionName: string,
    documents: any[],
    options?: { skipLogging?: boolean; ordered?: boolean },
  ): Promise<any[]> {
    this.checkDuration();

    if (documents.length === 0) {
      return [];
    }

    const predictedIds = documents.map(() => new ObjectId());
    const lockResources = predictedIds.map((id) => ({
      type: collectionName,
      id: id.toString(),
      mode: 'write' as const,
    }));

    const lockResult = await this.lockService.acquireLocks(
      this.txId,
      lockResources,
    );

    if (!lockResult.success) {
      throw new DatabaseException(
        `Cannot acquire locks for batch insert on ${collectionName}`,
        {
          txId: this.txId,
          failedLocks: lockResult.failedLocks,
        },
      );
    }

    const docsWithIds = documents.map((doc, index) => ({
      ...doc,
      _id: predictedIds[index],
      __txId: this.txId,
    }));

    let logEntries: IOperationLog[] = [];

    if (!options?.skipLogging) {
      logEntries = [];
      for (let i = 0; i < predictedIds.length; i++) {
        const entry = await this.logService.logOperation(
          this.txId,
          'insert',
          collectionName,
          predictedIds[i],
          null,
          documents[i],
        );
        logEntries.push(entry);
      }
    }

    try {
      const collection = this.mongoService.getDb().collection(collectionName);
      await collection.insertMany(docsWithIds, {
        ordered: options?.ordered ?? true,
      });

      for (const doc of docsWithIds) {
        this.trackModifiedDocument(collectionName, doc._id.toString());
      }

      if (logEntries.length > 0) {
        await Promise.all(
          logEntries.map((entry) =>
            this.logService.markOperationCompleted(entry.operationId),
          ),
        );
      }

      return docsWithIds.map((doc) => ({
        ...stripInternalFields(doc),
        id: doc._id.toString(),
      }));
    } catch (error) {
      if (logEntries.length > 0) {
        await Promise.all(
          logEntries.map((entry) =>
            this.logService.markOperationFailed(
              entry.operationId,
              error.message,
            ),
          ),
        );
      }
      throw error;
    }
  }

  async updateMany(
    collectionName: string,
    updates: Array<{ id: string | ObjectId; data: any }>,
    options?: { skipLogging?: boolean; skipInverseRelations?: boolean },
  ): Promise<any[]> {
    this.checkDuration();

    if (updates.length === 0) {
      return [];
    }

    const objectIds = updates.map((u) =>
      typeof u.id === 'string' ? new ObjectId(u.id) : u.id,
    );
    const lockResources = objectIds.map((id) => ({
      type: collectionName,
      id: id.toString(),
      mode: 'write' as const,
    }));

    const lockResult = await this.lockService.acquireLocks(
      this.txId,
      lockResources,
    );

    if (!lockResult.success) {
      throw new DatabaseException(
        `Cannot acquire locks for batch update on ${collectionName}`,
        {
          txId: this.txId,
          failedLocks: lockResult.failedLocks,
        },
      );
    }

    const collection = this.mongoService.getDb().collection(collectionName);

    const oldDocs = await collection
      .find({ _id: { $in: objectIds } })
      .toArray();
    const oldDocMap = new Map(oldDocs.map((d) => [d._id.toString(), d]));

    const logEntries: IOperationLog[] = [];

    if (!options?.skipLogging) {
      for (let i = 0; i < updates.length; i++) {
        const oldDoc = oldDocMap.get(objectIds[i].toString()) || null;
        const entry = await this.logService.logOperation(
          this.txId,
          'update',
          collectionName,
          objectIds[i],
          oldDoc,
          updates[i].data,
        );
        logEntries.push(entry);
      }
    }

    try {
      const bulkOps = updates.map((update, index) => ({
        updateOne: {
          filter: { _id: objectIds[index] },
          update: { $set: { ...update.data, __txId: this.txId } },
        },
      }));

      await collection.bulkWrite(bulkOps, { ordered: true });

      for (const id of objectIds) {
        this.trackModifiedDocument(collectionName, id.toString());
      }

      if (logEntries.length > 0) {
        await Promise.all(
          logEntries.map((entry) =>
            this.logService.markOperationCompleted(entry.operationId),
          ),
        );
      }

      const results = await collection
        .find({ _id: { $in: objectIds } })
        .toArray();
      return results.map(stripInternalFields);
    } catch (error) {
      if (logEntries.length > 0) {
        await Promise.all(
          logEntries.map((entry) =>
            this.logService.markOperationFailed(
              entry.operationId,
              error.message,
            ),
          ),
        );
      }
      throw error;
    }
  }

  async deleteMany(
    collectionName: string,
    ids: Array<string | ObjectId>,
    options?: { skipLogging?: boolean },
  ): Promise<{ deletedCount: number; deletedIds: string[] }> {
    this.checkDuration();

    if (ids.length === 0) {
      return { deletedCount: 0, deletedIds: [] };
    }

    const objectIds = ids.map((id) =>
      typeof id === 'string' ? new ObjectId(id) : id,
    );
    const lockResources = objectIds.map((id) => ({
      type: collectionName,
      id: id.toString(),
      mode: 'write' as const,
    }));

    const lockResult = await this.lockService.acquireLocks(
      this.txId,
      lockResources,
    );

    if (!lockResult.success) {
      throw new DatabaseException(
        `Cannot acquire locks for batch delete on ${collectionName}`,
        {
          txId: this.txId,
          failedLocks: lockResult.failedLocks,
        },
      );
    }

    const collection = this.mongoService.getDb().collection(collectionName);
    const oldDocs = await collection
      .find({ _id: { $in: objectIds } })
      .toArray();

    if (oldDocs.length === 0) {
      return { deletedCount: 0, deletedIds: [] };
    }

    const logEntries: IOperationLog[] = [];

    if (!options?.skipLogging) {
      for (const oldDoc of oldDocs) {
        const entry = await this.logService.logOperation(
          this.txId,
          'delete',
          collectionName,
          oldDoc._id,
          oldDoc,
          null,
        );
        logEntries.push(entry);
      }
    }

    const idsToDelete = oldDocs.map((d) => d._id);

    try {
      const result = await collection.deleteMany({ _id: { $in: idsToDelete } });

      if (logEntries.length > 0) {
        await Promise.all(
          logEntries.map((entry) =>
            this.logService.markOperationCompleted(entry.operationId),
          ),
        );
      }

      return {
        deletedCount: result.deletedCount || 0,
        deletedIds: idsToDelete.map((id) => id.toString()),
      };
    } catch (error) {
      if (logEntries.length > 0) {
        await Promise.all(
          logEntries.map((entry) =>
            this.logService.markOperationFailed(
              entry.operationId,
              error.message,
            ),
          ),
        );
      }
      throw error;
    }
  }

  async parallelRead(
    reads: Array<{
      collection: string;
      filter: any;
      projection?: any;
    }>,
  ): Promise<any[]> {
    const promises = reads.map((read) =>
      this.mongoService
        .getDb()
        .collection(read.collection)
        .findOne(read.filter, {
          projection: read.projection,
        })
        .then(stripInternalFields),
    );

    return Promise.all(promises);
  }

  createSagaPlan(): SagaPlan {
    return new SagaPlan(
      this.txId,
      this.lockService,
      this.logService,
      this.mongoService,
      this.options,
      this.context,
    );
  }
}
