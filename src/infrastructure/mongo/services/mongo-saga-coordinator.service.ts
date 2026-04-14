import {
  Injectable,
  Logger,
  Inject,
  Optional,
  forwardRef,
  OnModuleInit,
  OnModuleDestroy,
} from '@nestjs/common';
import { Db, ObjectId, AggregationCursor } from 'mongodb';
import { AsyncLocalStorage } from 'async_hooks';
import { MongoService } from './mongo.service';
import { MongoSagaLockService, ILockAcquisitionResult } from './mongo-saga-lock.service';
import { MongoOperationLogService, IOperationLog, IRollbackResult, TOperationType } from './mongo-operation-log.service';
import { DatabaseException, ValidationException } from '../../../core/exceptions/custom-exceptions';
import { CacheService } from '../../cache/services/cache.service';
import { InstanceService } from '../../../shared/services/instance.service';
import {
  REDIS_TTL,
  SAGA_ORPHAN_RECOVERY_LOCK_KEY,
} from '../../../shared/utils/constant';

const TX_INTERNAL_FIELDS = ['__txId'];

function stripInternalFields(doc: any): any {
  if (!doc || typeof doc !== 'object') return doc;
  const cleaned = { ...doc };
  for (const field of TX_INTERNAL_FIELDS) {
    delete cleaned[field];
  }
  return cleaned;
}

export interface ISagaContext {
  txId: string;
  status: 'active' | 'committing' | 'rolling_back' | 'completed' | 'aborted' | 'failed';
  lockedResources: Set<string>;
  operations: IOperationLog[];
  modifiedDocuments: Array<{ collection: string; id: string }>;
  metadata: {
    startedAt: Date;
    lastActivityAt: Date;
    maxDurationMs: number;
  };
}

export interface ISagaOptions {
  maxDurationMs?: number;
  lockTimeoutMs?: number;
  maxRetries?: number;
  waitTimeout?: number;
  autoRollbackOnError?: boolean;
}

export interface ISagaResult<T> {
  success: boolean;
  data?: T;
  error?: Error;
  txId: string;
  rollbackResult?: IRollbackResult;
  stats?: {
    durationMs: number;
    operationsCount: number;
    locksAcquired: number;
  };
}

export interface ISagaRecoveryMetrics {
  totalRuns: number;
  bootRuns: number;
  periodicRuns: number;
  skippedDueToRedisLock: number;
  lastRunAt: Date | null;
  lastCleaned: number;
  lastRecovered: number;
  lastError: string | null;
}

@Injectable()
export class MongoSagaCoordinator implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(MongoSagaCoordinator.name);
  private readonly sagaContext = new AsyncLocalStorage<ISagaContext>();
  private readonly defaultOptions: Required<ISagaOptions> = {
    maxDurationMs: 30000,
    lockTimeoutMs: 10000,
    maxRetries: 3,
    waitTimeout: 5000,
    autoRollbackOnError: true,
  };
  private cleanupIntervalRef: ReturnType<typeof setInterval> | null = null;
  private cleanupFirstTimeoutRef: ReturnType<typeof setTimeout> | null = null;
  private static readonly RECOVERY_BATCH_SIZE = 500;
  private static readonly RECOVERY_MAX_PASSES_PER_COLLECTION = 100;
  private recoveryMetrics: ISagaRecoveryMetrics = {
    totalRuns: 0,
    bootRuns: 0,
    periodicRuns: 0,
    skippedDueToRedisLock: 0,
    lastRunAt: null,
    lastCleaned: 0,
    lastRecovered: 0,
    lastError: null,
  };

  constructor(
    @Inject(forwardRef(() => MongoService))
    private readonly mongoService: MongoService,
    private readonly lockService: MongoSagaLockService,
    private readonly logService: MongoOperationLogService,
    private readonly instanceService: InstanceService,
    @Optional() private readonly cacheService?: CacheService,
  ) {}

  async onModuleInit(): Promise<void> {
    try {
      this.mongoService.getDb();
    } catch {
      return;
    }
    try {
      await this.recoverOrphanedSagas('boot');
    } catch (error) {
      this.logger.error(
        `Saga boot recovery failed: ${error instanceof Error ? error.message : String(error)}`,
      );
    }
    this.startPeriodicCleanup(60_000);
  }

  onModuleDestroy(): void {
    this.stopPeriodicCleanup();
  }

  startPeriodicCleanup(intervalMs = 60_000): void {
    if (this.cleanupIntervalRef || this.cleanupFirstTimeoutRef) return;
    const jitterCap = Math.min(45_000, Math.floor(intervalMs * 0.5));
    const firstDelay = jitterCap > 0 ? Math.floor(Math.random() * (jitterCap + 1)) : 0;
    const run = async () => {
      try {
        await this.recoverOrphanedSagas('periodic');
      } catch (error) {
        this.logger.error(
          `Periodic cleanup failed: ${error instanceof Error ? error.message : String(error)}`,
        );
      }
    };
    this.cleanupFirstTimeoutRef = setTimeout(() => {
      this.cleanupFirstTimeoutRef = null;
      void run();
      this.cleanupIntervalRef = setInterval(() => void run(), intervalMs);
      if (this.cleanupIntervalRef.unref) {
        this.cleanupIntervalRef.unref();
      }
    }, firstDelay);
    if (this.cleanupFirstTimeoutRef.unref) {
      this.cleanupFirstTimeoutRef.unref();
    }
  }

  stopPeriodicCleanup(): void {
    if (this.cleanupFirstTimeoutRef) {
      clearTimeout(this.cleanupFirstTimeoutRef);
      this.cleanupFirstTimeoutRef = null;
    }
    if (this.cleanupIntervalRef) {
      clearInterval(this.cleanupIntervalRef);
      this.cleanupIntervalRef = null;
    }
  }

  private async recoverStaleMarkersInCollection(
    db: Db,
    collectionName: string,
  ): Promise<number> {
    const batchSize = MongoSagaCoordinator.RECOVERY_BATCH_SIZE;
    const maxPasses = MongoSagaCoordinator.RECOVERY_MAX_PASSES_PER_COLLECTION;
    let total = 0;
    for (let pass = 0; pass < maxPasses; pass++) {
      const stale = await db
        .collection(collectionName)
        .find(
          { __txId: { $exists: true, $ne: null } },
          { projection: { _id: 1, __txId: 1 } },
        )
        .limit(batchSize)
        .toArray();

      if (stale.length === 0) break;

      const byTx = new Map<string, Array<{ _id: unknown; __txId: string }>>();
      for (const d of stale) {
        const raw = (d as { __txId?: unknown }).__txId;
        const txKey =
          typeof raw === 'string'
            ? raw
            : raw != null
              ? String(raw)
              : '';
        if (!txKey) continue;
        const arr = byTx.get(txKey) || [];
        arr.push({ _id: (d as { _id: unknown })._id, __txId: txKey });
        byTx.set(txKey, arr);
      }

      let passModified = 0;
      for (const [txId, docs] of byTx) {
        const plan = await this.lockService.getOrphanMarkerRecoveryPlan(txId);
        if (!plan.shouldUnsetMarkers) {
          continue;
        }
        if (plan.needsRollbackFirst) {
          try {
            await this.logService.rollbackTransaction(txId);
            await this.lockService.abortTransaction(txId, 'orphan marker recovery');
          } catch (error) {
            this.logger.error(
              `Orphan recovery rollback failed for ${txId}: ${error.message}`,
            );
            continue;
          }
        }
        const ids = docs.map((x) => x._id);
        const result = await db.collection(collectionName).updateMany(
          { _id: { $in: ids as any }, __txId: txId },
          { $unset: { __txId: '' } },
        );
        passModified += result.modifiedCount || 0;
      }
      total += passModified;
      if (passModified === 0) break;
    }
    return total;
  }

  async recoverOrphanedSagas(
    source: 'boot' | 'periodic' = 'periodic',
  ): Promise<{ cleaned: number; recovered: number }> {
    if (this.cacheService) {
      const lockValue = this.instanceService.getInstanceId();
      const acquired = await this.cacheService.acquire(
        SAGA_ORPHAN_RECOVERY_LOCK_KEY,
        lockValue,
        REDIS_TTL.SAGA_ORPHAN_RECOVERY_LOCK_TTL,
      );
      if (!acquired) {
        this.recoveryMetrics.skippedDueToRedisLock++;
        this.logger.debug(
          `Saga orphan recovery skipped (${source}): another instance holds ${SAGA_ORPHAN_RECOVERY_LOCK_KEY}`,
        );
        return { cleaned: 0, recovered: 0 };
      }
      try {
        return await this.runOrphanRecoveryBody(source);
      } finally {
        await this.cacheService.release(
          SAGA_ORPHAN_RECOVERY_LOCK_KEY,
          lockValue,
        );
      }
    }
    return await this.runOrphanRecoveryBody(source);
  }

  private async runOrphanRecoveryBody(
    source: 'boot' | 'periodic',
  ): Promise<{ cleaned: number; recovered: number }> {
    try {
      const orphanedLocks = await this.lockService.cleanupOrphanedLocks();
      const oldLogs = await this.logService.cleanupOldLogs(7);

      const db = this.mongoService.getDb();
      const collections = await db.listCollections().toArray();
      let recovered = 0;

      for (const coll of collections) {
        if (coll.name.startsWith('system_')) continue;
        try {
          recovered += await this.recoverStaleMarkersInCollection(db, coll.name);
        } catch (err) {
          this.logger.warn(
            `Saga orphan marker recovery skipped for collection "${coll.name}": ${err instanceof Error ? err.message : String(err)}`,
          );
        }
      }

      const result = { cleaned: orphanedLocks + oldLogs, recovered };
      this.recordRecoverySuccess(source, result);
      if (source === 'boot') {
        this.logger.log(
          `Saga boot recovery done: cleaned=${result.cleaned} recovered=${result.recovered}`,
        );
      }
      return result;
    } catch (error) {
      this.recordRecoveryFailure(error);
      throw error;
    }
  }

  getSagaRecoveryMetrics(): ISagaRecoveryMetrics {
    return { ...this.recoveryMetrics };
  }

  private recordRecoverySuccess(
    source: 'boot' | 'periodic',
    result: { cleaned: number; recovered: number },
  ): void {
    this.recoveryMetrics.totalRuns++;
    if (source === 'boot') {
      this.recoveryMetrics.bootRuns++;
    } else {
      this.recoveryMetrics.periodicRuns++;
    }
    this.recoveryMetrics.lastRunAt = new Date();
    this.recoveryMetrics.lastCleaned = result.cleaned;
    this.recoveryMetrics.lastRecovered = result.recovered;
    this.recoveryMetrics.lastError = null;
  }

  private recordRecoveryFailure(error: unknown): void {
    this.recoveryMetrics.lastError =
      error instanceof Error ? error.message : String(error);
  }

  private getCurrentContext(): ISagaContext | undefined {
    return this.sagaContext.getStore();
  }

  async execute<T>(
    callback: (tx: MongoSagaSession) => Promise<T>,
    options?: ISagaOptions,
  ): Promise<ISagaResult<T>> {
    const existingContext = this.getCurrentContext();
    if (existingContext) {
      throw new DatabaseException(
        `Nested saga executions are not supported. Already in saga ${existingContext.txId}`,
        { existingTxId: existingContext.txId },
      );
    }

    const opts = { ...this.defaultOptions, ...options };
    const startTime = Date.now();
    let txId: string | null = null;
    let context: ISagaContext | undefined;

    try {
      txId = await this.lockService.beginTransaction();

      context = {
        txId,
        status: 'active',
        lockedResources: new Set(),
        operations: [],
        modifiedDocuments: [],
        metadata: {
          startedAt: new Date(),
          lastActivityAt: new Date(),
          maxDurationMs: opts.maxDurationMs,
        },
      };

      const session = new MongoSagaSession(
        txId,
        this.lockService,
        this.logService,
        this.mongoService,
        opts,
        context,
      );

      const heartbeatMs = Math.min(
        10_000,
        Math.max(2_000, Math.floor(opts.maxDurationMs / 5)),
      );
      let heartbeatTimer: ReturnType<typeof setInterval> | null = null;
      heartbeatTimer = setInterval(() => {
        void this.lockService.renewTransactionLease(txId).catch((err: Error) => {
          this.logger.warn(`[${txId}] Saga lease renew failed: ${err.message}`);
        });
      }, heartbeatMs);

      let result: T;
      try {
        result = await this.sagaContext.run(context, async () => {
          return await callback(session);
        });
      } finally {
        if (heartbeatTimer) {
          clearInterval(heartbeatTimer);
          heartbeatTimer = null;
        }
      }

      const duration = Date.now() - startTime;

      if (duration > opts.maxDurationMs) {
        throw new DatabaseException(`Transaction ${txId} exceeded max duration of ${opts.maxDurationMs}ms`, {
          txId,
          duration,
          maxDuration: opts.maxDurationMs,
        });
      }

      await this.commit(txId, context);

      const txStats = await this.logService.getTransactionStats(txId);

      return {
        success: true,
        data: result,
        txId,
        stats: {
          durationMs: duration,
          operationsCount: txStats.completed + txStats.failed + txStats.pending,
          locksAcquired: context.lockedResources.size,
        },
      };
    } catch (error) {
      const duration = Date.now() - startTime;
      let rollbackResult: IRollbackResult | undefined;

      if (txId && context && opts.autoRollbackOnError) {
        try {
          rollbackResult = await this.rollback(txId, context);
        } catch (rollbackError) {
          this.logger.error(`[${txId}] Rollback failed: ${rollbackError.message}`);
        }
      }

      return {
        success: false,
        error,
        txId: txId || 'unknown',
        rollbackResult,
        stats: txId && context
          ? {
              durationMs: duration,
              operationsCount: context.operations.length,
              locksAcquired: context.lockedResources.size,
            }
          : undefined,
      };
    }
  }

  private async commit(txId: string, context: ISagaContext): Promise<void> {
    context.status = 'committing';

    try {
      await this.cleanupTxIdMarkers(context);
      await this.lockService.commitTransaction(txId);
      context.status = 'completed';
    } catch (error) {
      context.status = 'aborted';
      throw error;
    }
  }

  private async cleanupTxIdMarkers(context: ISagaContext): Promise<void> {
    const byCollection = new Map<string, string[]>();
    for (const doc of context.modifiedDocuments) {
      const ids = byCollection.get(doc.collection) || [];
      ids.push(doc.id);
      byCollection.set(doc.collection, ids);
    }

    const errors: string[] = [];
    const promises: Promise<void>[] = [];
    for (const [collectionName, ids] of byCollection) {
      promises.push(
        (async () => {
          const collection = this.mongoService.getDb().collection(collectionName);
          const objectIds = ids.map((id) => {
            try {
              return new ObjectId(id);
            } catch {
              return id;
            }
          });
          try {
            await collection.updateMany(
              { _id: { $in: objectIds } as any },
              { $unset: { __txId: '' } },
            );
          } catch (error) {
            errors.push(`${collectionName}: ${error.message}`);
          }
        })(),
      );
    }

    await Promise.all(promises);

    if (errors.length > 0) {
      this.logger.error(`[${context.txId}] Partial __txId cleanup failure: ${errors.join('; ')}`);
      throw new DatabaseException('Failed to clean up transaction markers', {
        txId: context.txId,
        failures: errors,
      });
    }
  }

  private async rollback(txId: string, context: ISagaContext): Promise<IRollbackResult> {
    context.status = 'rolling_back';

    const rollbackResult = await this.logService.rollbackTransaction(txId);

    await this.lockService.abortTransaction(txId, rollbackResult.success ? 'user rollback' : 'rollback failed');

    context.status = rollbackResult.success ? 'aborted' : 'failed';

    return rollbackResult;
  }

  async abort(txId: string, reason?: string): Promise<void> {
    const context = this.getCurrentContext();
    if (context && context.txId === txId) {
      await this.rollback(txId, context);
    } else {
      await this.lockService.abortTransaction(txId, reason);
    }
  }

  getSagaStatus(txId: string) {
    return this.lockService.getSagaStatus(txId);
  }
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
      throw new DatabaseException(`Transaction ${this.txId} exceeded max duration of ${this.context.metadata.maxDurationMs}ms`, {
        txId: this.txId,
        elapsed,
        maxDuration: this.context.metadata.maxDurationMs,
      });
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

  async insertOne(collectionName: string, data: any, options?: { skipLogging?: boolean }): Promise<any> {
    this.checkDuration();

    const predictedId = new ObjectId();
    const resourceKey = `${collectionName}:${predictedId.toString()}`;

    const lockResult = await this.lockService.acquireLocks(this.txId, [
      { type: collectionName, id: predictedId.toString(), mode: 'write' },
    ]);

    if (!lockResult.success) {
      throw new DatabaseException(`Cannot acquire lock for insert on ${collectionName}`, {
        txId: this.txId,
        resource: resourceKey,
        failedLocks: lockResult.failedLocks,
      });
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
        await this.logService.markOperationFailed(logEntry.operationId, error.message);
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
      throw new DatabaseException(`Cannot acquire lock for update on ${collectionName}:${idString}`, {
        txId: this.txId,
        resource: resourceKey,
        failedLocks: lockResult.failedLocks,
      });
    }

    const collection = this.mongoService.getDb().collection(collectionName);
    const oldDoc = await collection.findOne({ _id: objectId });

    if (!oldDoc) {
      throw new DatabaseException(`Document not found: ${collectionName}:${idString}`, {
        collection: collectionName,
        id: idString,
      });
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
        await this.logService.markOperationFailed(logEntry.operationId, error.message);
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
      return { acknowledged: true, matchedCount: 0, modifiedCount: 0, upsertedCount: 0 };
    }
    const idString = oldDoc._id.toString();
    const lockResult = await this.lockService.acquireLocks(this.txId, [
      { type: collectionName, id: idString, mode: 'write' },
    ]);
    if (!lockResult.success) {
      throw new DatabaseException(`Cannot acquire lock for update on ${collectionName}:${idString}`, {
        txId: this.txId,
        resource: `${collectionName}:${idString}`,
        failedLocks: lockResult.failedLocks,
      });
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
        const operatorOnly = keys.length > 0 && keys.every((k) => k.startsWith('$'));
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
        await this.logService.markOperationFailed(logEntry.operationId, error.message);
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

  async deleteOne(collectionName: string, id: string | ObjectId, options?: { skipLogging?: boolean }): Promise<boolean> {
    this.checkDuration();

    const objectId = typeof id === 'string' ? new ObjectId(id) : id;
    const idString = objectId.toString();

    const lockResult = await this.lockService.acquireLocks(this.txId, [
      { type: collectionName, id: idString, mode: 'write' },
    ]);

    if (!lockResult.success) {
      throw new DatabaseException(`Cannot acquire lock for delete on ${collectionName}:${idString}`, {
        txId: this.txId,
        failedLocks: lockResult.failedLocks,
      });
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
        await this.logService.markOperationFailed(logEntry.operationId, error.message);
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
      const id = typeof filter._id === 'string' ? new ObjectId(filter._id) : filter._id;
      const lockResult = await this.lockService.acquireReadLocks(this.txId, [
        { type: collectionName, id: id.toString() },
      ]);
      if (!lockResult.success) {
        throw new DatabaseException(`Cannot acquire read lock on ${collectionName}:${id}`, {
          txId: this.txId,
          failedLocks: lockResult.failedLocks,
        });
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
        throw new DatabaseException(`Cannot acquire read locks on ${collectionName}`, {
          txId: this.txId,
          failedLocks: lockResult.failedLocks,
        });
      }

      const reDocs = await collection.find({ _id: { $in: docs.map((d) => d._id) } }).toArray();
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

    const lockResult = await this.lockService.acquireLocks(this.txId, lockResources);

    if (!lockResult.success) {
      throw new DatabaseException(`Cannot acquire locks for batch insert on ${collectionName}`, {
        txId: this.txId,
        failedLocks: lockResult.failedLocks,
      });
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
      await collection.insertMany(docsWithIds, { ordered: options?.ordered ?? true });

      for (const doc of docsWithIds) {
        this.trackModifiedDocument(collectionName, doc._id.toString());
      }

      if (logEntries.length > 0) {
        await Promise.all(
          logEntries.map((entry) => this.logService.markOperationCompleted(entry.operationId)),
        );
      }

      return docsWithIds.map((doc) => ({
        ...stripInternalFields(doc),
        id: doc._id.toString(),
      }));
    } catch (error) {
      if (logEntries.length > 0) {
        await Promise.all(
          logEntries.map((entry) => this.logService.markOperationFailed(entry.operationId, error.message)),
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

    const objectIds = updates.map((u) => (typeof u.id === 'string' ? new ObjectId(u.id) : u.id));
    const lockResources = objectIds.map((id) => ({
      type: collectionName,
      id: id.toString(),
      mode: 'write' as const,
    }));

    const lockResult = await this.lockService.acquireLocks(this.txId, lockResources);

    if (!lockResult.success) {
      throw new DatabaseException(`Cannot acquire locks for batch update on ${collectionName}`, {
        txId: this.txId,
        failedLocks: lockResult.failedLocks,
      });
    }

    const collection = this.mongoService.getDb().collection(collectionName);

    const oldDocs = await collection.find({ _id: { $in: objectIds } }).toArray();
    const oldDocMap = new Map(oldDocs.map((d) => [d._id.toString(), d]));

    let logEntries: IOperationLog[] = [];

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
          logEntries.map((entry) => this.logService.markOperationCompleted(entry.operationId)),
        );
      }

      const results = await collection.find({ _id: { $in: objectIds } }).toArray();
      return results.map(stripInternalFields);
    } catch (error) {
      if (logEntries.length > 0) {
        await Promise.all(
          logEntries.map((entry) => this.logService.markOperationFailed(entry.operationId, error.message)),
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

    const objectIds = ids.map((id) => (typeof id === 'string' ? new ObjectId(id) : id));
    const lockResources = objectIds.map((id) => ({
      type: collectionName,
      id: id.toString(),
      mode: 'write' as const,
    }));

    const lockResult = await this.lockService.acquireLocks(this.txId, lockResources);

    if (!lockResult.success) {
      throw new DatabaseException(`Cannot acquire locks for batch delete on ${collectionName}`, {
        txId: this.txId,
        failedLocks: lockResult.failedLocks,
      });
    }

    const collection = this.mongoService.getDb().collection(collectionName);
    const oldDocs = await collection.find({ _id: { $in: objectIds } }).toArray();

    if (oldDocs.length === 0) {
      return { deletedCount: 0, deletedIds: [] };
    }

    let logEntries: IOperationLog[] = [];

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
          logEntries.map((entry) => this.logService.markOperationCompleted(entry.operationId)),
        );
      }

      return {
        deletedCount: result.deletedCount || 0,
        deletedIds: idsToDelete.map((id) => id.toString()),
      };
    } catch (error) {
      if (logEntries.length > 0) {
        await Promise.all(
          logEntries.map((entry) => this.logService.markOperationFailed(entry.operationId, error.message)),
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
      this.mongoService.getDb().collection(read.collection).findOne(read.filter, {
        projection: read.projection,
      }).then(stripInternalFields),
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
      throw new DatabaseException(`Transaction ${this.txId} exceeded max duration`, {
        txId: this.txId,
        elapsed,
      });
    }

    if (this.operations.length === 0) {
      return { inserts: new Map(), updates: new Map(), deletes: new Map() };
    }

    const lockResources = this.operations.map((op) => {
      if (op.type === 'insert') {
        return { type: op.collection, id: op.predictedId.toString(), mode: 'write' as const };
      }
      return { type: op.collection, id: op.id.toString(), mode: 'write' as const };
    });

    const lockResult = await this.lockService.acquireLocks(this.txId, lockResources, {
      waitTimeout: this.options.waitTimeout,
      maxRetries: this.options.maxRetries,
    });

    if (!lockResult.success) {
      throw new DatabaseException('Cannot acquire locks for plan execution', {
        txId: this.txId,
        failedLocks: lockResult.failedLocks,
      });
    }

    const updateOps = this.operations.filter((op): op is IPlanUpdate => op.type === 'update');
    const deleteOps = this.operations.filter((op): op is IPlanDelete => op.type === 'delete');
    const insertOps = this.operations.filter((op): op is IPlanInsert => op.type === 'insert');

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
        throw new DatabaseException(`Document not found: ${key}`, { collection: op.collection, id: op.id.toString() });
      }
    }

    const logEntries = await this.logService.logOperationsBatch(
      this.txId,
      this.operations.map((op) => {
        if (op.type === 'insert') {
          return { operationType: 'insert' as TOperationType, collection: op.collection, documentId: op.predictedId, oldData: null, newData: op.data };
        }
        if (op.type === 'update') {
          const oldDoc = oldDocMap.get(`${op.collection}:${op.id.toString()}`);
          return { operationType: 'update' as TOperationType, collection: op.collection, documentId: op.id, oldData: oldDoc, newData: op.data };
        }
        const oldDoc = oldDocMap.get(`${op.collection}:${op.id.toString()}`);
        return { operationType: 'delete' as TOperationType, collection: op.collection, documentId: op.id, oldData: oldDoc, newData: null };
      }),
    );

    const result: IPlanExecuteResult = { inserts: new Map(), updates: new Map(), deletes: new Map() };
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
          const docs = ops.map((op) => ({ ...op.data, _id: op.predictedId, __txId: this.txId }));
          await coll.insertMany(docs, { ordered: false });
          for (const op of ops) {
            this.context.modifiedDocuments.push({ collection: collName, id: op.predictedId.toString() });
          }
          result.inserts.set(collName, docs.map((d) => stripInternalFields({ ...d, id: d._id.toString() })));
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
            this.context.modifiedDocuments.push({ collection: collName, id: op.id.toString() });
          }
          const updatedDocs = await coll.find({ _id: { $in: ops.map((o) => o.id) } }).toArray();
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
          result.deletes.set(collName, ids.map((id) => id.toString()));
        })(),
      );
    }

    try {
      await Promise.all(executePromises);

      await this.logService.markOperationsBatchCompleted(logEntries.map((e) => e.operationId));

      return result;
    } catch (error) {
      await this.logService.markOperationsBatchFailed(logEntries.map((e) => e.operationId), error.message);
      throw error;
    }
  }
}
