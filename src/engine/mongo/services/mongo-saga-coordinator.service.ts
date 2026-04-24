import { Logger } from '../../../shared/logger';
import { Db, ObjectId } from 'mongodb';
import { AsyncLocalStorage } from 'async_hooks';
import { MongoService } from './mongo.service';
import { MongoSagaLockService } from './mongo-saga-lock.service';
import { MongoSagaSession } from './mongo-saga-session';
import { getErrorMessage } from '../../../shared/utils/error.util';
import {
  MongoOperationLogService,
  IOperationLog,
  IRollbackResult,
} from './mongo-operation-log.service';
import { DatabaseException } from '../../../domain/exceptions/custom-exceptions';
import { CacheService } from '../../cache/services/cache.service';
import { InstanceService } from '../../../shared/services/instance.service';
import {
  REDIS_TTL,
  SAGA_ORPHAN_RECOVERY_LOCK_KEY,
} from '../../../shared/utils/constant';
import type {
  ISagaContext,
  ISagaOptions,
  ISagaResult,
  ISagaRecoveryMetrics,
} from './mongo-saga.types';
export class MongoSagaCoordinator {
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
  private readonly mongoService: MongoService;
  private readonly lockService: MongoSagaLockService;
  private readonly logService: MongoOperationLogService;
  private readonly instanceService: InstanceService;
  private readonly cacheService?: CacheService;

  constructor(deps: {
    mongoService: MongoService;
    lockService: MongoSagaLockService;
    logService: MongoOperationLogService;
    instanceService: InstanceService;
    cacheService?: CacheService;
  }) {
    this.mongoService = deps.mongoService;
    this.lockService = deps.lockService;
    this.logService = deps.logService;
    this.instanceService = deps.instanceService;
    this.cacheService = deps.cacheService;
  }

  async init(): Promise<void> {
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

  onDestroy(): void {
    this.stopPeriodicCleanup();
  }

  startPeriodicCleanup(intervalMs = 60_000): void {
    if (this.cleanupIntervalRef || this.cleanupFirstTimeoutRef) return;
    const jitterCap = Math.min(45_000, Math.floor(intervalMs * 0.5));
    const firstDelay =
      jitterCap > 0 ? Math.floor(Math.random() * (jitterCap + 1)) : 0;
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
          typeof raw === 'string' ? raw : raw != null ? String(raw) : '';
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
            await this.lockService.abortTransaction(
              txId,
              'orphan marker recovery',
            );
          } catch (error) {
            this.logger.error(
              `Orphan recovery rollback failed for ${txId}: ${getErrorMessage(error)}`,
            );
            continue;
          }
        }
        const ids = docs.map((x) => x._id);
        const result = await db
          .collection(collectionName)
          .updateMany(
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
          recovered += await this.recoverStaleMarkersInCollection(
            db,
            coll.name,
          );
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
    callback: (tx: any) => Promise<T>,
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
        void this.lockService
          .renewTransactionLease(txId)
          .catch((err: Error) => {
            this.logger.warn(
              `[${txId}] Saga lease renew failed: ${err.message}`,
            );
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
        throw new DatabaseException(
          `Transaction ${txId} exceeded max duration of ${opts.maxDurationMs}ms`,
          {
            txId,
            duration,
            maxDuration: opts.maxDurationMs,
          },
        );
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
          this.logger.error(
            `[${txId}] Rollback failed: ${getErrorMessage(rollbackError)}`,
          );
        }
      }

      return {
        success: false,
        error: error instanceof Error ? error : new Error(String(error)),
        txId: txId || 'unknown',
        rollbackResult,
        stats:
          txId && context
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
          const collection = this.mongoService
            .getDb()
            .collection(collectionName);
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
            errors.push(`${collectionName}: ${getErrorMessage(error)}`);
          }
        })(),
      );
    }

    await Promise.all(promises);

    if (errors.length > 0) {
      this.logger.error(
        `[${context.txId}] Partial __txId cleanup failure: ${errors.join('; ')}`,
      );
      throw new DatabaseException('Failed to clean up transaction markers', {
        txId: context.txId,
        failures: errors,
      });
    }
  }

  private async rollback(
    txId: string,
    context: ISagaContext,
  ): Promise<IRollbackResult> {
    context.status = 'rolling_back';

    const rollbackResult = await this.logService.rollbackTransaction(txId);

    await this.lockService.abortTransaction(
      txId,
      rollbackResult.success ? 'user rollback' : 'rollback failed',
    );

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

export type {
  ISagaContext,
  ISagaOptions,
  ISagaResult,
  ISagaRecoveryMetrics,
} from './mongo-saga.types';
