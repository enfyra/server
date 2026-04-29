import { Logger } from '../../../shared/logger';
import { AsyncLocalStorage } from 'async_hooks';
import { MongoService } from './mongo.service';
import { MongoSagaLockService } from './mongo-saga-lock.service';
import { MongoSagaSession } from './mongo-saga-session';
import { getErrorMessage } from '../../../shared/utils/error.util';
import {
  MongoSagaSnapshotService,
  IRollbackResult,
} from './mongo-saga-snapshot.service';
import { DatabaseException } from '../../../domain/exceptions';
import { CacheService } from '../../cache';
import { InstanceService } from '../../../shared/services';
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
  private readonly snapshotService: MongoSagaSnapshotService;
  private readonly instanceService: InstanceService;
  private readonly cacheService?: CacheService;

  constructor(deps: {
    mongoService: MongoService;
    lockService: MongoSagaLockService;
    snapshotService: MongoSagaSnapshotService;
    instanceService: InstanceService;
    cacheService?: CacheService;
  }) {
    this.mongoService = deps.mongoService;
    this.lockService = deps.lockService;
    this.snapshotService = deps.snapshotService;
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
      const recoveredOpenSessions =
        source === 'boot' ? await this.rollbackOpenSessionsOnBoot() : 0;
      const orphanedLocks = await this.lockService.cleanupOrphanedLocks();
      const oldSnapshots = await this.snapshotService.cleanupOldSnapshots(7);
      const recovered = recoveredOpenSessions;

      const result = { cleaned: orphanedLocks + oldSnapshots, recovered };
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

  private async rollbackOpenSessionsOnBoot(): Promise<number> {
    const sessions = await this.lockService.getOpenSessions();
    let recovered = 0;

    for (const session of sessions) {
      const txId = session.sessionId || session.txId;
      if (!txId) continue;
      try {
        const rollbackResult =
          await this.snapshotService.rollbackTransaction(txId);
        await this.lockService.abortTransaction(txId, 'boot recovery');
        if (rollbackResult.success) {
          recovered++;
        } else {
          this.logger.error(
            `Saga boot rollback failed for ${txId}: ${rollbackResult.failedSnapshots.length} failed snapshot(s)`,
          );
        }
      } catch (error) {
        this.logger.error(
          `Saga boot rollback failed for ${txId}: ${getErrorMessage(error)}`,
        );
      }
    }

    return recovered;
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
        snapshots: [],
        metadata: {
          startedAt: new Date(),
          lastActivityAt: new Date(),
          maxDurationMs: opts.maxDurationMs,
        },
      };

      const session = new MongoSagaSession(
        txId,
        this.lockService,
        this.snapshotService,
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
          .renewTransactionLease(txId!)
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

      const txStats = await this.snapshotService.getTransactionStats(txId);

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
                operationsCount: context.snapshots.length,
                locksAcquired: context.lockedResources.size,
              }
            : undefined,
      };
    }
  }

  private async commit(txId: string, context: ISagaContext): Promise<void> {
    context.status = 'committing';

    try {
      await this.lockService.commitTransaction(txId);
      context.status = 'completed';
    } catch (error) {
      context.status = 'aborted';
      throw error;
    }
  }

  private async rollback(
    txId: string,
    context: ISagaContext,
  ): Promise<IRollbackResult> {
    context.status = 'rolling_back';

    const rollbackResult = await this.snapshotService.rollbackTransaction(txId);

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
