import { Logger } from '../../../shared/logger';
import { MongoService } from './mongo.service';
import { randomUUID } from 'crypto';
import { CacheService } from '../../cache';
import { InstanceService } from '../../../shared/services';
import {
  MONGO_MIGRATION_SAGA_RECOVERY_LOCK_KEY,
  REDIS_TTL,
} from '../../../shared/utils/constant';
import { getErrorMessage } from '../../../shared/utils/error.util';
import { getMongoRawDb } from './mongo-raw-db.util';

export type MongoMigrationStatus =
  | 'pending'
  | 'running'
  | 'completed'
  | 'failed'
  | 'rolled_back';
export type MongoMigrationOperation = 'create' | 'update' | 'delete';
export class MongoMigrationJournalService {
  private readonly logger = new Logger(MongoMigrationJournalService.name);
  private readonly mongoService: MongoService;
  private readonly cacheService?: CacheService;
  private readonly instanceService?: InstanceService;
  private readonly collectionName = 'enfyra_schema_migration';

  constructor(deps: {
    mongoService: MongoService;
    cacheService?: CacheService;
    instanceService?: InstanceService;
  }) {
    this.mongoService = deps.mongoService;
    this.cacheService = deps.cacheService;
    this.instanceService = deps.instanceService;
  }

  private getCollection() {
    return getMongoRawDb(this.mongoService).collection(this.collectionName);
  }
  async record(params: {
    tableName: string;
    operation: MongoMigrationOperation;
    upDiff: any;
    downDiff: any;
    beforeSnapshot?: any;
    afterSnapshot?: any;
    rawBeforeSnapshot?: any;
  }): Promise<string> {
    if (params.operation === 'update' && !params.rawBeforeSnapshot) {
      throw new Error(
        `Mongo migration saga for ${params.tableName} requires rawBeforeSnapshot`,
      );
    }

    const uuid = `mj-${randomUUID()}`;
    const now = new Date();
    const saga = this.mongoService.getActiveSagaSession();
    await this.getCollection().insertOne({
      uuid,
      tableName: params.tableName,
      operation: params.operation,
      status: 'pending',
      upDiff: params.upDiff,
      downDiff: params.downDiff,
      beforeSnapshot: params.beforeSnapshot || null,
      afterSnapshot: params.afterSnapshot || null,
      rawBeforeSnapshot: params.rawBeforeSnapshot || null,
      sagaSessionId: saga?.txId ?? null,
      purpose: saga?.purpose ?? null,
      mutationId: saga?.mutationId ?? null,
      errorMessage: null,
      startedAt: null,
      completedAt: null,
      createdAt: now,
      updatedAt: now,
    });
    this.logger.log(
      `Journal recorded: ${uuid} [${params.operation}] ${params.tableName}`,
    );
    return uuid;
  }
  async markRunning(uuid: string): Promise<void> {
    await this.getCollection().updateOne(
      { uuid },
      {
        $set: {
          status: 'running',
          startedAt: new Date(),
          updatedAt: new Date(),
        },
      },
    );
  }
  async markCompleted(uuid: string): Promise<void> {
    await this.getCollection().updateOne(
      { uuid },
      {
        $set: {
          status: 'completed',
          completedAt: new Date(),
          updatedAt: new Date(),
        },
      },
    );
    this.logger.log(`Journal completed: ${uuid}`);
  }
  async markFailed(uuid: string, error: string): Promise<void> {
    await this.getCollection().updateOne(
      { uuid },
      {
        $set: {
          status: 'failed',
          errorMessage: error?.substring(0, 4000) || 'Unknown error',
          completedAt: new Date(),
          updatedAt: new Date(),
        },
      },
    );
    this.logger.warn(`Journal failed: ${uuid} — ${error?.substring(0, 200)}`);
  }
  async markRolledBack(uuid: string): Promise<void> {
    await this.getCollection().updateOne(
      { uuid },
      {
        $set: {
          status: 'rolled_back',
          completedAt: new Date(),
          updatedAt: new Date(),
        },
      },
    );
    this.logger.warn(`Journal rolled back: ${uuid}`);
  }
  async getEntry(uuid: string): Promise<any | null> {
    return this.getCollection().findOne({ uuid });
  }
  async executeRolldown(
    uuid: string,
    executeDiff: (diff: any, entry: any) => Promise<void>,
    restoreMetadataFn?: (entry: any) => Promise<void>,
  ): Promise<void> {
    const entry = await this.getEntry(uuid);
    if (!entry || !entry.downDiff) {
      throw new Error(
        `Cannot rollback journal ${uuid}: ${!entry ? 'entry not found' : 'missing downDiff'}`,
      );
    }
    this.logger.warn(`Executing rollback for ${uuid}`);
    const errors: string[] = [];
    try {
      await executeDiff(entry.downDiff, entry);
    } catch (error: any) {
      errors.push(`physical rolldown: ${getErrorMessage(error)}`);
    }
    if (restoreMetadataFn) {
      try {
        await restoreMetadataFn(entry);
        this.logger.warn(
          `Metadata restored for ${entry.uuid} from rawBeforeSnapshot`,
        );
      } catch (error: any) {
        errors.push(`metadata restore: ${getErrorMessage(error)}`);
      }
    }
    if (errors.length > 0) {
      const message = errors.join('; ');
      this.logger.error(`Rollback failed for ${uuid}: ${message}`);
      await this.markFailed(uuid, `Rollback failed: ${message}`);
      throw new Error(`Rollback failed for ${uuid}: ${message}`);
    }
    await this.markRolledBack(uuid);
  }
  async cleanup(maxAgeDays = 7): Promise<void> {
    const cutoff = new Date(Date.now() - maxAgeDays * 24 * 60 * 60 * 1000);
    try {
      const result = await this.getCollection().deleteMany({
        status: { $in: ['completed', 'rolled_back'] },
        completedAt: { $lt: cutoff },
      });
      if (result.deletedCount > 0) {
        this.logger.log(
          `Cleaned up ${result.deletedCount} old journal entries`,
        );
      }
    } catch {}
  }
  async recoverPending(
    executeDiff: (diff: any, entry: any) => Promise<void>,
    restoreMetadataFn?: (entry: any) => Promise<void>,
  ): Promise<void> {
    if (this.cacheService && this.instanceService) {
      const lockValue = this.instanceService.getInstanceId();
      const acquired = await this.cacheService.acquire(
        MONGO_MIGRATION_SAGA_RECOVERY_LOCK_KEY,
        lockValue,
        REDIS_TTL.MONGO_MIGRATION_SAGA_RECOVERY_LOCK_TTL,
        { global: true },
      );
      if (!acquired) {
        this.logger.log(
          `Mongo migration saga recovery: waiting for lock owner to finish`,
        );
        const deadline = Date.now() + 25_000;
        while (Date.now() < deadline) {
          await new Promise((r) => setTimeout(r, 2000));
          const lockValue2 = await this.cacheService.get(
            MONGO_MIGRATION_SAGA_RECOVERY_LOCK_KEY,
            { global: true },
          );
          if (!lockValue2) break;
        }
        const pending = await this.getCollection()
          .find({ status: { $in: ['pending', 'running', 'failed'] } })
          .toArray();
        if (pending.length > 0) {
          throw new Error(
            `Mongo migration recovery incomplete: ${pending.length} unresolved journal(s) (pending/running/failed) remain after lock owner finished`,
          );
        }
        return;
      }
      try {
        let leaseLost = false;
        const renewal = setInterval(() => {
          void this.cacheService!
            .renew(
              MONGO_MIGRATION_SAGA_RECOVERY_LOCK_KEY,
              lockValue,
              REDIS_TTL.MONGO_MIGRATION_SAGA_RECOVERY_LOCK_TTL,
              { global: true },
            )
            .then((renewed) => {
              if (!renewed) leaseLost = true;
            })
            .catch(() => {
              leaseLost = true;
            });
        }, Math.floor(REDIS_TTL.MONGO_MIGRATION_SAGA_RECOVERY_LOCK_TTL / 3));
        try {
        await this.recoverPendingBody(executeDiff, restoreMetadataFn);
          if (leaseLost) {
            throw new Error('Mongo migration recovery barrier lease was lost');
          }
        } finally {
          clearInterval(renewal);
        }
      } finally {
        await this.cacheService.release(
          MONGO_MIGRATION_SAGA_RECOVERY_LOCK_KEY,
          lockValue,
          { global: true },
        );
      }
      return;
    }

    await this.recoverPendingBody(executeDiff, restoreMetadataFn);
  }

  private async recoverPendingBody(
    executeDiff: (diff: any, entry: any) => Promise<void>,
    restoreMetadataFn?: (entry: any) => Promise<void>,
  ): Promise<void> {
    let pending: any[];
    try {
      pending = await this.getCollection()
        .find({ status: { $in: ['pending', 'running', 'failed'] } })
        .toArray();
    } catch (error: any) {
      const msg = String(error?.message ?? '');
      const isNsNotFound =
        msg.includes('ns not found') ||
        msg.includes('NamespaceNotFound') ||
        error?.code === 26;
      if (isNsNotFound) {
        this.logger.warn(
          `${this.collectionName} collection not found, skipping recovery`,
        );
        return;
      }
      throw new Error(
        `Mongo migration journal recovery query failed: ${msg}`,
      );
    }
    if (pending.length === 0) return;
    this.logger.warn(
      `Found ${pending.length} unresolved migration(s), rolling back...`,
    );
    const failures: string[] = [];
    for (const entry of pending) {
      this.logger.warn(
        `Recovering ${entry.uuid} [${entry.operation}] ${entry.tableName}`,
      );
      try {
        if (await this.reconcileCorrelatedSagaJournal(entry)) {
          continue;
        }
        await this.executeRolldown(entry.uuid, executeDiff, restoreMetadataFn);
      } catch (error) {
        this.logger.error(
          `Recovery failed for ${entry.uuid}: ${getErrorMessage(error)}`,
        );
        failures.push(`${entry.uuid}: ${getErrorMessage(error)}`);
      }
    }
    if (failures.length > 0) {
      throw new Error(
        `Mongo migration recovery failed for ${failures.length} entr${failures.length === 1 ? 'y' : 'ies'}: ${failures.join('; ')}`,
      );
    }
    const remaining = await this.getCollection()
      .find({ status: { $in: ['pending', 'running', 'failed'] } })
      .toArray();
    if (remaining.length > 0) {
      throw new Error(
        `Mongo migration recovery incomplete: ${remaining.length} unresolved journal(s) remain after recovery`,
      );
    }
  }

  private async reconcileCorrelatedSagaJournal(entry: any): Promise<boolean> {
    if (entry.purpose !== 'runtime-schema' || !entry.mutationId) {
      return false;
    }
    const db = getMongoRawDb(this.mongoService);
    const session = await db.collection('system_saga_sessions').findOne({
      $or: [
        ...(entry.sagaSessionId
          ? [{ txId: entry.sagaSessionId }, { sessionId: entry.sagaSessionId }]
          : []),
        { mutationId: entry.mutationId },
      ],
    });
    if (session) {
      const leaseExpiresAt = session.expiresAt
        ? new Date(session.expiresAt).getTime()
        : 0;
      const live =
        ['active', 'committing', 'rolling_back'].includes(session.status) &&
        leaseExpiresAt > Date.now();
      throw new Error(
        live
          ? `Correlated runtime saga ${session.txId} is still live`
          : `Correlated runtime saga ${session.txId} requires saga recovery before migration journal recovery`,
      );
    }

    const runtimeJournal = await db
      .collection('enfyra_runtime_schema_journal')
      .findOne({ mutationId: entry.mutationId });
    const stage = runtimeJournal?.stage;
    if (['captured', 'failed', 'rolled_back'].includes(stage)) {
      await this.markRolledBack(entry.uuid);
      this.logger.warn(
        `Correlated journal ${entry.uuid} already compensated by saga ${entry.sagaSessionId ?? 'unknown'}`,
      );
      return true;
    }
    if (
      [
        'target_attested',
        'db_committed',
        'activation_pending',
        'activated',
        'completed',
      ].includes(stage)
    ) {
      await this.markCompleted(entry.uuid);
      return true;
    }
    throw new Error(
      `Cannot safely classify correlated Mongo migration ${entry.uuid}: runtime journal stage=${stage ?? 'missing'}`,
    );
  }
}
