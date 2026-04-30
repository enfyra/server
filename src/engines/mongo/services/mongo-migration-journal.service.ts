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
  private readonly collectionName = 'schema_migration_definition';

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
    return this.mongoService.getDb().collection(this.collectionName);
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
      this.logger.warn(
        `No downDiff found for journal ${uuid}, skipping rollback`,
      );
      return;
    }
    this.logger.warn(`Executing rollback for ${uuid}`);
    try {
      await executeDiff(entry.downDiff, entry);
      if (restoreMetadataFn) {
        await restoreMetadataFn(entry);
        this.logger.warn(
          `Metadata restored for ${entry.uuid} from rawBeforeSnapshot`,
        );
      }
      await this.markRolledBack(uuid);
    } catch (error: any) {
      const message = getErrorMessage(error);
      this.logger.error(`Rollback failed for ${uuid}: ${message}`);
      await this.markFailed(uuid, `Rollback failed: ${message}`);
      throw error;
    }
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
      );
      if (!acquired) {
        this.logger.debug(
          `Mongo migration saga recovery skipped: another instance holds ${MONGO_MIGRATION_SAGA_RECOVERY_LOCK_KEY}`,
        );
        return;
      }
      try {
        await this.recoverPendingBody(executeDiff, restoreMetadataFn);
      } finally {
        await this.cacheService.release(
          MONGO_MIGRATION_SAGA_RECOVERY_LOCK_KEY,
          lockValue,
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
        .find({ status: { $in: ['pending', 'running'] } })
        .toArray();
    } catch {
      this.logger.warn(
        `${this.collectionName} collection not found, skipping recovery`,
      );
      return;
    }
    if (pending.length === 0) return;
    this.logger.warn(
      `Found ${pending.length} pending/running migration(s), rolling back...`,
    );
    for (const entry of pending) {
      this.logger.warn(
        `Recovering ${entry.uuid} [${entry.operation}] ${entry.tableName}`,
      );
      try {
        await this.executeRolldown(entry.uuid, executeDiff, restoreMetadataFn);
      } catch (error) {
        this.logger.error(
          `Recovery failed for ${entry.uuid}: ${getErrorMessage(error)}`,
        );
      }
    }
  }
}
