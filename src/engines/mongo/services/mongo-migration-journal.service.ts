import { Logger } from '../../../shared/logger';
import { MongoService } from './mongo.service';
import { randomUUID } from 'crypto';
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
  private readonly collectionName = 'schema_migration_definition';
  constructor(deps: { mongoService: MongoService }) {
    this.mongoService = deps.mongoService;
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
    rawBeforeSnapshot?: any;
  }): Promise<string> {
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
    executeDiff: (diff: any) => Promise<void>,
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
      await executeDiff(entry.downDiff);
      await this.markRolledBack(uuid);
    } catch (error: any) {
      this.logger.error(`Rollback failed for ${uuid}: ${error.message}`);
      await this.markFailed(uuid, `Rollback failed: ${error.message}`);
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
    executeDiff: (diff: any) => Promise<void>,
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
      await this.executeRolldown(entry.uuid, executeDiff);
      if (restoreMetadataFn && entry.beforeSnapshot) {
        try {
          await restoreMetadataFn(entry);
          this.logger.warn(
            `Metadata restored for ${entry.uuid} from beforeSnapshot`,
          );
        } catch (metaErr: any) {
          this.logger.error(
            `Metadata restore failed for ${entry.uuid}: ${metaErr.message}`,
          );
        }
      }
    }
  }
}
