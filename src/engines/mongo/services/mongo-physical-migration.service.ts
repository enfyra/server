import { randomUUID } from 'crypto';
import { Worker, type Job, type Queue } from 'bullmq';
import type { Db } from 'mongodb';
import { Logger } from '../../../shared/logger';
import {
  DatabaseConfigService,
  EnvService,
} from '../../../shared/services';
import { SYSTEM_QUEUES } from '../../../shared/utils/constant';
import { getErrorMessage } from '../../../shared/utils/error.util';
import { MongoService } from './mongo.service';

type MongoPhysicalMigrationStatus =
  | 'pending'
  | 'running'
  | 'failed'
  | 'completed';

export interface MongoFieldRenameMigration {
  migrationId: string;
  kind: 'field_rename';
  tableName: string;
  oldName: string;
  newName: string;
  status: MongoPhysicalMigrationStatus;
  processed: number;
  conflictCount: number;
  error?: string | null;
  createdAt: string;
  updatedAt: string;
  completedAt?: string | null;
}

interface FieldRenameJobData {
  migrationId: string;
}

const COLLECTION_NAME = 'schema_physical_migration_definition';
const ACTIVE_STATUSES: MongoPhysicalMigrationStatus[] = [
  'pending',
  'running',
  'failed',
];
const CACHE_TTL_MS = 1000;
const BATCH_SIZE = 500;

export class MongoPhysicalMigrationService {
  private readonly logger = new Logger(MongoPhysicalMigrationService.name);
  private readonly mongoService: MongoService;
  private readonly databaseConfigService: DatabaseConfigService;
  private readonly envService: EnvService;
  private readonly queue: Queue;
  private worker?: Worker;
  private activeRenameCache = new Map<
    string,
    { expiresAt: number; renames: MongoFieldRenameMigration[] }
  >();

  constructor(deps: {
    mongoService: MongoService;
    databaseConfigService: DatabaseConfigService;
    envService: EnvService;
    mongoPhysicalMigrationQueue: Queue;
  }) {
    this.mongoService = deps.mongoService;
    this.databaseConfigService = deps.databaseConfigService;
    this.envService = deps.envService;
    this.queue = deps.mongoPhysicalMigrationQueue;
  }

  async init(): Promise<void> {
    if (!this.databaseConfigService.isMongoDb()) return;

    await this.ensureIndexes();
    const nodeName = this.envService.get('NODE_NAME') || 'enfyra';
    this.worker = new Worker(
      SYSTEM_QUEUES.MONGO_PHYSICAL_MIGRATION,
      async (job: Job<FieldRenameJobData>) => {
        if (job.name === 'mongo-field-rename') {
          return await this.processFieldRename(job.data.migrationId);
        }
        return null;
      },
      {
        prefix: nodeName,
        connection: {
          url: this.envService.get('REDIS_URI'),
          maxRetriesPerRequest: null,
        },
        concurrency: 1,
      },
    );

    this.worker.on('failed', (job, error) => {
      this.logger.error(
        `Mongo physical migration job ${job?.id} failed: ${error.message}`,
      );
    });
    this.logger.log(
      `Mongo physical migration worker started on ${SYSTEM_QUEUES.MONGO_PHYSICAL_MIGRATION}`,
    );
  }

  async onDestroy(): Promise<void> {
    if (this.worker) {
      await this.worker.close();
    }
  }

  async enqueueFieldRenames(
    tableName: string,
    renames: Array<{ oldName: string; newName: string }>,
  ): Promise<void> {
    if (!this.databaseConfigService.isMongoDb() || renames.length === 0) return;
    await this.ensureIndexes();

    const collection = this.collection();
    for (const rename of renames) {
      if (!rename.oldName || !rename.newName || rename.oldName === rename.newName) {
        continue;
      }
      const now = new Date().toISOString();
      const existing = await collection.findOne({
        kind: 'field_rename',
        tableName,
        oldName: rename.oldName,
        newName: rename.newName,
        status: { $in: ACTIVE_STATUSES },
      });
      const migrationId = existing?.migrationId ?? randomUUID();
      if (!existing) {
        const record: MongoFieldRenameMigration = {
          migrationId,
          kind: 'field_rename',
          tableName,
          oldName: rename.oldName,
          newName: rename.newName,
          status: 'pending',
          processed: 0,
          conflictCount: 0,
          error: null,
          createdAt: now,
          updatedAt: now,
          completedAt: null,
        };
        await collection.insertOne(record);
      }

      await this.queue.add(
        'mongo-field-rename',
        { migrationId },
        {
          jobId: `mongo-field-rename-${migrationId}`,
          attempts: 5,
          backoff: { type: 'exponential', delay: 1000 },
          removeOnComplete: true,
          removeOnFail: 100,
        },
      );
    }
    this.invalidateActiveRenameCache(tableName);
  }

  async getActiveFieldRenames(
    tableName: string,
  ): Promise<MongoFieldRenameMigration[]> {
    if (!this.databaseConfigService.isMongoDb()) return [];
    const cached = this.activeRenameCache.get(tableName);
    if (cached && cached.expiresAt > Date.now()) return cached.renames;

    const renames = (await this.collection()
      .find({
        kind: 'field_rename',
        tableName,
        status: { $in: ACTIVE_STATUSES },
      })
      .sort({ createdAt: 1 })
      .toArray()) as unknown as MongoFieldRenameMigration[];
    this.activeRenameCache.set(tableName, {
      expiresAt: Date.now() + CACHE_TTL_MS,
      renames,
    });
    return renames;
  }

  async getActiveFieldRenamesByTable(): Promise<
    Map<string, MongoFieldRenameMigration[]>
  > {
    if (!this.databaseConfigService.isMongoDb()) return new Map();
    const rows = (await this.collection()
      .find({
        kind: 'field_rename',
        status: { $in: ACTIVE_STATUSES },
      })
      .sort({ createdAt: 1 })
      .toArray()) as unknown as MongoFieldRenameMigration[];

    const byTable = new Map<string, MongoFieldRenameMigration[]>();
    for (const row of rows) {
      const list = byTable.get(row.tableName) ?? [];
      list.push(row);
      byTable.set(row.tableName, list);
    }
    return byTable;
  }

  async augmentScalarProjection(
    tableName: string,
    scalarFields: string[],
  ): Promise<{ scalarFields: string[]; hiddenFields: string[] }> {
    const renames = await this.getActiveFieldRenames(tableName);
    if (renames.length === 0) {
      return { scalarFields, hiddenFields: [] };
    }

    const nextFields = [...scalarFields];
    const hiddenFields: string[] = [];
    for (const rename of renames) {
      if (
        nextFields.includes(rename.newName) &&
        !nextFields.includes(rename.oldName)
      ) {
        nextFields.push(rename.oldName);
        hiddenFields.push(rename.oldName);
      }
    }
    return { scalarFields: nextFields, hiddenFields };
  }

  async applyReadFallback(
    tableName: string,
    rows: any[],
    hiddenFields: string[] = [],
  ): Promise<any[]> {
    if (rows.length === 0) return rows;
    const renames = await this.getActiveFieldRenames(tableName);
    if (renames.length === 0) return rows;

    const hidden = new Set(hiddenFields);
    for (const row of rows) {
      if (!row || typeof row !== 'object') continue;
      for (const rename of renames) {
        if (row[rename.newName] === undefined && row[rename.oldName] !== undefined) {
          row[rename.newName] = row[rename.oldName];
        }
        if (hidden.has(rename.oldName) || row[rename.newName] !== undefined) {
          delete row[rename.oldName];
        }
      }
    }
    return rows;
  }

  private async processFieldRename(migrationId: string): Promise<any> {
    const migration = await this.collection().findOne({
      migrationId,
      kind: 'field_rename',
    });
    if (!migration) return { skipped: true };
    if (migration.status === 'completed') return { skipped: true };

    await this.markRunning(migrationId);
    try {
      const result = await this.renameFieldInBatches(migration as any);
      await this.collection().updateOne(
        { migrationId },
        {
          $set: {
            status: 'completed',
            processed: result.processed,
            conflictCount: result.conflictCount,
            error: null,
            completedAt: new Date().toISOString(),
            updatedAt: new Date().toISOString(),
          },
        },
      );
      this.invalidateActiveRenameCache(migration.tableName);
      return result;
    } catch (error) {
      await this.collection().updateOne(
        { migrationId },
        {
          $set: {
            status: 'failed',
            error: getErrorMessage(error),
            updatedAt: new Date().toISOString(),
          },
        },
      );
      this.invalidateActiveRenameCache(migration.tableName);
      throw error;
    }
  }

  private async renameFieldInBatches(
    migration: MongoFieldRenameMigration,
  ): Promise<{ processed: number; conflictCount: number }> {
    const collection = this.mongoService.getDb().collection(migration.tableName);
    let processed = migration.processed || 0;
    let conflictCount = migration.conflictCount || 0;
    let lastId: any;

    while (true) {
      const filter = {
        [migration.oldName]: { $exists: true },
        [migration.newName]: { $exists: false },
        ...(lastId !== undefined ? { _id: { $gt: lastId } } : {}),
      };
      const docs = await collection
        .find(filter, { projection: { _id: 1 } })
        .sort({ _id: 1 })
        .limit(BATCH_SIZE)
        .toArray();
      if (docs.length === 0) break;
      lastId = docs[docs.length - 1]._id;

      const result = await collection.bulkWrite(
        docs.map((doc) => ({
          updateOne: {
            filter: {
              _id: doc._id,
              [migration.oldName]: { $exists: true },
              [migration.newName]: { $exists: false },
            },
            update: { $rename: { [migration.oldName]: migration.newName } },
          },
        })),
        { ordered: false },
      );
      processed += result.modifiedCount;
      conflictCount += docs.length - result.modifiedCount;

      await this.collection().updateOne(
        { migrationId: migration.migrationId },
        {
          $set: {
            processed,
            conflictCount,
            updatedAt: new Date().toISOString(),
          },
        },
      );
    }

    return { processed, conflictCount };
  }

  private async markRunning(migrationId: string): Promise<void> {
    await this.collection().updateOne(
      { migrationId },
      {
        $set: {
          status: 'running',
          error: null,
          updatedAt: new Date().toISOString(),
        },
      },
    );
  }

  private async ensureIndexes(): Promise<void> {
    const collection = this.collection();
    await collection.createIndex(
      { kind: 1, tableName: 1, status: 1 },
      { name: 'schema_physical_migration_active_idx' },
    );
    await collection.createIndex(
      { kind: 1, tableName: 1, oldName: 1, newName: 1, status: 1 },
      { name: 'schema_physical_migration_field_rename_idx' },
    );
  }

  private collection() {
    return this.db().collection(COLLECTION_NAME);
  }

  private db(): Db {
    return this.mongoService.getDb();
  }

  private invalidateActiveRenameCache(tableName: string): void {
    this.activeRenameCache.delete(tableName);
  }
}
