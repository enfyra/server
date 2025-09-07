import * as path from 'path';
import * as fs from 'fs';
import { forwardRef, Inject, Injectable, Logger } from '@nestjs/common';
import { AutoService } from '../../code-generation/services/auto.service';
import { buildTypeScriptToJs } from '../../code-generation/utils/build-helper';
import {
  generateMigrationFile,
  runMigration,
} from '../../code-generation/utils/migration-helper';
import { SchemaHistoryService } from './schema-history.service';
import { DataSourceService } from '../../../core/database/data-source/data-source.service';
import { clearOldEntitiesJs } from '../utils/clear-old-entities';
import { GraphqlService } from '../../graphql/services/graphql.service';
import { LoggingService } from '../../../core/exceptions/services/logging.service';
import { ResourceNotFoundException } from '../../../core/exceptions/custom-exceptions';
import { SchemaReloadService } from './schema-reload.service';
import { RedisLockService } from '../../../infrastructure/redis/services/redis-lock.service';
import {
  SCHEMA_SYNC_LATEST_KEY,
  SCHEMA_SYNC_PROCESSING_LOCK_KEY,
  SCHEMA_SYNC_MAX_RETRIES,
  SCHEMA_SYNC_RETRY_DELAY,
  SCHEMA_SYNC_LATEST_TTL,
  SCHEMA_SYNC_LOCK_TTL,
} from '../../../shared/utils/constant';

@Injectable()
export class MetadataSyncService {
  private readonly logger = new Logger(MetadataSyncService.name);

  constructor(
    @Inject(forwardRef(() => AutoService))
    private autoService: AutoService,
    private schemaHistoryService: SchemaHistoryService,
    private dataSourceService: DataSourceService,
    @Inject(forwardRef(() => GraphqlService))
    private graphqlService: GraphqlService,
    @Inject(forwardRef(() => LoggingService))
    private loggingService: LoggingService,
    @Inject(forwardRef(() => SchemaReloadService))
    private schemaReloadService: SchemaReloadService,
    private redisLockService: RedisLockService,
  ) {}

  async pullMetadataFromDb() {
    const tableDefRepo =
      this.dataSourceService.getRepository('table_definition');
    if (!tableDefRepo) {
      this.loggingService.error('Table definition repository not found', {
        context: 'pullMetadataFromDb',
      });
      throw new ResourceNotFoundException('Repository', 'table_definition');
    }

    const tables: any = await tableDefRepo
      .createQueryBuilder('table')
      .leftJoinAndSelect('table.columns', 'columns')
      .leftJoinAndSelect('table.relations', 'relations')
      .leftJoinAndSelect('relations.targetTable', 'targetTable')
      .getMany();

    if (tables.length === 0) return;

    tables.forEach((table) => {
      table.columns.sort((a, b) => {
        if (a.isPrimary && !b.isPrimary) return -1;
        if (!a.isPrimary && b.isPrimary) return 1;
        return a.name.localeCompare(b.name);
      });

      table.relations.sort((a, b) =>
        a.propertyName.localeCompare(b.propertyName),
      );
    });

    const inverseRelationMap = this.autoService.buildInverseRelationMap(tables);

    const entityDir = path.resolve('src', 'core', 'database', 'entities');
    const validFileNames = tables.map(
      (table) => `${table.name.toLowerCase()}.entity.ts`,
    );

    if (!fs.existsSync(entityDir)) {
      fs.mkdirSync(entityDir, { recursive: true });
    }
    const existingFiles = fs.readdirSync(entityDir);

    for (const file of existingFiles) {
      if (!file.endsWith('.entity.ts')) continue;
      if (!validFileNames.includes(file)) {
        const fullPath = path.join(entityDir, file);
        fs.unlinkSync(fullPath);
        this.logger.warn(`🗑️ Đã xoá entity không hợp lệ: ${file}`);
      }
    }

    clearOldEntitiesJs();

    await Promise.all(
      tables.map(
        async (table) =>
          await this.autoService.entityGenerate(table, inverseRelationMap),
      ),
    );
  }

  async syncAll(options?: {
    entityName?: string;
    fromRestore?: boolean;
    type: 'create' | 'update';
  }): Promise<{ status: string; result?: any; reason?: string }> {
    const syncId = `sync_${Date.now()}_${Math.random().toString(36).substring(2, 11)}`;
    const instanceId = this.schemaReloadService.sourceInstanceId;

    try {
      this.logger.debug(
        `🔄 Initiating sync: ${syncId} (instance: ${instanceId})`,
      );

      // 1. Set as latest sync in Redis with TTL
      try {
        await this.redisLockService.set(
          SCHEMA_SYNC_LATEST_KEY,
          syncId,
          SCHEMA_SYNC_LATEST_TTL * 1000,
        );
      } catch (redisError) {
        this.logger.error(
          `❌ Redis set failed for sync ${syncId}:`,
          redisError.message,
        );
        return { status: 'error', reason: 'redis_set_failed' };
      }

      // 2. Try to acquire Redis processing lock with retry mechanism
      for (let attempt = 1; attempt <= SCHEMA_SYNC_MAX_RETRIES; attempt++) {
        this.logger.debug(
          `🔒 Attempting to acquire processing lock (attempt ${attempt}/${SCHEMA_SYNC_MAX_RETRIES}): ${syncId}`,
        );

        let lockAcquired: boolean;
        try {
          lockAcquired = await this.redisLockService.acquire(
            SCHEMA_SYNC_PROCESSING_LOCK_KEY,
            syncId,
            SCHEMA_SYNC_LOCK_TTL,
          );
        } catch (lockError) {
          this.logger.error(
            `❌ Redis lock acquisition failed for sync ${syncId}:`,
            lockError.message,
          );
          return { status: 'error', reason: 'redis_lock_failed' };
        }

        if (lockAcquired) {
          this.logger.debug(`✅ Processing lock acquired: ${syncId}`);

          try {
            // 3. Double-check we're still the latest sync
            let currentLatest: string | null;
            try {
              currentLatest = await this.redisLockService.get(
                SCHEMA_SYNC_LATEST_KEY,
              );
            } catch (redisError) {
              this.logger.error(
                `❌ Redis get failed for sync ${syncId}:`,
                redisError.message,
              );
              return { status: 'error', reason: 'redis_get_failed' };
            }
            if (currentLatest !== syncId) {
              this.logger.debug(
                `⏩ No longer latest sync, exiting: ${syncId} (current: ${currentLatest})`,
              );
              return { status: 'skipped', reason: 'newer_sync_exists' };
            }

            // 4. Execute the actual sync
            this.logger.debug(`🚀 Executing sync: ${syncId}`);
            const result = await this.syncAllInternal(options);

            this.logger.log(`✅ Sync completed: ${syncId}`);
            return { status: 'completed', result };
          } finally {
            // Always release the Redis processing lock
            try {
              await this.redisLockService.release(
                SCHEMA_SYNC_PROCESSING_LOCK_KEY,
                syncId,
              );
              this.logger.debug(`🔓 Processing lock released: ${syncId}`);
            } catch (lockReleaseError) {
              this.logger.error(
                'Failed to release processing lock:',
                lockReleaseError.message,
              );
            }
          }
        } else {
          // 5. Lock acquisition failed, check if we're still latest before retry
          let currentLatest: string | null;
          try {
            currentLatest = await this.redisLockService.get(
              SCHEMA_SYNC_LATEST_KEY,
            );
          } catch (redisError) {
            this.logger.error(
              `❌ Redis get failed during retry for sync ${syncId}:`,
              redisError.message,
            );
            return { status: 'error', reason: 'redis_get_failed' };
          }
          if (currentLatest !== syncId) {
            this.logger.debug(
              `⏩ No longer latest sync, stopping retries: ${syncId} (current: ${currentLatest})`,
            );
            return { status: 'skipped', reason: 'newer_sync_exists' };
          }

          // 6. Still latest, wait before retry (unless it's the last attempt)
          if (attempt < SCHEMA_SYNC_MAX_RETRIES) {
            this.logger.debug(
              `⏸️ DB lock busy, waiting ${SCHEMA_SYNC_RETRY_DELAY}ms before retry: ${syncId}`,
            );
            await new Promise((resolve) =>
              setTimeout(resolve, SCHEMA_SYNC_RETRY_DELAY),
            );
          }
        }
      }

      // Max retries exceeded
      this.logger.warn(`⏰ Max retries exceeded for sync: ${syncId}`);
      return { status: 'timeout', reason: 'max_retries_exceeded' };
    } catch (unexpectedError) {
      // Catch any other unexpected errors to prevent crashes
      this.logger.error(
        `💥 Unexpected error in syncAll for sync ${syncId}:`,
        unexpectedError.message,
      );
      return { status: 'error', reason: 'unexpected_error' };
    }
  }

  private async syncAllInternal(options?: {
    entityName?: string;
    fromRestore?: boolean;
    type: 'create' | 'update';
  }): Promise<any> {
    const startTime = Date.now();
    const timings: Record<string, number> = {};

    try {
      // Step 1: Pull metadata + clear migrations (must complete before build)
      const step1Start = Date.now();
      await Promise.all([
        this.pullMetadataFromDb(),
        this.autoService.clearMigrationsTable(),
      ]);
      timings.step1 = Date.now() - step1Start;
      this.logger.debug(
        `Step 1 (Pull metadata + Clear migrations): ${timings.step1}ms`,
      );

      // Step 2: Build JS entities (needs pulled metadata)
      const step2Start = Date.now();
      await buildTypeScriptToJs({
        targetDir: path.resolve('src/core/database/entities'),
        outDir: path.resolve('dist/src/core/database/entities'),
      });
      timings.step2 = Date.now() - step2Start;
      this.logger.debug(`Step 2 (Build JS entities): ${timings.step2}ms`);

      // Step 3: Generate Migration first (needs built entities)
      const step3Start = Date.now();
      if (!options?.fromRestore) {
        const migrationStart = Date.now();
        await generateMigrationFile();
        timings.generateMigration = Date.now() - migrationStart;
      } else {
        this.logger.debug(
          'Skipping migration generation for restore operation',
        );
        timings.generateMigration = 0;
      }

      // Step 4: Reload services + Run Migration (can run in parallel)
      await Promise.all([
        // Services reload (I/O bound)
        Promise.all([
          this.dataSourceService.reloadDataSource(),
          this.graphqlService.reloadSchema(),
        ]),
        // Run migration (now that it's generated)
        (async () => {
          if (!options?.fromRestore) {
            const runStart = Date.now();
            await runMigration();
            timings.runMigration = Date.now() - runStart;
          } else {
            this.logger.debug('Skipping migration run for restore operation');
            timings.runMigration = 0;
          }
        })(),
      ]);
      timings.step3 = Date.now() - step3Start;
      this.logger.debug(`Step 3-4 (Migration + Reload): ${timings.step3}ms`);

      // Step 5: Backup
      const step4Start = Date.now();
      const version = await this.schemaHistoryService.backup();
      timings.step4 = Date.now() - step4Start;

      // Step 6: Publish schema update event (only if not from restore)
      if (!options?.fromRestore) {
        await this.schemaReloadService.publishSchemaUpdated(version);
      }

      timings.total = Date.now() - startTime;
      this.logger.log(`🏁 syncAll completed in ${timings.total}ms`, timings);

      return version;
    } catch (err) {
      this.loggingService.error(
        'Schema synchronization failed, initiating restore',
        {
          context: 'syncAll',
          error: err.message,
          stack: err.stack,
          entityName: options?.entityName,
          operationType: options?.type,
          fromRestore: options?.fromRestore,
        },
      );

      try {
        await this.schemaHistoryService.restore({
          entityName: options?.entityName,
          type: options?.type,
        });
        this.logger.log('✅ Schema restored successfully after sync failure');
      } catch (restoreError) {
        this.loggingService.error('Schema restore also failed', {
          context: 'syncAll.restore',
          error: restoreError.message,
          stack: restoreError.stack,
          originalError: err.message,
        });
      }

      // Log warning instead of throwing to prevent app crash in async context
      this.logger.warn(
        `⚠️ Schema synchronization failed but was restored: ${err.message || 'Please check your table schema'}`,
        {
          entityName: options?.entityName,
          operationType: options?.type,
          originalError: err.message,
          restored: true,
        },
      );
    }
  }
}
