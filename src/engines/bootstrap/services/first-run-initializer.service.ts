import { Logger } from '../../../shared/logger';
import {
  DatabaseConfigService,
  InstanceService,
} from '../../../shared/services';
import { CommonService } from '../../../shared/common';
import { QueryBuilderService } from '@enfyra/kernel';
import { CacheService, MetadataCacheService } from '../../cache';
import { MetadataProvisionService } from './metadata-provision.service';
import { MetadataMigrationService } from './metadata-migration.service';
import { DataProvisionService } from './data-provision.service';
import { DataMigrationService } from './data-migration.service';
import { SchemaHealingService } from './schema-healing.service';
import { SnapshotTargetVerifierService } from './snapshot-target-verifier.service';
import { RouteDefinitionProcessor } from '../../../domain/bootstrap';
import { REDIS_TTL, PROVISION_LOCK_KEY } from '../../../shared/utils/constant';
import { isBootstrapVerbose } from '../utils/bootstrap-logging.util';
import { runWithBootstrapLogMode } from '../../../shared/bootstrap-log-context';

export class FirstRunInitializer {
  private readonly logger = new Logger(FirstRunInitializer.name);
  private readonly commonService: CommonService;
  private readonly queryBuilderService: QueryBuilderService;
  private readonly cacheService: CacheService;
  private readonly instanceService: InstanceService;
  private readonly metadataCacheService: MetadataCacheService;
  private readonly metadataProvisionService: MetadataProvisionService;
  private readonly metadataMigrationService: MetadataMigrationService;
  private readonly dataProvisionService: DataProvisionService;
  private readonly dataMigrationService: DataMigrationService;
  private readonly schemaHealingService: SchemaHealingService;
  private readonly snapshotTargetVerifierService: SnapshotTargetVerifierService;
  private readonly routeDefinitionProcessor: RouteDefinitionProcessor;
  private lastProgressLineLength = 0;

  constructor(deps: {
    commonService: CommonService;
    queryBuilderService: QueryBuilderService;
    cacheService: CacheService;
    instanceService: InstanceService;
    metadataCacheService: MetadataCacheService;
    metadataProvisionService: MetadataProvisionService;
    metadataMigrationService: MetadataMigrationService;
    dataProvisionService: DataProvisionService;
    dataMigrationService: DataMigrationService;
    schemaHealingService: SchemaHealingService;
    snapshotTargetVerifierService: SnapshotTargetVerifierService;
    routeDefinitionProcessor: RouteDefinitionProcessor;
  }) {
    this.commonService = deps.commonService;
    this.queryBuilderService = deps.queryBuilderService;
    this.cacheService = deps.cacheService;
    this.instanceService = deps.instanceService;
    this.metadataCacheService = deps.metadataCacheService;
    this.metadataProvisionService = deps.metadataProvisionService;
    this.metadataMigrationService = deps.metadataMigrationService;
    this.dataProvisionService = deps.dataProvisionService;
    this.dataMigrationService = deps.dataMigrationService;
    this.schemaHealingService = deps.schemaHealingService;
    this.snapshotTargetVerifierService = deps.snapshotTargetVerifierService;
    this.routeDefinitionProcessor = deps.routeDefinitionProcessor;
  }

  async isNeeded(): Promise<boolean> {
    try {
      const setting = await this.findFirstSetting();
      return !setting || !setting.isInit;
    } catch (error: any) {
      if (
        error.code === 'ER_NO_SUCH_TABLE' ||
        error.code === '42P01' ||
        (error.code === 'SQLITE_ERROR' &&
          error.message?.includes('no such table'))
      ) {
        return true;
      }
      throw error;
    }
  }

  async run(): Promise<void> {
    return runWithBootstrapLogMode(
      isBootstrapVerbose() ? 'verbose' : 'quiet',
      () => this.runWithBootstrapConsoleMode(() => this.runWithProgress()),
    );
  }

  private async runWithProgress(): Promise<void> {
    if (!(await this.isNeeded())) return;

    const start = Date.now();
    const lockValue = this.instanceService.getInstanceId();
    const mode = await this.getInitMode();
    this.logProgress(mode, 0, 'starting');
    const acquired = await this.cacheService.acquire(
      PROVISION_LOCK_KEY,
      lockValue,
      REDIS_TTL.PROVISION_LOCK_TTL,
    );

    if (!acquired) {
      this.logProgress(mode, 0, 'another instance is running, waiting');
      await this.waitUntilDone();
      this.logProgress(mode, 100, `ready in ${Date.now() - start}ms`);
      return;
    }

    const lease = this.startProvisionLease(lockValue);
    try {
      const coreT0 = Date.now();
      this.logProgress(mode, 3, 'migrating core metadata tables');
      await this.metadataMigrationService.runCoreTableRenamesBeforeMetadataSync();
      this.logVerbose(`Core system table migration: ${Date.now() - coreT0}ms`);

      if (!(await this.isNeeded())) {
        this.logProgress(
          mode,
          100,
          `already initialized by another instance, ready in ${Date.now() - start}ms`,
        );
        return;
      }

      this.logProgress(mode, 5, 'acquired init lock');

      const t0 = Date.now();
      this.logProgress(mode, 8, 'preparing system schema');
      await this.metadataMigrationService.runTableRenamesBeforeMetadataSync();
      await this.metadataMigrationService.prepareMigrationExecutionPlan();
      await this.metadataMigrationService.runPhysicalTableRenamesAndDropsAfterCoverage();
      await this.metadataMigrationService.runPhysicalMigrationsBeforeMetadataSync();
      if (this.metadataMigrationService.hasMigrations()) {
        const t2 = Date.now();
        this.logProgress(mode, 10, 'applying metadata migrations');
        await this.metadataMigrationService.runMigrations();
        await this.metadataCacheService.clearMetadataCache();
        this.logVerbose(`Metadata migrations: ${Date.now() - t2}ms`);
      }
      await this.schemaHealingService.repairSystemPhysicalColumnsBeforeMetadataProvision();
      this.logVerbose(`System schema preflight: ${Date.now() - t0}ms`);

      const t1 = Date.now();
      this.logProgress(mode, 20, 'provisioning metadata');
      await this.metadataProvisionService.createInitMetadata();
      await this.metadataCacheService.clearMetadataCache();
      this.logVerbose(`createInitMetadata: ${Date.now() - t1}ms`);

      const t2b = Date.now();
      this.logProgress(mode, 45, 'healing system metadata');
      await this.schemaHealingService.repairSystemMetadataFromSnapshot();
      await this.metadataCacheService.clearMetadataCache();
      this.logVerbose(`System metadata healing: ${Date.now() - t2b}ms`);

      const t3 = Date.now();
      this.logProgress(mode, 50, 'repairing derived schema contracts');
      await this.schemaHealingService.repairDerivedContracts();
      this.logProgress(mode, 55, 'applying explicit schema repairs');
      await this.schemaHealingService.runExplicitRepairsIfNeeded();
      this.logVerbose(`Schema repair: ${Date.now() - t3}ms`);

      const t4 = Date.now();
      this.logProgress(mode, 60, 'warming metadata cache');
      await this.metadataCacheService.reload();
      await lease.assertOwned();
      await this.snapshotTargetVerifierService.assertSchemaTargetState();
      this.logVerbose(`Metadata cache warmed: ${Date.now() - t4}ms`);

      const t5 = Date.now();
      this.logProgress(mode, 65, 'seeding default data');
      await this.dataProvisionService.insertAllDefaultRecords();
      this.logVerbose(`Default records: ${Date.now() - t5}ms`);

      try {
        this.logProgress(mode, 80, 'ensuring route handlers');
        await this.routeDefinitionProcessor.ensureMissingHandlers();
      } catch (error) {
        this.logger.error(
          `Error ensuring route handlers: ${(error as Error).message}`,
        );
        throw error;
      }

      if (this.dataMigrationService.hasMigrations()) {
        const t6 = Date.now();
        this.logProgress(mode, 90, 'applying data migrations');
        await this.dataMigrationService.runMigrations();
        this.logVerbose(`Data migrations: ${Date.now() - t6}ms`);
      }

      this.logProgress(mode, 96, 'attesting data target state');
      await this.snapshotTargetVerifierService.assertSchemaTargetState();
      await this.snapshotTargetVerifierService.assertDataTargetState();

      this.logProgress(mode, 98, 'finalizing');
      await lease.assertOwned();
      await this.markInitialized();

      this.logProgress(mode, 100, `completed in ${Date.now() - start}ms`);
    } catch (error) {
      this.logProgress(mode, 100, `failed after ${Date.now() - start}ms`);
      this.logger.error(`${mode} failed after ${Date.now() - start}ms`, error);
      throw error;
    } finally {
      await lease.stop();
      await this.cacheService.release(PROVISION_LOCK_KEY, lockValue);
    }
  }

  private startProvisionLease(lockValue: string): {
    assertOwned: () => Promise<void>;
    stop: () => Promise<void>;
  } {
    let stopped = false;
    let lost = false;
    let pending: Promise<boolean> | null = null;
    const renew = (): Promise<boolean> => {
      if (stopped || lost) return Promise.resolve(false);
      if (pending) return pending;
      pending = this.cacheService
        .renew(PROVISION_LOCK_KEY, lockValue, REDIS_TTL.PROVISION_LOCK_TTL)
        .then((renewed) => {
          if (!renewed) lost = true;
          return renewed;
        })
        .catch(() => {
          lost = true;
          return false;
        })
        .finally(() => {
          pending = null;
        });
      return pending;
    };
    const timer = setInterval(
      () => void renew(),
      Math.max(1000, Math.floor(REDIS_TTL.PROVISION_LOCK_TTL / 3)),
    );
    timer.unref?.();

    return {
      assertOwned: async () => {
        if (lost || !(await renew())) {
          throw new Error(
            'Snapshot initialization lost the provision lease before target publication.',
          );
        }
      },
      stop: async () => {
        stopped = true;
        clearInterval(timer);
        if (pending) await pending;
      },
    };
  }

  private async getInitMode(): Promise<'Installing' | 'Upgrading'> {
    try {
      return (await this.findFirstSetting()) ? 'Upgrading' : 'Installing';
    } catch (error: any) {
      if (
        error.code === 'ER_NO_SUCH_TABLE' ||
        error.code === '42P01' ||
        (error.code === 'SQLITE_ERROR' &&
          error.message?.includes('no such table'))
      ) {
        return 'Installing';
      }
      throw error;
    }
  }

  private logProgress(
    mode: 'Installing' | 'Upgrading',
    percent: number,
    message: string,
  ): void {
    if (process.env.LOG_DISABLE_CONSOLE === '1') return;

    const now = new Date();
    const pad = (n: number) => n.toString().padStart(2, '0');
    const time = `${pad(now.getHours())}:${pad(now.getMinutes())}:${pad(
      now.getSeconds(),
    )}`;
    const line = `[${time}] ${mode} (${percent}%) ${message}`;
    const padding = ' '.repeat(
      Math.max(0, this.lastProgressLineLength - line.length),
    );
    process.stdout.write(`\r${line}${padding}`);
    this.lastProgressLineLength = line.length;
    if (percent >= 100) {
      process.stdout.write('\n');
      this.lastProgressLineLength = 0;
    }
  }

  private logVerbose(message: string): void {
    if (isBootstrapVerbose()) {
      this.logger.log(message);
    }
  }

  private async runWithBootstrapConsoleMode<T>(
    callback: () => Promise<T>,
  ): Promise<T> {
    if (isBootstrapVerbose()) {
      return callback();
    }

    const originalLog = console.log;
    const originalWarn = console.warn;
    console.log = () => {};
    console.warn = () => {};

    try {
      return await callback();
    } finally {
      console.log = originalLog;
      console.warn = originalWarn;
    }
  }

  private async markInitialized(): Promise<void> {
    const setting = await this.findFirstSetting();

    if (!setting) {
      throw new Error(
        'Setting record not found. DataProvisionService may have failed.',
      );
    }

    const settingId = setting._id || setting.id;
    if (DatabaseConfigService.instanceIsMongoDb()) {
      const collectionName = await this.findMongoSettingCollectionName();
      if (!collectionName) {
        throw new Error('Setting collection not found.');
      }
      const result = await this.queryBuilderService
        .getMongoDb()
        .collection(collectionName)
        .updateOne(
          { _id: settingId, isInit: false },
          { $set: { isInit: true } },
        );
      if (result.matchedCount !== 1) {
        throw new Error(
          'Snapshot initialization failed because the setting document was not updated.',
        );
      }
      return;
    }

    const tableName = await this.findSqlSettingTableName();
    if (!tableName) {
      throw new Error('Setting table not found.');
    }
    const updatedCount = await this.queryBuilderService
      .getKnex()(tableName)
      .where({ id: settingId, isInit: false })
      .update({ isInit: true });
    if (Number(updatedCount) !== 1) {
      throw new Error(
        'Snapshot initialization failed because the setting row was not updated.',
      );
    }
  }

  private async waitUntilDone(maxWaitMs = 120000): Promise<void> {
    const interval = 2000;
    const maxAttempts = Math.ceil(maxWaitMs / interval);
    for (let i = 0; i < maxAttempts; i++) {
      await this.commonService.delay(interval);
      try {
        if ((await this.findFirstSetting())?.isInit) return;
      } catch {}
    }
    throw new Error(
      `Timed out waiting for snapshot initialization after ${maxWaitMs}ms`,
    );
  }

  private async findFirstSetting(): Promise<any | null> {
    if (DatabaseConfigService.instanceIsMongoDb()) {
      const collectionName = await this.findMongoSettingCollectionName();
      if (!collectionName) return null;
      return this.queryBuilderService
        .getMongoDb()
        .collection(collectionName)
        .findOne({});
    }

    const tableName = await this.findSqlSettingTableName();
    if (!tableName) return null;
    return this.queryBuilderService
      .getKnex()(tableName)
      .orderBy('id', 'asc')
      .first();
  }

  private async findMongoSettingCollectionName(): Promise<string | null> {
    const db = this.queryBuilderService.getMongoDb();
    const matches = await db
      .listCollections({ name: 'enfyra_setting' })
      .toArray();
    return matches.length > 0 ? 'enfyra_setting' : null;
  }

  private async findSqlSettingTableName(): Promise<string | null> {
    const knex = this.queryBuilderService.getKnex();
    return (await knex.schema.hasTable('enfyra_setting'))
      ? 'enfyra_setting'
      : null;
  }
}
