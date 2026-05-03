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
    this.routeDefinitionProcessor = deps.routeDefinitionProcessor;
  }

  async isNeeded(): Promise<boolean> {
    try {
      const sortField = DatabaseConfigService.getPkField();
      const result = await this.queryBuilderService.find({
        table: 'setting_definition',
        sort: [sortField],
        limit: 1,
      });
      const setting = result.data[0] || null;
      return !setting || !setting.isInit;
    } catch (error: any) {
      if (
        error.code === 'ER_NO_SUCH_TABLE' ||
        error.code === '42P01' ||
        (error.code === 'SQLITE_ERROR' && error.message?.includes('no such table'))
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

    try {
      if (!(await this.isNeeded())) {
        this.logProgress(
          mode,
          100,
          `already initialized by another instance, ready in ${Date.now() - start}ms`,
        );
        return;
      }

      this.logProgress(mode, 5, 'acquired init lock');

      const t1 = Date.now();
      this.logProgress(mode, 10, 'provisioning metadata');
      await this.metadataProvisionService.createInitMetadata();
      await this.metadataCacheService.clearMetadataCache();
      this.logVerbose(`createInitMetadata: ${Date.now() - t1}ms`);

      if (this.metadataMigrationService.hasMigrations()) {
        const t2 = Date.now();
        this.logProgress(mode, 35, 'applying metadata migrations');
        await this.metadataMigrationService.runMigrations();
        await this.metadataCacheService.clearMetadataCache();
        this.logVerbose(`Metadata migrations: ${Date.now() - t2}ms`);
      }

      const t3 = Date.now();
      this.logProgress(mode, 50, 'warming metadata cache');
      await this.metadataCacheService.getMetadata();
      this.logVerbose(`Metadata cache warmed: ${Date.now() - t3}ms`);

      const t4 = Date.now();
      this.logProgress(mode, 60, 'healing schema');
      await this.schemaHealingService.runIfNeeded();
      await this.metadataCacheService.clearMetadataCache();
      await this.metadataCacheService.getMetadata();
      this.logVerbose(`Schema healing: ${Date.now() - t4}ms`);

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
      }

      if (this.dataMigrationService.hasMigrations()) {
        const t6 = Date.now();
        this.logProgress(mode, 90, 'applying data migrations');
        await this.dataMigrationService.runMigrations();
        this.logVerbose(`Data migrations: ${Date.now() - t6}ms`);
      }

      this.logProgress(mode, 98, 'finalizing');
      await this.markInitialized();

      this.logProgress(mode, 100, `completed in ${Date.now() - start}ms`);
    } finally {
      await this.cacheService.release(PROVISION_LOCK_KEY, lockValue);
    }
  }

  private async getInitMode(): Promise<'Installing' | 'Upgrading'> {
    try {
      const sortField = DatabaseConfigService.getPkField();
      const result = await this.queryBuilderService.find({
        table: 'setting_definition',
        sort: [sortField],
        limit: 1,
      });
      return result.data[0] ? 'Upgrading' : 'Installing';
    } catch (error: any) {
      if (
        error.code === 'ER_NO_SUCH_TABLE' ||
        error.code === '42P01' ||
        (error.code === 'SQLITE_ERROR' && error.message?.includes('no such table'))
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
    const sortField = DatabaseConfigService.getPkField();
    const result = await this.queryBuilderService.find({
      table: 'setting_definition',
      sort: [sortField],
      limit: 1,
    });
    const setting = result.data[0] || null;

    if (!setting) {
      throw new Error(
        'Setting record not found. DataProvisionService may have failed.',
      );
    }

    const settingId = setting._id || setting.id;
    const idField = DatabaseConfigService.getPkField();
    await this.queryBuilderService.update(
      'setting_definition',
      { where: [{ field: idField, operator: '=', value: settingId }] },
      { isInit: true },
    );
  }

  private async waitUntilDone(maxWaitMs = 120000): Promise<void> {
    const interval = 2000;
    const maxAttempts = Math.ceil(maxWaitMs / interval);
    const sortField = DatabaseConfigService.getPkField();
    for (let i = 0; i < maxAttempts; i++) {
      await this.commonService.delay(interval);
      try {
        const result = await this.queryBuilderService.find({
          table: 'setting_definition',
          sort: [sortField],
          limit: 1,
        });
        if (result.data[0]?.isInit) return;
      } catch {}
    }
    this.logger.warn(
      'Timed out waiting for init by another instance, proceeding...',
    );
  }
}
