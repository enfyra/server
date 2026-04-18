import { Logger } from '../../../shared/logger';
import { DatabaseConfigService } from '../../../shared/services/database-config.service';
import { CommonService } from '../../../shared/common/services/common.service';
import { QueryBuilderService } from '../../../infrastructure/query-builder/query-builder.service';
import { CacheService } from '../../../infrastructure/cache/services/cache.service';
import { InstanceService } from '../../../shared/services/instance.service';
import { MetadataCacheService } from '../../../infrastructure/cache/services/metadata-cache.service';
import { MetadataProvisionService } from './metadata-provision.service';
import { MetadataMigrationService } from './metadata-migration.service';
import { DataProvisionService } from './data-provision.service';
import { DataMigrationService } from './data-migration.service';
import { RouteDefinitionProcessor } from '../processors/route-definition.processor';
import { REDIS_TTL, PROVISION_LOCK_KEY } from '../../../shared/utils/constant';

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
  private readonly routeDefinitionProcessor: RouteDefinitionProcessor;

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
      if (error.code === 'ER_NO_SUCH_TABLE' || error.code === '42P01') {
        return true;
      }
      throw error;
    }
  }

  async run(): Promise<void> {
    const start = Date.now();
    const lockValue = this.instanceService.getInstanceId();
    const acquired = await this.cacheService.acquire(
      PROVISION_LOCK_KEY,
      lockValue,
      REDIS_TTL.PROVISION_LOCK_TTL,
    );

    if (!acquired) {
      this.logger.log('Another instance is initializing, waiting...');
      await this.waitUntilDone();
      this.logger.log(`Waited for init, ready in ${Date.now() - start}ms`);
      return;
    }

    try {
      if (!(await this.isNeeded())) {
        this.logger.log(
          `Already initialized by another instance, ready in ${Date.now() - start}ms`,
        );
        return;
      }

      this.logger.log('First time initialization...');

      const t1 = Date.now();
      await this.metadataProvisionService.createInitMetadata();
      this.logger.log(`createInitMetadata: ${Date.now() - t1}ms`);

      if (this.metadataMigrationService.hasMigrations()) {
        const t2 = Date.now();
        this.logger.log('Running metadata migrations...');
        await this.metadataMigrationService.runMigrations();
        this.logger.log(`Metadata migrations: ${Date.now() - t2}ms`);
      }

      const t3 = Date.now();
      await this.metadataCacheService.getMetadata();
      this.logger.log(`Metadata cache warmed: ${Date.now() - t3}ms`);

      const t4 = Date.now();
      await this.dataProvisionService.insertAllDefaultRecords();
      this.logger.log(`Default records: ${Date.now() - t4}ms`);

      try {
        await this.routeDefinitionProcessor.ensureMissingHandlers();
      } catch (error) {
        this.logger.error(
          `Error ensuring route handlers: ${(error as Error).message}`,
        );
      }

      if (this.dataMigrationService.hasMigrations()) {
        const t5 = Date.now();
        this.logger.log('Running data migrations...');
        await this.dataMigrationService.runMigrations();
        this.logger.log(`Data migrations: ${Date.now() - t5}ms`);
      }

      await this.markInitialized();

      this.logger.log(`Initialization completed in ${Date.now() - start}ms`);
    } finally {
      await this.cacheService.release(PROVISION_LOCK_KEY, lockValue);
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
