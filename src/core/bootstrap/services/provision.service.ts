import { Inject, Injectable, Logger, OnModuleInit, forwardRef } from '@nestjs/common';
import { RouteDefinitionProcessor } from '../processors/route-definition.processor';
import { CommonService } from '../../../shared/common/services/common.service';
import { DataProvisionService } from './data-provision.service';
import { MetadataProvisionService } from './metadata-provision.service';
import { DataMigrationService } from './data-migration.service';
import { MetadataMigrationService } from './metadata-migration.service';
import { QueryBuilderService } from '../../../infrastructure/query-builder/query-builder.service';
import { CacheService } from '../../../infrastructure/cache/services/cache.service';
import { InstanceService } from '../../../shared/services/instance.service';
import { MetadataCacheService } from '../../../infrastructure/cache/services/metadata-cache.service';
import { REDIS_TTL, PROVISION_LOCK_KEY } from '../../../shared/utils/constant';

@Injectable()
export class ProvisionService implements OnModuleInit {
  private readonly logger = new Logger(ProvisionService.name);

  constructor(
    private readonly commonService: CommonService,
    private readonly dataProvisionService: DataProvisionService,
    private readonly metadataProvisionService: MetadataProvisionService,
    private readonly dataMigrationService: DataMigrationService,
    private readonly metadataMigrationService: MetadataMigrationService,
    private readonly queryBuilder: QueryBuilderService,
    private readonly cacheService: CacheService,
    private readonly instanceService: InstanceService,
    @Inject(forwardRef(() => MetadataCacheService))
    private readonly metadataCacheService: MetadataCacheService,
    @Inject(forwardRef(() => RouteDefinitionProcessor))
    private readonly routeDefinitionProcessor: RouteDefinitionProcessor,
  ) {}

  private async waitForDatabaseConnection(
    maxRetries = 10,
    delayMs = 1000,
  ): Promise<void> {
    for (let i = 0; i < maxRetries; i++) {
      try {
        await this.queryBuilder.raw('SELECT 1');
        return;
      } catch (error) {
        this.logger.warn(
          `Unable to connect to DB, retrying after ${delayMs}ms...`,
        );
        await this.commonService.delay(delayMs);
      }
    }
    throw new Error(`Unable to connect to DB after ${maxRetries} attempts.`);
  }

  async onModuleInit() {
    const start = Date.now();
    try {
      await this.waitForDatabaseConnection();
    } catch (err) {
      this.logger.error('Error during application provision:', err);
      return;
    }

    try {
      await this.routeDefinitionProcessor.ensureMissingHandlers();
    } catch (error) {
      this.logger.error(`Error ensuring route handlers: ${error.message}`);
    }

    const isMongoDB = this.queryBuilder.isMongoDb();
    const sortField = DatabaseConfigService.getPkField();
    const settingsResult = await this.queryBuilder.find({
      table: 'setting_definition',
      sort: [sortField],
      limit: 1,
    });
    const setting = settingsResult.data[0] || null;

    if (!setting || !setting.isInit) {
      const lockValue = this.instanceService.getInstanceId();
      const lockAcquired = await this.cacheService.acquire(
        PROVISION_LOCK_KEY,
        lockValue,
        REDIS_TTL.PROVISION_LOCK_TTL,
      );

      if (!lockAcquired) {
        this.logger.log('Another instance is initializing, waiting...');
        await this.waitForInitComplete(sortField);
        this.logger.log(`Waited for init, ready in ${Date.now() - start}ms`);
        return;
      }

      try {
        const recheckResult = await this.queryBuilder.find({
          table: 'setting_definition',
          sort: [sortField],
          limit: 1,
        });
        const recheckSetting = recheckResult.data[0] || null;
        if (recheckSetting?.isInit) {
          this.logger.log(
            `Already initialized, ready in ${Date.now() - start}ms`,
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

        if (this.routeDefinitionProcessor) {
          this.logger.log('Ensuring missing route handlers after init...');
          try {
            await this.routeDefinitionProcessor.ensureMissingHandlers();
          } catch (error) {
            this.logger.error(`Error ensuring route handlers: ${error.message}`);
          }
        }

        if (this.dataMigrationService.hasMigrations()) {
          const t5 = Date.now();
          this.logger.log('Running data migrations...');
          await this.dataMigrationService.runMigrations();
          this.logger.log(`Data migrations: ${Date.now() - t5}ms`);
        }

        const settings2Result = await this.queryBuilder.find({
          table: 'setting_definition',
          sort: [sortField],
          limit: 1,
        });
        const newSetting = settings2Result.data[0] || null;

        if (!newSetting) {
          this.logger.error('Setting record not found after initialization');
          throw new Error(
            'Setting record not found. DataProvisionService may have failed.',
          );
        }

        const settingId = newSetting._id || newSetting.id;
        const idField = DatabaseConfigService.getPkField();
        await this.queryBuilder.update('setting_definition', { where: [{ field: idField, operator: '=', value: settingId }] }, { isInit: true });

        this.logger.log(`Initialization completed in ${Date.now() - start}ms`);
      } finally {
        await this.cacheService.release(PROVISION_LOCK_KEY, lockValue);
      }
    } else {
      this.logger.log(`System ready in ${Date.now() - start}ms`);
    }
  }

  private async waitForInitComplete(
    sortField: string,
    maxWaitMs = 120000,
  ): Promise<void> {
    const interval = 2000;
    const maxAttempts = Math.ceil(maxWaitMs / interval);
    for (let i = 0; i < maxAttempts; i++) {
      await this.commonService.delay(interval);
      try {
        const result = await this.queryBuilder.find({
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
