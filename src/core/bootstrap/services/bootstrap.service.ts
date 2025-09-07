import { Injectable, Logger, OnApplicationBootstrap } from '@nestjs/common';
import { CommonService } from '../../../shared/common/services/common.service';
import { MetadataSyncService } from '../../../modules/schema-management/services/metadata-sync.service';
import { SchemaStateService } from '../../../modules/schema-management/services/schema-state.service';
import { DefaultDataService } from './default-data.service';
import { CoreInitService } from './core-init.service';
import { DataSourceService } from '../../../core/database/data-source/data-source.service';
import { SchemaReloadService } from '../../../modules/schema-management/services/schema-reload.service';
import { RedisLockService } from '../../../infrastructure/redis/services/redis-lock.service';

@Injectable()
export class BootstrapService implements OnApplicationBootstrap {
  private readonly logger = new Logger(BootstrapService.name);

  constructor(
    private readonly commonService: CommonService,
    private readonly metadataSyncService: MetadataSyncService,
    private readonly schemaStateService: SchemaStateService,
    private readonly defaultDataService: DefaultDataService,
    private readonly coreInitService: CoreInitService,
    private dataSourceService: DataSourceService,
    private schemaReloadService: SchemaReloadService,
    private redisLockService: RedisLockService,
  ) {}

  private async waitForDatabaseConnection(
    maxRetries = 10,
    delayMs = 1000,
  ): Promise<void> {
    let settingRepo =
      this.dataSourceService.getRepository('setting_definition');

    for (let i = 0; i < maxRetries; i++) {
      try {
        await settingRepo.query('SELECT 1');
        this.logger.log('Database connection successful.');
        return;
      } catch (error) {
        this.logger.warn(
          `Unable to connect to DB, retrying after ${delayMs}ms...`,
        );
        await this.commonService.delay(delayMs);
        await this.dataSourceService.reloadDataSource();
        settingRepo =
          this.dataSourceService.getRepository('setting_definition');
      }
    }
    throw new Error(`Unable to connect to DB after ${maxRetries} attempts.`);
  }

  async onApplicationBootstrap() {
    // return;
    try {
      await this.waitForDatabaseConnection();
    } catch (err) {
      this.logger.error('❌ Error during application bootstrap:', err);
    }
    let settingRepo =
      this.dataSourceService.getRepository('setting_definition');
    let schemaHistoryRepo =
      this.dataSourceService.getRepository('schema_history');

    if (!settingRepo || !schemaHistoryRepo) {
      this.logger.error(
        '❌ Failed to get repositories. Database may not be initialized properly.',
      );
      return;
    }

    let setting: any = await settingRepo.findOne({ 
      where: {},
      order: { id: 'ASC' }  // Get first setting record
    });

    if (!setting || !setting.isInit) {
      await this.coreInitService.createInitMetadata();

      await this.defaultDataService.insertAllDefaultRecords();
      const syncResult = await this.metadataSyncService.syncAll();
      this.logger.debug(`Bootstrap sync result: ${syncResult.status}`, syncResult);

      settingRepo = this.dataSourceService.getRepository('setting_definition');
      setting = await settingRepo.findOne({ 
        where: {},
        order: { id: 'ASC' }  // Get first setting record
      });
      
      if (!setting) {
        this.logger.error('❌ Setting record not found after initialization');
        throw new Error('Setting record not found after initialization. DefaultDataService may have failed.');
      }
      
      await settingRepo.update(setting.id, { isInit: true });
      schemaHistoryRepo =
        this.dataSourceService.getRepository('schema_history');
      this.logger.debug('Initialization successful');

      const lastVersion: any = await schemaHistoryRepo.findOne({
        where: {},
        order: { createdAt: 'DESC' },
      });

      if (lastVersion) {
        this.schemaStateService.setVersion(lastVersion.id);
      }
    } else {
      await this.commonService.delay(Math.random() * 500);

      this.logger.log('🔄 Running upsert to sync default data...');
      await this.defaultDataService.insertAllDefaultRecords();

      const acquired = await this.redisLockService.acquire(
        'global:boot',
        this.schemaReloadService.sourceInstanceId,
        15000,
      );
      if (acquired) {
        const syncResult = await this.metadataSyncService.syncAll();
      this.logger.debug(`Bootstrap sync result: ${syncResult.status}`, syncResult);
        this.logger.warn('Lock acquired successfully', acquired);
        schemaHistoryRepo =
          this.dataSourceService.getRepository('schema_history');
        const lastVersion: any = await schemaHistoryRepo.findOne({
          where: {},
          order: { createdAt: 'DESC' },
        });
        await this.schemaReloadService.publishSchemaUpdated(lastVersion?.id);
      }
    }
  }
}
