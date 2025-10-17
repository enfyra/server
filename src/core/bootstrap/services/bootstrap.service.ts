import { Injectable, Logger, OnModuleInit } from '@nestjs/common';
import { CommonService } from '../../../shared/common/services/common.service';
import { DefaultDataService } from './default-data.service';
import { CoreInitService } from './core-init.service';
import { QueryBuilderService } from '../../../infrastructure/query-builder/query-builder.service';

@Injectable()
export class BootstrapService implements OnModuleInit {
  private readonly logger = new Logger(BootstrapService.name);

  constructor(
    private readonly commonService: CommonService,
    // private readonly schemaStateService: SchemaStateService,
    private readonly defaultDataService: DefaultDataService,
    private readonly coreInitService: CoreInitService,
    private readonly queryBuilder: QueryBuilderService,
    // private readonly cacheService: CacheService,
  ) {}

  private async waitForDatabaseConnection(
    maxRetries = 10,
    delayMs = 1000,
  ): Promise<void> {
    for (let i = 0; i < maxRetries; i++) {
      try {
        await this.queryBuilder.raw('SELECT 1');
        this.logger.log('Database connection successful.');
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
    try {
      await this.waitForDatabaseConnection();
    } catch (err) {
      this.logger.error('❌ Error during application bootstrap:', err);
      return;
    }

    // Find first setting record
    const isMongoDB = this.queryBuilder.isMongoDb();
    const sortField = isMongoDB ? '_id' : 'id';
    const settingsResult = await this.queryBuilder.select({
      tableName: 'setting_definition',
      sort: [sortField],
      limit: 1,
    });
    let setting = settingsResult.data[0] || null;

    if (!setting || !setting.isInit) {
      this.logger.log('🚀 First time initialization...');
      
      // Create metadata (needed for both SQL and MongoDB to track schema)
      await this.coreInitService.createInitMetadata();
      
      await this.defaultDataService.insertAllDefaultRecords();

      // Re-fetch setting after default data insertion
      const settings2Result = await this.queryBuilder.select({
        tableName: 'setting_definition',
        sort: [sortField],
        limit: 1,
      });
      setting = settings2Result.data[0] || null;
      
      if (!setting) {
        this.logger.error('❌ Setting record not found after initialization');
        throw new Error('Setting record not found. DefaultDataService may have failed.');
      }
      
      // Update isInit flag
      const settingId = setting._id || setting.id;
      await this.queryBuilder.update({
        table: 'setting_definition',
        where: [{ field: 'id', operator: '=', value: settingId }],
        data: { isInit: true },
      });
      
      this.logger.log('✅ Initialization successful');

      // const lastVersion: any = await knex('schema_history')
      //   .select('*')
      //   .orderBy('createdAt', 'desc')
      //   .first();

      // if (lastVersion) {
      //   this.schemaStateService.setVersion(lastVersion.id);
      // }
    } else {
      this.logger.log('✅ System already initialized, skipping data sync');
    }
  }
}
