import { Injectable, Logger, OnApplicationBootstrap } from '@nestjs/common';
import { CommonService } from '../../../shared/common/services/common.service';
// import { SchemaStateService } from '../../../modules/schema-management/services/schema-state.service';
import { DefaultDataService } from './default-data.service';
import { CoreInitService } from './core-init.service';
import { KnexService } from '../../../infrastructure/knex/knex.service';
// import { CacheService } from '../../../infrastructure/cache/services/cache.service';

@Injectable()
export class BootstrapService implements OnApplicationBootstrap {
  private readonly logger = new Logger(BootstrapService.name);

  constructor(
    private readonly commonService: CommonService,
    // private readonly schemaStateService: SchemaStateService,
    private readonly defaultDataService: DefaultDataService,
    private readonly coreInitService: CoreInitService,
    private readonly knexService: KnexService,
    // private readonly cacheService: CacheService,
  ) {}

  private async waitForDatabaseConnection(
    maxRetries = 10,
    delayMs = 1000,
  ): Promise<void> {
    const knex = this.knexService.getKnex();

    for (let i = 0; i < maxRetries; i++) {
      try {
        await knex.raw('SELECT 1');
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

  async onApplicationBootstrap() {
    try {
      await this.waitForDatabaseConnection();
    } catch (err) {
      this.logger.error('❌ Error during application bootstrap:', err);
      return;
    }

    const knex = this.knexService.getKnex();

    let setting: any = await knex('setting_definition')
      .select('*')
      .orderBy('id', 'asc')
      .first();

    if (!setting || !setting.isInit) {
      this.logger.log('🚀 First time initialization...');
      
      await this.coreInitService.createInitMetadata();
      await this.defaultDataService.insertAllDefaultRecords();

      setting = await knex('setting_definition')
        .select('*')
        .orderBy('id', 'asc')
        .first();
      
      if (!setting) {
        this.logger.error('❌ Setting record not found after initialization');
        throw new Error('Setting record not found. DefaultDataService may have failed.');
      }
      
      await knex('setting_definition')
        .where('id', setting.id)
        .update({ isInit: true });
      
      this.logger.log('✅ Initialization successful');

      // const lastVersion: any = await knex('schema_history')
      //   .select('*')
      //   .orderBy('createdAt', 'desc')
      //   .first();

      // if (lastVersion) {
      //   this.schemaStateService.setVersion(lastVersion.id);
      // }
    } else {
      await this.commonService.delay(Math.random() * 500);

      this.logger.log('🔄 Syncing default data...');
      await this.defaultDataService.insertAllDefaultRecords();

      this.logger.log('✅ Default data sync completed');
    }
  }
}
