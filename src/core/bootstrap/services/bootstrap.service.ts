import { Injectable, Logger, OnModuleInit } from '@nestjs/common';
import { CommonService } from '../../../shared/common/services/common.service';
import { DefaultDataService } from './default-data.service';
import { CoreInitService } from './core-init.service';
import { QueryBuilderService } from '../../../infrastructure/query-builder/query-builder.service';
import * as fs from 'fs';
import * as path from 'path';

const initJson = JSON.parse(
  fs.readFileSync(
    path.join(process.cwd(), 'src/core/bootstrap/data/init.json'),
    'utf8',
  ),
);

@Injectable()
export class BootstrapService implements OnModuleInit {
  private readonly logger = new Logger(BootstrapService.name);

  constructor(
    private readonly commonService: CommonService,
    private readonly defaultDataService: DefaultDataService,
    private readonly coreInitService: CoreInitService,
    private readonly queryBuilder: QueryBuilderService,
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
      this.logger.error('Error during application bootstrap:', err);
      return;
    }

    const isMongoDB = this.queryBuilder.isMongoDb();
    const sortField = isMongoDB ? '_id' : 'id';
    const settingsResult = await this.queryBuilder.select({
      tableName: 'setting_definition',
      sort: [sortField],
      limit: 1,
    });
    let setting = settingsResult.data[0] || null;

    if (!setting || !setting.isInit) {
      this.logger.log('First time initialization...');

      await this.coreInitService.createInitMetadata();

      await this.defaultDataService.insertAllDefaultRecords();

      const settings2Result = await this.queryBuilder.select({
        tableName: 'setting_definition',
        sort: [sortField],
        limit: 1,
      });
      setting = settings2Result.data[0] || null;

      if (!setting) {
        this.logger.error('Setting record not found after initialization');
        throw new Error('Setting record not found. DefaultDataService may have failed.');
      }

      const settingId = setting._id || setting.id;
      const idField = isMongoDB ? '_id' : 'id';
      await this.queryBuilder.update({
        table: 'setting_definition',
        where: [{ field: idField, operator: '=', value: settingId }],
        data: { isInit: true },
      });

      this.logger.log('Initialization successful');
    } else {
      this.logger.log('System already initialized, skipping data sync');
      
      await this.ensureCriticalRecords();
    }
  }

  private async ensureCriticalRecords(): Promise<void> {
    this.logger.log('Checking critical default records...');
    
    try {
      const aiConfigResult = await this.queryBuilder.select({
        tableName: 'ai_config_definition',
        limit: 1,
      });
      
      if (!aiConfigResult.data || aiConfigResult.data.length === 0) {
        this.logger.log('No ai_config_definition records found, creating default...');
        const result = await this.defaultDataService.insertTableRecords('ai_config_definition');
        this.logger.log(`ai_config_definition: ${result.created} created, ${result.skipped} skipped`);
      }
    } catch (error) {
      this.logger.warn(`Error checking critical records: ${error.message}`);
    }
  }
}
