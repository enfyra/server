import { Injectable, Logger, OnModuleInit } from '@nestjs/common';
import { CommonService } from '../../../shared/common/services/common.service';
import { DataProvisionService } from './data-provision.service';
import { MetadataProvisionService } from './metadata-provision.service';
import { DataMigrationService } from './data-migration.service';
import { QueryBuilderService } from '../../../infrastructure/query-builder/query-builder.service';

@Injectable()
export class ProvisionService implements OnModuleInit {
  private readonly logger = new Logger(ProvisionService.name);

  constructor(
    private readonly commonService: CommonService,
    private readonly dataProvisionService: DataProvisionService,
    private readonly metadataProvisionService: MetadataProvisionService,
    private readonly dataMigrationService: DataMigrationService,
    private readonly queryBuilder: QueryBuilderService,
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

      await this.metadataProvisionService.createInitMetadata();

      await this.dataProvisionService.insertAllDefaultRecords();

      const settings2Result = await this.queryBuilder.select({
        tableName: 'setting_definition',
        sort: [sortField],
        limit: 1,
      });
      setting = settings2Result.data[0] || null;

      if (!setting) {
        this.logger.error('Setting record not found after initialization');
        throw new Error('Setting record not found. DataProvisionService may have failed.');
      }

      const settingId = setting._id || setting.id;
      const idField = isMongoDB ? '_id' : 'id';
      await this.queryBuilder.update({
        table: 'setting_definition',
        where: [{ field: idField, operator: '=', value: settingId }],
        data: { isInit: true },
      });

      this.logger.log(`Initialization completed in ${Date.now() - start}ms`);
    } else {
      if (this.dataMigrationService.hasMigrations()) {
        this.logger.log('Running data migrations from data-migration.json...');
        await this.dataMigrationService.runMigrations();
      }
      this.logger.log(`System ready in ${Date.now() - start}ms`);
    }
  }
}
