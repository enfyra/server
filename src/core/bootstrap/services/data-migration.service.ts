import { Injectable, Logger } from '@nestjs/common';
import { QueryBuilderService } from '../../../infrastructure/query-builder/query-builder.service';
import * as fs from 'fs';
import * as path from 'path';

interface InitOld {
  [tableName: string]: any | any[];
  _deletedTables?: string[];
}

@Injectable()
export class DataMigrationService {
  private readonly logger = new Logger(DataMigrationService.name);
  private initOld: InitOld | null = null;

  constructor(private readonly queryBuilder: QueryBuilderService) {
    this.loadInitOld();
  }

  private loadInitOld(): void {
    try {
      const filePath = path.join(process.cwd(), 'data/data-migration.json');
      if (fs.existsSync(filePath)) {
        const content = fs.readFileSync(filePath, 'utf8');
        const parsed = JSON.parse(content);
        if (parsed && Object.keys(parsed).length > 0) {
          this.initOld = parsed;
          this.logger.log(`Loaded data-migration.json with ${Object.keys(parsed).length} table(s) to migrate`);
        }
      }
    } catch (error) {
      this.logger.warn(`Failed to load data-migration.json: ${error.message}`);
      this.initOld = null;
    }
  }

  hasMigrations(): boolean {
    if (!this.initOld) return false;
    const dataKeys = Object.keys(this.initOld).filter(k => !k.startsWith('_'));
    return dataKeys.length > 0 || (this.initOld._deletedTables && this.initOld._deletedTables.length > 0);
  }

  async runMigrations(): Promise<void> {
    if (!this.hasMigrations()) {
      this.logger.log('No data migrations to run');
      return;
    }

    this.logger.log('Running data migrations from data-migration.json...');

    if (this.initOld!._deletedTables && this.initOld!._deletedTables.length > 0) {
      await this.deleteTableData(this.initOld!._deletedTables);
    }

    let totalMigrated = 0;
    for (const [tableName, records] of Object.entries(this.initOld!)) {
      if (tableName.startsWith('_')) continue;
      const count = await this.migrateTable(tableName, records);
      totalMigrated += count;
    }

    this.logger.log(`Data migrations completed: ${totalMigrated} record(s) migrated`);
  }

  private async deleteTableData(tableNames: string[]): Promise<void> {
    this.logger.log(`Deleting data from ${tableNames.length} table(s)...`);
    for (const tableName of tableNames) {
      try {
        await this.queryBuilder.delete({
          table: tableName,
          where: [],
        });
        this.logger.log(`Deleted all data from ${tableName}`);
      } catch (error) {
        this.logger.warn(`Failed to delete data from ${tableName}: ${error.message}`);
      }
    }
  }

  private async migrateTable(tableName: string, records: any | any[]): Promise<number> {
    const recordsArray = Array.isArray(records) ? records : [records];
    let migratedCount = 0;
    const isMongoDB = this.queryBuilder.isMongoDb();
    const idField = isMongoDB ? '_id' : 'id';

    for (const oldRecord of recordsArray) {
      try {
        const uniqueFilter = this.getUniqueFilter(tableName, oldRecord);
        if (!uniqueFilter) {
          this.logger.debug(`Skipping ${tableName}: no unique identifier for record`);
          continue;
        }

        const existing = await this.queryBuilder.select({
          tableName,
          filter: uniqueFilter,
          limit: 1,
          fields: [idField],
        });

        if (!existing.data || existing.data.length === 0) {
          this.logger.debug(`Record not found in ${tableName}, skipping migration`);
          continue;
        }

        const existingId = existing.data[0][idField];
        const newRecord = this.transformRecord(oldRecord);

        await this.queryBuilder.update({
          table: tableName,
          where: [{ field: idField, operator: '=', value: existingId }],
          data: newRecord,
        });

        migratedCount++;
        this.logger.debug(`Migrated record in ${tableName}`);
      } catch (error) {
        this.logger.warn(`Failed to migrate record in ${tableName}: ${error.message}`);
      }
    }

    if (migratedCount > 0) {
      this.logger.log(`Migrated ${migratedCount} record(s) in ${tableName}`);
    }

    return migratedCount;
  }

  private transformRecord(oldRecord: any): any {
    const { _unique, ...data } = oldRecord;
    return data;
  }

  private getUniqueFilter(_tableName: string, record: any): any | null {
    if (record._unique) {
      return record._unique;
    }

    if (record.path) {
      return { path: { _eq: record.path } };
    }
    if (record.name) {
      return { name: { _eq: record.name } };
    }
    if (record.method) {
      return { method: { _eq: record.method } };
    }
    if (record.label && record.type) {
      return { _and: [{ label: { _eq: record.label } }, { type: { _eq: record.type } }] };
    }
    if (record.key) {
      return { key: { _eq: record.key } };
    }
    if (record.eventName) {
      return { eventName: { _eq: record.eventName } };
    }

    return null;
  }
}
