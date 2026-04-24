import { DatabaseConfigService } from '../../../shared/services/database-config.service';
import { Logger } from '../../../shared/logger';
import { QueryBuilderService } from '../../../engine/query-builder/query-builder.service';
import * as fs from 'fs';
import * as path from 'path';
import { getErrorMessage } from '../../../shared/utils/error.util';

interface InitOld {
  [tableName: string]: any | any[];
  _deletedTables?: string[];
  _deletedRecords?: { table: string; filter: Record<string, any> }[];
}

const RELATION_FIELD_PREFIXES = [
  'publishedMethods',
  'skipRoleGuardMethods',
  'availableMethods',
];

export class DataMigrationService {
  private readonly logger = new Logger(DataMigrationService.name);
  private readonly queryBuilderService: QueryBuilderService;
  private initOld: InitOld | null = null;

  constructor(deps: { queryBuilderService: QueryBuilderService }) {
    this.queryBuilderService = deps.queryBuilderService;
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
          this.logger.log(
            `Loaded data-migration.json with ${Object.keys(parsed).length} table(s) to migrate`,
          );
        }
      }
    } catch (error) {
      this.logger.warn(
        `Failed to load data-migration.json: ${getErrorMessage(error)}`,
      );
      this.initOld = null;
    }
  }

  hasMigrations(): boolean {
    if (!this.initOld) return false;
    const dataKeys = Object.keys(this.initOld).filter(
      (k) => !k.startsWith('_'),
    );
    return (
      dataKeys.length > 0 ||
      (this.initOld._deletedTables && this.initOld._deletedTables.length > 0) ||
      (this.initOld._deletedRecords && this.initOld._deletedRecords.length > 0)
    );
  }

  async runMigrations(): Promise<void> {
    if (!this.hasMigrations()) {
      this.logger.log('No data migrations to run');
      return;
    }

    this.logger.log('Running data migrations from data-migration.json...');

    if (
      this.initOld!._deletedTables &&
      this.initOld!._deletedTables.length > 0
    ) {
      await this.deleteTableData(this.initOld!._deletedTables);
    }

    if (
      this.initOld!._deletedRecords &&
      this.initOld!._deletedRecords.length > 0
    ) {
      await this.deleteRecords(this.initOld!._deletedRecords);
    }

    let totalMigrated = 0;
    for (const [tableName, records] of Object.entries(this.initOld!)) {
      if (tableName.startsWith('_')) continue;
      const count = await this.migrateTable(tableName, records);
      totalMigrated += count;
    }

    this.logger.log(
      `Data migrations completed: ${totalMigrated} record(s) migrated`,
    );
  }

  private async deleteTableData(tableNames: string[]): Promise<void> {
    this.logger.log(`Deleting data from ${tableNames.length} table(s)...`);
    for (const tableName of tableNames) {
      try {
        await this.queryBuilderService.delete(tableName, { where: [] });
        this.logger.log(`Deleted all data from ${tableName}`);
      } catch (error) {
        this.logger.warn(
          `Failed to delete data from ${tableName}: ${getErrorMessage(error)}`,
        );
      }
    }
  }

  private async deleteRecords(
    records: { table: string; filter: Record<string, any> }[],
  ): Promise<void> {
    const idField = DatabaseConfigService.getPkField();

    for (const { table, filter } of records) {
      try {
        const existing = await this.queryBuilderService.find({
          table: table,
          filter,
          limit: -1,
          fields: [idField],
        });

        for (const row of existing.data || []) {
          await this.queryBuilderService.delete(table, row[idField]);
        }

        const count = existing.data?.length || 0;
        if (count > 0) {
          this.logger.log(`Deleted ${count} record(s) from ${table}`);
        }
      } catch (error) {
        this.logger.warn(
          `Failed to delete records from ${table}: ${getErrorMessage(error)}`,
        );
      }
    }
  }

  private async migrateTable(
    tableName: string,
    records: any | any[],
  ): Promise<number> {
    const recordsArray = Array.isArray(records) ? records : [records];
    let migratedCount = 0;
    const idField = DatabaseConfigService.getPkField();

    for (const oldRecord of recordsArray) {
      try {
        const uniqueFilter = this.getUniqueFilter(tableName, oldRecord);
        if (!uniqueFilter) {
          this.logger.debug(
            `Skipping ${tableName}: no unique identifier for record`,
          );
          continue;
        }

        const existing = await this.queryBuilderService.find({
          table: tableName,
          filter: uniqueFilter,
          limit: 1,
          fields: [idField],
        });

        if (!existing.data || existing.data.length === 0) {
          this.logger.debug(
            `Record not found in ${tableName}, skipping migration`,
          );
          continue;
        }

        const existingId = existing.data[0][idField];
        const { newRecord, relationUpdates } = this.transformRecord(
          tableName,
          oldRecord,
        );

        await this.queryBuilderService.update(
          tableName,
          { where: [{ field: idField, operator: '=', value: existingId }] },
          newRecord,
        );

        if (Object.keys(relationUpdates).length > 0) {
          await this.updateRelations(tableName, existingId, relationUpdates);
        }

        migratedCount++;
        this.logger.debug(`Migrated record in ${tableName}`);
      } catch (error) {
        this.logger.warn(
          `Failed to migrate record in ${tableName}: ${getErrorMessage(error)}`,
        );
      }
    }

    if (migratedCount > 0) {
      this.logger.log(`Migrated ${migratedCount} record(s) in ${tableName}`);
    }

    return migratedCount;
  }

  private transformRecord(
    _tableName: string,
    oldRecord: any,
  ): { newRecord: any; relationUpdates: any } {
    const { _unique, ...data } = oldRecord;
    const relationUpdates: any = {};

    for (const field of RELATION_FIELD_PREFIXES) {
      if (Array.isArray(data[field])) {
        relationUpdates[field] = data[field];
        delete data[field];
      }
    }

    return { newRecord: data, relationUpdates };
  }

  private async updateRelations(
    tableName: string,
    recordId: any,
    relationUpdates: any,
  ): Promise<void> {
    if (tableName === 'route_definition') {
      for (const [field, methodNames] of Object.entries(relationUpdates)) {
        if (
          field === 'publishedMethods' ||
          field === 'skipRoleGuardMethods' ||
          field === 'availableMethods'
        ) {
          const idField = DatabaseConfigService.getPkField();
          const result = await this.queryBuilderService.find({
            table: 'method_definition',
            filter: { method: { _in: methodNames as string[] } },
            fields: [idField],
          });
          const methodIds = result.data
            .map((m: any) => m._id || m.id)
            .filter(Boolean);
          await this.queryBuilderService.update('route_definition', recordId, {
            [field]: methodIds,
          });
          if (methodIds.length > 0) {
            this.logger.log(`Linked ${methodIds.length} ${field} to route`);
          } else {
            this.logger.log(`Cleared ${field} for route`);
          }
        }
      }
    }
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
      return {
        _and: [
          { label: { _eq: record.label } },
          { type: { _eq: record.type } },
        ],
      };
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
