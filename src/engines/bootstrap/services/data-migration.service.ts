import { DatabaseConfigService } from '../../../shared/services';
import { Logger } from '../../../shared/logger';
import { QueryBuilderService } from '@enfyra/kernel';
import * as fs from 'fs';
import * as path from 'path';
import { getErrorMessage } from '../../../shared/utils/error.util';
import { ObjectId } from 'mongodb';
import { bootstrapVerboseLog } from '../utils/bootstrap-logging.util';
import { getSqlJunctionMetadata } from '../../../domain/bootstrap/utils/sql-junction-metadata.util';
import { replaceSqlJunctionRows } from '../../../domain/bootstrap/utils/sql-junction-writer.util';
import { getSqlJunctionPhysicalNames } from '../../../modules/table-management/utils/sql-junction-naming.util';

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
          this.verbose(
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
      (this.initOld._deletedTables?.length ?? 0) > 0 ||
      (this.initOld._deletedRecords?.length ?? 0) > 0
    );
  }

  async runMigrations(): Promise<void> {
    if (!this.hasMigrations()) {
      this.verbose('No data migrations to run');
      return;
    }

    if (this.queryBuilderService.runWithTelemetryContext) {
      await this.queryBuilderService.runWithTelemetryContext('migration', () =>
        this.runMigrationBatch(),
      );
      return;
    }
    await this.runMigrationBatch();
  }

  private async runMigrationBatch(): Promise<void> {
    this.verbose('Running data migrations from data-migration.json...');

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

    this.verbose(
      `Data migrations completed: ${totalMigrated} record(s) migrated`,
    );
  }

  private async deleteTableData(tableNames: string[]): Promise<void> {
    this.verbose(`Deleting data from ${tableNames.length} table(s)...`);
    for (const tableName of tableNames) {
      try {
        await this.queryBuilderService.delete(tableName, { where: [] });
        this.verbose(`Deleted all data from ${tableName}`);
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
          this.verbose(`Deleted ${count} record(s) from ${table}`);
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
        await this.normalizeRouteMainTable(tableName, newRecord);

        if (Object.keys(newRecord).length > 0) {
          await this.queryBuilderService.update(
            tableName,
            { where: [{ field: idField, operator: '=', value: existingId }] },
            newRecord,
          );
        }

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
      this.verbose(`Migrated ${migratedCount} record(s) in ${tableName}`);
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

  private async normalizeRouteMainTable(
    tableName: string,
    data: any,
  ): Promise<void> {
    if (tableName === 'route_definition' && data.mainTable) {
      const mainTable = await this.queryBuilderService.findOne({
        table: 'table_definition',
        where: { name: data.mainTable },
      });
      if (!mainTable) {
        this.logger.warn(
          `Table '${data.mainTable}' not found for route data migration`,
        );
        delete data.mainTable;
      } else if (DatabaseConfigService.instanceIsMongoDb()) {
        const mainTableId = mainTable._id ?? mainTable.id;
        data.mainTable =
          typeof mainTableId === 'string' ? new ObjectId(mainTableId) : mainTableId;
      } else {
        data.mainTableId = mainTable.id;
        delete data.mainTable;
      }
    }
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
          const methodIds = await this.resolveMethodIds(methodNames as string[]);
          if (DatabaseConfigService.instanceIsMongoDb()) {
            await this.updateMongoRouteMethodRelation(
              recordId,
              field,
              methodIds,
            );
          } else {
            await this.updateSqlRouteMethodRelation(recordId, field, methodIds);
          }
          if (methodIds.length > 0) {
            this.verbose(`Linked ${methodIds.length} ${field} to route`);
          } else {
            this.verbose(`Cleared ${field} for route`);
          }
        }
      }
    }
  }

  private async resolveMethodIds(methodNames: string[]): Promise<any[]> {
    if (methodNames.length === 0) return [];

    if (DatabaseConfigService.instanceIsMongoDb()) {
      const idField = DatabaseConfigService.getPkField();
      const result = await this.queryBuilderService.find({
        table: 'method_definition',
        filter: { method: { _in: methodNames } },
        fields: [idField],
      });
      return result.data.map((m: any) => m._id || m.id).filter(Boolean);
    }

    const rows = await this.queryBuilderService
      .getKnex()('method_definition')
      .select('id', 'method')
      .whereIn('method', methodNames);
    return rows.map((m: any) => m.id).filter(Boolean);
  }

  private async updateSqlRouteMethodRelation(
    routeId: any,
    field: string,
    methodIds: any[],
  ): Promise<void> {
    const { junctionTable, sourceColumn, targetColumn } =
      await getSqlJunctionMetadata(this.queryBuilderService as any, {
        sourceTable: 'route_definition',
        propertyName: field,
        targetTable: 'method_definition',
      });
    try {
      await replaceSqlJunctionRows(this.queryBuilderService as any, {
        junctionTable,
        sourceColumn,
        targetColumn,
        sourceId: routeId,
        targetIds: methodIds,
      });
    } catch (error) {
      const rows = methodIds.map((methodId) => ({
        [sourceColumn]: routeId,
        [targetColumn]: methodId,
      }));
      throw new Error(
        `Failed to migrate route_definition.${field}: routeId=${String(routeId)}, methodIds=${JSON.stringify(methodIds)}, rows=${JSON.stringify(rows)}, junction=${junctionTable}(${sourceColumn},${targetColumn}): ${error instanceof Error ? error.message : String(error)}`,
      );
    }
  }

  private async updateMongoRouteMethodRelation(
    routeId: any,
    field: string,
    methodIds: any[],
  ): Promise<void> {
    const db = this.queryBuilderService.getMongoDb();
    const { junctionTable, sourceColumn, targetColumn } =
      await this.getMongoJunctionMetadata(field);
    const sourceId = this.toObjectId(routeId);
    const targetIds = methodIds.map((id) => this.toObjectId(id));
    try {
      const collection = db.collection(junctionTable);
      await collection.deleteMany({ [sourceColumn]: sourceId });
      if (targetIds.length === 0) return;
      await collection.insertMany(
        targetIds.map((methodId) => ({
          [sourceColumn]: sourceId,
          [targetColumn]: methodId,
        })),
        { ordered: false },
      );
    } catch (error) {
      const rows = targetIds.map((methodId) => ({
        [sourceColumn]: sourceId,
        [targetColumn]: methodId,
      }));
      throw new Error(
        `Failed to migrate route_definition.${field}: routeId=${String(routeId)}, methodIds=${JSON.stringify(methodIds.map(String))}, rows=${JSON.stringify(rows)}, junction=${junctionTable}(${sourceColumn},${targetColumn}): ${error instanceof Error ? error.message : String(error)}`,
      );
    }
  }

  private async getMongoJunctionMetadata(field: string): Promise<{
    junctionTable: string;
    sourceColumn: string;
    targetColumn: string;
  }> {
    const db = this.queryBuilderService.getMongoDb();
    const [sourceTable, targetTable] = await Promise.all([
      db.collection('table_definition').findOne({ name: 'route_definition' }),
      db.collection('table_definition').findOne({ name: 'method_definition' }),
    ]);
    const relation = await db.collection('relation_definition').findOne({
      sourceTable: sourceTable?._id,
      targetTable: targetTable?._id,
      propertyName: field,
    });
    const fallback = getSqlJunctionPhysicalNames({
      sourceTable: 'route_definition',
      propertyName: field,
      targetTable: 'method_definition',
    });
    return {
      junctionTable: relation?.junctionTableName || fallback.junctionTableName,
      sourceColumn:
        relation?.junctionSourceColumn || fallback.junctionSourceColumn,
      targetColumn:
        relation?.junctionTargetColumn || fallback.junctionTargetColumn,
    };
  }

  private toObjectId(value: any): ObjectId {
    if (value instanceof ObjectId) return value;
    return new ObjectId(String(value));
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

  private verbose(message: string): void {
    bootstrapVerboseLog(this.logger, message);
  }
}
