import { DatabaseConfigService } from '../../../shared/services';
import { Logger } from '../../../shared/logger';
import { QueryBuilderService } from '@enfyra/kernel';
import { getErrorMessage } from '../../../shared/utils/error.util';
import { ObjectId } from 'mongodb';
import { bootstrapVerboseLog } from '../utils/bootstrap-logging.util';
import { getSqlJunctionMetadata } from '../../../domain/bootstrap/utils/sql-junction-metadata.util';
import { replaceSqlJunctionRows } from '../../../domain/bootstrap/utils/sql-junction-writer.util';
import { getSqlJunctionPhysicalNames } from '../../../modules/table-management/utils/sql-junction-naming.util';
import { isCanonicalTableRoutePath } from '../../../domain/bootstrap/utils/canonical-table-route.util';
import { BootstrapDefinitionService } from './bootstrap-definition.service';

interface InitOld {
  [tableName: string]: any | any[];
  _deletedTables?: string[];
  _deletedRecords?: { table: string; filter: Record<string, any> }[];
}

const RELATION_FIELD_PREFIXES = [
  'publicMethods',
  'skipRoleGuardMethods',
  'availableMethods',
];

export class DataMigrationService {
  private readonly logger = new Logger(DataMigrationService.name);
  private readonly queryBuilderService: QueryBuilderService;
  private initOld: InitOld | null = null;

  constructor(deps: {
    queryBuilderService: QueryBuilderService;
    bootstrapDefinitionService?: BootstrapDefinitionService;
  }) {
    this.queryBuilderService = deps.queryBuilderService;
    const bootstrapDefinitionService =
      deps.bootstrapDefinitionService ?? new BootstrapDefinitionService();
    const dataMigration = bootstrapDefinitionService.getDataMigration();
    if (Object.keys(dataMigration).length > 0) {
      this.initOld = dataMigration;
      this.verbose(
        `Loaded data-migration.ts with ${Object.keys(dataMigration).length} table(s) to migrate`,
      );
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

  async assertTargetState(): Promise<void> {
    if (!this.hasMigrations()) return;

    for (const tableName of this.initOld!._deletedTables ?? []) {
      const count = await this.countRawRecords(tableName, {});
      if (count > 0) {
        throw new Error(
          `Data migration target mismatch: ${tableName} still contains ${count} record(s)`,
        );
      }
    }

    for (const { table, filter } of this.initOld!._deletedRecords ?? []) {
      const exactWhere = this.toExactDeleteWhere(filter);
      if (!exactWhere || Object.keys(exactWhere).length === 0) {
        throw new Error(
          `Data migration target mismatch: ${table} has an unsupported delete filter`,
        );
      }
      const count = await this.countRawRecords(table, exactWhere);
      if (count > 0) {
        throw new Error(
          `Data migration target mismatch: ${table} still contains ${count} deleted record(s)`,
        );
      }
    }

    for (const [tableName, records] of Object.entries(this.initOld!)) {
      if (tableName.startsWith('_')) continue;
      const recordsArray = Array.isArray(records) ? records : [records];
      for (const migrationRecord of recordsArray) {
        await this.assertRecordTarget(tableName, migrationRecord);
      }
    }

    await this.assertCustomRouteMainTablesCleared();
  }

  private async runMigrationBatch(): Promise<void> {
    this.verbose('Running data migrations from data-migration.ts...');

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

    totalMigrated += await this.clearCustomRouteMainTables();

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
        throw new Error(
          `Failed to delete data from ${tableName}: ${getErrorMessage(error)}`,
        );
      }
    }
  }

  private async deleteRecords(
    records: { table: string; filter: Record<string, any> }[],
  ): Promise<void> {
    for (const { table, filter } of records) {
      try {
        const exactWhere = this.toExactDeleteWhere(filter);
        if (!exactWhere || Object.keys(exactWhere).length === 0) {
          this.logger.warn(
            `Skipping deleted-record migration for ${table}: only exact _eq filters are supported`,
          );
          continue;
        }

        let count = 0;
        if (DatabaseConfigService.instanceIsMongoDb()) {
          const result = await this.queryBuilderService
            .getMongoDb()
            .collection(table)
            .deleteMany(exactWhere);
          count = result.deletedCount || 0;
        } else {
          count = await this.queryBuilderService
            .getKnex()(table)
            .where(exactWhere)
            .delete();
        }
        if (count > 0) {
          this.verbose(`Deleted ${count} record(s) from ${table}`);
        }
      } catch (error) {
        throw new Error(
          `Failed to delete records from ${table}: ${getErrorMessage(error)}`,
        );
      }
    }
  }

  private toExactDeleteWhere(
    filter: Record<string, any>,
  ): Record<string, any> | null {
    if (!filter || typeof filter !== 'object' || Array.isArray(filter)) {
      return null;
    }

    const where: Record<string, any> = {};
    for (const [field, value] of Object.entries(filter)) {
      if (!field || field.startsWith('_')) return null;
      if (
        value &&
        typeof value === 'object' &&
        !Array.isArray(value) &&
        Object.keys(value).length === 1 &&
        Object.prototype.hasOwnProperty.call(value, '_eq')
      ) {
        where[field] = value._eq;
        continue;
      }
      if (
        value === null ||
        ['string', 'number', 'boolean'].includes(typeof value)
      ) {
        where[field] = value;
        continue;
      }
      return null;
    }

    return where;
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
        throw new Error(
          `Failed to migrate record in ${tableName}: ${getErrorMessage(error)}`,
        );
      }
    }

    if (migratedCount > 0) {
      this.verbose(`Migrated ${migratedCount} record(s) in ${tableName}`);
    }

    return migratedCount;
  }

  private async assertRecordTarget(
    tableName: string,
    migrationRecord: any,
  ): Promise<void> {
    const uniqueFilter = this.getUniqueFilter(tableName, migrationRecord);
    if (!uniqueFilter) {
      throw new Error(
        `Data migration target mismatch: ${tableName} record has no unique identifier`,
      );
    }
    const idField = DatabaseConfigService.getPkField();
    const existing = await this.queryBuilderService.find({
      table: tableName,
      filter: uniqueFilter,
      limit: 1,
      fields: ['*'],
    });
    const current = existing.data?.[0];
    if (!current) return;

    const { newRecord, relationUpdates } = this.transformRecord(
      tableName,
      migrationRecord,
    );
    await this.normalizeRouteMainTable(tableName, newRecord);
    const mismatchedFields = Object.entries(newRecord)
      .filter(
        ([field, expected]) => !this.valuesEquivalent(expected, current[field]),
      )
      .map(([field]) => field);
    if (mismatchedFields.length > 0) {
      throw new Error(
        `Data migration target mismatch: ${tableName} differs on ${mismatchedFields.join(', ')}`,
      );
    }

    if (
      tableName === 'enfyra_route' &&
      Object.keys(relationUpdates).length > 0
    ) {
      await this.assertRouteMethodRelationTargets(
        current[idField],
        relationUpdates,
      );
    }
  }

  private async assertRouteMethodRelationTargets(
    routeId: any,
    relationUpdates: Record<string, string[]>,
  ): Promise<void> {
    for (const [field, methodNames] of Object.entries(relationUpdates)) {
      if (!RELATION_FIELD_PREFIXES.includes(field)) continue;
      const expectedIds = (await this.resolveMethodIds(methodNames)).map(
        String,
      );
      let currentIds: string[];
      if (DatabaseConfigService.instanceIsMongoDb()) {
        const { junctionTable, sourceColumn, targetColumn } =
          await this.getMongoJunctionMetadata(field);
        const rows = await this.queryBuilderService
          .getMongoDb()
          .collection(junctionTable)
          .find({ [sourceColumn]: this.toObjectId(routeId) })
          .project({ [targetColumn]: 1 })
          .toArray();
        currentIds = rows.map((row: any) => String(row[targetColumn]));
      } else {
        const { junctionTable, sourceColumn, targetColumn } =
          await getSqlJunctionMetadata(this.queryBuilderService as any, {
            sourceTable: 'enfyra_route',
            propertyName: field,
            targetTable: 'enfyra_method',
          });
        const rows = await this.queryBuilderService
          .getKnex()(junctionTable)
          .where(sourceColumn, routeId)
          .select(targetColumn);
        currentIds = rows.map((row: any) => String(row[targetColumn]));
      }
      if (this.idSetKey(currentIds) !== this.idSetKey(expectedIds)) {
        throw new Error(
          `Data migration target mismatch: enfyra_route.${field} differs from target`,
        );
      }
    }
  }

  private async countRawRecords(
    tableName: string,
    where: Record<string, any>,
  ): Promise<number> {
    if (DatabaseConfigService.instanceIsMongoDb()) {
      return this.queryBuilderService
        .getMongoDb()
        .collection(tableName)
        .countDocuments(where);
    }
    const result = await this.queryBuilderService
      .getKnex()(tableName)
      .where(where)
      .count({ count: '*' });
    return Number(result[0]?.count ?? 0);
  }

  private async assertCustomRouteMainTablesCleared(): Promise<void> {
    const idField = DatabaseConfigService.getPkField();
    const routes = await this.queryBuilderService.find({
      table: 'enfyra_route',
      filter: {},
      limit: -1,
      fields: [idField, 'path', 'mainTable.name'],
    });
    const mismatches = (routes.data || []).filter((route: any) => {
      const tableName = route.mainTable?.name;
      return tableName && !isCanonicalTableRoutePath(route.path, tableName);
    });
    if (mismatches.length > 0) {
      throw new Error(
        `Data migration target mismatch: ${mismatches.length} custom route(s) still reference mainTable`,
      );
    }
  }

  private valuesEquivalent(expected: any, current: any): boolean {
    if (typeof expected === 'boolean') {
      return (
        expected ===
        (current === true ||
          current === 1 ||
          String(current).toLowerCase() === 'true' ||
          String(current) === '1')
      );
    }
    if (typeof expected === 'number') {
      return Number(current) === expected;
    }
    return this.canonical(expected) === this.canonical(current);
  }

  private canonical(value: any): string {
    if (value instanceof ObjectId) return JSON.stringify(value.toHexString());
    if (Array.isArray(value)) {
      return `[${value.map((entry) => this.canonical(entry)).join(',')}]`;
    }
    if (value && typeof value === 'object') {
      return `{${Object.keys(value)
        .sort()
        .map((key) => `${JSON.stringify(key)}:${this.canonical(value[key])}`)
        .join(',')}}`;
    }
    if (
      typeof value === 'string' &&
      ((value.startsWith('{') && value.endsWith('}')) ||
        (value.startsWith('[') && value.endsWith(']')))
    ) {
      try {
        return this.canonical(JSON.parse(value));
      } catch {}
    }
    return JSON.stringify(value ?? null);
  }

  private idSetKey(values: string[]): string {
    return [...new Set(values)].sort().join('|');
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

    if (!DatabaseConfigService.instanceIsMongoDb()) {
      for (const [field, value] of Object.entries(data)) {
        if (value && typeof value === 'object') {
          data[field] = JSON.stringify(value);
        }
      }
    }

    return { newRecord: data, relationUpdates };
  }

  private async normalizeRouteMainTable(
    tableName: string,
    data: any,
  ): Promise<void> {
    if (tableName === 'enfyra_route' && data.mainTable) {
      const mainTable = await this.queryBuilderService.findOne({
        table: 'enfyra_table',
        where: { name: data.mainTable },
      });
      if (!mainTable) {
        this.logger.warn(
          `Table '${data.mainTable}' not found for route data migration`,
        );
        delete data.mainTable;
      } else if (DatabaseConfigService.instanceIsMongoDb()) {
        const mainTableId = mainTable._id ?? mainTable.id;
        data.mainTable = this.normalizeMongoId(mainTableId);
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
    if (tableName === 'enfyra_route') {
      for (const [field, methodNames] of Object.entries(relationUpdates)) {
        if (
          field === 'publicMethods' ||
          field === 'skipRoleGuardMethods' ||
          field === 'availableMethods'
        ) {
          const methodIds = await this.resolveMethodIds(
            methodNames as string[],
          );
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

  private async clearCustomRouteMainTables(): Promise<number> {
    const idField = DatabaseConfigService.getPkField();

    try {
      const routes = await this.queryBuilderService.find({
        table: 'enfyra_route',
        filter: {},
        limit: -1,
        fields: [idField, 'path', 'mainTable.name'],
      });

      let cleared = 0;
      for (const route of routes.data || []) {
        const tableName = route.mainTable?.name;
        if (!tableName || isCanonicalTableRoutePath(route.path, tableName)) {
          continue;
        }

        const data = DatabaseConfigService.instanceIsMongoDb()
          ? { mainTable: null }
          : { mainTableId: null };

        await this.queryBuilderService.update(
          'enfyra_route',
          { where: [{ field: idField, operator: '=', value: route[idField] }] },
          data,
        );
        cleared++;
      }

      if (cleared > 0) {
        this.verbose(`Cleared mainTable from ${cleared} custom route(s)`);
      }
      return cleared;
    } catch (error) {
      throw new Error(
        `Failed to clear custom route mainTable links: ${getErrorMessage(error)}`,
      );
    }
  }

  private async resolveMethodIds(methodNames: string[]): Promise<any[]> {
    if (methodNames.length === 0) return [];

    if (DatabaseConfigService.instanceIsMongoDb()) {
      const idField = DatabaseConfigService.getPkField();
      const result = await this.queryBuilderService.find({
        table: 'enfyra_method',
        filter: { name: { _in: methodNames } },
        fields: [idField],
      });
      return result.data.map((m: any) => m._id || m.id).filter(Boolean);
    }

    const rows = await this.queryBuilderService
      .getKnex()('enfyra_method')
      .select('id', 'name')
      .whereIn('name', methodNames);
    return rows.map((m: any) => m.id).filter(Boolean);
  }

  private async updateSqlRouteMethodRelation(
    routeId: any,
    field: string,
    methodIds: any[],
  ): Promise<void> {
    const { junctionTable, sourceColumn, targetColumn } =
      await getSqlJunctionMetadata(this.queryBuilderService as any, {
        sourceTable: 'enfyra_route',
        propertyName: field,
        targetTable: 'enfyra_method',
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
        `Failed to migrate enfyra_route.${field}: routeId=${String(routeId)}, methodIds=${JSON.stringify(methodIds)}, rows=${JSON.stringify(rows)}, junction=${junctionTable}(${sourceColumn},${targetColumn}): ${error instanceof Error ? error.message : String(error)}`,
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
        `Failed to migrate enfyra_route.${field}: routeId=${String(routeId)}, methodIds=${JSON.stringify(methodIds.map(String))}, rows=${JSON.stringify(rows)}, junction=${junctionTable}(${sourceColumn},${targetColumn}): ${error instanceof Error ? error.message : String(error)}`,
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
      db.collection('enfyra_table').findOne({ name: 'enfyra_route' }),
      db.collection('enfyra_table').findOne({ name: 'enfyra_method' }),
    ]);
    const relation = await db.collection('enfyra_relation').findOne({
      sourceTable: sourceTable?._id,
      targetTable: targetTable?._id,
      propertyName: field,
    });
    const fallback = getSqlJunctionPhysicalNames({
      sourceTable: 'enfyra_route',
      propertyName: field,
      targetTable: 'enfyra_method',
    });
    return {
      junctionTable: relation?.junctionTableName || fallback.junctionTableName,
      sourceColumn:
        relation?.junctionSourceColumn || fallback.junctionSourceColumn,
      targetColumn:
        relation?.junctionTargetColumn || fallback.junctionTargetColumn,
    };
  }

  private toObjectId(value: any): any {
    return this.normalizeMongoId(value);
  }

  private normalizeMongoId(value: any): any {
    if (value instanceof ObjectId) return value;
    if (typeof value === 'string' && ObjectId.isValid(value)) {
      return new ObjectId(value);
    }
    return value;
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
