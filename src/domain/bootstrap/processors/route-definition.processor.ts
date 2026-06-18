import { BaseTableProcessor } from './base-table-processor';
import { IQueryBuilder } from '../../shared/interfaces/query-builder.interface';
import { ObjectId } from 'mongodb';
import {
  DEFAULT_REST_HANDLER_LOGIC,
  isCanonicalTableRoutePath,
} from '../utils/canonical-table-route.util';
import { DatabaseConfigService } from '../../../shared/services';
import { compileScriptSource } from '../../../shared/utils/script-code.util';
import { getSqlJunctionMetadata } from '../utils/sql-junction-metadata.util';
import { replaceSqlJunctionRows } from '../utils/sql-junction-writer.util';
import { getSqlJunctionPhysicalNames } from '../../../modules/table-management/utils/sql-junction-naming.util';

const ROUTE_METHOD_RELATION_FIELDS = [
  'publicMethods',
  'skipRoleGuardMethods',
  'availableMethods',
] as const;

export class RouteDefinitionProcessor extends BaseTableProcessor {
  private readonly queryBuilderService: IQueryBuilder;

  constructor(deps: { queryBuilderService: IQueryBuilder }) {
    super();
    this.queryBuilderService = deps.queryBuilderService;
  }
  async transformRecords(records: any[], _context?: any): Promise<any[]> {
    const isMongoDB = DatabaseConfigService.instanceIsMongoDb();
    const pkField = isMongoDB ? '_id' : 'id';
    const transformedRecords = await Promise.all(
      records.map(async (record) => {
        const transformedRecord = { ...record };
        if (transformedRecord.description === undefined)
          transformedRecord.description = null;
        if (transformedRecord.icon === undefined)
          transformedRecord.icon = 'lucide:route';
        if (transformedRecord.isSystem === undefined)
          transformedRecord.isSystem = false;
        if (transformedRecord.isEnabled === undefined)
          transformedRecord.isEnabled = false;
        if (isMongoDB) {
          const now = new Date();
          if (!transformedRecord.createdAt) transformedRecord.createdAt = now;
          if (!transformedRecord.updatedAt) transformedRecord.updatedAt = now;
        }
        if (record.mainTable) {
          if (isMongoDB) {
            const mainTable = await this.queryBuilderService.findOne({
              table: 'enfyra_table',
              where: {
                name: record.mainTable,
              },
            });
            if (!mainTable) {
              this.logger.warn(
                `Table '${record.mainTable}' not found for route ${record.path}, skipping.`,
              );
              return null;
            }
            transformedRecord.mainTable =
              typeof mainTable._id === 'string'
                ? new ObjectId(mainTable._id)
                : mainTable._id;
          } else {
            const mainTable = await this.queryBuilderService.findOne({
              table: 'enfyra_table',
              where: {
                name: record.mainTable,
              },
            });
            if (!mainTable) {
              this.logger.warn(
                `Table '${record.mainTable}' not found for route ${record.path}, skipping.`,
              );
              return null;
            }
            transformedRecord.mainTableId = mainTable.id;
            delete transformedRecord.mainTable;
          }
        }
        if (record.publicMethods && Array.isArray(record.publicMethods)) {
          transformedRecord.publicMethods = await this.resolveMethodIds(
            record.publicMethods,
            isMongoDB,
            pkField,
          );
        }
        if (
          record.skipRoleGuardMethods &&
          Array.isArray(record.skipRoleGuardMethods)
        ) {
          transformedRecord.skipRoleGuardMethods = await this.resolveMethodIds(
            record.skipRoleGuardMethods,
            isMongoDB,
            pkField,
          );
        }
        if (record.availableMethods && Array.isArray(record.availableMethods)) {
          transformedRecord.availableMethods = await this.resolveMethodIds(
            record.availableMethods,
            isMongoDB,
            pkField,
          );
        }

        return transformedRecord;
      }),
    );
    return transformedRecords.filter(Boolean);
  }

  private async resolveMethodIds(
    methodNames: string[],
    isMongoDB: boolean,
    pkField: string,
  ): Promise<any[]> {
    if (methodNames.length === 0) return [];
    if (isMongoDB) {
      const methods = await this.queryBuilderService
        .getMongoDb()
        .collection('enfyra_method')
        .find({ name: { $in: methodNames } })
        .project({ [pkField]: 1, name: 1 })
        .toArray();
      return methods.map((method: any) => method[pkField]).filter(Boolean);
    }

    const methods = await this.queryBuilderService
      .getKnex()('enfyra_method')
      .select(pkField, 'name')
      .whereIn('name', methodNames);
    return methods.map((method: any) => method[pkField]).filter(Boolean);
  }
  async afterUpsert(
    record: any,
    isNew: boolean,
    _context?: any,
  ): Promise<void> {
    const isMongoDB = DatabaseConfigService.instanceIsMongoDb();
    await this.syncRouteMethodRelations(record, isMongoDB);
    if (!isNew) return;
    await this.ensureDefaultCrudHandlers(record, isMongoDB);
  }

  protected prepareRecordForWrite(record: any, tableName: string): any {
    if (tableName !== 'enfyra_route') {
      return record;
    }

    const prepared = { ...record };
    for (const field of ROUTE_METHOD_RELATION_FIELDS) {
      delete prepared[field];
    }
    return prepared;
  }

  private async syncRouteMethodRelations(
    record: any,
    isMongoDB: boolean,
  ): Promise<void> {
    const routeId = await this.resolveRouteId(record, isMongoDB);
    if (!routeId) return;

    for (const field of ROUTE_METHOD_RELATION_FIELDS) {
      const methodIds = record[field];
      if (!Array.isArray(methodIds)) continue;

      const uniqueMethodIds = [...new Set(methodIds.filter(Boolean))];
      if (isMongoDB) {
        await this.syncMongoRouteMethodRelation(
          record,
          routeId,
          field,
          uniqueMethodIds,
        );
      } else {
        await this.syncSqlRouteMethodRelation(
          record,
          routeId,
          field,
          uniqueMethodIds,
        );
      }
    }
  }

  private async resolveRouteId(record: any, isMongoDB: boolean): Promise<any> {
    const routeId = record.id ?? record._id;
    if (routeId || !record.path) return routeId;

    if (isMongoDB) {
      const route = await this.queryBuilderService
        .getMongoDb()
        .collection('enfyra_route')
        .findOne({ path: record.path }, { projection: { _id: 1 } });
      return route?._id;
    }

    const route = await this.queryBuilderService
      .getKnex()('enfyra_route')
      .select('id')
      .where({ path: record.path })
      .first();
    return route?.id;
  }

  private async syncSqlRouteMethodRelation(
    record: any,
    routeId: any,
    field: (typeof ROUTE_METHOD_RELATION_FIELDS)[number],
    uniqueMethodIds: any[],
  ): Promise<void> {
    const { junctionTable, sourceColumn, targetColumn } =
      await getSqlJunctionMetadata(this.queryBuilderService, {
        sourceTable: 'enfyra_route',
        propertyName: field,
        targetTable: 'enfyra_method',
      });
    try {
      await replaceSqlJunctionRows(this.queryBuilderService, {
        junctionTable,
        sourceColumn,
        targetColumn,
        sourceId: routeId,
        targetIds: uniqueMethodIds,
      });
    } catch (error) {
      const rows = uniqueMethodIds.map((methodId) => ({
        [sourceColumn]: routeId,
        [targetColumn]: methodId,
      }));
      throw new Error(
        `Failed to sync enfyra_route.${field} for ${record.path}: routeId=${String(routeId)}, methodIds=${JSON.stringify(uniqueMethodIds)}, rows=${JSON.stringify(rows)}, junction=${junctionTable}(${sourceColumn},${targetColumn}): ${error instanceof Error ? error.message : String(error)}`,
      );
    }
  }

  private async syncMongoRouteMethodRelation(
    record: any,
    routeId: any,
    field: (typeof ROUTE_METHOD_RELATION_FIELDS)[number],
    uniqueMethodIds: any[],
  ): Promise<void> {
    const { junctionTable, sourceColumn, targetColumn } =
      await this.getMongoRouteMethodJunctionMetadata(field);
    const sourceId = this.toObjectId(routeId);
    const targetIds = uniqueMethodIds.map((methodId) =>
      this.toObjectId(methodId),
    );
    try {
      const collection = this.queryBuilderService
        .getMongoDb()
        .collection(junctionTable);
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
        `Failed to sync enfyra_route.${field} for ${record.path}: routeId=${String(routeId)}, methodIds=${JSON.stringify(uniqueMethodIds.map(String))}, rows=${JSON.stringify(rows)}, junction=${junctionTable}(${sourceColumn},${targetColumn}): ${error instanceof Error ? error.message : String(error)}`,
      );
    }
  }

  private async getMongoRouteMethodJunctionMetadata(
    field: (typeof ROUTE_METHOD_RELATION_FIELDS)[number],
  ): Promise<{
    junctionTable: string;
    sourceColumn: string;
    targetColumn: string;
  }> {
    const db = this.queryBuilderService.getMongoDb();
    const [routeTable, methodTable] = await Promise.all([
      db.collection('enfyra_table').findOne({ name: 'enfyra_route' }),
      db.collection('enfyra_table').findOne({ name: 'enfyra_method' }),
    ]);
    const relation = await db.collection('enfyra_relation').findOne({
      sourceTable: routeTable?._id,
      targetTable: methodTable?._id,
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

  private toObjectId(value: any): ObjectId {
    if (value instanceof ObjectId) return value;
    return new ObjectId(String(value));
  }

  async ensureMissingHandlers(): Promise<void> {
    const isMongoDB = DatabaseConfigService.instanceIsMongoDb();

    this.logger.log('[ensureMissingHandlers] Starting handler check...');

    const { data: routes } = await this.queryBuilderService.find({
      table: 'enfyra_route',
      filter: { isEnabled: { _eq: true } },
    });

    if (!routes || routes.length === 0) return;

    this.logger.log(
      `[ensureMissingHandlers] Found ${routes.length} enabled routes`,
    );

    const unrouted = await this.deleteUnroutedHandlers(isMongoDB);
    if (unrouted > 0) {
      this.logger.warn(
        `[ensureMissingHandlers] Removed ${unrouted} handler(s) with NULL routeId (orphans from deleted routes)`,
      );
    }

    let totalOrphansRemoved = unrouted;
    for (const route of routes) {
      const routeId = DatabaseConfigService.getRecordId(route);

      const methodless = await this.deleteMethodlessHandlers(
        routeId,
        isMongoDB,
      );
      if (methodless > 0) {
        totalOrphansRemoved += methodless;
        this.logger.warn(
          `[ensureMissingHandlers] Removed ${methodless} handler(s) with NULL methodId on route "${route.path}"`,
        );
      }

      await this.ensureDefaultCrudHandlers(route, isMongoDB);
    }

    if (totalOrphansRemoved > 0) {
      this.logger.warn(
        `[ensureMissingHandlers] Total orphan handlers removed: ${totalOrphansRemoved}`,
      );
    }
  }

  private async deleteMethodlessHandlers(
    routeId: any,
    isMongoDB: boolean,
  ): Promise<number> {
    if (isMongoDB) {
      const db = this.queryBuilderService.getMongoDb();
      const routeIdObj =
        typeof routeId === 'string' ? new ObjectId(routeId) : routeId;
      const result = await db
        .collection('enfyra_route_handler')
        .deleteMany({ route: routeIdObj, method: null });
      return result.deletedCount || 0;
    }
    const knex = this.queryBuilderService.getKnex();
    return await knex('enfyra_route_handler')
      .where({ routeId })
      .whereNull('methodId')
      .delete();
  }

  private async deleteUnroutedHandlers(isMongoDB: boolean): Promise<number> {
    if (isMongoDB) {
      const db = this.queryBuilderService.getMongoDb();
      const result = await db
        .collection('enfyra_route_handler')
        .deleteMany({ route: null });
      return result.deletedCount || 0;
    }
    const knex = this.queryBuilderService.getKnex();
    return await knex('enfyra_route_handler').whereNull('routeId').delete();
  }

  private async ensureDefaultCrudHandlers(
    record: any,
    isMongoDB: boolean,
  ): Promise<void> {
    const path = record.path;

    let tableName: string | undefined;
    const mainTableValue = record.mainTable;
    if (
      mainTableValue &&
      typeof mainTableValue === 'object' &&
      mainTableValue.name
    ) {
      tableName = mainTableValue.name;
    } else {
      let mainTableFk = isMongoDB
        ? mainTableValue
        : (record.mainTableId ?? mainTableValue?.id ?? mainTableValue?._id);
      if (!mainTableFk) return;
      if (isMongoDB && typeof mainTableFk === 'string') {
        mainTableFk = new ObjectId(mainTableFk);
      }
      const tableRow = isMongoDB
        ? await this.queryBuilderService
            .getMongoDb()
            .collection('enfyra_table')
            .findOne({ _id: mainTableFk })
        : await this.queryBuilderService
            .getKnex()('enfyra_table')
            .where({ id: mainTableFk })
            .first();
      tableName = tableRow?.name;
    }
    if (!tableName || !isCanonicalTableRoutePath(path, tableName)) return;

    this.logger.log(
      `[${path}] Creating default CRUD handlers for table "${tableName}"...`,
    );
    const routeId = DatabaseConfigService.getRecordId(record);
    if (!routeId) return;

    let methodIds: any[] = [];
    const raw = record.availableMethods;
    if (Array.isArray(raw) && raw.length > 0) {
      methodIds =
        typeof raw[0] === 'object' && raw[0] !== null
          ? raw.map((m: any) => m?.id ?? m?._id).filter(Boolean)
          : [...raw];
    } else {
      if (isMongoDB) {
        const junction = getSqlJunctionPhysicalNames({
          sourceTable: 'enfyra_route',
          propertyName: 'availableMethods',
          targetTable: 'enfyra_method',
        });
        const mongoService = this.queryBuilderService.getMongoDb();
        const routeIdObj =
          typeof routeId === 'string' ? new ObjectId(routeId) : routeId;
        const rows = await mongoService
          .collection(junction.junctionTableName)
          .find({ [junction.junctionSourceColumn]: routeIdObj })
          .toArray();
        methodIds = rows.map((r: any) => r[junction.junctionTargetColumn]);
      } else {
        const { junctionTable, sourceColumn, targetColumn } =
          await getSqlJunctionMetadata(this.queryBuilderService, {
            sourceTable: 'enfyra_route',
            propertyName: 'availableMethods',
            targetTable: 'enfyra_method',
          });
        const knex = this.queryBuilderService.getKnex();
        const rows = await knex(junctionTable)
          .select(targetColumn)
          .where({ [sourceColumn]: routeId });
        methodIds = rows.map((r: any) => r[targetColumn]);
      }
    }

    if (methodIds.length === 0) return;

    const available: string[] = isMongoDB
      ? (
          await this.queryBuilderService
            .getMongoDb()
            .collection('enfyra_method')
            .find({
              _id: {
                $in: methodIds.map((id: any) =>
                  id instanceof ObjectId ? id : new ObjectId(String(id)),
                ),
              },
            })
            .project({ name: 1 })
            .toArray()
        )
          .map((m: any) => m.name)
          .filter(Boolean)
      : (
          await this.queryBuilderService
            .getKnex()('enfyra_method')
            .select('name')
            .whereIn('id', methodIds)
        )
          .map((m: any) => m.name)
          .filter(Boolean);

    if (available.length === 0) return;

    for (const methodName of available) {
      const logic = DEFAULT_REST_HANDLER_LOGIC[methodName];
      if (!logic) continue;

      const methodRow = await this.queryBuilderService.findOne({
        table: 'enfyra_method',
        where: { name: methodName },
      });
      if (!methodRow) {
        this.logger.warn(`[${path}] Method row not found: ${methodName}`);
        continue;
      }

      const methodKeyId = isMongoDB
        ? (methodRow._id ?? methodRow.id)
        : methodRow.id;
      if (methodKeyId == null) {
        this.logger.error(
          `[${path}] methodRow for "${methodName}" is missing id — refusing to seed handler with NULL methodId`,
        );
        continue;
      }

      let existing;
      if (isMongoDB) {
        const mongoService = this.queryBuilderService.getMongoDb();
        const routeIdObj =
          typeof routeId === 'string' ? new ObjectId(routeId) : routeId;
        const methodIdObj =
          typeof methodKeyId === 'string'
            ? new ObjectId(methodKeyId)
            : methodKeyId;
        existing = await mongoService
          .collection('enfyra_route_handler')
          .findOne({
            route: routeIdObj,
            method: methodIdObj,
          });
      } else {
        existing = await this.queryBuilderService.findOne({
          table: 'enfyra_route_handler',
          where: {
            routeId,
            methodId: methodKeyId,
          },
        });
      }

      if (existing) {
        continue;
      }

      let data: Record<string, any>;
      if (isMongoDB) {
        data = {
          route: typeof routeId === 'string' ? new ObjectId(routeId) : routeId,
          method:
            typeof methodKeyId === 'string'
              ? new ObjectId(methodKeyId)
              : methodKeyId,
          sourceCode: logic,
          scriptLanguage: 'typescript',
          compiledCode: compileScriptSource(logic, 'typescript'),
          timeout: 30000,
        };
      } else {
        data = {
          routeId,
          methodId: methodKeyId,
          sourceCode: logic,
          scriptLanguage: 'typescript',
          compiledCode: compileScriptSource(logic, 'typescript'),
          timeout: 30000,
        };
      }

      if (isMongoDB) {
        const mongoService = this.queryBuilderService.getMongoDb();
        await mongoService
          .collection('enfyra_route_handler')
          .insertOne(data);
      } else {
        await this.queryBuilderService.insertWithOptions({
          table: 'enfyra_route_handler',
          data,
        });
      }
      this.logger.log(`   Default ${methodName} handler → ${path}`);
    }
  }
  getUniqueIdentifier(record: any): object {
    return { path: record.path };
  }
  protected getCompareFields(): string[] {
    return [
      'path',
      'isEnabled',
      'icon',
      'description',
      'isSystem',
      'maxUploadFileSize',
      'mainTable',
      'publicMethods',
      'skipRoleGuardMethods',
      'availableMethods',
    ];
  }
  protected getRecordIdentifier(record: any): string {
    return `[Route] ${record.path}`;
  }
}
