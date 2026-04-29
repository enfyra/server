import { BaseTableProcessor } from './base-table-processor';
import { IQueryBuilder } from '../../shared/interfaces/query-builder.interface';
import { ObjectId } from 'mongodb';
import {
  DEFAULT_REST_HANDLER_LOGIC,
  isCanonicalTableRoutePath,
  REST_HANDLER_METHOD_NAMES,
} from '../utils/canonical-table-route.util';
import { DatabaseConfigService } from '../../../shared/services';
import { compileScriptSource } from '../../../kernel/execution';
import {
  getJunctionColumnNames,
  getJunctionTableName,
} from '../../../kernel/query';

const ROUTE_METHOD_RELATION_FIELDS = [
  'publishedMethods',
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
    const pkField = DatabaseConfigService.getPkField();
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
              table: 'table_definition',
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
              table: 'table_definition',
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
        if (record.publishedMethods && Array.isArray(record.publishedMethods)) {
          const methodNames = record.publishedMethods;
          const result = await this.queryBuilderService.find({
            table: 'method_definition',
            filter: { method: { _in: methodNames } },
            fields: [pkField, 'method'],
          });
          const methods = result.data || [];
          transformedRecord.publishedMethods = methods.map(
            (m: any) => m[pkField],
          );
        }
        if (
          record.skipRoleGuardMethods &&
          Array.isArray(record.skipRoleGuardMethods)
        ) {
          const methodNames = record.skipRoleGuardMethods;
          const result = await this.queryBuilderService.find({
            table: 'method_definition',
            filter: { method: { _in: methodNames } },
            fields: [pkField, 'method'],
          });
          const methods = result.data || [];
          transformedRecord.skipRoleGuardMethods = methods.map(
            (m: any) => m[pkField],
          );
        }
        if (record.availableMethods && Array.isArray(record.availableMethods)) {
          const methodNames = record.availableMethods;
          const result = await this.queryBuilderService.find({
            table: 'method_definition',
            filter: { method: { _in: methodNames } },
            fields: [pkField, 'method'],
          });
          const methods = result.data || [];
          transformedRecord.availableMethods = methods.map(
            (m: any) => m[pkField],
          );
        }

        return transformedRecord;
      }),
    );
    return transformedRecords.filter(Boolean);
  }
  async afterUpsert(
    record: any,
    isNew: boolean,
    _context?: any,
  ): Promise<void> {
    const isMongoDB = DatabaseConfigService.instanceIsMongoDb();
    if (!isMongoDB) {
      await this.syncRouteMethodRelations(record);
    }
    if (!isNew) return;
    await this.ensureDefaultCrudHandlers(record, isMongoDB);
  }

  protected prepareRecordForWrite(record: any, tableName: string): any {
    if (
      tableName !== 'route_definition' ||
      DatabaseConfigService.instanceIsMongoDb()
    ) {
      return record;
    }

    const prepared = { ...record };
    for (const field of ROUTE_METHOD_RELATION_FIELDS) {
      delete prepared[field];
    }
    return prepared;
  }

  private async syncRouteMethodRelations(record: any): Promise<void> {
    const routeId = record.id;
    if (!routeId) return;

    for (const field of ROUTE_METHOD_RELATION_FIELDS) {
      const methodIds = record[field];
      if (!Array.isArray(methodIds)) continue;

      await this.queryBuilderService.update('route_definition', routeId, {
        [field]: [...new Set(methodIds.filter(Boolean))],
      });
    }
  }

  async ensureMissingHandlers(): Promise<void> {
    const isMongoDB = DatabaseConfigService.instanceIsMongoDb();

    this.logger.log('[ensureMissingHandlers] Starting handler check...');

    const { data: routes } = await this.queryBuilderService.find({
      table: 'route_definition',
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
        .collection('route_handler_definition')
        .deleteMany({ route: routeIdObj, method: null });
      return result.deletedCount || 0;
    }
    const knex = this.queryBuilderService.getKnex();
    return await knex('route_handler_definition')
      .where({ routeId })
      .whereNull('methodId')
      .delete();
  }

  private async deleteUnroutedHandlers(isMongoDB: boolean): Promise<number> {
    if (isMongoDB) {
      const db = this.queryBuilderService.getMongoDb();
      const result = await db
        .collection('route_handler_definition')
        .deleteMany({ route: null });
      return result.deletedCount || 0;
    }
    const knex = this.queryBuilderService.getKnex();
    return await knex('route_handler_definition').whereNull('routeId').delete();
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
      let mainTableFk = isMongoDB ? mainTableValue : record.mainTableId;
      if (!mainTableFk) return;
      if (isMongoDB && typeof mainTableFk === 'string') {
        mainTableFk = new ObjectId(mainTableFk);
      }
      const tableRow = isMongoDB
        ? await this.queryBuilderService
            .getMongoDb()
            .collection('table_definition')
            .findOne({ _id: mainTableFk })
        : await this.queryBuilderService.findOne({
            table: 'table_definition',
            where: { id: mainTableFk },
          });
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
      const junctionName =
        getJunctionTableName(
          'route_definition',
          'availableMethods',
          'method_definition',
        );
      const { sourceColumn, targetColumn } = getJunctionColumnNames(
        'route_definition',
        'availableMethods',
        'method_definition',
      );
      if (isMongoDB) {
        const mongoService = this.queryBuilderService.getMongoDb();
        const routeIdObj =
          typeof routeId === 'string' ? new ObjectId(routeId) : routeId;
        const rows = await mongoService
          .collection(junctionName)
          .find({ [sourceColumn]: routeIdObj })
          .toArray();
        methodIds = rows.map((r: any) => r[targetColumn]);
      } else {
        const knex = this.queryBuilderService.getKnex();
        const rows = await knex(junctionName)
          .select(targetColumn)
          .where({ [sourceColumn]: routeId });
        methodIds = rows.map((r: any) => r[targetColumn]);
      }
    }

    if (methodIds.length === 0) return;

    const idStrings = methodIds.map((id: any) => id.toString());
    const methodResult = await this.queryBuilderService.find({
      table: 'method_definition',
      filter: { id: { _in: idStrings } },
      fields: ['method'],
    });
    const available: string[] = (methodResult.data || [])
      .map((m: any) => m.method)
      .filter(Boolean);

    if (available.length === 0) return;

    for (const methodName of REST_HANDLER_METHOD_NAMES) {
      if (!available.includes(methodName)) continue;

      const logic = DEFAULT_REST_HANDLER_LOGIC[methodName];
      if (!logic) {
        this.logger.warn(
          `[${path}] No default logic for method: ${methodName}`,
        );
        continue;
      }

      const methodRow = await this.queryBuilderService.findOne({
        table: 'method_definition',
        where: { method: methodName },
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
          .collection('route_handler_definition')
          .findOne({
            route: routeIdObj,
            method: methodIdObj,
          });
      } else {
        existing = await this.queryBuilderService.findOne({
          table: 'route_handler_definition',
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
          .collection('route_handler_definition')
          .insertOne(data);
      } else {
        await this.queryBuilderService.insertWithOptions({
          table: 'route_handler_definition',
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
      'mainTable',
      'publishedMethods',
      'skipRoleGuardMethods',
      'availableMethods',
    ];
  }
  protected getRecordIdentifier(record: any): string {
    return `[Route] ${record.path}`;
  }
}
