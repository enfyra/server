import { Injectable } from '@nestjs/common';
import { BaseTableProcessor } from './base-table-processor';
import { QueryBuilderService } from '../../../infrastructure/query-builder/query-builder.service';
import { ObjectId } from 'mongodb';
import {
  DEFAULT_REST_HANDLER_LOGIC,
  isCanonicalTableRoutePath,
  REST_HANDLER_METHOD_NAMES,
} from '../utils/canonical-table-route.util';
import { DatabaseConfigService } from '../../../shared/services/database-config.service';
@Injectable()
export class RouteDefinitionProcessor extends BaseTableProcessor {
  constructor(private readonly queryBuilder: QueryBuilderService) {
    super();
  }
  async transformRecords(records: any[], context?: any): Promise<any[]> {
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
            const mainTable = await this.queryBuilder.findOne({
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
            const mainTable = await this.queryBuilder.findOne({
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
          const result = await this.queryBuilder.find({
            table: 'method_definition',
            filter: { method: { _in: methodNames } },
            fields: [pkField, 'method'],
          });
          const methods = result.data || [];
          transformedRecord.publishedMethods = methods.map((m: any) => m[pkField]);
        }
        if (record.availableMethods && Array.isArray(record.availableMethods)) {
          const methodNames = record.availableMethods;
          const result = await this.queryBuilder.find({
            table: 'method_definition',
            filter: { method: { _in: methodNames } },
            fields: [pkField, 'method'],
          });
          const methods = result.data || [];
          transformedRecord.availableMethods = methods.map((m: any) => m[pkField]);
        }


        return transformedRecord;
      }),
    );
    return transformedRecords.filter(Boolean);
  }
  async afterUpsert(record: any, isNew: boolean, context?: any): Promise<void> {
    if (!isNew) return;
    const isMongoDB = DatabaseConfigService.instanceIsMongoDb();
    await this.ensureDefaultCrudHandlers(record, isMongoDB);
  }

  async ensureMissingHandlers(): Promise<void> {
    const isMongoDB = DatabaseConfigService.instanceIsMongoDb();

    this.logger.log('[ensureMissingHandlers] Starting handler check...');

    const { data: routes } = await this.queryBuilder.find({
      table: 'route_definition',
      filter: { isEnabled: { _eq: true } },
    });

    if (!routes || routes.length === 0) return;

    this.logger.log(`[ensureMissingHandlers] Found ${routes.length} enabled routes`);

    for (const route of routes) {
      const routeId = DatabaseConfigService.getRecordId(route);

      const filter = isMongoDB
        ? { route: { _eq: routeId } }
        : { routeId: { _eq: routeId } };

      const handlerCount = await this.queryBuilder.countRecords(
        'route_handler_definition',
        filter
      );

      if (handlerCount > 0) continue;

      await this.ensureDefaultCrudHandlers(route, isMongoDB);
    }
  }

  private async ensureDefaultCrudHandlers(
    record: any,
    isMongoDB: boolean,
  ): Promise<void> {
    const path = record.path;

    let tableName: string | undefined;
    const mainTableValue = record.mainTable;
    if (mainTableValue && typeof mainTableValue === 'object' && mainTableValue.name) {
      tableName = mainTableValue.name;
    } else {
      let mainTableFk = isMongoDB ? mainTableValue : record.mainTableId;
      if (!mainTableFk) return;
      if (isMongoDB && typeof mainTableFk === 'string') {
        const { ObjectId } = require('mongodb');
        mainTableFk = new ObjectId(mainTableFk);
      }
      const tableRow = isMongoDB
        ? await this.queryBuilder.getMongoDb()
            .collection('table_definition')
            .findOne({ _id: mainTableFk })
        : await this.queryBuilder.findOne({
            table: 'table_definition',
            where: { id: mainTableFk },
          });
      tableName = tableRow?.name;
    }
    if (!tableName || !isCanonicalTableRoutePath(path, tableName)) return;

    this.logger.log(`[${path}] Creating default CRUD handlers for table "${tableName}"...`);
    const routeId = DatabaseConfigService.getRecordId(record);
    if (!routeId) return;

    let methodIds: any[] = [];
    const raw = record.availableMethods;
    if (Array.isArray(raw) && raw.length > 0) {
      methodIds = typeof raw[0] === 'object' && raw[0] !== null
        ? raw.map((m: any) => m?.id ?? m?._id).filter(Boolean)
        : [...raw];
    } else {
      const junctionName = 'route_definition_availableMethods_method_definition';
      if (isMongoDB) {
        const mongoService = this.queryBuilder.getMongoDb();
        const routeIdObj = typeof routeId === 'string' ? new ObjectId(routeId) : routeId;
        const rows = await mongoService
          .collection(junctionName)
          .find({ route_definitionId: routeIdObj })
          .toArray();
        methodIds = rows.map((r: any) => r.method_definitionId);
      } else {
        const knex = this.queryBuilder.getKnex();
        const rows = await knex(junctionName)
          .select('method_definitionId')
          .where({ route_definitionId: routeId });
        methodIds = rows.map((r: any) => r.method_definitionId);
      }
    }

    if (methodIds.length === 0) return;

    const idStrings = methodIds.map((id: any) => id.toString());
    const methodResult = await this.queryBuilder.find({
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
        this.logger.warn(`[${path}] No default logic for method: ${methodName}`);
        continue;
      }

      const methodRow = await this.queryBuilder.findOne({
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

      let existing;
      if (isMongoDB) {
        const { ObjectId } = require('mongodb');
        const mongoService = this.queryBuilder.getMongoDb();
        const routeIdObj = typeof routeId === 'string' ? new ObjectId(routeId) : routeId;
        const methodIdObj = typeof methodKeyId === 'string' ? new ObjectId(methodKeyId) : methodKeyId;
        existing = await mongoService.collection('route_handler_definition').findOne({
          route: routeIdObj,
          method: methodIdObj,
        });
      } else {
        existing = await this.queryBuilder.findOne({
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
        const { ObjectId } = require('mongodb');
        data = {
          route: typeof routeId === 'string' ? new ObjectId(routeId) : routeId,
          method: typeof methodKeyId === 'string' ? new ObjectId(methodKeyId) : methodKeyId,
          logic,
          timeout: 30000
        };
      } else {
        data = { routeId, methodId: methodKeyId, logic, timeout: 30000 };
      }

      if (isMongoDB) {
        const mongoService = this.queryBuilder.getMongoDb();
        await mongoService.collection('route_handler_definition').insertOne(data);
      } else {
        await this.queryBuilder.insertWithOptions({
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
      'availableMethods',
    ];
  }
  protected getRecordIdentifier(record: any): string {
    return `[Route] ${record.path}`;
  }
}
