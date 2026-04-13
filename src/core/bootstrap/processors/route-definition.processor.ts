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
    const isMongoDB = DatabaseConfigService.instanceIsMongoDb();
    await this.ensureDefaultCrudHandlers(record, isMongoDB);
  }

  async ensureMissingHandlers(): Promise<void> {
    const isMongoDB = DatabaseConfigService.instanceIsMongoDb();

    this.logger.log('='.repeat(80));
    this.logger.log('[ensureMissingHandlers] Starting handler check...');
    this.logger.log('='.repeat(80));

    const { data: routes } = await this.queryBuilder.find({
      table: 'route_definition',
      filter: { isEnabled: { _eq: true } },
    });

    if (!routes || routes.length === 0) return;

    this.logger.log(`[ensureMissingHandlers] Found ${routes.length} enabled routes`);

    for (const route of routes) {
      const routeId = isMongoDB ? route._id : route.id;

      const filter = isMongoDB
        ? { route: { _eq: routeId } }
        : { routeId: { _eq: routeId } };

      const handlerCount = await this.queryBuilder.countRecords(
        'route_handler_definition',
        filter
      );

      this.logger.log(`[${route.path}] Has ${handlerCount} existing handlers`);

      if (handlerCount > 0) continue;

      this.logger.log(`[${route.path}] No handlers found, creating default handlers...`);
      await this.ensureDefaultCrudHandlers(route, isMongoDB);
    }
  }

  private async ensureDefaultCrudHandlers(
    record: any,
    isMongoDB: boolean,
  ): Promise<void> {
    const path = record.path;
    let mainTableFk = isMongoDB ? record.mainTable : record.mainTableId;
    if (!mainTableFk) return;


    let tableRow: any;
    if (isMongoDB) {
      if (typeof mainTableFk === 'string') {
        const { ObjectId } = require('mongodb');
        mainTableFk = new ObjectId(mainTableFk);
      }
      const mongoService = this.queryBuilder.getMongoDb();
      const collection = mongoService.collection('table_definition');
      tableRow = await collection.findOne({ _id: mainTableFk });
    } else {
      tableRow = await this.queryBuilder.findOne({
        table: 'table_definition',
        where: { id: mainTableFk },
      });
    }
    const tableName = tableRow?.name;
    if (!tableName || !isCanonicalTableRoutePath(path, tableName)) {
      this.logger.log(`[${path}] Skipping - tableName=${tableName}, isCanonical=${isCanonicalTableRoutePath(path, tableName)}`);
      return;
    }


    let availableIds = record.availableMethods;

    const { ObjectId } = require('mongodb');
    const junctionName = 'route_definition_availableMethods_method_definition';

    if (isMongoDB && (!Array.isArray(availableIds) || availableIds.length === 0)) {
      const routeId = record._id;
      if (!routeId) {
        this.logger.warn(`[${path}] No _id found, skipping handler creation`);
        return;
      }

      const routeIdObjectId = typeof routeId === 'string' ? new ObjectId(routeId) : routeId;
      const mongoService = this.queryBuilder.getMongoDb();
      const junctionData = await mongoService.collection(junctionName)
        .find({ route_definitionId: routeIdObjectId })
        .toArray();

      availableIds = junctionData.map((row: any) => row.method_definitionId);
    }

    if (!Array.isArray(availableIds) || availableIds.length === 0) {
      this.logger.warn(`[${path}] No available methods found, skipping handler creation`);
      return;
    }

    // IMPORTANT: availableIds are ObjectId objects, need to convert to strings for queryBuilder
    const availableIdStrings = availableIds.map((id: any) => id.toString());

    const pkField = isMongoDB ? '_id' : 'id';
    const methodResult = await this.queryBuilder.find({
      table: 'method_definition',
      filter: { id: { _in: availableIdStrings } },
      fields: ['method'],
    });
    const methods = methodResult.data || [];
    const available = methods.map((m: any) => m.method);


    if (available.length === 0) {
      this.logger.warn(`[${path}] No available methods found, skipping`);
      return;
    }

    const routeId = isMongoDB ? record._id : record.id;
    if (!routeId) {
      this.logger.warn(`[${path}] No routeId found, skipping`);
      return;
    }

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
