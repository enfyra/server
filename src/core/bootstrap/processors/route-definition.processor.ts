import { Injectable } from '@nestjs/common';
import { BaseTableProcessor } from './base-table-processor';
import { QueryBuilderService } from '../../../infrastructure/query-builder/query-builder.service';
import { ObjectId } from 'mongodb';
import {
  DEFAULT_REST_HANDLER_LOGIC,
  isCanonicalTableRoutePath,
  REST_HANDLER_METHOD_NAMES,
} from '../utils/canonical-table-route.util';
@Injectable()
export class RouteDefinitionProcessor extends BaseTableProcessor {
  constructor(private readonly queryBuilder: QueryBuilderService) {
    super();
  }
  async transformRecords(records: any[], context?: any): Promise<any[]> {
    const isMongoDB = process.env.DB_TYPE === 'mongodb';
    const transformedRecords = await Promise.all(
      records.map(async (record) => {
        const transformedRecord = { ...record };
        if (transformedRecord.description === undefined) transformedRecord.description = null;
        if (transformedRecord.icon === undefined) transformedRecord.icon = 'lucide:route';
        if (transformedRecord.isSystem === undefined) transformedRecord.isSystem = false;
        if (transformedRecord.isEnabled === undefined) transformedRecord.isEnabled = false;
        if (isMongoDB) {
          const now = new Date();
          if (!transformedRecord.createdAt) transformedRecord.createdAt = now;
          if (!transformedRecord.updatedAt) transformedRecord.updatedAt = now;
        }
        if (record.mainTable) {
          if (isMongoDB) {
            const mainTable = await this.queryBuilder.findOneWhere('table_definition', {
              name: record.mainTable,
            });
            if (!mainTable) {
              this.logger.warn(
                `Table '${record.mainTable}' not found for route ${record.path}, skipping.`,
              );
              return null;
            }
            transformedRecord.mainTable = typeof mainTable._id === 'string'
              ? new ObjectId(mainTable._id)
              : mainTable._id;
          } else {
            const mainTable = await this.queryBuilder.findOneWhere('table_definition', {
              name: record.mainTable,
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
          if (isMongoDB) {
            transformedRecord._publishedMethods = record.publishedMethods;
            delete transformedRecord.publishedMethods;
          } else {
            transformedRecord._publishedMethods = record.publishedMethods;
            delete transformedRecord.publishedMethods;
          }
        }
        if (record.availableMethods && Array.isArray(record.availableMethods)) {
          if (isMongoDB) {
            transformedRecord._availableMethods = record.availableMethods;
            delete transformedRecord.availableMethods;
          } else {
            transformedRecord._availableMethods = record.availableMethods;
            delete transformedRecord.availableMethods;
          }
        }
        if (isMongoDB && record.path === '/route_definition') {
          this.logger.log(`📋 Sample route document to insert: ${JSON.stringify(transformedRecord, null, 2)}`);
        }
        return transformedRecord;
      }),
    );
    return transformedRecords.filter(Boolean);
  }
  async afterUpsert(record: any, isNew: boolean, context?: any): Promise<void> {
    const isMongoDB = process.env.DB_TYPE === 'mongodb';
    if (!isMongoDB && record._publishedMethods && Array.isArray(record._publishedMethods)) {
      const methodNames = record._publishedMethods;
      const result = await this.queryBuilder.select({
        tableName: 'method_definition',
        filter: { method: { _in: methodNames } },
        fields: ['id', 'method'],
      });
      const methods = result.data;
      const methodIds = methods.map((m: any) => m.id);
      if (methodIds.length > 0) {
        await this.queryBuilder.updateById('route_definition', record.id, {
          publishedMethods: methodIds
        });
        this.logger.log(
          `   🔗 Linked ${methodIds.length} published methods to route ${record.path}`,
        );
      }
    }
    if (!isMongoDB && record._availableMethods && Array.isArray(record._availableMethods)) {
      const methodNames = record._availableMethods;
      const result = await this.queryBuilder.select({
        tableName: 'method_definition',
        filter: { method: { _in: methodNames } },
        fields: ['id', 'method'],
      });
      const methods = result.data;
      const methodIds = methods.map((m: any) => m.id);
      if (methodIds.length > 0) {
        await this.queryBuilder.updateById('route_definition', record.id, {
          availableMethods: methodIds
        });
        this.logger.log(
          `   🔗 Linked ${methodIds.length} available methods to route ${record.path}`,
        );
      }
    }
    await this.ensureDefaultCrudHandlers(record, isMongoDB);
  }

  private async ensureDefaultCrudHandlers(record: any, isMongoDB: boolean): Promise<void> {
    const path = record.path;
    const mainTableFk = isMongoDB ? record.mainTable : record.mainTableId;
    if (!mainTableFk) return;

    const tableRow = await this.queryBuilder.findById('table_definition', mainTableFk);
    const tableName = tableRow?.name;
    if (!tableName || !isCanonicalTableRoutePath(path, tableName)) return;

    const available = record._availableMethods;
    if (!Array.isArray(available) || available.length === 0) return;

    const routeId = isMongoDB ? record._id : record.id;
    if (!routeId) return;

    for (const methodName of REST_HANDLER_METHOD_NAMES) {
      if (!available.includes(methodName)) continue;
      const logic = DEFAULT_REST_HANDLER_LOGIC[methodName];
      if (!logic) continue;

      const methodRow = await this.queryBuilder.findOneWhere('method_definition', {
        method: methodName,
      });
      if (!methodRow) continue;

      const methodKeyId = isMongoDB ? methodRow._id ?? methodRow.id : methodRow.id;

      const existing = isMongoDB
        ? await this.queryBuilder.findOneWhere('route_handler_definition', {
            route: routeId,
            method: methodKeyId,
          })
        : await this.queryBuilder.findOneWhere('route_handler_definition', {
            routeId,
            methodId: methodKeyId,
          });

      if (existing) continue;

      const data = isMongoDB
        ? { route: routeId, method: methodKeyId, logic, timeout: 30000 }
        : { routeId, methodId: methodKeyId, logic, timeout: 30000 };

      await this.queryBuilder.insert({ table: 'route_handler_definition', data });
      this.logger.log(`   Default ${methodName} handler → ${path}`);
    }
  }
  getUniqueIdentifier(record: any): object {
    return { path: record.path };
  }
  protected getCompareFields(): string[] {
    return ['path', 'isEnabled', 'icon', 'description', 'isSystem', 'mainTable', 'publishedMethods', 'availableMethods'];
  }
  protected getRecordIdentifier(record: any): string {
    return `[Route] ${record.path}`;
  }
}