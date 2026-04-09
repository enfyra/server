import { Injectable } from '@nestjs/common';
import { BaseTableProcessor } from './base-table-processor';
import { QueryBuilderService } from '../../../infrastructure/query-builder/query-builder.service';
import { ObjectId } from 'mongodb';
@Injectable()
export class RouteHandlerDefinitionProcessor extends BaseTableProcessor {
  constructor(private readonly queryBuilder: QueryBuilderService) {
    super();
  }
  async transformRecords(records: any[], context?: any): Promise<any[]> {
    const isMongoDB = process.env.DB_TYPE === 'mongodb';
    const transformedRecords = await Promise.all(
      records.map(async (record) => {
        const transformedRecord = { ...record };
        if (isMongoDB) {
          const now = new Date();
          if (!transformedRecord.createdAt) transformedRecord.createdAt = now;
          if (!transformedRecord.updatedAt) transformedRecord.updatedAt = now;
        }
        if (record.route) {
          const routeEntity = await this.queryBuilder.findOneWhere(
            'route_definition',
            {
              path: record.route,
            },
          );
          if (!routeEntity) {
            this.logger.warn(
              `Route '${record.route}' not found for handler, skipping.`,
            );
            return null;
          }
          if (isMongoDB) {
            transformedRecord.route =
              typeof routeEntity._id === 'string'
                ? new ObjectId(routeEntity._id)
                : routeEntity._id;
          } else {
            transformedRecord.routeId = routeEntity.id;
            delete transformedRecord.route;
          }
        }
        if (record.method) {
          const methodEntity = await this.queryBuilder.findOneWhere(
            'method_definition',
            {
              method: record.method,
            },
          );
          if (!methodEntity) {
            this.logger.warn(
              `Method '${record.method}' not found for handler, skipping.`,
            );
            return null;
          }
          if (isMongoDB) {
            transformedRecord.method =
              typeof methodEntity._id === 'string'
                ? new ObjectId(methodEntity._id)
                : methodEntity._id;
          } else {
            transformedRecord.methodId = methodEntity.id;
            delete transformedRecord.method;
          }
        }
        return transformedRecord;
      }),
    );
    return transformedRecords.filter(Boolean);
  }
  getUniqueIdentifier(record: any): object {
    return {
      routeId: record.routeId,
      methodId: record.methodId,
    };
  }
  protected getCompareFields(): string[] {
    return ['logic', 'description'];
  }
  protected getRecordIdentifier(record: any): string {
    const routePath = record.route?.path || record._route || 'unknown';
    const methodName = record.method?.method || record._method || 'unknown';
    return `[Handler] ${routePath} (${methodName})`;
  }
}
