import { Injectable } from '@nestjs/common';
import { BaseTableProcessor } from './base-table-processor';
import { QueryBuilderService } from '../../../infrastructure/query-builder/query-builder.service';
import { DatabaseConfigService } from '../../../shared/services/database-config.service';

@Injectable()
export class RouteHandlerDefinitionProcessor extends BaseTableProcessor {
  constructor(private readonly queryBuilder: QueryBuilderService) {
    super();
  }

  async transformRecords(records: any[], context?: any): Promise<any[]> {
    const isMongoDB = DatabaseConfigService.instanceIsMongoDb();
    const transformedRecords = await Promise.all(
      records.map(async (record) => {
        const transformed = { ...record };
        if (isMongoDB) {
          const now = new Date();
          if (!transformed.createdAt) transformed.createdAt = now;
          if (!transformed.updatedAt) transformed.updatedAt = now;
        }

        const result = await this.autoTransformFkFields(
          transformed,
          'route_handler_definition',
          this.queryBuilder,
        );
        if (!result.route && !result.routeId) return null;
        if (!result.method && !result.methodId) return null;
        return result;
      }),
    );
    return transformedRecords.filter(Boolean);
  }

  getUniqueIdentifier(record: any): object {
    return this.autoGetUniqueIdentifier(record, 'route_handler_definition');
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
