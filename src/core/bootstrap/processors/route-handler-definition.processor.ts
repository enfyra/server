import { Injectable } from '@nestjs/common';
import { BaseTableProcessor } from './base-table-processor';
import { KnexService } from '../../../infrastructure/knex/knex.service';

@Injectable()
export class RouteHandlerDefinitionProcessor extends BaseTableProcessor {
  constructor(private readonly knexService: KnexService) {
    super();
  }

  async transformRecords(records: any[], context?: any): Promise<any[]> {
    const knex = context?.knex || this.knexService.getKnex();
    
    const transformedRecords = await Promise.all(
      records.map(async (record) => {
        const transformedRecord = { ...record };
        
        // Handle route reference
        if (record.route) {
          const routeEntity = await knex('route_definition')
            .where('path', record.route)
            .first();
          
          if (!routeEntity) {
            this.logger.warn(
              `⚠️ Route '${record.route}' not found for handler, skipping.`,
            );
            return null;
          }
          
          transformedRecord.routeId = routeEntity.id;
          delete transformedRecord.route;
        }
        
        // Handle method reference
        if (record.method) {
          const methodEntity = await knex('method_definition')
            .where('method', record.method)
            .first();
          
          if (!methodEntity) {
            this.logger.warn(
              `⚠️ Method '${record.method}' not found for handler, skipping.`,
            );
            return null;
          }
          
          transformedRecord.methodId = methodEntity.id;
          delete transformedRecord.method;
        }
        
        return transformedRecord;
      }),
    );

    return transformedRecords.filter(Boolean);
  }

  getUniqueIdentifier(record: any): object {
    return { 
      routeId: record.routeId,
      methodId: record.methodId
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