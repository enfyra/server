import { Injectable } from '@nestjs/common';
import { BaseTableProcessor } from './base-table-processor';
import { DataSourceService } from '../../../core/database/data-source/data-source.service';

@Injectable()
export class RouteHandlerDefinitionProcessor extends BaseTableProcessor {
  constructor(private readonly dataSourceService: DataSourceService) {
    super();
  }

  async transformRecords(records: any[]): Promise<any[]> {
    const routeDefRepo = this.dataSourceService.getRepository('route_definition');
    const methodDefRepo = this.dataSourceService.getRepository('method_definition');
    
    const transformedRecords = await Promise.all(
      records.map(async (record) => {
        const transformedRecord = { ...record };
        
        // Handle route reference - convert string to Route_definition entity
        if (record.route) {
          const routeEntity = await routeDefRepo.findOne({
            where: { path: record.route },
          });
          
          if (!routeEntity) {
            this.logger.warn(
              `⚠️ Route '${record.route}' not found for handler, skipping.`,
            );
            return null;
          }
          
          transformedRecord.route = routeEntity;
        }
        
        // Handle method reference - convert string to Method_definition entity
        if (record.method) {
          const methodEntity = await methodDefRepo.findOne({
            where: { method: record.method },
          });
          
          if (!methodEntity) {
            this.logger.warn(
              `⚠️ Method '${record.method}' not found for handler, skipping.`,
            );
            return null;
          }
          
          transformedRecord.method = methodEntity;
        }
        
        return transformedRecord;
      }),
    );

    // Filter out null records (where route/method wasn't found)
    return transformedRecords.filter(Boolean);
  }

  getUniqueIdentifier(record: any): object {
    // Use entity IDs for unique identification (TypeORM compatible)
    return { 
      route: { id: record.route?.id },
      method: { id: record.method?.id }
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