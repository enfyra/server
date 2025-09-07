import { Injectable } from '@nestjs/common';
import { BaseTableProcessor } from './base-table-processor';
import { DataSourceService } from '../../../core/database/data-source/data-source.service';

@Injectable()
export class RouteDefinitionProcessor extends BaseTableProcessor {
  constructor(private readonly dataSourceService: DataSourceService) {
    super();
  }

  async transformRecords(records: any[]): Promise<any[]> {
    const tableDefRepo = this.dataSourceService.getRepository('table_definition');
    const methodRepo = this.dataSourceService.getRepository('method_definition');
    
    const transformedRecords = await Promise.all(
      records.map(async (record) => {
        const transformedRecord = { ...record };
        
        // Handle mainTable reference
        const mainTable = await tableDefRepo.findOne({
          where: { name: record.mainTable },
        });
        
        if (!mainTable) {
          this.logger.warn(
            `⚠️ Table '${record.mainTable}' not found for route ${record.path}, skipping.`,
          );
          return null;
        }
        
        transformedRecord.mainTable = mainTable;
        
        // Handle publishedMethods - convert string array to method entities
        if (record.publishedMethods && Array.isArray(record.publishedMethods)) {
          const methodEntities = await Promise.all(
            record.publishedMethods.map(async (methodName: string) => {
              const method = await methodRepo.findOne({ where: { method: methodName } });
              if (!method) {
                this.logger.warn(`⚠️ Method '${methodName}' not found for route ${record.path}`);
              }
              return method;
            })
          );
          
          // Filter out null values and assign
          transformedRecord.publishedMethods = methodEntities.filter(Boolean);
          
          if (transformedRecord.publishedMethods.length > 0) {
            this.logger.debug(`🔗 Route ${record.path} linked to methods: ${transformedRecord.publishedMethods.map((m: any) => m.method).join(', ')}`);
          }
        }
        
        return transformedRecord;
      }),
    );

    // Filter out null records (where mainTable wasn't found)
    return transformedRecords.filter(Boolean);
  }

  getUniqueIdentifier(record: any): object {
    return { path: record.path };
  }

  protected getCompareFields(): string[] {
    return ['path', 'isEnabled', 'icon', 'description'];
  }

  protected getRecordIdentifier(record: any): string {
    const methods = record.publishedMethods || record._publishedMethods;
    const methodsList = methods && Array.isArray(methods) 
      ? methods.map(m => typeof m === 'string' ? m : m.method).join(', ')
      : '';
    
    return `[Route] ${record.path}${methodsList ? ` (${methodsList})` : ''}`;
  }
}