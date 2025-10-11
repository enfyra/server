import { Injectable } from '@nestjs/common';
import { BaseTableProcessor } from './base-table-processor';
import { KnexService } from '../../../infrastructure/knex/knex.service';
import { getForeignKeyColumnName } from '../../../shared/utils/naming-helpers';

@Injectable()
export class RouteDefinitionProcessor extends BaseTableProcessor {
  constructor(private readonly knexService: KnexService) {
    super();
  }

  async transformRecords(records: any[], context?: any): Promise<any[]> {
    const knex = context?.knex || this.knexService.getKnex();
    
    const transformedRecords = await Promise.all(
      records.map(async (record) => {
        const transformedRecord = { ...record };
        
        // Handle mainTable reference
        if (record.mainTable) {
          const mainTable = await knex('table_definition')
            .where('name', record.mainTable)
            .first();
          
          if (!mainTable) {
            this.logger.warn(
              `⚠️ Table '${record.mainTable}' not found for route ${record.path}, skipping.`,
            );
            return null;
          }
          
          transformedRecord.mainTableId = mainTable.id;
          delete transformedRecord.mainTable;
        }
        
        // Store publishedMethods for later processing
        if (record.publishedMethods) {
          transformedRecord._publishedMethods = record.publishedMethods;
          delete transformedRecord.publishedMethods;
        }
        
        return transformedRecord;
      }),
    );

    return transformedRecords.filter(Boolean);
  }

  async afterUpsert(record: any, isNew: boolean, context?: any): Promise<void> {
    const knex = context?.knex || this.knexService.getKnex();

    // Handle publishedMethods junction table
    if (record._publishedMethods && Array.isArray(record._publishedMethods)) {
      const methodNames = record._publishedMethods;
      
      // Get method IDs
      const methods = await knex('method_definition')
        .whereIn('method', methodNames)
        .select('id', 'method');
      
      const methodIds = methods.map((m: any) => m.id);
      
      if (methodIds.length > 0) {
        // Get FK column names using naming convention
        const routeIdCol = getForeignKeyColumnName('route_definition');
        const methodIdCol = getForeignKeyColumnName('method_definition');
        
        // Clear existing junction records
        await knex('method_definition_routes_route_definition')
          .where(routeIdCol, record.id)
          .delete();
        
        // Insert new junction records
        const junctionData = methodIds.map((methodId) => ({
          [methodIdCol]: methodId,
          [routeIdCol]: record.id,
        }));
        
        await knex('method_definition_routes_route_definition').insert(junctionData);
        
        this.logger.log(
          `   🔗 Linked ${methodIds.length} published methods to route ${record.path}`,
        );
      }
    }
  }

  getUniqueIdentifier(record: any): object {
    return { path: record.path };
  }

  protected getCompareFields(): string[] {
    return ['path', 'isEnabled', 'icon', 'description'];
  }

  protected getRecordIdentifier(record: any): string {
    return `[Route] ${record.path}`;
  }
}