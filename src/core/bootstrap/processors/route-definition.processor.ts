import { Injectable } from '@nestjs/common';
import { BaseTableProcessor } from './base-table-processor';
import { QueryBuilderService } from '../../../infrastructure/query-builder/query-builder.service';
import { ObjectId } from 'mongodb';
import { getForeignKeyColumnName } from '../../../shared/utils/naming-helpers';

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
        
        // Handle mainTable reference differently for SQL vs MongoDB
        if (record.mainTable) {
          if (isMongoDB) {
            // MongoDB: Store mainTable as ObjectId
            const mainTable = await this.queryBuilder.findOneWhere('table_definition', {
              name: record.mainTable,
            });
            
            if (!mainTable) {
              this.logger.warn(
                `⚠️ Table '${record.mainTable}' not found for route ${record.path}, skipping.`,
              );
              return null;
            }
            transformedRecord.mainTable = typeof mainTable._id === 'string' 
              ? new ObjectId(mainTable._id) 
              : mainTable._id;
          } else {
            // SQL: Convert mainTable name to mainTableId (foreign key)
            const mainTable = await this.queryBuilder.findOneWhere('table_definition', {
              name: record.mainTable,
            });
            
            if (!mainTable) {
              this.logger.warn(
                `⚠️ Table '${record.mainTable}' not found for route ${record.path}, skipping.`,
              );
              return null;
            }
            
            transformedRecord.mainTableId = mainTable.id;
            delete transformedRecord.mainTable;
          }
        }
        
        // Handle publishedMethods differently for SQL vs MongoDB
        if (record.publishedMethods && Array.isArray(record.publishedMethods)) {
          if (isMongoDB) {
            // MongoDB: Convert method names to method IDs (array of ObjectIds)
            const methods = await this.queryBuilder.select({
              table: 'method_definition',
              where: [{ field: 'method', operator: 'in', value: record.publishedMethods }],
              select: ['_id', 'method'],
            });
            
            transformedRecord.publishedMethods = methods.map((m: any) => 
              typeof m._id === 'string' ? new ObjectId(m._id) : m._id
            );
          } else {
            // SQL: Store for junction table processing
            transformedRecord._publishedMethods = record.publishedMethods;
            delete transformedRecord.publishedMethods;
          }
        }

        // MongoDB: Add inverse fields for relations
        if (isMongoDB) {
          // Initialize inverse fields as empty arrays
          transformedRecord.routePermissions = []; // From route_permission_definition.route
          transformedRecord.handlers = []; // From route_handler_definition.route  
          transformedRecord.hooks = []; // From hook_definition.route
        }
        
        return transformedRecord;
      }),
    );

    return transformedRecords.filter(Boolean);
  }

  async afterUpsert(record: any, isNew: boolean, context?: any): Promise<void> {
    // Handle publishedMethods junction table
    if (record._publishedMethods && Array.isArray(record._publishedMethods)) {
      const methodNames = record._publishedMethods;
      
      // Get method IDs
      const methods = await this.queryBuilder.select({
        table: 'method_definition',
        where: [{ field: 'method', operator: 'in', value: methodNames }],
        select: ['id', 'method'],
      });
      
      const methodIds = methods.map((m: any) => m.id);
      
      if (methodIds.length > 0) {
        // Get FK column names using naming convention
        const routeIdCol = getForeignKeyColumnName('route_definition');
        const methodIdCol = getForeignKeyColumnName('method_definition');
        const junctionTable = 'method_definition_routes_route_definition';
        
        // Clear existing junction records
        await this.queryBuilder.delete({
          table: junctionTable,
          where: [{ field: routeIdCol, operator: '=', value: record.id }],
        });
        
        // Insert new junction records
        const junctionData = methodIds.map((methodId) => ({
          [methodIdCol]: methodId,
          [routeIdCol]: record.id,
        }));
        
        await this.queryBuilder.insert({
          table: junctionTable,
          data: junctionData,
        });
        
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