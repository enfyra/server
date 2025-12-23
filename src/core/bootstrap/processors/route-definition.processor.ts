import { Injectable } from '@nestjs/common';
import { BaseTableProcessor } from './base-table-processor';
import { QueryBuilderService } from '../../../infrastructure/query-builder/query-builder.service';
import { ObjectId } from 'mongodb';
import { getForeignKeyColumnName } from '../../../infrastructure/knex/utils/naming-helpers';

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

        // Set default values for optional fields
        if (transformedRecord.description === undefined) transformedRecord.description = null;
        if (transformedRecord.icon === undefined) transformedRecord.icon = 'lucide:route';
        if (transformedRecord.isSystem === undefined) transformedRecord.isSystem = false;
        if (transformedRecord.isEnabled === undefined) transformedRecord.isEnabled = false;

        // Add timestamps for MongoDB
        if (isMongoDB) {
          const now = new Date();
          if (!transformedRecord.createdAt) transformedRecord.createdAt = now;
          if (!transformedRecord.updatedAt) transformedRecord.updatedAt = now;

          // Initialize owner M2M relations
          if (!transformedRecord.targetTables) transformedRecord.targetTables = [];

        }

        // Handle mainTable reference differently for SQL vs MongoDB
        if (record.mainTable) {
          if (isMongoDB) {
            // MongoDB: Store mainTable as ObjectId
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
            // SQL: Convert mainTable name to mainTableId (foreign key)
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

        // Handle publishedMethods - INVERSE M2M relation (from method_definition.routes)
        if (record.publishedMethods && Array.isArray(record.publishedMethods)) {
          if (isMongoDB) {
            // MongoDB: publishedMethods is INVERSE - NOT stored
            transformedRecord._publishedMethods = record.publishedMethods;
            delete transformedRecord.publishedMethods;
          } else {
            // SQL: Store for junction table processing
            transformedRecord._publishedMethods = record.publishedMethods;
            delete transformedRecord.publishedMethods;
          }
        }


        // Debug: log first route to see what we're trying to insert
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

    // MongoDB: No inverse relation updates
    // publishedMethods is INVERSE - computed from method.routes via $lookup

    // SQL: Handle publishedMethods using automatic cascade
    if (!isMongoDB && record._publishedMethods && Array.isArray(record._publishedMethods)) {
      const methodNames = record._publishedMethods;

      // Get method IDs
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
  }

  getUniqueIdentifier(record: any): object {
    return { path: record.path };
  }

  protected getCompareFields(): string[] {
    return ['path', 'isEnabled', 'icon', 'description', 'isSystem', 'mainTable', 'publishedMethods'];
  }

  protected getRecordIdentifier(record: any): string {
    return `[Route] ${record.path}`;
  }
}