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
        if (!transformedRecord.description) transformedRecord.description = null;
        if (!transformedRecord.icon) transformedRecord.icon = 'lucide:route';
        if (transformedRecord.isSystem === undefined) transformedRecord.isSystem = false;

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

        // Handle publishedMethods differently for SQL vs MongoDB
        if (record.publishedMethods && Array.isArray(record.publishedMethods)) {
          if (isMongoDB) {
            // MongoDB: Convert method names to method IDs (array of ObjectIds)
            const result = await this.queryBuilder.select({
              tableName: 'method_definition',
              filter: { method: { _in: record.publishedMethods } },
              fields: ['_id', 'method'],
            });
            const methods = result.data;

            transformedRecord.publishedMethods = methods.map((m: any) =>
              typeof m._id === 'string' ? new ObjectId(m._id) : m._id
            );
          } else {
            // SQL: Store for junction table processing
            transformedRecord._publishedMethods = record.publishedMethods;
            delete transformedRecord.publishedMethods;
          }
        } else {
          // No publishedMethods provided - set empty array for MongoDB
          if (isMongoDB) {
            transformedRecord.publishedMethods = [];
          }
        }

        // MongoDB: Initialize inverse fields as empty arrays
        // These MUST be stored for performance - other processors will $addToSet to them
        if (isMongoDB) {
          if (!transformedRecord.hooks) transformedRecord.hooks = [];
          if (!transformedRecord.handlers) transformedRecord.handlers = [];
          if (!transformedRecord.routePermissions) transformedRecord.routePermissions = [];
          if (!transformedRecord.targetTables) transformedRecord.targetTables = [];
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

    // MongoDB: Add inverse reference to method.routes for performance
    if (isMongoDB && record.publishedMethods && Array.isArray(record.publishedMethods) && record.publishedMethods.length > 0) {
      const db = context?.db;
      if (!db) {
        this.logger.warn(`   ⚠️ No db in context, cannot update method.routes for route: ${record.path}`);
      } else {
        const routeId = typeof record._id === 'string' ? new ObjectId(record._id) : record._id;

        this.logger.log(`   🔗 Updating ${record.publishedMethods.length} methods with route ${routeId}`);

        // Add routeId to each method's routes array
        for (const methodId of record.publishedMethods) {
          const mId = typeof methodId === 'string' ? new ObjectId(methodId) : methodId;

          const updateResult = await db.collection('method_definition').updateOne(
            { _id: mId },
            { $addToSet: { routes: routeId } }
          );

          this.logger.log(`   🔗 Added route to method ${mId} routes array (matched: ${updateResult.matchedCount}, modified: ${updateResult.modifiedCount})`);
        }
      }
    }

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
        // Update the record with publishedMethods relation
        // This will automatically trigger cascade handling in KnexService hooks
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