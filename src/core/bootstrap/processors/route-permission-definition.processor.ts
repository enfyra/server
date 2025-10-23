import { Injectable } from '@nestjs/common';
import { BaseTableProcessor } from './base-table-processor';
import { QueryBuilderService } from '../../../infrastructure/query-builder/query-builder.service';
import { ObjectId } from 'mongodb';

@Injectable()
export class RoutePermissionDefinitionProcessor extends BaseTableProcessor {
  constructor(private readonly queryBuilder: QueryBuilderService) {
    super();
  }

  async transformRecords(records: any[], context?: any): Promise<any[]> {
    const isMongoDB = process.env.DB_TYPE === 'mongodb';
    
    const transformedRecords = await Promise.all(
      records.map(async (record) => {
        const transformedRecord = { ...record };

        // Add default values
        if (transformedRecord.isEnabled === undefined) transformedRecord.isEnabled = true;

        // Add timestamps for MongoDB
        if (isMongoDB) {
          const now = new Date();
          if (!transformedRecord.createdAt) transformedRecord.createdAt = now;
          if (!transformedRecord.updatedAt) transformedRecord.updatedAt = now;

          // Initialize owner M2M relations
          if (!transformedRecord.allowedUsers) transformedRecord.allowedUsers = [];

          // NOTE: methods is inverse M2M - NOT stored
        }

        // Handle route reference
        if (record.route) {
          const routeEntity = await this.queryBuilder.findOneWhere('route_definition', {
            path: record.route,
          });
          
          if (!routeEntity) {
            this.logger.warn(
              `Route '${record.route}' not found for permission, skipping.`,
            );
            return null;
          }
          
          if (isMongoDB) {
            // MongoDB: Store route as ObjectId
            transformedRecord.route = typeof routeEntity._id === 'string' 
              ? new ObjectId(routeEntity._id) 
              : routeEntity._id;
          } else {
            // SQL: Convert to routeId
            transformedRecord.routeId = routeEntity.id;
            delete transformedRecord.route;
          }
        }
        
        // Handle role reference
        if (record.role) {
          const roleEntity = await this.queryBuilder.findOneWhere('role_definition', {
            name: record.role,
          });
          
          if (!roleEntity) {
            this.logger.warn(
              `Role '${record.role}' not found for permission, skipping.`,
            );
            return null;
          }
          
          if (isMongoDB) {
            // MongoDB: Store role as ObjectId
            transformedRecord.role = typeof roleEntity._id === 'string' 
              ? new ObjectId(roleEntity._id) 
              : roleEntity._id;
          } else {
            // SQL: Convert to roleId
            transformedRecord.roleId = roleEntity.id;
            delete transformedRecord.role;
          }
        }

        return transformedRecord;
      }),
    );

    return transformedRecords.filter(Boolean);
  }

  getUniqueIdentifier(record: any): object {
    return { route: record.route, role: record.role };
  }

  protected getCompareFields(): string[] {
    return ['isEnabled', 'isSystem'];
  }
}
