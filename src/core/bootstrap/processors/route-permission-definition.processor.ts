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
        if (transformedRecord.isEnabled === undefined)
          transformedRecord.isEnabled = true;
        if (isMongoDB) {
          const now = new Date();
          if (!transformedRecord.createdAt) transformedRecord.createdAt = now;
          if (!transformedRecord.updatedAt) transformedRecord.updatedAt = now;
          if (!transformedRecord.allowedUsers)
            transformedRecord.allowedUsers = [];
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
              `Route '${record.route}' not found for permission, skipping.`,
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
        if (record.role) {
          const roleEntity = await this.queryBuilder.findOneWhere(
            'role_definition',
            {
              name: record.role,
            },
          );
          if (!roleEntity) {
            this.logger.warn(
              `Role '${record.role}' not found for permission, skipping.`,
            );
            return null;
          }
          if (isMongoDB) {
            transformedRecord.role =
              typeof roleEntity._id === 'string'
                ? new ObjectId(roleEntity._id)
                : roleEntity._id;
          } else {
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
