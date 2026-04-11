import { Injectable } from '@nestjs/common';
import { BaseTableProcessor } from './base-table-processor';
import { DatabaseConfigService } from '../../../shared/services/database-config.service';
@Injectable()
export class MethodDefinitionProcessor extends BaseTableProcessor {
  constructor() {
    super();
  }
  async transformRecords(records: any[]): Promise<any[]> {
    const isMongoDB = DatabaseConfigService.instanceIsMongoDb();
    return records.map((record) => {
      const transformed = {
        ...record,
        isSystem: true,
      };
      if (isMongoDB) {
        if (!transformed.routes) transformed.routes = [];
        if (!transformed.routePermissions) transformed.routePermissions = [];
        const now = new Date();
        if (!transformed.createdAt) transformed.createdAt = now;
        if (!transformed.updatedAt) transformed.updatedAt = now;
      }
      return transformed;
    });
  }
  getUniqueIdentifier(record: any): object {
    return { method: record.method };
  }
  protected getCompareFields(): string[] {
    return ['method', 'isSystem'];
  }
}
