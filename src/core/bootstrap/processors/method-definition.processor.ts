import { Injectable } from '@nestjs/common';
import { BaseTableProcessor } from './base-table-processor';

@Injectable()
export class MethodDefinitionProcessor extends BaseTableProcessor {
  constructor() {
    super();
  }

  async transformRecords(records: any[]): Promise<any[]> {
    const isMongoDB = process.env.DB_TYPE === 'mongodb';

    return records.map((record) => {
      const transformed = {
        ...record,
        isSystem: true,
      };

      // MongoDB: Add inverse fields for relations and timestamps
      if (isMongoDB) {
        // Initialize inverse fields as empty arrays
        transformed.routes = []; // From route_definition.publishedMethods (many-to-many)
        transformed.hooks = []; // From hook_definition.methods (many-to-many)

        // Add timestamps
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