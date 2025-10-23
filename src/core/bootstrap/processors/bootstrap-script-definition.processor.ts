import { Injectable } from '@nestjs/common';
import { BaseTableProcessor } from './base-table-processor';

@Injectable()
export class BootstrapScriptDefinitionProcessor extends BaseTableProcessor {
  async transformRecords(records: any[]): Promise<any[]> {
    const isMongoDB = process.env.DB_TYPE === 'mongodb';

    return records.map((record) => {
      const transformed = {
        ...record,
        isSystem: true, // Override: bootstrap scripts are always system
      };

      // Add default values
      if (transformed.priority === undefined) transformed.priority = 0;
      if (transformed.isEnabled === undefined) transformed.isEnabled = true;

      // Add timestamps for MongoDB
      if (isMongoDB) {
        const now = new Date();
        if (!transformed.createdAt) transformed.createdAt = now;
        if (!transformed.updatedAt) transformed.updatedAt = now;
      }

      return transformed;
    });
  }

  getUniqueIdentifier(record: any): object {
    return { name: record.name };
  }

  protected getCompareFields(): string[] {
    return ['name', 'description', 'logic', 'timeout', 'priority', 'isEnabled', 'environment', 'runOnce', 'runOnStartup', 'dependsOn', 'conditions', 'isSystem'];
  }
}
