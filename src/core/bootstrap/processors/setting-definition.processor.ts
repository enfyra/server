import { Injectable } from '@nestjs/common';
import { BaseTableProcessor } from './base-table-processor';

@Injectable()
export class SettingDefinitionProcessor extends BaseTableProcessor {
  async transformRecords(records: any[]): Promise<any[]> {
    const isMongoDB = process.env.DB_TYPE === 'mongodb';

    return records.map((record) => {
      const transformed = { ...record };

      // Add default values
      if (transformed.isInit === undefined) transformed.isInit = false;
      if (transformed.isSystem === undefined) transformed.isSystem = false;

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
    // Setting table should have only one record - find first one
    return {};  // Empty where condition means findOne() will get first record
  }

  protected getCompareFields(): string[] {
    return ['isInit', 'projectName', 'projectDescription', 'projectUrl'];
  }
}