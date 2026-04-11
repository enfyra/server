import { Injectable } from '@nestjs/common';
import { BaseTableProcessor } from './base-table-processor';
import { DatabaseConfigService } from '../../../shared/services/database-config.service';
@Injectable()
export class SettingDefinitionProcessor extends BaseTableProcessor {
  async transformRecords(records: any[]): Promise<any[]> {
    const isMongoDB = DatabaseConfigService.instanceIsMongoDb();
    return records.map((record) => {
      const transformed = { ...record };
      if (transformed.isInit === undefined) transformed.isInit = false;
      if (transformed.isSystem === undefined) transformed.isSystem = false;
      if (isMongoDB) {
        const now = new Date();
        if (!transformed.createdAt) transformed.createdAt = now;
        if (!transformed.updatedAt) transformed.updatedAt = now;
      }
      return transformed;
    });
  }
  getUniqueIdentifier(record: any): object {
    return {};
  }
  protected getCompareFields(): string[] {
    return ['isInit', 'projectName', 'projectDescription', 'projectUrl'];
  }
}
