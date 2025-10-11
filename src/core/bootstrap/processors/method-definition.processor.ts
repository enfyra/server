import { Injectable } from '@nestjs/common';
import { BaseTableProcessor } from './base-table-processor';

@Injectable()
export class MethodDefinitionProcessor extends BaseTableProcessor {
  constructor() {
    super();
  }

  async transformRecords(records: any[]): Promise<any[]> {
    return records.map((record) => ({
      ...record,
      isSystem: true,
    }));
  }

  getUniqueIdentifier(record: any): object {
    return { method: record.method };
  }

  protected getCompareFields(): string[] {
    return ['method', 'isSystem'];
  }
}