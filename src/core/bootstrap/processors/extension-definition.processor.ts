import { Injectable } from '@nestjs/common';
import { BaseTableProcessor } from './base-table-processor';
@Injectable()
export class ExtensionDefinitionProcessor extends BaseTableProcessor {
  async transformRecords(records: any[]): Promise<any[]> {
    const isMongoDB = process.env.DB_TYPE === 'mongodb';
    return records.map((record) => {
      const transformed = { ...record };
      if (transformed.type === undefined) transformed.type = 'page';
      if (transformed.version === undefined) transformed.version = '1.0.0';
      if (transformed.isEnabled === undefined) transformed.isEnabled = true;
      if (transformed.isSystem === undefined) transformed.isSystem = false;
      if (isMongoDB) {
        const now = new Date();
        if (!transformed.createdAt) transformed.createdAt = now;
        if (!transformed.updatedAt) transformed.updatedAt = now;
      }
      return transformed;
    });
  }
  getUniqueIdentifier(record: any): object[] {
    const identifiers = [];
    if (record.extensionId) {
      identifiers.push({ extensionId: record.extensionId });
    }
    if (record.name) {
      identifiers.push({ name: record.name });
    }
    if (record.menu) {
      identifiers.push({ menu: record.menu });
    }
    return identifiers.length > 0 ? identifiers : [{ id: record.id }];
  }
  protected getCompareFields(): string[] {
    return [
      'name',
      'type',
      'version',
      'isEnabled',
      'description',
      'code',
      'compiledCode',
    ];
  }
}
