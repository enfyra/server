import { BaseTableProcessor } from './base-table-processor';

export class FlowDefinitionProcessor extends BaseTableProcessor {
  async transformRecords(records: any[]): Promise<any[]> {
    return records.map((record) => {
      const transformed = { ...record, isSystem: true };
      if (transformed.isEnabled === undefined) transformed.isEnabled = true;
      if (transformed.timeout === undefined) transformed.timeout = 30000;
      if (transformed.icon === undefined) transformed.icon = 'lucide:workflow';
      if (
        transformed.triggerConfig &&
        typeof transformed.triggerConfig === 'object'
      ) {
        transformed.triggerConfig = JSON.stringify(transformed.triggerConfig);
      }
      return transformed;
    });
  }

  getUniqueIdentifier(record: any): object {
    return { name: record.name };
  }

  protected getCompareFields(): string[] {
    return [
      'name',
      'description',
      'icon',
      'triggerType',
      'triggerConfig',
      'timeout',
      'isEnabled',
    ];
  }

  protected getRecordIdentifier(record: any): string {
    return `[Flow] ${record.name}`;
  }
}
