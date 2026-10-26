import { BaseTableProcessor } from './base-table-processor';

export class FlowDefinitionProcessor extends BaseTableProcessor {
  override async transformRecords(records: any[]): Promise<any[]> {
    return records.map((record) => {
      const transformed = { ...record, isSystem: true };
      if (transformed.isEnabled === undefined) transformed.isEnabled = true;
      if (transformed.timeout === undefined) transformed.timeout = 30000;
      if (transformed.icon === undefined) transformed.icon = 'lucide:workflow';
      return transformed;
    });
  }

  getUniqueIdentifier(record: any): object {
    return { name: record.name };
  }

  protected override getCompareFields(): string[] {
    return [
      'name',
      'description',
      'icon',
      'timeout',
      'isEnabled',
    ];
  }

  protected override getRecordIdentifier(record: any): string {
    return `[Flow] ${record.name}`;
  }
}
