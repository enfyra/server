import { BaseTableProcessor } from './base-table-processor';

export class FlowExecutionDefinitionProcessor extends BaseTableProcessor {
  override async transformRecords(records: any[]): Promise<any[]> {
    return records.map((record) => ({ ...record }));
  }

  getUniqueIdentifier(record: any): object {
    return { id: record.id };
  }

  protected override getCompareFields(): string[] {
    return ['status'];
  }

  protected override getRecordIdentifier(record: any): string {
    return `[FlowExecution] ${record.id}`;
  }
}
