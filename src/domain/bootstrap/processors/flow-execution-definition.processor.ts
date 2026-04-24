import { BaseTableProcessor } from './base-table-processor';

export class FlowExecutionDefinitionProcessor extends BaseTableProcessor {
  async transformRecords(records: any[]): Promise<any[]> {
    return records.map((record) => ({ ...record }));
  }

  getUniqueIdentifier(record: any): object {
    return { id: record.id };
  }

  protected getCompareFields(): string[] {
    return ['status'];
  }

  protected getRecordIdentifier(record: any): string {
    return `[FlowExecution] ${record.id}`;
  }
}
