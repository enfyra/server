import { Injectable } from '@nestjs/common';
import { BaseTableProcessor } from './base-table-processor';

@Injectable()
export class BootstrapScriptDefinitionProcessor extends BaseTableProcessor {
  async transformRecords(records: any[]): Promise<any[]> {
    return records.map((record) => ({
      ...record,
      isSystem: true,
    }));
  }

  getUniqueIdentifier(record: any): object {
    return { name: record.name };
  }

  protected getCompareFields(): string[] {
    return ['name', 'description', 'logic', 'timeout', 'priority', 'isEnabled', 'environment', 'runOnce', 'runOnStartup', 'dependsOn', 'conditions', 'isSystem'];
  }
}
