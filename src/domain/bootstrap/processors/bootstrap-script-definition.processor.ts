import { BaseTableProcessor } from './base-table-processor';
import { DatabaseConfigService } from '../../../shared/services';
import { normalizeScriptRecord } from '@enfyra/kernel';

export class BootstrapScriptDefinitionProcessor extends BaseTableProcessor {
  async transformRecords(records: any[]): Promise<any[]> {
    const isMongoDB = DatabaseConfigService.instanceIsMongoDb();
    return records.map((record) => {
      const transformed = {
        ...record,
        isSystem: true,
      };
      if (transformed.priority === undefined) transformed.priority = 0;
      if (transformed.isEnabled === undefined) transformed.isEnabled = true;
      if (isMongoDB) {
        const now = new Date();
        if (!transformed.createdAt) transformed.createdAt = now;
        if (!transformed.updatedAt) transformed.updatedAt = now;
      }
      return normalizeScriptRecord('bootstrap_script_definition', transformed);
    });
  }
  getUniqueIdentifier(record: any): object {
    return { name: record.name };
  }
  protected getCompareFields(): string[] {
    return [
      'name',
      'description',
      'sourceCode',
      'scriptLanguage',
      'compiledCode',
      'timeout',
      'priority',
      'isEnabled',
      'environment',
      'runOnce',
      'runOnStartup',
      'dependsOn',
      'conditions',
      'isSystem',
    ];
  }
}
