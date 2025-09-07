import { Injectable } from '@nestjs/common';
import { BaseTableProcessor } from './base-table-processor';

@Injectable()
export class SettingDefinitionProcessor extends BaseTableProcessor {
  getUniqueIdentifier(record: any): object {
    // Setting table should have only one record - find first one
    return {};  // Empty where condition means findOne() will get first record
  }

  protected getCompareFields(): string[] {
    return ['isInit', 'projectName', 'projectDescription', 'projectUrl'];
  }
}