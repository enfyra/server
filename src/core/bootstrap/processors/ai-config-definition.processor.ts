import { Injectable } from '@nestjs/common';
import { BaseTableProcessor } from './base-table-processor';

@Injectable()
export class AiConfigDefinitionProcessor extends BaseTableProcessor {
  getUniqueIdentifier(record: any): object | object[] {
    return { provider: record.provider };
  }

  protected getCompareFields(): string[] {
    return ['model', 'isEnabled', 'maxConversationMessages', 'summaryThreshold', 'llmTimeout', 'description'];
  }
}

