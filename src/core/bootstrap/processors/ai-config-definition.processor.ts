import { Injectable } from '@nestjs/common';
import { BaseTableProcessor } from './base-table-processor';

@Injectable()
export class AiConfigDefinitionProcessor extends BaseTableProcessor {
  getUniqueIdentifier(record: any): object | object[] {
    // Use provider as unique identifier
    return { provider: record.provider };
  }

  protected getCompareFields(): string[] {
    return ['model', 'isEnabled', 'maxConversationMessages', 'summaryThreshold', 'discoveredMetadataTtl', 'discoverLockTtl', 'llmTimeout', 'description'];
  }
}

