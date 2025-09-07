import { Injectable } from '@nestjs/common';
import { BaseTableProcessor } from './base-table-processor';

@Injectable()
export class ExtensionDefinitionProcessor extends BaseTableProcessor {
  getUniqueIdentifier(record: any): object[] {
    // Extension can be identified by multiple strategies
    const identifiers = [];
    
    // Primary: by extensionId if provided
    if (record.extensionId) {
      identifiers.push({ extensionId: record.extensionId });
    }
    
    // Secondary: by name
    if (record.name) {
      identifiers.push({ name: record.name });
    }
    
    // Tertiary: by menu relation if it's a one-to-one
    if (record.menu) {
      identifiers.push({ menu: record.menu });
    }
    
    return identifiers.length > 0 ? identifiers : [{ id: record.id }];
  }

  protected getCompareFields(): string[] {
    return ['name', 'type', 'version', 'isEnabled', 'description', 'code', 'compiledCode'];
  }
}