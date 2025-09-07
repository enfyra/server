import { Injectable } from '@nestjs/common';
import { Repository } from 'typeorm';
import { BaseTableProcessor } from './base-table-processor';

@Injectable()
export class FolderDefinitionProcessor extends BaseTableProcessor {
  async transformRecords(records: any[], context?: any): Promise<any[]> {
    const transformedRecords = [];
    
    // Process parent folders first (those without parent field)
    const rootFolders = records.filter(r => !r.parent);
    const childFolders = records.filter(r => r.parent);
    
    // Add root folders first
    transformedRecords.push(...rootFolders);
    
    // Then add child folders
    transformedRecords.push(...childFolders);
    
    return transformedRecords;
  }
  
  getUniqueIdentifier(record: any): object[] {
    const identifiers = [];
    
    // Primary: by path (unique)
    if (record.path) {
      identifiers.push({ path: record.path });
    }
    
    // Secondary: by slug and parent combination (unique)
    if (record.slug && record.parent !== undefined) {
      identifiers.push({ slug: record.slug, parent: record.parent });
    } else if (record.slug) {
      // For root folders (no parent)
      identifiers.push({ slug: record.slug, parent: null });
    }
    
    // Tertiary: by name and parent (for finding similar)
    if (record.name && record.parent !== undefined) {
      identifiers.push({ name: record.name, parent: record.parent });
    } else if (record.name) {
      identifiers.push({ name: record.name, parent: null });
    }
    
    return identifiers.length > 0 ? identifiers : [{ id: record.id }];
  }

  protected getCompareFields(): string[] {
    return ['name', 'slug', 'path', 'order', 'icon', 'description', 'isSystem'];
  }
}