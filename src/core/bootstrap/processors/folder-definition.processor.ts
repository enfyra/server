import { Injectable } from '@nestjs/common';
import { BaseTableProcessor } from './base-table-processor';

@Injectable()
export class FolderDefinitionProcessor extends BaseTableProcessor {
  async transformRecords(records: any[], context?: any): Promise<any[]> {
    const isMongoDB = process.env.DB_TYPE === 'mongodb';
    const transformedRecords = [];

    // Process parent folders first (those without parent field)
    const rootFolders = records.filter(r => !r.parent);
    const childFolders = records.filter(r => r.parent);

    // Process root folders
    for (const record of rootFolders) {
      const transformed = { ...record };

      // Add default values
      if (transformed.order === undefined) transformed.order = 0;
      if (transformed.icon === undefined) transformed.icon = 'lucide:folder';
      if (transformed.isSystem === undefined) transformed.isSystem = false;

      // Add timestamps for MongoDB
      if (isMongoDB) {
        const now = new Date();
        if (!transformed.createdAt) transformed.createdAt = now;
        if (!transformed.updatedAt) transformed.updatedAt = now;
      }

      transformedRecords.push(transformed);
    }

    // Process child folders
    for (const record of childFolders) {
      const transformed = { ...record };

      // Add default values
      if (transformed.order === undefined) transformed.order = 0;
      if (transformed.icon === undefined) transformed.icon = 'lucide:folder';
      if (transformed.isSystem === undefined) transformed.isSystem = false;

      // Add timestamps for MongoDB
      if (isMongoDB) {
        const now = new Date();
        if (!transformed.createdAt) transformed.createdAt = now;
        if (!transformed.updatedAt) transformed.updatedAt = now;
      }

      transformedRecords.push(transformed);
    }

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