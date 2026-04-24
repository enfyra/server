import { BaseTableProcessor } from './base-table-processor';
import { DatabaseConfigService } from '../../../shared/services/database-config.service';

export class FolderDefinitionProcessor extends BaseTableProcessor {
  async transformRecords(records: any[], _context?: any): Promise<any[]> {
    const isMongoDB = DatabaseConfigService.instanceIsMongoDb();
    const transformedRecords = [];
    const rootFolders = records.filter((r) => !r.parent);
    const childFolders = records.filter((r) => r.parent);
    for (const record of rootFolders) {
      const transformed = { ...record };
      if (transformed.order === undefined) transformed.order = 0;
      if (transformed.icon === undefined) transformed.icon = 'lucide:folder';
      if (transformed.isSystem === undefined) transformed.isSystem = false;
      if (isMongoDB) {
        const now = new Date();
        if (!transformed.createdAt) transformed.createdAt = now;
        if (!transformed.updatedAt) transformed.updatedAt = now;
      }
      transformedRecords.push(transformed);
    }
    for (const record of childFolders) {
      const transformed = { ...record };
      if (transformed.order === undefined) transformed.order = 0;
      if (transformed.icon === undefined) transformed.icon = 'lucide:folder';
      if (transformed.isSystem === undefined) transformed.isSystem = false;
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
    if (record.path) {
      identifiers.push({ path: record.path });
    }
    if (record.slug && record.parent !== undefined) {
      identifiers.push({ slug: record.slug, parent: record.parent });
    } else if (record.slug) {
      identifiers.push({ slug: record.slug, parent: null });
    }
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
