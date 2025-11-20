import { Injectable } from '@nestjs/common';
import { BaseTableProcessor } from './base-table-processor';

@Injectable()
export class GenericTableProcessor extends BaseTableProcessor {
  constructor(private readonly tableName: string) {
    super();
  }

  async transformRecords(records: any[]): Promise<any[]> {
    const isMongoDB = process.env.DB_TYPE === 'mongodb';

    return records.map((record) => {
      const transformed = { ...record };

      if (isMongoDB) {
        const now = new Date();
        if (!transformed.createdAt) transformed.createdAt = now;
        if (!transformed.updatedAt) transformed.updatedAt = now;
      }

      return transformed;
    });
  }

  getUniqueIdentifier(record: any): object | object[] {
    const identifiers: object[] = [];
    
    const criticalUniqueKeys: Record<string, string | string[]> = {
      'column_definition': ['table', 'name'],
      'relation_definition': ['table', 'propertyName'], 
      'route_permission_definition': ['route', 'role'],
      'route_handler_definition': ['route', 'method'],
      'ai_config_definition': ['provider'],
    };
    
    const knownKey = criticalUniqueKeys[this.tableName];
    if (knownKey) {
      if (Array.isArray(knownKey)) {
        const whereCondition: any = {};
        for (const key of knownKey) {
          if (record[key] !== undefined) {
            whereCondition[key] = record[key];
          }
        }
        if (Object.keys(whereCondition).length > 0) {
          identifiers.push(whereCondition);
        }
      } else {
        if (record[knownKey] !== undefined) {
          identifiers.push({ [knownKey]: record[knownKey] });
        }
      }
    }
    
    const commonUniqueFields = ['name', 'username', 'email', 'method', 'path', 'label', 'key'];
    for (const field of commonUniqueFields) {
      if (record[field] !== undefined) {
        identifiers.push({ [field]: record[field] });
      }
    }
    
    if (record.id !== undefined) {
      identifiers.push({ id: record.id });
    }
    
    if (record.name && record.type) {
      identifiers.push({ name: record.name, type: record.type });
    }
    
    if (identifiers.length === 0) {
      const firstKey = Object.keys(record).find(key => 
        record[key] !== null && 
        record[key] !== undefined && 
        key !== 'createdAt' && 
        key !== 'updatedAt' &&
        !Array.isArray(record[key])
      );
      if (firstKey) {
        identifiers.push({ [firstKey]: record[firstKey] });
      }
    }
    
    return identifiers.length > 1 ? identifiers : identifiers[0] || { id: record.id };
  }

  protected getCompareFields(): string[] {
    const fieldMap: Record<string, string[]> = {
      'role_definition': ['name', 'description'],
      'setting_definition': ['projectName', 'projectDescription', 'projectUrl'],
      'route_permission_definition': ['isEnabled'],
      'route_handler_definition': ['description', 'logic'],
      'extension_definition': ['name', 'type', 'version', 'isEnabled', 'description', 'code'],
      'folder_definition': ['name', 'order', 'icon', 'description'],
    };
    
    return fieldMap[this.tableName] || ['name', 'description'];
  }
}