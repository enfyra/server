import { BaseTableProcessor } from './base-table-processor';
import { DatabaseConfigService } from '../../../shared/services';

export class GenericTableProcessor extends BaseTableProcessor {
  private readonly tableName: string;

  constructor(deps: { tableName: string }) {
    super();
    this.tableName = deps.tableName;
  }

  async transformRecords(records: any[]): Promise<any[]> {
    const isMongoDB = DatabaseConfigService.instanceIsMongoDb();

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
      enfyra_column: ['table', 'name'],
      enfyra_relation: ['table', 'propertyName'],
      enfyra_route_permission: ['route', 'role'],
      enfyra_route_handler: ['route', 'method'],
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

    const commonUniqueFields = [
      'name',
      'username',
      'email',
      'method',
      'path',
      'label',
      'key',
    ];
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
      const firstKey = Object.keys(record).find(
        (key) =>
          record[key] !== null &&
          record[key] !== undefined &&
          key !== 'createdAt' &&
          key !== 'updatedAt' &&
          !Array.isArray(record[key]),
      );
      if (firstKey) {
        identifiers.push({ [firstKey]: record[firstKey] });
      }
    }

    return identifiers.length > 1
      ? identifiers
      : identifiers[0] || { id: record.id };
  }

  protected getCompareFields(): string[] {
    const fieldMap: Record<string, string[]> = {
      enfyra_role: ['name', 'description'],
      enfyra_setting: ['projectName', 'projectDescription', 'projectUrl'],
      enfyra_route_permission: ['isEnabled'],
      enfyra_route_handler: [
        'description',
        'sourceCode',
        'scriptLanguage',
        'compiledCode',
      ],
      enfyra_extension: [
        'name',
        'type',
        'version',
        'isEnabled',
        'description',
        'code',
      ],
      enfyra_folder: ['name', 'order', 'icon', 'description'],
    };

    return fieldMap[this.tableName] || ['name', 'description'];
  }
}
