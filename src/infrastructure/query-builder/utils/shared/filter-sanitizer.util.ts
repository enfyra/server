import { BadRequestException } from '@nestjs/common';
import {
  FIELD_OPERATORS,
  ALL_SUPPORTED_OPERATORS,
} from '../../planner/types/filter-ast';
import { DatabaseConfigService } from '../../../../shared/services/database-config.service';

export function throwUnsupportedFieldOperator(
  operator: string,
  fieldName: string,
  tableName?: string,
): never {
  throw new BadRequestException({
    message: `Unsupported filter operator "${operator}" on field "${fieldName}"${tableName ? ` (table "${tableName}")` : ''}. Supported operators: ${ALL_SUPPORTED_OPERATORS.join(', ')}.`,
    error: 'UnsupportedFilterOperator',
    operator,
    field: fieldName,
    supportedOperators: ALL_SUPPORTED_OPERATORS,
  });
}

export function assertFieldOperatorValueIsClean(
  fieldName: string,
  value: any,
  tableName?: string,
): void {
  if (!value || typeof value !== 'object' || Array.isArray(value)) return;
  const isMongoDB = DatabaseConfigService.instanceIsMongoDb();
  for (const k of Object.keys(value)) {
    if (k.startsWith('_') && !FIELD_OPERATORS.has(k)) {
      if (isMongoDB && k === '_id') continue;
      throwUnsupportedFieldOperator(k, fieldName, tableName);
    }
  }
}

const LOGICAL_KEYS = new Set(['_and', '_or', '_not']);

export function validateFilterShape(
  filter: any,
  tableName: string,
  metadata: any,
): void {
  if (!filter || typeof filter !== 'object') return;
  const tableMeta = metadata?.tables?.get(tableName);
  const columnNames = new Set<string>(
    (tableMeta?.columns ?? []).map((c: any) => c.name),
  );
  const relationMap = new Map<string, any>(
    (tableMeta?.relations ?? []).map((r: any) => [r.propertyName, r]),
  );

  for (const [key, value] of Object.entries(filter)) {
    if (LOGICAL_KEYS.has(key)) {
      if (Array.isArray(value)) {
        for (const item of value) validateFilterShape(item, tableName, metadata);
      } else if (value && typeof value === 'object') {
        validateFilterShape(value, tableName, metadata);
      }
      continue;
    }

    const relation = relationMap.get(key);
    if (relation) {
      if (value && typeof value === 'object' && !Array.isArray(value)) {
        assertFieldOperatorValueIsClean(key, value, tableName);
        const targetTable = relation.targetTableName || relation.targetTable;
        if (targetTable) {
          const nestedKeys = Object.keys(value).filter((k) => !k.startsWith('_'));
          if (nestedKeys.length > 0) {
            const nested: Record<string, any> = {};
            for (const k of nestedKeys) nested[k] = (value as any)[k];
            validateFilterShape(nested, targetTable, metadata);
          }
        }
      }
      continue;
    }

    if (key.startsWith('_') && !columnNames.has(key)) {
      if (key !== '_id') {
        throwUnsupportedFieldOperator(key, key, tableName);
      }
    }

    if (value && typeof value === 'object' && !Array.isArray(value)) {
      assertFieldOperatorValueIsClean(key, value, tableName);
    }
  }
}
