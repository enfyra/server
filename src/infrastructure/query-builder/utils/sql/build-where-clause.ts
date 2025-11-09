import { Knex } from 'knex';
import { hasLogicalOperators } from '../shared/logical-operators.util';
import { TableMetadata } from '../../../knex/types/knex-types';

const LOGICAL_OPERATORS = ['_and', '_or', '_not'];

const FIELD_OPERATORS = [
  '_eq',
  '_neq',
  '_gt',
  '_gte',
  '_lt',
  '_lte',
  '_in',
  '_not_in',
  '_contains',
  '_starts_with',
  '_ends_with',
  '_between',
  '_is_null',
  '_is_not_null',
];

function convertOperator(op: string): string {
  const operatorMap: Record<string, string> = {
    _eq: '=',
    _neq: '!=',
    _gt: '>',
    _gte: '>=',
    _lt: '<',
    _lte: '<=',
    _in: 'in',
    _not_in: 'not in',
  };
  return operatorMap[op] || op;
}

function isUUIDType(field: string, tablePrefix: string | undefined, metadata?: TableMetadata): boolean {
  if (!metadata) return false;
  let fieldName = field;
  if (field.includes('.')) {
    const parts = field.split('.');
    fieldName = parts[parts.length - 1];
  }
  const column = metadata.columns.find(c => c.name === fieldName);
  if (!column) return false;
  const type = column.type?.toLowerCase() || '';
  return type === 'uuid' || type === 'uuidv4' || type.includes('uuid');
}

function castValueForPostgres(field: string, value: any, tablePrefix: string | undefined, dbType: string | undefined, metadata?: TableMetadata): any {
  if (dbType !== 'postgres') return value;
  if (typeof value !== 'string') return value;
  
  const uuidPattern = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
  if (!uuidPattern.test(value)) return value;
  
  if (isUUIDType(field, tablePrefix, metadata)) {
    return value;
  }
  
  return value;
}

function applyFieldCondition(
  query: Knex.QueryBuilder,
  field: string,
  operator: string,
  value: any,
  tablePrefix?: string,
  dbType?: string,
  metadata?: TableMetadata,
): void {
  const fullField = tablePrefix && !field.includes('.')
    ? `${tablePrefix}.${field}`
    : field;

  const isUUID = isUUIDType(field, tablePrefix, metadata);
  const castedValue = castValueForPostgres(field, value, tablePrefix, dbType, metadata);

  switch (operator) {
    case '_eq':
      if (isUUID && dbType === 'postgres' && typeof value === 'string') {
        query.whereRaw(`${fullField} = ?::uuid`, [value]);
      } else {
        query.where(fullField, '=', castedValue);
      }
      break;
    case '_neq':
      if (isUUID && dbType === 'postgres' && typeof value === 'string') {
        query.whereRaw(`${fullField} != ?::uuid`, [value]);
      } else {
        query.where(fullField, '!=', castedValue);
      }
      break;
    case '_gt':
      if (isUUID && dbType === 'postgres' && typeof value === 'string') {
        query.whereRaw(`${fullField} > ?::uuid`, [value]);
      } else {
        query.where(fullField, '>', castedValue);
      }
      break;
    case '_gte':
      if (isUUID && dbType === 'postgres' && typeof value === 'string') {
        query.whereRaw(`${fullField} >= ?::uuid`, [value]);
      } else {
        query.where(fullField, '>=', castedValue);
      }
      break;
    case '_lt':
      if (isUUID && dbType === 'postgres' && typeof value === 'string') {
        query.whereRaw(`${fullField} < ?::uuid`, [value]);
      } else {
        query.where(fullField, '<', castedValue);
      }
      break;
    case '_lte':
      if (isUUID && dbType === 'postgres' && typeof value === 'string') {
        query.whereRaw(`${fullField} <= ?::uuid`, [value]);
      } else {
        query.where(fullField, '<=', castedValue);
      }
      break;
    case '_in':
      let inValues = value;
      if (!Array.isArray(inValues)) {
        inValues = typeof inValues === 'string' && inValues.includes(',')
          ? inValues.split(',').map(v => v.trim())
          : [inValues];
      }
      if (isUUID && dbType === 'postgres' && inValues.every((v: any) => typeof v === 'string')) {
        query.whereRaw(`${fullField} = ANY(?::uuid[])`, [inValues]);
      } else {
      query.whereIn(fullField, inValues);
      }
      break;
    case '_not_in':
      let notInValues = value;
      if (!Array.isArray(notInValues)) {
        notInValues = typeof notInValues === 'string' && notInValues.includes(',')
          ? notInValues.split(',').map(v => v.trim())
          : [notInValues];
      }
      if (isUUID && dbType === 'postgres' && notInValues.every((v: any) => typeof v === 'string')) {
        query.whereRaw(`${fullField} != ALL(?::uuid[])`, [notInValues]);
      } else {
      query.whereNotIn(fullField, notInValues);
      }
      break;
    case '_contains':
      if (dbType === 'postgres') {
        query.whereRaw(
          `lower(unaccent(${fullField})) ILIKE '%' || lower(unaccent(?)) || '%'`,
          [value]
        );
      } else if (dbType === 'sqlite') {
        query.whereRaw(
          `lower(${fullField}) LIKE '%' || lower(?) || '%'`,
          [value]
        );
      } else {
        query.whereRaw(
          `lower(unaccent(${fullField})) COLLATE utf8mb4_general_ci LIKE CONCAT('%', lower(unaccent(?)) COLLATE utf8mb4_general_ci, '%')`,
          [value]
        );
      }
      break;
    case '_starts_with':
      if (dbType === 'postgres') {
        query.whereRaw(
          `lower(unaccent(${fullField})) ILIKE lower(unaccent(?)) || '%'`,
          [value]
        );
      } else if (dbType === 'sqlite') {
        query.whereRaw(
          `lower(${fullField}) LIKE lower(?) || '%'`,
          [value]
        );
      } else {
        query.whereRaw(
          `lower(unaccent(${fullField})) COLLATE utf8mb4_general_ci LIKE CONCAT(lower(unaccent(?)) COLLATE utf8mb4_general_ci, '%')`,
          [value]
        );
      }
      break;
    case '_ends_with':
      if (dbType === 'postgres') {
        query.whereRaw(
          `lower(unaccent(${fullField})) ILIKE '%' || lower(unaccent(?))`,
          [value]
        );
      } else if (dbType === 'sqlite') {
        query.whereRaw(
          `lower(${fullField}) LIKE '%' || lower(?)`,
          [value]
        );
      } else {
        query.whereRaw(
          `lower(unaccent(${fullField})) COLLATE utf8mb4_general_ci LIKE CONCAT('%', lower(unaccent(?)) COLLATE utf8mb4_general_ci)`,
          [value]
        );
      }
      break;
    case '_between':
      if (Array.isArray(value) && value.length === 2) {
        query.whereBetween(fullField, [value[0], value[1]]);
      }
      break;
    case '_is_null':
      if (value === true) {
        query.whereNull(fullField);
      } else {
        query.whereNotNull(fullField);
      }
      break;
    case '_is_not_null':
      if (value === true) {
        query.whereNotNull(fullField);
      } else {
        query.whereNull(fullField);
      }
      break;
    default:
      query.where(fullField, convertOperator(operator), value);
  }
}

function processFilter(
  query: Knex.QueryBuilder,
  filter: any,
  tablePrefix?: string,
  logicalOperator: 'and' | 'or' = 'and',
  dbType?: string,
  metadata?: TableMetadata,
): void {
  if (!filter || typeof filter !== 'object') {
    return;
  }

  if (Array.isArray(filter)) {
    for (const item of filter) {
      if (logicalOperator === 'and') {
        query.where(function() {
          processFilter(this, item, tablePrefix, 'and', dbType, metadata);
        });
      } else {
        query.orWhere(function() {
          processFilter(this, item, tablePrefix, 'and', dbType, metadata);
        });
      }
    }
    return;
  }

  for (const [key, value] of Object.entries(filter)) {
    if (key === '_and') {
      if (Array.isArray(value)) {
        query.where(function() {
          for (const condition of value) {
            this.where(function() {
              processFilter(this, condition, tablePrefix, 'and', dbType, metadata);
            });
          }
        });
      }
      continue;
    }

    if (key === '_or') {
      if (Array.isArray(value)) {
        query.where(function() {
          for (const condition of value) {
            this.orWhere(function() {
              processFilter(this, condition, tablePrefix, 'and', dbType, metadata);
            });
          }
        });
      }
      continue;
    }

    if (key === '_not') {
      query.whereNot(function() {
        processFilter(this, value, tablePrefix, 'and', dbType, metadata);
      });
      continue;
    }

    if (typeof value === 'object' && value !== null) {
      const hasFieldOperator = Object.keys(value).some(k => FIELD_OPERATORS.includes(k));

      if (hasFieldOperator) {
        for (const [operator, operatorValue] of Object.entries(value)) {
          if (FIELD_OPERATORS.includes(operator)) {
            if (logicalOperator === 'and') {
              query.where(function() {
          applyFieldCondition(this, key, operator, operatorValue, tablePrefix, dbType, metadata);
              });
            } else {
              query.orWhere(function() {
          applyFieldCondition(this, key, operator, operatorValue, tablePrefix, dbType, metadata);
              });
            }
          }
        }
      } else {
        const fullField = tablePrefix && !key.includes('.')
          ? `${tablePrefix}.${key}`
          : key;

        const isUUID = isUUIDType(key, tablePrefix, metadata);
        if (isUUID && dbType === 'postgres' && typeof value === 'string') {
          const uuidPattern = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
          if (uuidPattern.test(value)) {
            if (logicalOperator === 'and') {
              query.whereRaw(`${fullField} = ?::uuid`, [value]);
            } else {
              query.orWhereRaw(`${fullField} = ?::uuid`, [value]);
            }
          } else {
        if (logicalOperator === 'and') {
          query.where(fullField, '=', value);
        } else {
          query.orWhere(fullField, '=', value);
            }
          }
        } else {
          if (logicalOperator === 'and') {
            query.where(fullField, '=', value);
          } else {
            query.orWhere(fullField, '=', value);
          }
        }
      }
    } else {
      const fullField = tablePrefix && !key.includes('.')
        ? `${tablePrefix}.${key}`
        : key;

      const isUUID = isUUIDType(key, tablePrefix, metadata);
      if (isUUID && dbType === 'postgres' && typeof value === 'string') {
        const uuidPattern = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
        if (uuidPattern.test(value)) {
          if (logicalOperator === 'and') {
            query.whereRaw(`${fullField} = ?::uuid`, [value]);
          } else {
            query.orWhereRaw(`${fullField} = ?::uuid`, [value]);
          }
        } else {
      if (logicalOperator === 'and') {
        query.where(fullField, '=', value);
      } else {
        query.orWhere(fullField, '=', value);
          }
        }
      } else {
        if (logicalOperator === 'and') {
          query.where(fullField, '=', value);
        } else {
          query.orWhere(fullField, '=', value);
        }
      }
    }
  }
}

export function buildWhereClause(
  query: Knex.QueryBuilder,
  filter: any,
  tablePrefix?: string,
  dbType?: string,
  metadata?: TableMetadata,
): Knex.QueryBuilder {
  if (!filter || typeof filter !== 'object') {
    return query;
  }

  processFilter(query, filter, tablePrefix, 'and', dbType, metadata);

  return query;
}

// Re-export for backwards compatibility
export { hasLogicalOperators } from '../shared/logical-operators.util';