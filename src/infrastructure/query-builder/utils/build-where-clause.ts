import { Knex } from 'knex';

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

function applyFieldCondition(
  query: Knex.QueryBuilder,
  field: string,
  operator: string,
  value: any,
  tablePrefix?: string,
  dbType?: string,
): void {
  const fullField = tablePrefix && !field.includes('.')
    ? `${tablePrefix}.${field}`
    : field;

  switch (operator) {
    case '_eq':
      query.where(fullField, '=', value);
      break;
    case '_neq':
      query.where(fullField, '!=', value);
      break;
    case '_gt':
      query.where(fullField, '>', value);
      break;
    case '_gte':
      query.where(fullField, '>=', value);
      break;
    case '_lt':
      query.where(fullField, '<', value);
      break;
    case '_lte':
      query.where(fullField, '<=', value);
      break;
    case '_in':
      if (Array.isArray(value)) {
        query.whereIn(fullField, value);
      }
      break;
    case '_not_in':
      if (Array.isArray(value)) {
        query.whereNotIn(fullField, value);
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
): void {
  if (!filter || typeof filter !== 'object') {
    return;
  }

  if (Array.isArray(filter)) {
    for (const item of filter) {
      if (logicalOperator === 'and') {
        query.where(function() {
          processFilter(this, item, tablePrefix, 'and', dbType);
        });
      } else {
        query.orWhere(function() {
          processFilter(this, item, tablePrefix, 'and', dbType);
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
              processFilter(this, condition, tablePrefix, 'and', dbType);
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
              processFilter(this, condition, tablePrefix, 'and', dbType);
            });
          }
        });
      }
      continue;
    }

    if (key === '_not') {
      query.whereNot(function() {
        processFilter(this, value, tablePrefix, 'and', dbType);
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
                applyFieldCondition(this, key, operator, operatorValue, tablePrefix, dbType);
              });
            } else {
              query.orWhere(function() {
                applyFieldCondition(this, key, operator, operatorValue, tablePrefix, dbType);
              });
            }
          }
        }
      } else {
        const fullField = tablePrefix && !key.includes('.')
          ? `${tablePrefix}.${key}`
          : key;

        if (logicalOperator === 'and') {
          query.where(fullField, '=', value);
        } else {
          query.orWhere(fullField, '=', value);
        }
      }
    } else {
      const fullField = tablePrefix && !key.includes('.')
        ? `${tablePrefix}.${key}`
        : key;

      if (logicalOperator === 'and') {
        query.where(fullField, '=', value);
      } else {
        query.orWhere(fullField, '=', value);
      }
    }
  }
}

export function buildWhereClause(
  query: Knex.QueryBuilder,
  filter: any,
  tablePrefix?: string,
  dbType?: string,
): Knex.QueryBuilder {
  if (!filter || typeof filter !== 'object') {
    return query;
  }

  processFilter(query, filter, tablePrefix, 'and', dbType);

  return query;
}

export function hasLogicalOperators(filter: any): boolean {
  if (!filter || typeof filter !== 'object') {
    return false;
  }

  if (Array.isArray(filter)) {
    return filter.some(item => hasLogicalOperators(item));
  }

  // Note: _contains, _starts_with, _ends_with, _between are NOT logical operators
  // They are field operators and should be handled in the simple conversion path

  for (const key of Object.keys(filter)) {
    if (LOGICAL_OPERATORS.includes(key)) {
      return true;
    }

    if (typeof filter[key] === 'object' && filter[key] !== null) {
      if (hasLogicalOperators(filter[key])) {
        return true;
      }
    }
  }

  return false;
}