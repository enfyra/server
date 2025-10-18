import { Knex } from 'knex';

/**
 * Build WHERE clause with support for _and, _or, _not logical operators
 * Converts Directus-style filters to Knex query builder calls
 *
 * Supports nested logical operators:
 * {
 *   _and: [
 *     { status: { _eq: 'published' } },
 *     { _or: [
 *       { title: { _contains: 'search' } },
 *       { content: { _contains: 'search' } }
 *     ]},
 *     { _not: { author: { _eq: 'blocked' } } }
 *   ]
 * }
 */

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

/**
 * Convert operator string to Knex operator
 */
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

/**
 * Apply a single field condition to query builder
 */
function applyFieldCondition(
  query: Knex.QueryBuilder,
  field: string,
  operator: string,
  value: any,
  tablePrefix?: string,
  dbType?: string, // 'mysql', 'postgres', 'sqlite'
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
        // SQLite: no unaccent, just use lower() for case-insensitive
        query.whereRaw(
          `lower(${fullField}) LIKE '%' || lower(?) || '%'`,
          [value]
        );
      } else {
        // MySQL
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
        // SQLite: no unaccent, just use lower() for case-insensitive
        query.whereRaw(
          `lower(${fullField}) LIKE lower(?) || '%'`,
          [value]
        );
      } else {
        // MySQL
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
        // SQLite: no unaccent, just use lower() for case-insensitive
        query.whereRaw(
          `lower(${fullField}) LIKE '%' || lower(?)`,
          [value]
        );
      } else {
        // MySQL
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
      // Unknown operator, try as direct comparison
      query.where(fullField, convertOperator(operator), value);
  }
}

/**
 * Process filter object and apply to query builder
 * Recursive function to handle nested _and, _or, _not
 */
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

  // Handle array of conditions (for _and, _or)
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

  // Process each key in filter object
  for (const [key, value] of Object.entries(filter)) {
    // Handle logical operators
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

    // Handle field conditions
    if (typeof value === 'object' && value !== null) {
      // Check if value contains field operators
      const hasFieldOperator = Object.keys(value).some(k => FIELD_OPERATORS.includes(k));

      if (hasFieldOperator) {
        // Process field operators
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
        // Nested object without operators - treat as direct equality
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
      // Direct value - treat as equality
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

/**
 * Build WHERE clause from Directus-style filter object
 * Main entry point
 *
 * @param query - Knex query builder instance
 * @param filter - Filter object with _and, _or, _not support
 * @param tablePrefix - Optional table name to prefix field names
 * @param dbType - Database type ('mysql', 'postgres', 'sqlite')
 * @returns Modified query builder
 */
export function buildWhereClause(
  query: Knex.QueryBuilder,
  filter: any,
  tablePrefix?: string,
  dbType?: string,
): Knex.QueryBuilder {
  if (!filter || typeof filter !== 'object') {
    return query;
  }

  // Start processing filter
  processFilter(query, filter, tablePrefix, 'and', dbType);

  return query;
}

/**
 * Check if filter contains logical operators OR advanced field operators
 * that need special handling (like _contains with unaccent)
 */
export function hasLogicalOperators(filter: any): boolean {
  if (!filter || typeof filter !== 'object') {
    return false;
  }

  if (Array.isArray(filter)) {
    return filter.some(item => hasLogicalOperators(item));
  }

  // Advanced field operators that need special handling
  const ADVANCED_FIELD_OPERATORS = ['_contains', '_starts_with', '_ends_with', '_between'];

  for (const key of Object.keys(filter)) {
    // Check for logical operators
    if (LOGICAL_OPERATORS.includes(key)) {
      return true;
    }

    // Check for advanced field operators
    if (typeof filter[key] === 'object' && filter[key] !== null) {
      for (const operator of Object.keys(filter[key])) {
        if (ADVANCED_FIELD_OPERATORS.includes(operator)) {
          return true;
        }
      }

      // Recursively check nested objects
      if (hasLogicalOperators(filter[key])) {
        return true;
      }
    }
  }

  return false;
}