import { Knex } from 'knex';

/**
 * Apply filters to query
 */
export function applyFilters(
  query: Knex.QueryBuilder,
  filterParts: any[]
): Knex.QueryBuilder {
  let result = query;

  for (const part of filterParts) {
    if (part.operator === 'AND') {
      result = result.whereRaw(part.sql, part.params);
    } else {
      result = result.orWhereRaw(part.sql, part.params);
    }
  }

  return result;
}

/**
 * Apply sorting to query
 */
export function applySorting(
  query: Knex.QueryBuilder,
  sortArr: any[],
  parsedSort: any[]
): Knex.QueryBuilder {
  let result = query;

  for (const sortItem of sortArr) {
    const direction = parsedSort.find((parsed) => 
      parsed.field === sortItem.fullPath
    )?.direction ?? 'ASC';
    
    result = result.orderBy(`${sortItem.alias}.${sortItem.field}`, direction);
  }

  return result;
}

/**
 * Apply pagination to query
 */
export function applyPagination(
  query: Knex.QueryBuilder,
  page?: number,
  limit?: number
): Knex.QueryBuilder {
  let result = query;

  if (limit !== undefined && limit > 0) {
    result = result.limit(limit);
    
    if (page !== undefined && page > 1) {
      const offset = (page - 1) * limit;
      result = result.offset(offset);
    }
  }

  return result;
}


