import { Knex } from 'knex';
import { QueryOptions, WhereCondition, DatabaseType } from '../../shared/types/query-builder.types';
import { expandFieldsToJoinsAndSelect } from './utils/expand-fields';
import { buildWhereClause, hasLogicalOperators } from './utils/build-where-clause';
import { separateFilters, applyRelationFilters } from './utils/relation-filter.util';

/**
 * SQL Query Executor - Executes queries with Directus/queryEngine-style parameters
 * This is the target method for SqlQueryEngine
 *
 * @param options - Query options in queryEngine format (tableName, fields, filter, sort, page, limit, meta, deep)
 * @param knex - Knex instance for SQL queries
 * @param expandFields - Function to expand fields into joins and selects
 * @param metadataGetter - Function to get table metadata
 * @param dbType - Database type (mysql, postgres)
 * @returns {data, meta?} - Results wrapped in data property with optional metadata
 */
export async function sqlExecutor(
  options: {
    tableName: string;
    fields?: string | string[];
    filter?: any;
    sort?: string | string[];
    page?: number;
    limit?: number;
    meta?: string;
    deep?: Record<string, any>;
    debugMode?: boolean;
  },
  knex: Knex,
  expandFields: (
    tableName: string,
    fields: string[],
    sortOptions?: Array<{ field: string; direction: 'asc' | 'desc' }>
  ) => Promise<{ joins: any[]; select: string[] }>,
  metadataGetter: (tableName: string) => Promise<any>,
  dbType: DatabaseType
): Promise<any> {
  const debugLog: any[] = [];

  const pushDebug = (key: string, data: any): void => {
    debugLog.push({ [key]: data });
  };

  // Convert queryEngine-style params to QueryOptions format
  const queryOptions: QueryOptions = {
    table: options.tableName,
  };

  // Convert fields
  if (options.fields) {
    if (Array.isArray(options.fields)) {
      queryOptions.fields = options.fields;
    } else if (typeof options.fields === 'string') {
      queryOptions.fields = options.fields.split(',').map(f => f.trim());
    }
  }

  // Store original filter for new buildWhereClause approach
  const originalFilter = options.filter;

  // Convert filter to where conditions (backward compatible with legacy approach)
  // If filter contains _and/_or/_not, we'll use buildWhereClause later
  if (options.filter && !hasLogicalOperators(options.filter)) {
    queryOptions.where = [];
    // Simple conversion for backward compatibility
    for (const [field, value] of Object.entries(options.filter)) {
      if (typeof value === 'object' && value !== null) {
        // Handle operators like {_eq: value}
        for (const [op, val] of Object.entries(value)) {
          // Convert operator: _eq -> =, _neq -> !=, _in -> in, _is_null -> is null, etc.
          let operator: string;
          if (op === '_eq') operator = '=';
          else if (op === '_neq') operator = '!=';
          else if (op === '_in') operator = 'in';
          else if (op === '_not_in') operator = 'not in';
          else if (op === '_gt') operator = '>';
          else if (op === '_gte') operator = '>=';
          else if (op === '_lt') operator = '<';
          else if (op === '_lte') operator = '<=';
          else if (op === '_contains') operator = 'like';
          else if (op === '_is_null') operator = 'is null';
          else operator = op.replace('_', ' ');

          queryOptions.where.push({ field, operator, value: val } as WhereCondition);
        }
      } else {
        // Direct equality
        queryOptions.where.push({ field, operator: '=', value } as WhereCondition);
      }
    }
  }

  // Convert sort
  if (options.sort) {
    const sortArray = Array.isArray(options.sort)
      ? options.sort
      : options.sort.split(',').map(s => s.trim());
    queryOptions.sort = sortArray.map(s => {
      const trimmed = s.trim();
      if (trimmed.startsWith('-')) {
        return { field: trimmed.substring(1), direction: 'desc' as const };
      }
      return { field: trimmed, direction: 'asc' as const };
    });
  }

  // Convert pagination
  if (options.page && options.limit) {
    queryOptions.offset = (options.page - 1) * options.limit;
    queryOptions.limit = options.limit;
  } else if (options.limit) {
    queryOptions.limit = options.limit;
  }

  // Separate main table sorts from relation sorts
  let mainTableSorts: Array<{ field: string; direction: 'asc' | 'desc' }> = [];
  let relationSorts: Array<{ field: string; direction: 'asc' | 'desc' }> = [];

  if (queryOptions.sort) {
    for (const sortOpt of queryOptions.sort) {
      if (sortOpt.field.includes('.')) {
        relationSorts.push(sortOpt);
      } else {
        mainTableSorts.push({
          ...sortOpt,
          field: `${queryOptions.table}.${sortOpt.field}`,
        });
      }
    }
  }

  // Auto-expand `fields` into `join` + `select` if provided
  if (queryOptions.fields && queryOptions.fields.length > 0) {
    const expanded = await expandFields(queryOptions.table, queryOptions.fields, relationSorts);
    queryOptions.join = [...(queryOptions.join || []), ...expanded.joins];
    queryOptions.select = [...(queryOptions.select || []), ...expanded.select];
  }

  // Auto-prefix table name to where conditions if not already qualified
  if (queryOptions.where) {
    queryOptions.where = queryOptions.where.map(condition => {
      if (!condition.field.includes('.')) {
        return {
          ...condition,
          field: `${queryOptions.table}.${condition.field}`,
        };
      }
      return condition;
    });
  }

  // Use only main table sorts for the query
  queryOptions.sort = mainTableSorts;

  // Execute SQL query using Knex
  let query: any = knex(queryOptions.table);

  // Parse meta requirements early
  const metaParts = Array.isArray(options.meta)
    ? options.meta
    : (options.meta || '').split(',').map((x) => x.trim()).filter(Boolean);

  const needsFilterCount = metaParts.includes('filterCount') || metaParts.includes('*');

  if (queryOptions.select) {
    // Convert subqueries to knex.raw to prevent double-escaping
    const selectItems = queryOptions.select.map(field => {
      // Detect if field contains subquery (starts with parenthesis)
      if (typeof field === 'string' && field.trim().startsWith('(')) {
        return knex.raw(field);
      }
      return field;
    });
    query = query.select(selectItems);
  }

  if (needsFilterCount) {
    query.select(knex.raw('COUNT(*) OVER() as __filter_count__'));
  }

  // Apply WHERE clause with relation filtering support
  // Skip relation filtering for system metadata tables to avoid infinite loop
  const isSystemTable = ['table_definition', 'column_definition', 'relation_definition', 'method_definition'].includes(queryOptions.table);

  // Helper to apply WHERE to knex
  const applyWhereToKnex = (query: any, conditions: WhereCondition[]): any => {
    for (const condition of conditions) {
      switch (condition.operator) {
        case '=':
          query = query.where(condition.field, '=', condition.value);
          break;
        case '!=':
          query = query.where(condition.field, '!=', condition.value);
          break;
        case '>':
          query = query.where(condition.field, '>', condition.value);
          break;
        case '<':
          query = query.where(condition.field, '<', condition.value);
          break;
        case '>=':
          query = query.where(condition.field, '>=', condition.value);
          break;
        case '<=':
          query = query.where(condition.field, '<=', condition.value);
          break;
        case 'like':
          query = query.where(condition.field, 'like', condition.value);
          break;
        case 'in':
          query = query.whereIn(condition.field, condition.value);
          break;
        case 'not in':
          query = query.whereNotIn(condition.field, condition.value);
          break;
        case 'is null':
          query = query.whereNull(condition.field);
          break;
        case 'is not null':
          query = query.whereNotNull(condition.field);
          break;
      }
    }
    return query;
  };

  if (originalFilter && (hasLogicalOperators(originalFilter) || Object.keys(originalFilter).length > 0)) {
    if (!isSystemTable) {
      // Try to get metadata for relation filtering
      const metadata = await metadataGetter(queryOptions.table);

      if (metadata && metadata.relations && metadata.relations.length > 0) {
        // This table has relations - check if filter uses any relations
        const { hasRelations } = separateFilters(originalFilter, metadata);

        if (hasRelations) {
          // Debug: log metadata and filter
          pushDebug('table_metadata', {
            tableName: queryOptions.table,
            relations: metadata.relations,
          });
          pushDebug('original_filter', originalFilter);

          // Use applyRelationFilters which handles both field and relation filters with logical operators
          await applyRelationFilters(
            knex,
            query,
            originalFilter,  // Pass the full filter
            queryOptions.table,
            metadata,
            dbType,
            metadataGetter,
          );
        } else {
          // No relation filters, use regular buildWhereClause
          query = buildWhereClause(query, originalFilter, queryOptions.table, dbType);
        }
      } else {
        // No metadata or no relations, use regular buildWhereClause
        query = buildWhereClause(query, originalFilter, queryOptions.table, dbType);
      }
    } else {
      // System tables: skip relation filtering to prevent infinite loop
      query = buildWhereClause(query, originalFilter, queryOptions.table, dbType);
    }
  } else if (queryOptions.where && queryOptions.where.length > 0) {
    // Use legacy applyWhereToKnex for simple filters (backward compatible)
    query = applyWhereToKnex(query, queryOptions.where);
  }

  if (queryOptions.join) {
    for (const joinOpt of queryOptions.join) {
      const joinMethod = `${joinOpt.type}Join` as 'innerJoin' | 'leftJoin' | 'rightJoin';
      query = query[joinMethod](joinOpt.table, joinOpt.on.local, joinOpt.on.foreign);
    }
  }

  if (queryOptions.sort) {
    for (const sortOpt of queryOptions.sort) {
      // Add table prefix if field doesn't contain dot (nested relation sort)
      const sortField = sortOpt.field.includes('.')
        ? sortOpt.field
        : `${queryOptions.table}.${sortOpt.field}`;
      query = query.orderBy(sortField, sortOpt.direction);
    }
  }

  if (queryOptions.groupBy) {
    query = query.groupBy(queryOptions.groupBy);
  }

  if (queryOptions.offset) {
    query = query.offset(queryOptions.offset);
  }

  // limit=0 means no limit (fetch all), undefined/null means use default
  if (queryOptions.limit !== undefined && queryOptions.limit !== null && queryOptions.limit > 0) {
    query = query.limit(queryOptions.limit);
  }

  // Add SQL to debug if debugMode is enabled
  if (options.debugMode) {
    pushDebug('sql', query.toString());
  }

  // Execute totalCount query separately if needed
  let totalCount = 0;

  if (metaParts.includes('totalCount') || metaParts.includes('*')) {
    // Total count (no filters)
    const totalQuery = knex(queryOptions.table);
    const totalResult = await totalQuery.count('* as count').first();
    totalCount = Number(totalResult?.count || 0);
  }

  // Execute main query (now includes __filter_count__ via window function if needed)
  const results = await query;

  let filterCount = 0;

  if (needsFilterCount && results.length > 0) {
    // Extract filterCount from first row (all rows have same value from window function)
    filterCount = Number(results[0].__filter_count__ || 0);

    // Clean up: Remove __filter_count__ column from all result rows
    results.forEach((row: any) => {
      delete row.__filter_count__;
    });
  }

  // Return in queryEngine format with optional meta and debug
  return {
    data: results,
    ...((metaParts.length > 0) && {
      meta: {
        ...(metaParts.includes('totalCount') || metaParts.includes('*')
          ? { totalCount }
          : {}),
        ...(metaParts.includes('filterCount') || metaParts.includes('*')
          ? { filterCount }
          : {}),
      },
    }),
    ...(options.debugMode ? { debug: debugLog } : {}),
  };
}
