import { Collection } from 'mongodb';
import { QueryOptions, WhereCondition } from '../../shared/types/query-builder.types';
import { hasLogicalOperators } from './utils/build-where-clause';

/**
 * MongoDB Query Executor - Handles MongoDB query execution
 * Converts queryEngine-style params to MongoDB queries
 *
 * Note: MongoDB already handles _and/_or/_not via walkFilter in MongoQueryEngine
 * This method is primarily for backward compatibility with simple queries
 *
 * @param options - Query options in queryEngine format
 * @param selectLegacy - Legacy select function from QueryBuilder
 * @returns {data, meta?} - Results wrapped in data property with optional metadata
 */
export async function mongoExecutor(
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
    pipeline?: any[]; // MongoDB aggregation pipeline (optional)
  },
  selectLegacy: (queryOptions: QueryOptions) => Promise<any[]>
): Promise<any> {
  // Convert to QueryOptions format for now
  const queryOptions: QueryOptions = {
    table: options.tableName,
  };

  // Pass through pipeline if provided
  if (options.pipeline) {
    queryOptions.pipeline = options.pipeline;
  }

  // Convert fields
  if (options.fields) {
    if (Array.isArray(options.fields)) {
      queryOptions.fields = options.fields;
    } else if (typeof options.fields === 'string') {
      queryOptions.fields = options.fields.split(',').map(f => f.trim());
    }
  }

  // Convert filter to where (only for simple filters without logical operators)
  // Complex filters with _and/_or/_not should use MongoQueryEngine directly
  if (options.filter && !hasLogicalOperators(options.filter)) {
    queryOptions.where = [];
    for (const [field, value] of Object.entries(options.filter)) {
      if (typeof value === 'object' && value !== null) {
        for (const [op, val] of Object.entries(value)) {
          // Convert operator: _eq -> =, _neq -> !=, _is_null -> is null, etc.
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

  // Use internal MongoDB execution logic
  const results = await selectLegacy(queryOptions);
  return { data: results };
}
