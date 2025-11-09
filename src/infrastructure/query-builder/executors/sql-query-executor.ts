import { Knex } from 'knex';
import { Logger } from '@nestjs/common';
import {
  DatabaseType,
  QueryOptions,
  WhereCondition,
} from '../../../shared/types/query-builder.types';
import { buildWhereClause, hasLogicalOperators } from '../utils/sql/build-where-clause';
import { separateFilters, applyRelationFilters } from '../utils/sql/relation-filter.util';

export class SqlQueryExecutor {
  private readonly logger = new Logger(SqlQueryExecutor.name);
  private debugLog: any[] = [];
  private metadata: any;

  constructor(
    private readonly knex: Knex,
    private readonly dbType: 'postgres' | 'mysql' | 'sqlite',
  ) {}

  async execute(options: {
    tableName: string;
    fields?: string | string[];
    filter?: any;
    sort?: string | string[];
    page?: number;
    limit?: number;
    meta?: string;
    deep?: Record<string, any>;
    debugLog?: any[];
    metadata?: any;
  }): Promise<any> {
    this.metadata = options.metadata;
    const debugLog = options.debugLog || [];
    this.debugLog = debugLog;

    const queryOptions: QueryOptions = {
      table: options.tableName,
    };

    if (options.fields) {
      if (Array.isArray(options.fields)) {
        queryOptions.fields = options.fields;
      } else if (typeof options.fields === 'string') {
        queryOptions.fields = options.fields.split(',').map(f => f.trim());
      }
    }

    const originalFilter = options.filter;

    if (options.filter && !hasLogicalOperators(options.filter)) {
      queryOptions.where = [];
      for (const [field, value] of Object.entries(options.filter)) {
        if (typeof value === 'object' && value !== null) {
          for (const [op, val] of Object.entries(value)) {
            let operator: string;
            if (op === '_eq') operator = '=';
            else if (op === '_neq') operator = '!=';
            else if (op === '_in') operator = 'in';
            else if (op === '_not_in') operator = 'not in';
            else if (op === '_gt') operator = '>';
            else if (op === '_gte') operator = '>=';
            else if (op === '_lt') operator = '<';
            else if (op === '_lte') operator = '<=';
            else if (op === '_contains') operator = '_contains';
            else if (op === '_starts_with') operator = '_starts_with';
            else if (op === '_ends_with') operator = '_ends_with';
            else if (op === '_between') operator = '_between';
            else if (op === '_is_null') operator = '_is_null';
            else if (op === '_is_not_null') operator = '_is_not_null';
            else operator = op.replace('_', ' ');

            queryOptions.where.push({ field, operator, value: val } as WhereCondition);
          }
        } else {
          queryOptions.where.push({ field, operator: '=', value } as WhereCondition);
        }
      }
    }

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

    if (options.page && options.limit) {
      const page = typeof options.page === 'string' ? parseInt(options.page, 10) : options.page;
      const limit = typeof options.limit === 'string' ? parseInt(options.limit, 10) : options.limit;
      queryOptions.offset = (page - 1) * limit;
      queryOptions.limit = limit;
    } else if (options.limit) {
      queryOptions.limit = typeof options.limit === 'string' ? parseInt(options.limit, 10) : options.limit;
    }

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

    if (queryOptions.fields && queryOptions.fields.length > 0) {
      const expandedSelects = await this.expandFieldsToSelect(queryOptions.table, queryOptions.fields, relationSorts);
      queryOptions.select = [...(queryOptions.select || []), ...expandedSelects];
    }

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

    queryOptions.sort = mainTableSorts;

    let query: any = this.knex(queryOptions.table);

    const metaParts = Array.isArray(options.meta)
      ? options.meta
      : (options.meta || '').split(',').map((x) => x.trim()).filter(Boolean);

    const needsFilterCount = metaParts.includes('filterCount') || metaParts.includes('*');

    if (queryOptions.select) {
      const selectItems = queryOptions.select.map(field => {
        if (typeof field === 'string' && field.trim().startsWith('(')) {
          return this.knex.raw(field);
        }
        return field;
      });
      query = query.select(selectItems);
    }

    if (needsFilterCount) {
      query.select(this.knex.raw('COUNT(*) OVER() as __filter_count__'));
    }

    const isSystemTable = ['table_definition', 'column_definition', 'relation_definition', 'method_definition'].includes(queryOptions.table);

    if (originalFilter && (hasLogicalOperators(originalFilter) || Object.keys(originalFilter).length > 0)) {
      if (!isSystemTable) {
        const metadata = this.metadata?.tables?.get(queryOptions.table);

        if (metadata && metadata.relations && metadata.relations.length > 0) {
          const { hasRelations } = separateFilters(originalFilter, metadata);

          if (hasRelations) {
            await applyRelationFilters(
              this.knex,
              query,
              originalFilter,
              queryOptions.table,
              metadata,
              this.dbType,
              (tableName: string) => this.metadata?.tables?.get(tableName),
            );
          } else {
            query = buildWhereClause(query, originalFilter, queryOptions.table, this.dbType, metadata);
          }
        } else {
          const metadata = this.metadata?.tables?.get(queryOptions.table);
          query = buildWhereClause(query, originalFilter, queryOptions.table, this.dbType, metadata);
        }
      } else {
        const metadata = this.metadata?.tables?.get(queryOptions.table);
        query = buildWhereClause(query, originalFilter, queryOptions.table, this.dbType, metadata);
      }
    } else if (queryOptions.where && queryOptions.where.length > 0) {
      query = this.applyWhereToKnex(query, queryOptions.where, queryOptions.table);
    }

    if (queryOptions.sort) {
      for (const sortOpt of queryOptions.sort) {
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

    if (queryOptions.limit !== undefined && queryOptions.limit !== null && queryOptions.limit > 0) {
      query = query.limit(queryOptions.limit);
    }

    let totalCount = 0;

    if (metaParts.includes('totalCount') || metaParts.includes('*')) {
      const totalQuery = this.knex(queryOptions.table);
      const totalResult = await totalQuery.count('* as count').first();
      totalCount = Number(totalResult?.count || 0);
    }

    if (this.debugLog && this.debugLog.length >= 0) {
      this.debugLog.push({
        type: 'SQL Query',
        table: queryOptions.table,
        sql: query.toSQL().toNative(),
      });
    }

    const results = await query;

    let filterCount = 0;

    if (needsFilterCount && results.length > 0) {
      filterCount = Number(results[0].__filter_count__ || 0);

      results.forEach((row: any) => {
        delete row.__filter_count__;
      });
    }

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
    };
  }

  private convertValueByType(tableName: string, field: string, value: any): any {
    if (value === null || value === undefined) {
      return value;
    }

    const tableMeta = this.metadata?.tables?.get(tableName);
    if (!tableMeta?.columns) {
      return value;
    }

    const column = tableMeta.columns.find(col => col.name === field);
    if (!column) {
      return value;
    }

    switch (column.type) {
      case 'int':
      case 'integer':
      case 'bigint':
      case 'smallint':
      case 'tinyint':
        return typeof value === 'string' ? parseInt(value, 10) : Number(value);

      case 'float':
      case 'double':
      case 'decimal':
      case 'numeric':
      case 'real':
        return typeof value === 'string' ? parseFloat(value) : Number(value);

      case 'boolean':
      case 'bool':
        if (typeof value === 'string') {
          return value === 'true' || value === '1';
        }
        return Boolean(value);

      case 'date':
      case 'datetime':
      case 'timestamp':
        if (typeof value === 'string') {
          return new Date(value);
        }
        return value;

      default:
        return value;
    }
  }

  private applyWhereToKnex(query: any, conditions: WhereCondition[], tableName?: string): any {
    for (const condition of conditions) {
      const fieldParts = condition.field.split('.');
      const tableForConversion = tableName || fieldParts[0];
      const columnName = fieldParts[fieldParts.length - 1];
      const convertedValue = this.convertValueByType(tableForConversion, columnName, condition.value);

      switch (condition.operator) {
        case '=':
          query = query.where(condition.field, '=', convertedValue);
          break;
        case '!=':
          query = query.where(condition.field, '!=', convertedValue);
          break;
        case '>':
          query = query.where(condition.field, '>', convertedValue);
          break;
        case '<':
          query = query.where(condition.field, '<', convertedValue);
          break;
        case '>=':
          query = query.where(condition.field, '>=', convertedValue);
          break;
        case '<=':
          query = query.where(condition.field, '<=', convertedValue);
          break;
        case 'like':
          query = query.where(condition.field, 'like', convertedValue);
          break;
        case 'in':
          const inValues = Array.isArray(condition.value)
            ? condition.value.map(v => this.convertValueByType(tableForConversion, columnName, v))
            : [convertedValue];
          query = query.whereIn(condition.field, inValues);
          break;
        case 'not in':
          const ninValues = Array.isArray(condition.value)
            ? condition.value.map(v => this.convertValueByType(tableForConversion, columnName, v))
            : [convertedValue];
          query = query.whereNotIn(condition.field, ninValues);
          break;
        case 'is null':
          query = query.whereNull(condition.field);
          break;
        case 'is not null':
          query = query.whereNotNull(condition.field);
          break;
        case '_contains':
          query = query.where(condition.field, 'like', `%${condition.value}%`);
          break;
        case '_starts_with':
          query = query.where(condition.field, 'like', `${condition.value}%`);
          break;
        case '_ends_with':
          query = query.where(condition.field, 'like', `%${condition.value}`);
          break;
        case '_between':
          let betweenValues = condition.value;
          if (typeof betweenValues === 'string') {
            betweenValues = betweenValues.split(',').map(v => v.trim());
          }
          if (Array.isArray(betweenValues) && betweenValues.length === 2) {
            const val0 = this.convertValueByType(tableForConversion, columnName, betweenValues[0]);
            const val1 = this.convertValueByType(tableForConversion, columnName, betweenValues[1]);
            query = query.whereBetween(condition.field, [val0, val1]);
          }
          break;
        case '_is_null':
          const isNullBool = convertedValue === true || convertedValue === 'true';
          query = isNullBool ? query.whereNull(condition.field) : query.whereNotNull(condition.field);
          break;
        case '_is_not_null':
          const isNotNullBool = convertedValue === true || convertedValue === 'true';
          query = isNotNullBool ? query.whereNotNull(condition.field) : query.whereNull(condition.field);
          break;
      }
    }
    return query;
  }

  private async expandFieldsToSelect(
    tableName: string,
    fields: string[],
    sortOptions: Array<{ field: string; direction: 'asc' | 'desc' }> = []
  ): Promise<string[]> {
    if (!this.metadata) {
      return fields;
    }

    const allMetadata = this.metadata;

    const metadataGetter = async (tName: string) => {
      try {
        const tableMeta = allMetadata.tables.get(tName);
        if (!tableMeta) {
          return null;
        }

        return {
          name: tableMeta.name,
          columns: tableMeta.columns || [],
          relations: tableMeta.relations || [],
        };
      } catch (error) {
        return null;
      }
    };

    try {
      const { expandFieldsToJoinsAndSelect } = await import('../utils/sql/expand-fields');
      const result = await expandFieldsToJoinsAndSelect(tableName, fields, metadataGetter, this.dbType, sortOptions);
      return result.select;
    } catch (error) {
      return fields;
    }
  }
}
