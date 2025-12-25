import { Knex } from 'knex';
import { Logger } from '@nestjs/common';
import {
  DatabaseType,
  QueryOptions,
  WhereCondition,
} from '../../../shared/types/query-builder.types';
import { buildWhereClause, hasLogicalOperators } from '../utils/sql/build-where-clause';
import { separateFilters, applyRelationFilters } from '../utils/sql/relation-filter.util';
import { quoteIdentifier } from '../../knex/utils/migration/sql-dialect';
import { KnexService } from '../../knex/knex.service';

export class SqlQueryExecutor {
  private readonly logger = new Logger(SqlQueryExecutor.name);
  private debugLog: any[] = [];
  private metadata: any;

  constructor(
    private readonly knex: Knex,
    private readonly dbType: 'postgres' | 'mysql' | 'sqlite',
    private readonly knexService?: KnexService,
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

    let cteClauses: string[] | undefined = undefined;
    let useCTE = false;
    let whereClauseForCTE: string | undefined = undefined;

    if (queryOptions.fields && queryOptions.fields.length > 0) {
      const orderByClause = mainTableSorts.length > 0
        ? `ORDER BY ${mainTableSorts.map(s => {
            const field = s.field.includes('.') ? s.field : `${queryOptions.table}.${s.field}`;
            return `${quoteIdentifier(field.split('.')[0], this.dbType)}.${quoteIdentifier(field.split('.')[1] || field, this.dbType)}`;
          }).join(', ')} ${mainTableSorts[0].direction.toUpperCase()}`
        : undefined;

      if (queryOptions.limit && orderByClause && this.dbType === 'postgres') {
        const metadata = this.metadata?.tables?.get(queryOptions.table);
        if (originalFilter && (hasLogicalOperators(originalFilter) || Object.keys(originalFilter).length > 0)) {
          if (metadata) {
            const { hasRelations } = separateFilters(originalFilter, metadata);
            if (!hasRelations) {
              const buildWhereFromFilter = (filter: any, tablePrefix: string): string[] => {
                const parts: string[] = [];
                for (const [field, value] of Object.entries(filter)) {
                  if (field === '_and' && Array.isArray(value)) {
                    const andParts = value.map(f => {
                      const subParts = buildWhereFromFilter(f, tablePrefix);
                      return subParts.length > 0 ? `(${subParts.join(' AND ')})` : null;
                    }).filter(p => p !== null);
                    if (andParts.length > 0) {
                      parts.push(`(${andParts.join(' AND ')})`);
                    }
                  } else if (field === '_or' && Array.isArray(value)) {
                    const orParts = value.map(f => {
                      const subParts = buildWhereFromFilter(f, tablePrefix);
                      return subParts.length > 0 ? `(${subParts.join(' AND ')})` : null;
                    }).filter(p => p !== null);
                    if (orParts.length > 0) {
                      parts.push(`(${orParts.join(' OR ')})`);
                    }
                  } else if (field === '_not' && typeof value === 'object' && value !== null) {
                    const notParts = buildWhereFromFilter(value, tablePrefix);
                    if (notParts.length > 0) {
                      parts.push(`NOT (${notParts.join(' AND ')})`);
                    }
                  } else if (typeof value === 'object' && value !== null && !Array.isArray(value)) {
                    for (const [op, val] of Object.entries(value)) {
                      const quotedField = `${quoteIdentifier(tablePrefix, this.dbType)}.${quoteIdentifier(field, this.dbType)}`;
                      let sqlValue: string;
                      if (val === null) {
                        sqlValue = 'NULL';
                      } else if (typeof val === 'string') {
                        const uuidPattern = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
                        const column = metadata.columns.find(c => c.name === field);
                        const isUUID = column && (column.type?.toLowerCase() === 'uuid' || column.type?.toLowerCase().includes('uuid'));
                        if (isUUID && uuidPattern.test(val)) {
                          sqlValue = `'${val}'::uuid`;
                        } else {
                          sqlValue = `'${val.replace(/'/g, "''")}'`;
                        }
                      } else if (typeof val === 'boolean') {
                        sqlValue = val ? 'true' : 'false';
                      } else if (typeof val === 'number') {
                        sqlValue = String(val);
                      } else {
                        sqlValue = `'${String(val).replace(/'/g, "''")}'`;
                      }
                      if (op === '_eq') {
                        parts.push(`${quotedField} = ${sqlValue}`);
                      } else if (op === '_neq') {
                        parts.push(`${quotedField} != ${sqlValue}`);
                      } else if (op === '_gt') {
                        parts.push(`${quotedField} > ${sqlValue}`);
                      } else if (op === '_gte') {
                        parts.push(`${quotedField} >= ${sqlValue}`);
                      } else if (op === '_lt') {
                        parts.push(`${quotedField} < ${sqlValue}`);
                      } else if (op === '_lte') {
                        parts.push(`${quotedField} <= ${sqlValue}`);
                      } else if (op === '_is_null') {
                        parts.push(`${quotedField} IS NULL`);
                      } else if (op === '_is_not_null') {
                        parts.push(`${quotedField} IS NOT NULL`);
                      } else if (op === '_in') {
                        const inValues = Array.isArray(val) ? val : [val];
                        const inSql = inValues.map(v => {
                          if (typeof v === 'string') {
                            return `'${v.replace(/'/g, "''")}'`;
                          }
                          return String(v);
                        }).join(', ');
                        parts.push(`${quotedField} IN (${inSql})`);
                      } else if (op === '_not_in') {
                        const notInValues = Array.isArray(val) ? val : [val];
                        const notInSql = notInValues.map(v => {
                          if (typeof v === 'string') {
                            return `'${v.replace(/'/g, "''")}'`;
                          }
                          return String(v);
                        }).join(', ');
                        parts.push(`${quotedField} NOT IN (${notInSql})`);
                      }
                    }
                  } else {
                    const quotedField = `${quoteIdentifier(tablePrefix, this.dbType)}.${quoteIdentifier(field, this.dbType)}`;
                    let sqlValue: string;
                    if (value === null) {
                      sqlValue = 'NULL';
                    } else if (typeof value === 'string') {
                      sqlValue = `'${value.replace(/'/g, "''")}'`;
                    } else if (typeof value === 'boolean') {
                      sqlValue = value ? 'true' : 'false';
                    } else {
                      sqlValue = String(value);
                    }
                    parts.push(`${quotedField} = ${sqlValue}`);
                  }
                }
                return parts;
              };
              const whereParts = buildWhereFromFilter(originalFilter, queryOptions.table);
              if (whereParts.length > 0) {
                whereClauseForCTE = `WHERE ${whereParts.join(' AND ')}`;
              }
            }
          }
        } else if (queryOptions.where && queryOptions.where.length > 0) {
          const whereParts: string[] = [];
          for (const condition of queryOptions.where) {
            const field = condition.field.includes('.') 
              ? condition.field 
              : `${quoteIdentifier(queryOptions.table, this.dbType)}.${quoteIdentifier(condition.field, this.dbType)}`;
            let value: string;
            if (typeof condition.value === 'string') {
              value = `'${condition.value.replace(/'/g, "''")}'`;
            } else if (condition.value === null) {
              value = 'NULL';
            } else if (typeof condition.value === 'boolean') {
              value = condition.value ? 'true' : 'false';
            } else {
              value = String(condition.value);
            }
            whereParts.push(`${field} ${condition.operator} ${value}`);
          }
          if (whereParts.length > 0) {
            whereClauseForCTE = `WHERE ${whereParts.join(' AND ')}`;
          }
        }
      }

      const expandedResult = await this.expandFieldsToSelect(
        queryOptions.table,
        queryOptions.fields,
        relationSorts,
        queryOptions.limit,
        orderByClause,
        whereClauseForCTE,
        queryOptions.offset,
      );
      queryOptions.select = [...(queryOptions.select || []), ...expandedResult.select];
      cteClauses = expandedResult.cteClauses;
      useCTE = cteClauses !== undefined && cteClauses.length > 0 && this.dbType === 'postgres';
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
    let rawSQLQuery: string | null = null;

    const metaParts = Array.isArray(options.meta)
      ? options.meta
      : (options.meta || '').split(',').map((x) => x.trim()).filter(Boolean);

    const needsFilterCount = metaParts.includes('filterCount') || metaParts.includes('*');

    if (useCTE && cteClauses) {
      const limitedCTEName = `limited_${queryOptions.table}`;
      const tableAlias = 't';
      const quotedTable = quoteIdentifier(queryOptions.table, this.dbType);

      const aggregationCTENames = new Set<string>();
      
      cteClauses.forEach(cte => {
        const match = cte.match(/^(\w+)\s+AS\s*\(/i);
        if (match) {
          const cteName = match[1];
          if (cteName !== limitedCTEName) {
            aggregationCTENames.add(cteName);
          }
        }
      });

      const selectItems = queryOptions.select.map(item => {
        if (typeof item === 'string') {
          const quotedTable = quoteIdentifier(queryOptions.table, this.dbType);
          const tableRefPattern = new RegExp(`"${queryOptions.table}"\\.`, 'g');
          const tableRefPattern2 = new RegExp(`${quotedTable}\\.`, 'g');
          const tableRefPattern3 = new RegExp(`${queryOptions.table}\\.`, 'g');
          let result = item.replace(tableRefPattern, `${tableAlias}.`);
          result = result.replace(tableRefPattern2, `${tableAlias}.`);
          result = result.replace(tableRefPattern3, `${tableAlias}.`);
          
          if (this.dbType === 'postgres' && !result.trim().startsWith('(') && !result.includes('select ') && !result.includes('COALESCE')) {
            result = result.replace(
              new RegExp(`${tableAlias}\\.([a-zA-Z_][a-zA-Z0-9_]*)`, 'g'),
              (match, columnName) => {
                return `${tableAlias}.${quoteIdentifier(columnName, this.dbType)}`;
              }
            );
          }
          
          return result;
        }
        return item;
      });

      const selectSQL = selectItems.join(', ');

      const leftJoins = Array.from(aggregationCTENames).map(cteName => {
        return `LEFT JOIN ${cteName} ON ${tableAlias}.${quoteIdentifier('id', this.dbType)} = ${cteName}.parent_id`;
      }).join(' ');

      const orderBySQL = mainTableSorts.length > 0
        ? `ORDER BY ${mainTableSorts.map(s => {
            let field = s.field;
            if (field.includes('.')) {
              const parts = field.split('.');
              if (parts[0] === queryOptions.table) {
                field = `${tableAlias}.${quoteIdentifier(parts[1], this.dbType)}`;
              } else {
                field = field.replace(new RegExp(`^${queryOptions.table}\\.`, 'g'), `${tableAlias}.`);
              }
            } else {
              field = `${tableAlias}.${quoteIdentifier(field, this.dbType)}`;
            }
            return field;
          }).join(', ')} ${mainTableSorts[0].direction.toUpperCase()}`
        : '';

      rawSQLQuery = `
WITH ${cteClauses.join(',\n')}
SELECT ${selectSQL}
FROM ${limitedCTEName}
INNER JOIN ${quotedTable} ${tableAlias} ON ${limitedCTEName}.${quoteIdentifier('id', this.dbType)} = ${tableAlias}.${quoteIdentifier('id', this.dbType)}
${leftJoins ? leftJoins : ''}${orderBySQL ? ' ' + orderBySQL : ''}
      `.trim();
    } else {
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
    }

    if (!useCTE) {
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
    }

    let totalCount = 0;

    if (metaParts.includes('totalCount') || metaParts.includes('*')) {
      const totalQuery = this.knex(queryOptions.table);
      const totalResult = await totalQuery.count('* as count').first();
      totalCount = Number(totalResult?.count || 0);
    }

    if (this.debugLog && this.debugLog.length >= 0) {
      if (useCTE && rawSQLQuery) {
        this.debugLog.push({
          type: 'SQL Query (CTE)',
          table: queryOptions.table,
          sql: rawSQLQuery,
        });
      } else {
      this.debugLog.push({
        type: 'SQL Query',
        table: queryOptions.table,
        sql: query.toSQL().toNative(),
      });
    }
    }

    let results: any[];
    if (useCTE && rawSQLQuery) {
      const rawResult = await this.knex.raw(rawSQLQuery);
      results = this.dbType === 'postgres' ? (rawResult as any).rows : rawResult;
    } else {
      results = await query;
    }

    let filterCount = 0;

    if (needsFilterCount && results.length > 0) {
      filterCount = Number(results[0].__filter_count__ || 0);

      results.forEach((row: any) => {
        delete row.__filter_count__;
      });
    }

    if (this.knexService) {
      results = await this.knexService.parseResult(results, queryOptions.table);
    } else {
      const parseSimpleJsonFields = (data: any, tableName: string): any => {
        if (!data || !this.metadata) return data;
        
        const tableMeta = this.metadata.tables?.get(tableName);
        if (!tableMeta) return data;

        if (Array.isArray(data)) {
          return data.map(item => parseSimpleJsonFields(item, tableName));
        }

        if (typeof data !== 'object' || Buffer.isBuffer(data)) {
          return data;
        }

        const parsed = { ...data };

        for (const column of tableMeta.columns) {
          if (column.type === 'simple-json' && parsed[column.name] && typeof parsed[column.name] === 'string') {
            try {
              parsed[column.name] = JSON.parse(parsed[column.name]);
            } catch (e) {
            }
          }
        }

        for (const [key, value] of Object.entries(parsed)) {
          if (value && typeof value === 'object' && !Array.isArray(value) && !Buffer.isBuffer(value)) {
            const valueAny = value as any;
            if (valueAny.id !== undefined || valueAny.createdAt !== undefined) {
              const relation = tableMeta.relations?.find(r => r.propertyName === key);
              if (relation) {
                parsed[key] = parseSimpleJsonFields(value, relation.targetTableName);
              }
            }
          } else if (Array.isArray(value) && value.length > 0 && typeof value[0] === 'object') {
            const firstItem = value[0] as any;
            if (firstItem.id !== undefined) {
              const relation = tableMeta.relations?.find(r => r.propertyName === key);
              if (relation) {
                parsed[key] = (value as any[]).map(item => parseSimpleJsonFields(item, relation.targetTableName));
              }
            }
          }
        }

        return parsed;
      };

      results = parseSimpleJsonFields(results, queryOptions.table);
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
    sortOptions: Array<{ field: string; direction: 'asc' | 'desc' }> = [],
    limit?: number,
    orderByClause?: string,
    whereClause?: string,
    offset?: number,
  ): Promise<{ select: string[]; cteClauses?: string[] }> {
    if (!this.metadata) {
      return { select: fields };
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
      const result = await expandFieldsToJoinsAndSelect(
        tableName,
        fields,
        metadataGetter,
        this.dbType,
        sortOptions,
        undefined,
        limit,
        orderByClause,
        whereClause,
        offset,
      );
      return { select: result.select, cteClauses: result.cteClauses };
    } catch (error) {
      return { select: fields };
    }
  }
}
