import { Knex } from 'knex';
import { Logger } from '@nestjs/common';
import {
  QueryOptions,
  WhereCondition,
} from '../../../shared/types/query-builder.types';
import {
  buildWhereClause,
  hasLogicalOperators,
} from '../utils/sql/build-where-clause';
import {
  separateFilters,
  applyRelationFilters,
  buildRelationSubquery,
} from '../utils/sql/relation-filter.util';
import { quoteIdentifier } from '../../knex/utils/migration/sql-dialect';
import { getPrimaryKeyColumn } from '../../knex/utils/metadata-loader';
import { getForeignKeyColumnName } from '../../knex/utils/sql-schema-naming.util';
import { KnexService } from '../../knex/knex.service';
import { QueryPlan, ResolvedSortItem } from '../planner/query-plan.types';
import { decideSqlStrategy } from '../planner/sql-strategy-decider';
import { renderFilterToKnex } from '../utils/sql/render-filter';

export class SqlQueryExecutor {
  private readonly logger = new Logger(SqlQueryExecutor.name);
  private debugLog: any[] = [];
  private metadata: any;

  constructor(
    private readonly knex: Knex,
    private readonly dbType: 'postgres' | 'mysql' | 'sqlite',
    private readonly knexService?: KnexService,
    private readonly maxQueryDepth?: number,
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
    debugMode?: boolean;
    metadata?: any;
    plan?: QueryPlan;
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
        queryOptions.fields = options.fields.split(',').map((f) => f.trim());
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
            else if (op === '_not_in' || op === '_nin') operator = 'not in';
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
            else if (op === '_like') operator = 'like';
            else if (op === '_ilike') operator = 'ilike';
            else continue;

            queryOptions.where.push({
              field,
              operator,
              value: val,
            } as WhereCondition);
          }
        } else {
          queryOptions.where.push({
            field,
            operator: '=',
            value,
          } as WhereCondition);
        }
      }
    }

    if (options.sort) {
      const sortArray = Array.isArray(options.sort)
        ? options.sort
        : options.sort.split(',').map((s) => s.trim());
      queryOptions.sort = sortArray.map((s) => {
        const trimmed = s.trim();
        if (trimmed.startsWith('-')) {
          return { field: trimmed.substring(1), direction: 'desc' as const };
        }
        return { field: trimmed, direction: 'asc' as const };
      });
    } else {
      queryOptions.sort = [{ field: 'id', direction: 'asc' as const }];
    }

    const rawLimit =
      options.limit === undefined || options.limit === null
        ? undefined
        : typeof options.limit === 'string'
          ? parseInt(options.limit, 10)
          : options.limit;

    const normalizedLimit =
      rawLimit === undefined || Number.isNaN(rawLimit)
        ? undefined
        : rawLimit < 0
          ? 0
          : rawLimit;

    const rawPage =
      options.page === undefined || options.page === null
        ? undefined
        : typeof options.page === 'string'
          ? parseInt(options.page, 10)
          : options.page;

    const normalizedPage =
      rawPage === undefined || Number.isNaN(rawPage) ? undefined : rawPage;

    if (normalizedPage && normalizedLimit !== undefined) {
      queryOptions.offset = (normalizedPage - 1) * normalizedLimit;
      queryOptions.limit = normalizedLimit;
    } else if (normalizedLimit !== undefined) {
      queryOptions.limit = normalizedLimit;
    }

    const resolvedSortItems: ResolvedSortItem[] =
      options.plan?.sortItems ??
      (queryOptions.sort ?? []).map((s) => ({
        joinId: null,
        field: s.field,
        direction: s.direction,
        fullPath: s.field,
      }));

    const planLimitedCteSortJoin = options.plan?.limitedCteSortJoin ?? null;
    const planSqlStrategy = options.plan
      ? decideSqlStrategy(options.plan, this.dbType as any)
      : undefined;
    const usePlanCTE =
      planSqlStrategy === 'cte-flat' || planSqlStrategy === 'cte-aggregate';

    const mainTableSorts: Array<{ field: string; direction: 'asc' | 'desc' }> =
      [];
    const relationSortSubqueries: Array<{
      sql: string;
      direction: 'asc' | 'desc';
    }> = [];

    for (const item of resolvedSortItems) {
      if (item.joinId === null) {
        if (item.field.includes('.') && !options.plan) {
          const prefix = item.field.split('.')[0];
          const tblMeta = this.metadata?.tables?.get(queryOptions.table);
          const isRelField = tblMeta?.relations?.some(
            (r: any) => r.propertyName === prefix,
          );
          if (isRelField) continue;
        }
        mainTableSorts.push({
          field: item.field.includes('.')
            ? item.field
            : `${queryOptions.table}.${item.field}`,
          direction: item.direction,
        });
      } else {
        if (
          usePlanCTE &&
          planLimitedCteSortJoin &&
          item.joinId === planLimitedCteSortJoin.id
        ) {
          continue;
        }
        const joinSpec = options.plan?.joins.find((j) => j.id === item.joinId);
        if (
          joinSpec &&
          (joinSpec.relationType === 'many-to-one' ||
            joinSpec.relationType === 'one-to-one')
        ) {
          const subquerySql = this.buildRelationSortSubquery(
            joinSpec.relationMeta,
            item.field,
            queryOptions.table,
          );
          if (subquerySql) {
            relationSortSubqueries.push({
              sql: subquerySql,
              direction: item.direction,
            });
          }
        }
      }
    }

    let cteClauses: string[] | undefined = undefined;
    let useCTE = false;
    let whereClauseForCTE: string | undefined = undefined;

    if (queryOptions.fields && queryOptions.fields.length > 0) {
      const orderByParts: string[] = [];
      for (const s of mainTableSorts) {
        const field = s.field.includes('.')
          ? s.field
          : `${queryOptions.table}.${s.field}`;
        orderByParts.push(
          `${quoteIdentifier(field.split('.')[0], this.dbType)}.${quoteIdentifier(field.split('.')[1] || field, this.dbType)} ${s.direction.toUpperCase()}`,
        );
      }
      for (const rs of relationSortSubqueries) {
        orderByParts.push(`${rs.sql} ${rs.direction.toUpperCase()}`);
      }
      const orderByClause =
        orderByParts.length > 0
          ? `ORDER BY ${orderByParts.join(', ')}`
          : undefined;

      let builtLimitedCteSortJoin: any = undefined;
      if (usePlanCTE && planLimitedCteSortJoin) {
        const sortItem = resolvedSortItems.find(
          (s) => s.joinId === planLimitedCteSortJoin.id,
        );
        if (sortItem) {
          const joinMap = new Map(
            (options.plan!.joins ?? []).map((j: any) => [j.id, j]),
          );
          const chain: any[] = [];
          let cur: any = planLimitedCteSortJoin;
          while (cur) {
            chain.unshift(cur);
            cur = cur.parentJoinId ? joinMap.get(cur.parentJoinId) : undefined;
          }
          const steps = chain.map((joinSpec: any) => {
            const fkCol =
              joinSpec.relationMeta?.foreignKeyColumn ||
              getForeignKeyColumnName(joinSpec.propertyName);
            const targetMeta = this.metadata?.tables?.get(joinSpec.targetTable);
            const pkCol =
              targetMeta?.columns?.find((c: any) => c.isPrimary)?.name || 'id';
            return { targetTable: joinSpec.targetTable, fkCol, pkCol };
          });
          builtLimitedCteSortJoin = {
            steps,
            sortField: sortItem.field,
            direction: sortItem.direction,
          };
        }
      }

      if (
        queryOptions.limit !== undefined &&
        (orderByClause || builtLimitedCteSortJoin) &&
        (this.dbType === 'postgres' || this.dbType === 'mysql')
      ) {
        const metadata = this.metadata?.tables?.get(queryOptions.table);
        if (
          originalFilter &&
          (hasLogicalOperators(originalFilter) ||
            Object.keys(originalFilter).length > 0)
        ) {
          if (metadata) {
            const sqlExpr = await this.compileFilterToSqlWhereExpression(
              originalFilter,
              queryOptions.table,
              metadata,
            );
            if (sqlExpr) {
              whereClauseForCTE = `WHERE ${sqlExpr}`;
            }
          }
        } else if (queryOptions.where && queryOptions.where.length > 0) {
          const SAFE_CTE_OPERATORS = new Set([
            '=',
            '!=',
            '<>',
            '>',
            '>=',
            '<',
            '<=',
            'in',
            'not in',
            'like',
            'ilike',
            'is',
            'is not',
          ]);
          const whereParts: string[] = [];
          for (const condition of queryOptions.where) {
            const normalizedOp = String(condition.operator)
              .toLowerCase()
              .trim();
            if (!SAFE_CTE_OPERATORS.has(normalizedOp)) continue;
            let field: string;
            if (condition.field.includes('.')) {
              const parts = condition.field.split('.');
              field = parts
                .map((p) => quoteIdentifier(p, this.dbType))
                .join('.');
            } else {
              field = `${quoteIdentifier(queryOptions.table, this.dbType)}.${quoteIdentifier(condition.field, this.dbType)}`;
            }
            let value: string;
            if (typeof condition.value === 'string') {
              value = `'${condition.value.replace(/'/g, "''")}'`;
            } else if (condition.value === null) {
              value = 'NULL';
            } else if (typeof condition.value === 'boolean') {
              value = condition.value ? 'true' : 'false';
            } else if (
              typeof condition.value === 'number' &&
              Number.isFinite(condition.value)
            ) {
              value = String(condition.value);
            } else {
              continue;
            }
            whereParts.push(`${field} ${normalizedOp} ${value}`);
          }
          if (whereParts.length > 0) {
            whereClauseForCTE = `WHERE ${whereParts.join(' AND ')}`;
          }
        }
      }

      const expandedResult = await this.expandFieldsToSelect(
        queryOptions.table,
        queryOptions.fields,
        queryOptions.limit,
        orderByClause,
        whereClauseForCTE,
        queryOptions.offset,
        builtLimitedCteSortJoin,
      );
      queryOptions.select = [
        ...(queryOptions.select || []),
        ...expandedResult.select,
      ];
      cteClauses = expandedResult.cteClauses || [];
      useCTE = cteClauses.length > 0;
      var pendingBatchFetches = expandedResult.batchFetchDescriptors || [];
    }

    if (queryOptions.where) {
      queryOptions.where = queryOptions.where.map((condition) => {
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
      : (options.meta || '')
          .split(',')
          .map((x) => x.trim())
          .filter(Boolean);

    const needsFilterCount =
      metaParts.includes('filterCount') || metaParts.includes('*');
    const needsTotalCount =
      metaParts.includes('totalCount') || metaParts.includes('*');

    let filterCountBaseQuery: string | null = null;

    if (useCTE && cteClauses) {
      const limitedCTEName = `limited_${queryOptions.table}`;
      const tableAlias = 't';
      const quotedTable = quoteIdentifier(queryOptions.table, this.dbType);

      const aggregationCTENames = new Set<string>();

      cteClauses.forEach((cte) => {
        const match = cte.match(/^[`"]?(\w+)[`"]?\s+AS\s*\(/i);
        if (match) {
          const cteName = match[1];
          if (cteName !== limitedCTEName) {
            aggregationCTENames.add(cteName);
          }
        }
      });

      const selectItems = queryOptions.select.map((item) => {
        if (typeof item === 'string') {
          const quotedTable = quoteIdentifier(queryOptions.table, this.dbType);
          const tableRefPattern = new RegExp(`"${queryOptions.table}"\\.`, 'g');
          const tableRefPattern2 = new RegExp(`${quotedTable}\\.`, 'g');
          const tableRefPattern3 = new RegExp(`${queryOptions.table}\\.`, 'g');
          let result = item.replace(tableRefPattern, `${tableAlias}.`);
          result = result.replace(tableRefPattern2, `${tableAlias}.`);
          result = result.replace(tableRefPattern3, `${tableAlias}.`);

          if (
            this.dbType === 'postgres' &&
            !result.trim().startsWith('(') &&
            !result.includes('select ') &&
            !result.includes('COALESCE')
          ) {
            result = result.replace(
              new RegExp(`${tableAlias}\\.([a-zA-Z_][a-zA-Z0-9_]*)`, 'g'),
              (match, columnName) => {
                return `${tableAlias}.${quoteIdentifier(columnName, this.dbType)}`;
              },
            );
          }

          return result;
        }
        return item;
      });

      const selectSQL = selectItems.join(', ');

      const tableMeta = this.metadata?.tables?.get(queryOptions.table);
      const pkColumn = tableMeta ? getPrimaryKeyColumn(tableMeta) : null;
      const pkName = pkColumn?.name || 'id';
      const quotedPkName = quoteIdentifier(pkName, this.dbType);

      const leftJoins = Array.from(aggregationCTENames)
        .map((cteName) => {
          const quotedCTEName = quoteIdentifier(cteName, this.dbType);
          const quotedParentId = quoteIdentifier('parent_id', this.dbType);
          return `LEFT JOIN ${quotedCTEName} ON ${tableAlias}.${quotedPkName} = ${quotedCTEName}.${quotedParentId}`;
        })
        .join(' ');

      const orderBySQLParts: string[] = [];
      for (const s of mainTableSorts) {
        let field = s.field;
        if (field.includes('.')) {
          const parts = field.split('.');
          if (parts[0] === queryOptions.table) {
            field = `${tableAlias}.${quoteIdentifier(parts[1], this.dbType)}`;
          } else {
            field = field.replace(
              new RegExp(`^${queryOptions.table}\\.`, 'g'),
              `${tableAlias}.`,
            );
          }
        } else {
          field = `${tableAlias}.${quoteIdentifier(field, this.dbType)}`;
        }
        orderBySQLParts.push(`${field} ${s.direction.toUpperCase()}`);
      }
      for (const rs of relationSortSubqueries) {
        orderBySQLParts.push(`${rs.sql} ${rs.direction.toUpperCase()}`);
      }
      const orderBySQL =
        orderBySQLParts.length > 0
          ? `ORDER BY ${orderBySQLParts.join(', ')}`
          : '';

      const quotedLimitedCTE = quoteIdentifier(limitedCTEName, this.dbType);

      filterCountBaseQuery = null;
      if (needsFilterCount) {
        const limitedCTEDef = cteClauses.find(
          (cte) =>
            cte.startsWith(`${quotedLimitedCTE} AS`) ||
            cte.startsWith(`"${limitedCTEName}" AS`),
        );
        if (limitedCTEDef) {
          const asMatch = limitedCTEDef.match(
            /AS\s*\(\s*(SELECT\s+[\s\S]+)\s*\)$/i,
          );
          if (asMatch) {
            let baseQuery = asMatch[1].trim();
            baseQuery = baseQuery.replace(/\s+ORDER\s+BY\s+[\s\S]+$/i, '');
            baseQuery = baseQuery.replace(/\s+LIMIT\s+\d+$/i, '');
            baseQuery = baseQuery.replace(/\s+OFFSET\s+\d+$/i, '');
            filterCountBaseQuery = `SELECT COUNT(*) as cnt FROM (${baseQuery}) subq`;
          }
        }
      }

      const totalCountSelect = needsTotalCount
        ? `, (SELECT COUNT(*) FROM ${quotedTable}) as __total_count__`
        : '';

      rawSQLQuery = `
WITH ${cteClauses.join(',\n')}
SELECT ${selectSQL}${totalCountSelect}
FROM ${quotedLimitedCTE}
INNER JOIN ${quotedTable} ${tableAlias} ON ${quotedLimitedCTE}.${quotedPkName} = ${tableAlias}.${quotedPkName}
${leftJoins ? leftJoins : ''}${orderBySQL ? ' ' + orderBySQL : ''}
      `.trim();
    } else {
      if (queryOptions.select) {
        const selectItems = queryOptions.select.map((field) => {
          if (typeof field === 'string' && field.trim().startsWith('(')) {
            return this.knex.raw(field);
          }
          if (typeof field === 'string' && / as /i.test(field)) {
            return this.knex.raw(field);
          }
          return field;
        });
        query = query.select(selectItems);
      }

      if (needsFilterCount) {
        query.select(this.knex.raw('COUNT(*) OVER() as __filter_count__'));
      }

      if (needsTotalCount) {
        const quotedTbl = quoteIdentifier(queryOptions.table, this.dbType);
        query.select(
          this.knex.raw(
            `(SELECT COUNT(*) FROM ${quotedTbl}) as __total_count__`,
          ),
        );
      }
    }

    if (!useCTE) {
      const isSystemTable = [
        'table_definition',
        'column_definition',
        'relation_definition',
        'method_definition',
      ].includes(queryOptions.table);

      if (
        originalFilter &&
        (hasLogicalOperators(originalFilter) ||
          Object.keys(originalFilter).length > 0)
      ) {
        const metadata = this.metadata?.tables?.get(queryOptions.table);
        const hasRelations =
          !isSystemTable &&
          metadata &&
          metadata.relations &&
          metadata.relations.length > 0
            ? separateFilters(originalFilter, metadata).hasRelations
            : false;

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
        } else if (options.plan?.filterTree) {
          renderFilterToKnex(query, options.plan.filterTree, {
            dbType: this.dbType as any,
            rootTable: queryOptions.table,
          });
        } else {
          console.warn(
            `[sql-executor:fallback] buildWhereClause path hit for table=${queryOptions.table} filter=${JSON.stringify(originalFilter)}`,
          );
          query = buildWhereClause(
            query,
            originalFilter,
            queryOptions.table,
            this.dbType,
            metadata,
          );
        }
      } else if (queryOptions.where && queryOptions.where.length > 0) {
        query = this.applyWhereToKnex(
          query,
          queryOptions.where,
          queryOptions.table,
        );
      }

      if (queryOptions.sort) {
        for (const sortOpt of queryOptions.sort) {
          const sortField = sortOpt.field.includes('.')
            ? sortOpt.field
            : `${queryOptions.table}.${sortOpt.field}`;
          query = query.orderBy(sortField, sortOpt.direction);
        }
      }
      for (const rs of relationSortSubqueries) {
        query = query.orderByRaw(`${rs.sql} ${rs.direction.toUpperCase()}`);
      }

      if (queryOptions.groupBy) {
        query = query.groupBy(queryOptions.groupBy);
      }

      if (queryOptions.offset) {
        query = query.offset(queryOptions.offset);
      }

      if (
        queryOptions.limit !== undefined &&
        queryOptions.limit !== null &&
        queryOptions.limit > 0
      ) {
        query = query.limit(queryOptions.limit);
      }
    }

    let totalCount = 0;

    if (this.debugLog) {
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

    if (options.debugMode) {
      const sqlString =
        useCTE && rawSQLQuery ? rawSQLQuery : query.toSQL().toNative().sql;
      let explain: any;
      try {
        if (useCTE && rawSQLQuery) {
          const explainResult = await this.knex.raw(`EXPLAIN ${rawSQLQuery}`);
          explain =
            this.dbType === 'postgres'
              ? (explainResult as any).rows
              : (explainResult as any)[0];
        } else {
          const native = query.toSQL().toNative();
          const explainResult = await this.knex.raw(
            `EXPLAIN ${native.sql}`,
            native.bindings,
          );
          explain =
            this.dbType === 'postgres'
              ? (explainResult as any).rows
              : (explainResult as any)[0];
        }
      } catch (e) {
        explain = { error: String(e) };
      }
      return { sql: sqlString, explain };
    }

    let results: any[];
    if (useCTE && rawSQLQuery) {
      const rawResult = await this.knex.raw(rawSQLQuery);
      if (this.dbType === 'postgres') {
        results = (rawResult as any).rows;
      } else {
        if (Array.isArray(rawResult) && rawResult.length > 0) {
          const rows = rawResult[0];
          if (Array.isArray(rows)) {
            results = rows;
          } else if (rows && typeof rows === 'object') {
            const keys = Object.keys(rows);
            if (keys.length > 0 && /^\d+$/.test(keys[0])) {
              results = Object.values(rows);
            } else {
              results = [rows];
            }
          } else {
            results = [];
          }
        } else if (rawResult && typeof rawResult === 'object') {
          const keys = Object.keys(rawResult);
          if (keys.length > 0 && /^\d+$/.test(keys[0])) {
            results = Object.values(rawResult);
          } else {
            results = [rawResult];
          }
        } else {
          results = [];
        }
      }
    } else {
      results = await query;
    }

    let filterCount = 0;

    if (needsFilterCount && useCTE && filterCountBaseQuery) {
      const filterCountResult = await this.knex.raw(filterCountBaseQuery);
      if (this.dbType === 'postgres') {
        filterCount = Number((filterCountResult as any).rows?.[0]?.cnt || 0);
      } else {
        const row = Array.isArray(filterCountResult)
          ? filterCountResult[0]
          : filterCountResult;
        filterCount = Number(row?.cnt || row?.count || 0);
      }
    } else if (needsFilterCount && results.length > 0) {
      filterCount = Number(results[0].__filter_count__ || 0);

      results.forEach((row: any) => {
        delete row.__filter_count__;
      });
    }

    if (
      needsTotalCount &&
      results.length > 0 &&
      results[0].__total_count__ !== undefined
    ) {
      totalCount = Number(results[0].__total_count__ || 0);
      results.forEach((row: any) => {
        delete row.__total_count__;
      });
    }

    if (pendingBatchFetches && pendingBatchFetches.length > 0 && results.length > 0) {
      const metadataGetter = this.getMetadataGetter();
      if (metadataGetter) {
        const { executeBatchFetches } = await import(
          '../utils/sql/batch-relation-fetcher'
        );
        await executeBatchFetches(
          this.knex,
          results,
          pendingBatchFetches,
          metadataGetter,
          this.maxQueryDepth ?? 3,
          0,
          queryOptions.table,
          this.dbType as any,
        );
      }
    }

    if (this.knexService) {
      results = await this.knexService.parseResult(results, queryOptions.table);
    } else {
      const parseSimpleJsonFields = (data: any, tableName: string): any => {
        if (!data || !this.metadata) return data;

        const tableMeta = this.metadata.tables?.get(tableName);
        if (!tableMeta) return data;

        if (Array.isArray(data)) {
          return data.map((item) => parseSimpleJsonFields(item, tableName));
        }

        if (typeof data !== 'object' || Buffer.isBuffer(data)) {
          return data;
        }

        const parsed = { ...data };

        for (const column of tableMeta.columns) {
          if (
            column.type === 'simple-json' &&
            parsed[column.name] &&
            typeof parsed[column.name] === 'string'
          ) {
            try {
              parsed[column.name] = JSON.parse(parsed[column.name]);
            } catch (e) {}
          }
        }

        for (const [key, value] of Object.entries(parsed)) {
          if (
            value &&
            typeof value === 'object' &&
            !Array.isArray(value) &&
            !Buffer.isBuffer(value)
          ) {
            const valueAny = value as any;
            if (valueAny.id !== undefined || valueAny.createdAt !== undefined) {
              const relation = tableMeta.relations?.find(
                (r) => r.propertyName === key,
              );
              if (relation) {
                parsed[key] = parseSimpleJsonFields(
                  value,
                  relation.targetTableName,
                );
              }
            }
          } else if (
            Array.isArray(value) &&
            value.length > 0 &&
            typeof value[0] === 'object'
          ) {
            const firstItem = value[0] as any;
            if (firstItem.id !== undefined) {
              const relation = tableMeta.relations?.find(
                (r) => r.propertyName === key,
              );
              if (relation) {
                parsed[key] = (value as any[]).map((item) =>
                  parseSimpleJsonFields(item, relation.targetTableName),
                );
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
      ...(metaParts.length > 0 && {
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

  private convertValueByType(
    tableName: string,
    field: string,
    value: any,
  ): any {
    if (value === null || value === undefined) {
      return value;
    }

    const tableMeta = this.metadata?.tables?.get(tableName);
    if (!tableMeta?.columns) {
      return value;
    }

    const column = tableMeta.columns.find((col) => col.name === field);
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

  private applyWhereToKnex(
    query: any,
    conditions: WhereCondition[],
    tableName?: string,
  ): any {
    for (const condition of conditions) {
      const fieldParts = condition.field.split('.');
      const tableForConversion = tableName || fieldParts[0];
      const columnName = fieldParts[fieldParts.length - 1];
      const convertedValue = this.convertValueByType(
        tableForConversion,
        columnName,
        condition.value,
      );

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
            ? condition.value.map((v) =>
                this.convertValueByType(tableForConversion, columnName, v),
              )
            : [convertedValue];
          query = query.whereIn(condition.field, inValues);
          break;
        case 'not in':
          const ninValues = Array.isArray(condition.value)
            ? condition.value.map((v) =>
                this.convertValueByType(tableForConversion, columnName, v),
              )
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
            betweenValues = betweenValues.split(',').map((v) => v.trim());
          }
          if (Array.isArray(betweenValues) && betweenValues.length === 2) {
            const val0 = this.convertValueByType(
              tableForConversion,
              columnName,
              betweenValues[0],
            );
            const val1 = this.convertValueByType(
              tableForConversion,
              columnName,
              betweenValues[1],
            );
            query = query.whereBetween(condition.field, [val0, val1]);
          }
          break;
        case '_is_null':
          const isNullBool =
            convertedValue === true || convertedValue === 'true';
          query = isNullBool
            ? query.whereNull(condition.field)
            : query.whereNotNull(condition.field);
          break;
        case '_is_not_null':
          const isNotNullBool =
            convertedValue === true || convertedValue === 'true';
          query = isNotNullBool
            ? query.whereNotNull(condition.field)
            : query.whereNull(condition.field);
          break;
      }
    }
    return query;
  }

  private getMetadataGetter() {
    const allMetadata = this.metadata;
    if (!allMetadata) return null;
    return async (tName: string) => {
      const tableMeta = allMetadata.tables?.get(tName);
      if (!tableMeta) return null;
      return {
        name: tableMeta.name,
        columns: (tableMeta.columns || []).map((col: any) => ({
          name: col.name,
          type: col.type,
        })),
        relations: tableMeta.relations || [],
      };
    };
  }

  private async expandFieldsToSelect(
    tableName: string,
    fields: string[],
    limit?: number,
    orderByClause?: string,
    whereClause?: string,
    offset?: number,
    limitedCteSortJoin?: any,
  ): Promise<{ select: string[]; cteClauses?: string[]; batchFetchDescriptors?: any[] }> {
    const metadataGetter = this.getMetadataGetter();
    if (!metadataGetter) {
      return { select: fields };
    }

    try {
      const { expandFieldsToJoinsAndSelect } =
        await import('../utils/sql/expand-fields');
      const expanded = await expandFieldsToJoinsAndSelect(
        tableName,
        fields,
        metadataGetter,
        this.dbType,
        limit,
        orderByClause,
        whereClause,
        offset,
        limitedCteSortJoin,
        this.maxQueryDepth,
      );
      return {
        select: expanded.select,
        cteClauses: expanded.cteClauses,
        batchFetchDescriptors: expanded.batchFetchDescriptors,
      };
    } catch (error) {
      return { select: fields };
    }
  }

  private buildRelationSortSubquery(
    relationMeta: any,
    sortField: string,
    parentTable: string,
  ): string | null {
    const targetTable =
      relationMeta.targetTableName || relationMeta.targetTable;
    if (!targetTable) return null;

    const fkCol =
      relationMeta.foreignKeyColumn || getForeignKeyColumnName(targetTable);
    if (!fkCol) return null;

    const q = (s: string) => quoteIdentifier(s, this.dbType);

    const targetMeta = this.metadata?.tables?.get(targetTable);
    const pkCol = targetMeta ? getPrimaryKeyColumn(targetMeta) : null;
    const targetPk = pkCol?.name || 'id';

    return `(SELECT ${q(targetTable)}.${q(sortField)} FROM ${q(targetTable)} WHERE ${q(targetTable)}.${q(targetPk)} = ${q(parentTable)}.${q(fkCol)})`;
  }

  private buildSqlWherePartsFromFieldAst(
    filter: any,
    tablePrefix: string,
    tableMeta: any,
  ): string[] {
    const parts: string[] = [];
    const metadata = tableMeta;
    if (!filter || typeof filter !== 'object') {
      return parts;
    }
    for (const [field, value] of Object.entries(filter)) {
      if (field === '_and' && Array.isArray(value)) {
        const andParts = value
          .map((f) => {
            const subParts = this.buildSqlWherePartsFromFieldAst(
              f,
              tablePrefix,
              tableMeta,
            );
            return subParts.length > 0 ? `(${subParts.join(' AND ')})` : null;
          })
          .filter((p): p is string => p !== null);
        if (andParts.length > 0) {
          parts.push(`(${andParts.join(' AND ')})`);
        }
      } else if (field === '_or' && Array.isArray(value)) {
        const orParts = value
          .map((f) => {
            const subParts = this.buildSqlWherePartsFromFieldAst(
              f,
              tablePrefix,
              tableMeta,
            );
            return subParts.length > 0 ? `(${subParts.join(' AND ')})` : null;
          })
          .filter((p): p is string => p !== null);
        if (orParts.length > 0) {
          parts.push(`(${orParts.join(' OR ')})`);
        }
      } else if (
        field === '_not' &&
        typeof value === 'object' &&
        value !== null
      ) {
        const notParts = this.buildSqlWherePartsFromFieldAst(
          value,
          tablePrefix,
          tableMeta,
        );
        if (notParts.length > 0) {
          parts.push(`NOT (${notParts.join(' AND ')})`);
        }
      } else if (
        typeof value === 'object' &&
        value !== null &&
        !Array.isArray(value)
      ) {
        for (const [op, val] of Object.entries(value)) {
          const quotedField = `${quoteIdentifier(tablePrefix, this.dbType)}.${quoteIdentifier(field, this.dbType)}`;
          let sqlValue: string;
          if (val === null) {
            sqlValue = 'NULL';
          } else if (typeof val === 'string') {
            const uuidPattern =
              /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
            const column = metadata.columns?.find((c: any) => c.name === field);
            const isUUID =
              column &&
              (column.type?.toLowerCase() === 'uuid' ||
                column.type?.toLowerCase().includes('uuid'));
            if (isUUID && uuidPattern.test(val) && this.dbType === 'postgres') {
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
            const inSql = inValues
              .map((v) => {
                if (typeof v === 'string') {
                  return `'${v.replace(/'/g, "''")}'`;
                }
                return String(v);
              })
              .join(', ');
            parts.push(`${quotedField} IN (${inSql})`);
          } else if (op === '_not_in' || op === '_nin') {
            const notInValues = Array.isArray(val) ? val : [val];
            const notInSql = notInValues
              .map((v) => {
                if (typeof v === 'string') {
                  return `'${v.replace(/'/g, "''")}'`;
                }
                return String(v);
              })
              .join(', ');
            parts.push(`${quotedField} NOT IN (${notInSql})`);
          } else if (op === '_contains') {
            const escapedVal = String(val).replace(/'/g, "''");
            if (this.dbType === 'postgres') {
              parts.push(
                `lower(unaccent(${quotedField})) ILIKE '%' || lower(unaccent('${escapedVal}')) || '%'`,
              );
            } else if (this.dbType === 'mysql') {
              parts.push(
                `lower(unaccent(${quotedField})) COLLATE utf8mb4_general_ci LIKE CONCAT('%', lower(unaccent('${escapedVal}')) COLLATE utf8mb4_general_ci, '%')`,
              );
            } else {
              parts.push(
                `lower(${quotedField}) LIKE '%${escapedVal.toLowerCase()}%'`,
              );
            }
          } else if (op === '_starts_with') {
            const escapedVal = String(val).replace(/'/g, "''");
            if (this.dbType === 'postgres') {
              parts.push(
                `lower(unaccent(${quotedField})) ILIKE lower(unaccent('${escapedVal}')) || '%'`,
              );
            } else if (this.dbType === 'mysql') {
              parts.push(
                `lower(unaccent(${quotedField})) COLLATE utf8mb4_general_ci LIKE CONCAT(lower(unaccent('${escapedVal}')) COLLATE utf8mb4_general_ci, '%')`,
              );
            } else {
              parts.push(
                `lower(${quotedField}) LIKE '${escapedVal.toLowerCase()}%'`,
              );
            }
          } else if (op === '_ends_with') {
            const escapedVal = String(val).replace(/'/g, "''");
            if (this.dbType === 'postgres') {
              parts.push(
                `lower(unaccent(${quotedField})) ILIKE '%' || lower(unaccent('${escapedVal}'))`,
              );
            } else if (this.dbType === 'mysql') {
              parts.push(
                `lower(unaccent(${quotedField})) COLLATE utf8mb4_general_ci LIKE CONCAT('%', lower(unaccent('${escapedVal}')) COLLATE utf8mb4_general_ci)`,
              );
            } else {
              parts.push(
                `lower(${quotedField}) LIKE '%${escapedVal.toLowerCase()}'`,
              );
            }
          } else if (op === '_between') {
            if (Array.isArray(val) && val.length === 2) {
              const v1 =
                typeof val[0] === 'string'
                  ? `'${val[0].replace(/'/g, "''")}'`
                  : String(val[0]);
              const v2 =
                typeof val[1] === 'string'
                  ? `'${val[1].replace(/'/g, "''")}'`
                  : String(val[1]);
              parts.push(`${quotedField} BETWEEN ${v1} AND ${v2}`);
            }
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
  }

  private async compileFilterToSqlWhereExpression(
    filter: any,
    tableName: string,
    tableMeta: any,
  ): Promise<string | null> {
    if (!filter || typeof filter !== 'object') {
      return null;
    }
    if (filter._and && Array.isArray(filter._and)) {
      const chunks: string[] = [];
      for (const c of filter._and) {
        const e = await this.compileFilterToSqlWhereExpression(
          c,
          tableName,
          tableMeta,
        );
        if (e) {
          chunks.push(e);
        }
      }
      return chunks.length ? `(${chunks.join(' AND ')})` : null;
    }
    if (filter._or && Array.isArray(filter._or)) {
      const chunks: string[] = [];
      for (const c of filter._or) {
        const e = await this.compileFilterToSqlWhereExpression(
          c,
          tableName,
          tableMeta,
        );
        if (e) {
          chunks.push(e);
        }
      }
      return chunks.length ? `(${chunks.join(' OR ')})` : null;
    }
    if (
      filter._not &&
      typeof filter._not === 'object' &&
      filter._not !== null &&
      !Array.isArray(filter._not)
    ) {
      const inner = await this.compileFilterToSqlWhereExpression(
        filter._not,
        tableName,
        tableMeta,
      );
      return inner ? `NOT (${inner})` : null;
    }
    const { fieldFilters, relationFilters } = separateFilters(
      filter,
      tableMeta,
    );
    const chunks: string[] = [];
    if (Object.keys(fieldFilters).length > 0) {
      chunks.push(
        ...this.buildSqlWherePartsFromFieldAst(
          fieldFilters,
          tableName,
          tableMeta,
        ),
      );
    }
    for (const [relName, relFilter] of Object.entries(relationFilters)) {
      try {
        const subquery = await this.buildRelationSubqueryForCTE(
          tableName,
          relName,
          relFilter,
          tableMeta,
        );
        if (subquery) {
          chunks.push(`EXISTS (${subquery})`);
        }
      } catch (error: any) {
        this.logger.warn(
          `Failed to build relation subquery for ${relName}: ${error.message}`,
        );
      }
    }
    if (chunks.length === 0) {
      return null;
    }
    return chunks.length === 1 ? chunks[0] : `(${chunks.join(' AND ')})`;
  }

  private async buildRelationSubqueryForCTE(
    tableName: string,
    relationName: string,
    relationFilter: any,
    metadata: any,
  ): Promise<string | null> {
    return await buildRelationSubquery(
      this.knex,
      tableName,
      relationName,
      relationFilter,
      metadata,
      this.dbType,
      (tName: string) => this.metadata?.tables?.get(tName),
    );
  }
}
