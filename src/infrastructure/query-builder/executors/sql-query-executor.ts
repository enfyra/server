import { Knex } from 'knex';
import { Logger } from '../../../shared/logger';
import {
  QueryOptions,
  WhereCondition,
} from '../../../shared/types/query-builder.types';
import { DebugTrace } from '../../../shared/utils/debug-trace.util';
import {
  buildWhereClause,
  hasLogicalOperators,
} from '../utils/sql/build-where-clause';
import {
  separateFilters,
  applyRelationFilters,
} from '../utils/sql/relation-filter.util';
import { quoteIdentifier } from '../../knex/utils/migration/sql-dialect';
import { getPrimaryKeyColumn } from '../../knex/utils/metadata-loader';
import { getForeignKeyColumnName } from '../../knex/utils/sql-schema-naming.util';
import { KnexService } from '../../knex/knex.service';
import { QueryPlan, ResolvedSortItem } from '../planner/query-plan.types';
import { decideSqlStrategy } from '../planner/sql-strategy-decider';
import { renderFilterToKnex } from '../utils/sql/render-filter';
import { validateFilterShape } from '../utils/shared/filter-sanitizer.util';
import { QueryPlanner } from '../planner/query-planner';
import {
  applyWhereToKnex,
  compileFilterToSqlWhereExpression,
} from '../utils/sql/sql-where-builder';
import {
  expandFieldsToSelect,
  getMetadataGetter,
  buildRelationSortSubquery,
  buildRelationSubqueryForCTE,
} from '../utils/sql/sql-field-expander';

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
    debugTrace?: DebugTrace;
    metadata?: any;
    plan?: QueryPlan;
  }): Promise<any> {
    const execStart = performance.now();
    this.metadata = options.metadata;
    const debugLog = options.debugLog || [];
    this.debugLog = debugLog;
    const trace = options.debugTrace;

    if (options.filter) {
      validateFilterShape(options.filter, options.tableName, options.metadata);
    }

    if (!options.plan) {
      const planner = new QueryPlanner();
      options = {
        ...options,
        plan: planner.plan({
          tableName: options.tableName,
          fields: options.fields,
          filter: options.filter,
          sort: options.sort,
          page: options.page,
          limit: options.limit,
          meta: options.meta,
          metadata: options.metadata,
          dbType: this.dbType as any,
        }),
      };
    }

    const plan = options.plan!;
    const tableMeta = options.metadata?.tables?.get(options.tableName);
    const hasTableRelations =
      tableMeta?.relations && tableMeta.relations.length > 0;
    const hasDeepRelations =
      options.deep && typeof options.deep === 'object' && Object.keys(options.deep).length > 0;
    const hasExplicitFields =
      plan.rawFields && plan.rawFields.length > 0 && !plan.rawFields.includes('*');
    const isSimpleQuery =
      plan.joins.length === 0 &&
      !plan.hasRelationFilters &&
      !plan.hasRelationSort &&
      !hasDeepRelations &&
      !hasTableRelations &&
      hasExplicitFields;

    if (isSimpleQuery) {
      if (trace) trace.setQueryPath('simple');
      const result = await this.executeSimple(options, plan);
      if (trace) trace.dur('sql_executor', execStart, { table: options.tableName });
      return result;
    }

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
          const subquerySql = buildRelationSortSubquery(
            joinSpec.relationMeta,
            item.field,
            queryOptions.table,
            this.metadata,
            this.dbType,
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
            const sqlExpr = await compileFilterToSqlWhereExpression(
              this.knex,
              originalFilter,
              queryOptions.table,
              metadata,
              this.dbType,
              this.metadata,
              (
                tableName: string,
                relationName: string,
                relationFilter: any,
                meta: any,
              ) =>
                buildRelationSubqueryForCTE(
                  this.knex,
                  tableName,
                  relationName,
                  relationFilter,
                  meta,
                  this.dbType,
                  (tName: string) => this.metadata?.tables?.get(tName),
                ),
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

      const metadataGetter = getMetadataGetter(this.metadata);
      const expandedResult = await expandFieldsToSelect(
        this.knex,
        queryOptions.table,
        queryOptions.fields,
        metadataGetter,
        this.dbType,
        queryOptions.limit,
        orderByClause,
        whereClauseForCTE,
        queryOptions.offset,
        builtLimitedCteSortJoin,
        this.maxQueryDepth,
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
          this.logger.debug(
            `buildWhereClause fallback for table=${queryOptions.table}`,
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
        query = applyWhereToKnex(
          query,
          queryOptions.where,
          queryOptions.table,
          this.metadata,
          this.dbType,
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

    if (options.debugMode && trace) {
      const sqlString =
        useCTE && rawSQLQuery ? rawSQLQuery : query.toSQL().toNative().sql;
      trace.setSql(sqlString);
      try {
        let explainResult: any;
        if (useCTE && rawSQLQuery) {
          explainResult = await this.knex.raw(`EXPLAIN ${rawSQLQuery}`);
        } else {
          const native = query.toSQL().toNative();
          const explainSql = native.sql.replace(/\$\d+/g, '?');
          explainResult = await this.knex.raw(
            `EXPLAIN ${explainSql}`,
            native.bindings,
          );
        }
        trace.setExplain(
          this.dbType === 'postgres'
            ? (explainResult as any).rows
            : (explainResult as any)[0],
        );
      } catch (e) {
        trace.setExplain({ error: String(e) });
      }
    }

    let results: any[];
    if (useCTE && rawSQLQuery) {
      const dbStart = performance.now();
      const rawResult = await this.knex.raw(rawSQLQuery);
      if (trace) trace.dur('db_execute', dbStart, { table: options.tableName, path: 'cte' });
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
      const dbStart = performance.now();
      results = await query;
      if (trace) trace.dur('db_execute', dbStart, { table: options.tableName });
    }

    let filterCount = 0;

    if (needsFilterCount && useCTE && filterCountBaseQuery) {
      const filterCountResult = await this.knex.raw(filterCountBaseQuery);
      if (this.dbType === 'postgres') {
        filterCount = Number((filterCountResult as any).rows?.[0]?.cnt || 0);
      } else {
        const rowsArray = Array.isArray(filterCountResult)
          ? Array.isArray(filterCountResult[0])
            ? filterCountResult[0]
            : filterCountResult
          : null;
        const row = rowsArray?.[0];
        filterCount = Number(row?.cnt ?? row?.count ?? 0);
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

    if (
      pendingBatchFetches &&
      pendingBatchFetches.length > 0 &&
      results.length > 0
    ) {
      const metadataGetter = getMetadataGetter(this.metadata);
      if (metadataGetter) {
        const { executeBatchFetches } =
          await import('../utils/sql/batch-relation-fetcher');
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
      const parseStart = performance.now();
      results = await this.knexService.parseResult(results, queryOptions.table);
      if (trace) trace.dur('db_parseResult', parseStart, { table: options.tableName });
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

    if (trace) {
      trace.setQueryPath('full');
      trace.dur('sql_executor', execStart, { table: options.tableName });
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

  private async executeSimple(
    options: {
      tableName: string;
      fields?: string | string[];
      filter?: any;
      meta?: string;
      deep?: Record<string, any>;
      debugLog?: any[];
      debugMode?: boolean;
      metadata?: any;
    },
    plan: QueryPlan,
  ): Promise<any> {
    const table = options.tableName;

    const selectFields =
      plan.rawFields && plan.rawFields.length > 0
        ? plan.rawFields
        : ['*'];

    let query: any = this.knex(table).select(selectFields);

    if (plan.filterTree) {
      renderFilterToKnex(query, plan.filterTree, {
        dbType: this.dbType as any,
        rootTable: table,
      });
    }

    for (const s of plan.sortItems) {
      const field = s.field.includes('.')
        ? s.field
        : `${table}.${s.field}`;
      query = query.orderBy(field, s.direction);
    }

    if (plan.limit !== undefined && plan.limit !== null && plan.limit > 0) {
      query = query.limit(plan.limit);
    }
    if (plan.offset !== undefined) {
      query = query.offset(plan.offset);
    }

    const metaParts = Array.isArray(options.meta)
      ? options.meta
      : (options.meta || '').split(',').map((x: string) => x.trim()).filter(Boolean);
    const needsFilterCount =
      metaParts.includes('filterCount') || metaParts.includes('*');
    const needsTotalCount =
      metaParts.includes('totalCount') || metaParts.includes('*');

    if (needsFilterCount) {
      query.select(this.knex.raw('COUNT(*) OVER() as __filter_count__'));
    }
    if (needsTotalCount) {
      const quotedTbl = quoteIdentifier(table, this.dbType);
      query.select(
        this.knex.raw(
          `(SELECT COUNT(*) FROM ${quotedTbl}) as __total_count__`,
        ),
      );
    }

    const simpleTrace = (options as any).debugTrace as DebugTrace | undefined;

    if (options.debugMode && simpleTrace) {
      const native = query.toSQL().toNative();
      simpleTrace.setSql(native.sql);
      try {
        const explainSql = native.sql.replace(/\$\d+/g, '?');
        const explainResult = await this.knex.raw(
          `EXPLAIN ${explainSql}`,
          native.bindings,
        );
        simpleTrace.setExplain(
          this.dbType === 'postgres'
            ? (explainResult as any).rows
            : (explainResult as any)[0],
        );
      } catch (e) {
        simpleTrace.setExplain({ error: String(e) });
      }
    }

    const dbStart = performance.now();
    let results: any[];
    results = await query;
    if (simpleTrace) simpleTrace.dur('db_execute', dbStart, { table });

    if (this.knexService) {
      results = await this.knexService.parseResult(results, table);
    }

    let filterCount = 0;
    let totalCount = 0;
    if (needsFilterCount && results.length > 0) {
      filterCount = Number(results[0].__filter_count__ || 0);
      results.forEach((row: any) => delete row.__filter_count__);
    }
    if (needsTotalCount && results.length > 0) {
      totalCount = Number(results[0].__total_count__ || 0);
      results.forEach((row: any) => delete row.__total_count__);
    }

    return {
      data: results,
      ...(metaParts.length > 0 && {
        meta: {
          ...(needsTotalCount ? { totalCount } : {}),
          ...(needsFilterCount ? { filterCount } : {}),
        },
      }),
    };
  }
}
