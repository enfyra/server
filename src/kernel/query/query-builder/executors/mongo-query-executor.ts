import { Db, Collection } from 'mongodb';
import {
  AggregateQuery,
  NormalizedAggregateOperation,
  QueryOptions,
} from '../../../../shared/types/query-builder.types';
import { expandFieldsMongo } from '../utils/mongo/expand-fields';
import { executeAggregationPipeline } from '../utils/mongo/mongo-aggregation-builder';
import { renderFieldsToMongo } from '../utils/mongo/render-fields';
import { validateFilterShape } from '../../query-dsl/filter-sanitizer.util';
import { QueryPlanner } from '../../query-dsl/query-planner';
import { QueryPlan } from '../../query-dsl/query-plan.types';
import {
  normalizeMongoDocument,
  resolveMongoJunctionInfo,
} from '../../../../engines/mongo';
import { renderFilterToMongo } from '../utils/mongo/render-filter';
import { hasAnyRelations } from '../utils/shared/filter-separator.util';
import { resolveMongoFilter } from '../utils/mongo/mongo-filter-resolver';
import {
  buildAggregateFilter,
  mergeAggregateValue,
  normalizeAggregateQuery,
} from '../utils/aggregate-query.util';

export class MongoQueryExecutor {
  private debugLog: any[] = [];
  private readonly db: Db;
  private metadata: any;
  private dbType!: string;
  private lastBuiltPipeline: any[] | null = null;

  constructor(private readonly mongoService: any) {
    this.db = mongoService.getDb();
  }

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
    pipeline?: any[];
    metadata?: any;
    dbType?: string;
    plan?: QueryPlan;
    aggregate?: AggregateQuery;
  }): Promise<any> {
    this.metadata = options.metadata;
    this.dbType = options.dbType || 'mongodb';

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
          dbType: 'mongodb' as any,
        }),
      };
    }

    const debugLog = options.debugLog || [];
    this.debugLog = debugLog;

    const queryOptions: QueryOptions = {
      table: options.tableName,
      aggregate: options.aggregate,
      metadata: options.metadata,
    };

    if (options.pipeline) {
      queryOptions.pipeline = options.pipeline;
    }

    if (options.fields) {
      if (Array.isArray(options.fields)) {
        queryOptions.fields = options.fields;
      } else if (typeof options.fields === 'string') {
        queryOptions.fields = options.fields.split(',').map((f) => f.trim());
      }
    }

    if (options.filter) {
      validateFilterShape(options.filter, options.tableName, options.metadata);
      queryOptions.mongoRawFilter = options.filter;
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

    if (options.page && options.limit) {
      const page =
        typeof options.page === 'string'
          ? parseInt(options.page, 10)
          : options.page;
      const limit =
        typeof options.limit === 'string'
          ? parseInt(options.limit, 10)
          : options.limit;
      queryOptions.offset = (page - 1) * limit;
      queryOptions.limit = limit;
    } else if (options.limit) {
      queryOptions.limit =
        typeof options.limit === 'string'
          ? parseInt(options.limit, 10)
          : options.limit;
    }

    const metaParts = Array.isArray(options.meta)
      ? options.meta
      : (options.meta || '')
          .split(',')
          .map((x) => x.trim())
          .filter(Boolean);

    let totalCount = 0;
    let filterCount = 0;

    if (metaParts.includes('totalCount') || metaParts.includes('*')) {
      const collection = this.mongoService.collection(options.tableName);
      totalCount = await collection.countDocuments({});
    }

    const filterCountTableMeta = this.metadata?.tables?.get(options.tableName);
    const filterCountRelNames = new Set<string>(
      (filterCountTableMeta?.relations ?? []).map((r: any) => r.propertyName),
    );
    const hasRelationFilters =
      !!queryOptions.mongoRawFilter &&
      hasAnyRelations(queryOptions.mongoRawFilter, filterCountRelNames);

    if (metaParts.includes('filterCount') || metaParts.includes('*')) {
      if (!hasRelationFilters) {
        const collection = this.mongoService.collection(options.tableName);
        const filter = options.plan?.filterTree
          ? renderFilterToMongo(options.plan.filterTree, {
              metadata: this.metadata,
              rootTable: options.tableName,
            })
          : {};
        filterCount = await collection.countDocuments(filter);
      }
    }

    this.lastBuiltPipeline = null;
    queryOptions.plan = options.plan;
    queryOptions.deep = options.deep;
    const results = await this.selectLegacy(queryOptions);

    if (options.debugMode) {
      const builtPipeline = this.lastBuiltPipeline ?? [];
      let explain: any;
      try {
        const collection = this.db.collection(options.tableName);
        explain = await collection.aggregate(builtPipeline).explain();
      } catch (e) {
        explain = { error: String(e) };
      }
      return { pipeline: builtPipeline, explain };
    }

    if (
      hasRelationFilters &&
      (metaParts.includes('filterCount') || metaParts.includes('*'))
    ) {
      const countOpts = { ...queryOptions, mongoCountOnly: true };
      const countResults = await this.selectLegacy(countOpts);
      filterCount = countResults.length > 0 ? countResults[0].count : 0;
    }

    const aggregate = await this.executeAggregate(queryOptions, options.filter);

    return {
      data: results,
      ...((metaParts.length > 0 || aggregate) && {
        meta: {
          ...(metaParts.includes('totalCount') || metaParts.includes('*')
            ? { totalCount }
            : {}),
          ...(metaParts.includes('filterCount') || metaParts.includes('*')
            ? { filterCount }
            : {}),
          ...(aggregate ? { aggregate } : {}),
        },
      }),
    };
  }

  private async selectLegacy(options: QueryOptions): Promise<any[]> {
    if (options.fields && options.fields.length > 0) {
      const planForFields: QueryPlan | undefined = options.plan;
      if (planForFields?.fieldTree) {
        options.mongoFieldsExpanded = renderFieldsToMongo(
          planForFields.fieldTree,
          this.metadata,
        );
      } else {
        console.warn(
          `[mongo-executor:fallback] expandFieldsMongo path hit for table=${options.table} fields=${JSON.stringify(options.fields)}`,
        );
        options.mongoFieldsExpanded = await expandFieldsMongo(
          this.metadata,
          options.table,
          options.fields,
        );
      }
    }

    if (options.where) {
      options.where = options.where.map((condition) => {
        if (!condition.field.includes('.')) {
          return {
            ...condition,
            field: `${options.table}.${condition.field}`,
          };
        }
        return condition;
      });
    }

    const collection = this.db.collection(options.table);

    if (options.pipeline) {
      if (this.debugLog) {
        this.debugLog.push({
          type: 'MongoDB Custom Pipeline',
          collection: options.table,
          pipeline: JSON.parse(JSON.stringify(options.pipeline)),
        });
      }
      const results = await collection.aggregate(options.pipeline).toArray();
      return results.map(normalizeMongoDocument);
    }

    const { results, pipeline } = await executeAggregationPipeline(
      collection,
      options,
      {
        db: this.db,
        metadata: this.metadata,
        dbType: this.dbType,
        debugLog: this.debugLog,
      },
    );
    this.lastBuiltPipeline = pipeline;
    return results;
  }

  private async executeAggregationPipeline(
    collection: Collection,
    options: QueryOptions,
  ): Promise<any[]> {
    const { results, pipeline } = await executeAggregationPipeline(
      collection,
      options,
      {
        db: this.db,
        metadata: this.metadata,
        dbType: this.dbType,
        debugLog: this.debugLog,
      },
    );
    this.lastBuiltPipeline = pipeline;
    return results;
  }

  private async executeAggregate(
    queryOptions: Pick<QueryOptions, 'table' | 'aggregate' | 'metadata'>,
    baseFilter: any,
  ): Promise<Record<string, Record<string, any>> | undefined> {
    const operations = normalizeAggregateQuery(
      queryOptions.aggregate,
      queryOptions.table,
      queryOptions.metadata,
    );
    if (operations.length === 0) return undefined;

    const aggregate: Record<string, Record<string, any>> = {};
    const collection = this.db.collection(queryOptions.table);
    for (const operation of operations) {
      const filter = buildAggregateFilter(baseFilter, operation);
      validateFilterShape(filter, queryOptions.table, queryOptions.metadata);
      const planner = new QueryPlanner();
      const plan = planner.plan({
        tableName: queryOptions.table,
        filter,
        metadata: queryOptions.metadata,
        dbType: 'mongodb' as any,
      });

      if (operation.op === 'countRecords') {
        const value = await this.executeRelationCountRecords(
          queryOptions,
          baseFilter,
          operation,
        );
        mergeAggregateValue(aggregate, operation, value);
        continue;
      }

      const match = plan.hasRelationFilters
        ? await resolveMongoFilter(
            filter,
            queryOptions.table,
            queryOptions.metadata,
            this.db,
          )
        : plan.filterTree
          ? renderFilterToMongo(plan.filterTree, {
              metadata: queryOptions.metadata,
              rootTable: queryOptions.table,
            })
          : {};

      let value: any;
      if (operation.op === 'count') {
        value = await collection.countDocuments(match);
      } else {
        const [{ value: aggregateValue } = { value: null }] = await collection
          .aggregate([
            { $match: match },
            {
              $group: {
                _id: null,
                value: { [`$${operation.op}`]: `$${operation.field}` },
              },
            },
          ])
          .toArray();
        value = aggregateValue ?? 0;
      }
      mergeAggregateValue(aggregate, operation, value);
    }

    return aggregate;
  }

  private async executeRelationCountRecords(
    queryOptions: Pick<QueryOptions, 'table' | 'aggregate' | 'metadata'>,
    baseFilter: any,
    operation: NormalizedAggregateOperation,
  ): Promise<number> {
    const tableMeta = queryOptions.metadata?.tables?.get(queryOptions.table);
    const relation = tableMeta?.relations?.find(
      (r: any) => r.propertyName === operation.field,
    );
    const targetTable = relation?.targetTableName || relation?.targetTable;
    const targetMeta = targetTable
      ? queryOptions.metadata?.tables?.get(targetTable)
      : null;
    if (!relation || !targetTable || !targetMeta) return 0;

    const parentMatch = await this.resolveMatchFilter(
      queryOptions.table,
      baseFilter,
      queryOptions.metadata,
    );
    const targetMatch = await this.resolveMatchFilter(
      targetTable,
      operation.condition,
      queryOptions.metadata,
    );

    if (relation.type === 'one-to-many') {
      const parentIds = await this.db
        .collection(queryOptions.table)
        .find(parentMatch, { projection: { _id: 1 } })
        .toArray();
      const ids = parentIds.map((row: any) => row._id).filter((id: any) => id != null);
      if (ids.length === 0) return 0;
      return await this.db.collection(targetTable).countDocuments({
        $and: [targetMatch, { [relation.foreignKeyColumn]: { $in: ids } }],
      });
    }

    if (relation.type === 'many-to-many') {
      const info = resolveMongoJunctionInfo(queryOptions.table, relation);
      if (!info) return 0;
      const parentIds = await this.db
        .collection(queryOptions.table)
        .find(parentMatch, { projection: { _id: 1 } })
        .toArray();
      const ids = parentIds.map((row: any) => row._id).filter((id: any) => id != null);
      if (ids.length === 0) return 0;
      const junctionRows = await this.db
        .collection(info.junctionName)
        .find(
          { [info.selfColumn]: { $in: ids } },
          { projection: { [info.otherColumn]: 1 } },
        )
        .toArray();
      const targetIds = junctionRows
        .map((row: any) => row[info.otherColumn])
        .filter((id: any) => id != null);
      if (targetIds.length === 0) return 0;
      return await this.db.collection(targetTable).countDocuments({
        $and: [targetMatch, { _id: { $in: targetIds } }],
      });
    }

    if (relation.type === 'many-to-one' || relation.type === 'one-to-one') {
      const fkField = relation.foreignKeyColumn || `${operation.field}Id`;
      const parentRows = await this.db
        .collection(queryOptions.table)
        .find(parentMatch, { projection: { [fkField]: 1 } })
        .toArray();
      const ids = parentRows
        .map((row: any) => row[fkField])
        .filter((id: any) => id != null);
      if (ids.length === 0) return 0;
      return await this.db.collection(targetTable).countDocuments({
        $and: [targetMatch, { _id: { $in: ids } }],
      });
    }

    return 0;
  }

  private async resolveMatchFilter(
    tableName: string,
    filter: any,
    metadata: any,
  ): Promise<any> {
    if (!filter || Object.keys(filter).length === 0) return {};
    validateFilterShape(filter, tableName, metadata);
    const planner = new QueryPlanner();
    const plan = planner.plan({
      tableName,
      filter,
      metadata,
      dbType: 'mongodb' as any,
    });
    if (plan.hasRelationFilters) {
      return await resolveMongoFilter(filter, tableName, metadata, this.db);
    }
    return plan.filterTree
      ? renderFilterToMongo(plan.filterTree, {
          metadata,
          rootTable: tableName,
        })
      : {};
  }
}
