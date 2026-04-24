import { Db, Collection } from 'mongodb';
import { QueryOptions } from '../../../shared/types/query-builder.types';
import { expandFieldsMongo } from '../utils/mongo/expand-fields';
import { executeAggregationPipeline } from '../utils/mongo/mongo-aggregation-builder';
import { renderFieldsToMongo } from '../utils/mongo/render-fields';
import { validateFilterShape } from '../../../domain/query-dsl/filter-sanitizer.util';
import { QueryPlanner } from '../../../domain/query-dsl/query-planner';
import { QueryPlan } from '../../../domain/query-dsl/query-plan.types';
import { normalizeMongoDocument } from '../../mongo/utils/normalize-mongo-document.util';
import { renderFilterToMongo } from '../utils/mongo/render-filter';
import { hasAnyRelations } from '../utils/shared/filter-separator.util';

export class MongoQueryExecutor {
  private debugLog: any[] = [];
  private readonly db: Db;
  private metadata: any;
  private dbType: string;
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
}
