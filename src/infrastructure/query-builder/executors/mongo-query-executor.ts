import { Db, Collection, ObjectId } from 'mongodb';
import {
  QueryOptions,
  WhereCondition,
} from '../../../shared/types/query-builder.types';
import { hasLogicalOperators } from '../utils/shared/logical-operators.util';
import {
  whereToMongoFilter,
  convertLogicalFilterToMongo,
} from '../utils/mongo/filter-builder';
import { expandFieldsMongo } from '../utils/mongo/expand-fields';
import {
  buildNestedLookupPipeline,
  addProjectionStage,
} from '../utils/mongo/pipeline-builder';
import { applyMixedFilters } from '../utils/mongo/relation-filter';
import { QueryPlan } from '../planner/query-plan.types';

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
      queryOptions.mongoRawFilter = options.filter;

      if (!hasLogicalOperators(options.filter)) {
        queryOptions.where = [];

        for (const [field, value] of Object.entries(options.filter)) {
          if (typeof value === 'object' && value !== null) {
            const firstKey = Object.keys(value)[0];
            const isOperator =
              firstKey?.startsWith('_') ||
              ['eq', 'neq', 'gt', 'gte', 'lt', 'lte', 'in', 'like'].includes(
                firstKey,
              );

            if (!isOperator) {
              continue;
            }

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
              else operator = op.replace('_', ' ');

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
      } else {
        queryOptions.mongoLogicalFilter = convertLogicalFilterToMongo(
          this.metadata,
          options.filter,
          options.tableName,
          this.dbType,
        );
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

    const hasRelationFilters =
      queryOptions.mongoRawFilter &&
      Object.keys(queryOptions.mongoRawFilter).some((key) => {
        const value = queryOptions.mongoRawFilter[key];
        if (typeof value === 'object' && value !== null) {
          const firstKey = Object.keys(value)[0];
          const isOperator =
            firstKey?.startsWith('_') ||
            ['eq', 'neq', 'gt', 'gte', 'lt', 'lte', 'in', 'like'].includes(
              firstKey,
            );
          return !isOperator; // Has relation filter
        }
        return false;
      });

    if (metaParts.includes('filterCount') || metaParts.includes('*')) {
      if (!hasRelationFilters) {
        const collection = this.mongoService.collection(options.tableName);
        let filter = {};

        if (queryOptions.where && queryOptions.where.length > 0) {
          filter = whereToMongoFilter(
            this.metadata,
            queryOptions.where,
            options.tableName,
            this.dbType,
          );
        }

        filterCount = await collection.countDocuments(filter);
      }
    }

    this.lastBuiltPipeline = null;
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
      const expanded = await expandFieldsMongo(
        this.metadata,
        options.table,
        options.fields,
      );
      options.mongoFieldsExpanded = expanded; // Store for MongoDB usage
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
      return this.normalizeMongoResults(results);
    }

    return this.executeAggregationPipeline(collection, options);
  }

  private async executeAggregationPipeline(
    collection: Collection,
    options: QueryOptions,
  ): Promise<any[]> {
    const pipeline: any[] = [];

    const hasRelationFilters =
      options.mongoRawFilter &&
      this.metadata &&
      Object.keys(options.mongoRawFilter).some((key) => {
        const tableMeta = this.metadata.tables?.get(options.table);
        if (!tableMeta) return false;
        const relation = tableMeta.relations?.find(
          (r: any) => r.propertyName === key,
        );
        return !!relation; // Is a relation field
      });

    if (options.mongoRawFilter && this.metadata) {
      const tableMeta = this.metadata.tables?.get(options.table);
      if (tableMeta) {
        await applyMixedFilters(
          this.metadata,
          pipeline,
          options.mongoRawFilter,
          options.table,
          tableMeta,
          this.dbType,
        );
      }
    } else if (options.where) {
      const filter = whereToMongoFilter(
        this.metadata,
        options.where,
        options.table,
        this.dbType,
      );
      pipeline.push({ $match: filter });
    } else if (options.mongoLogicalFilter) {
      pipeline.push({ $match: options.mongoLogicalFilter });
    }

    const hasSortOnRelation =
      options.sort?.some((s) => {
        if (!s.field.includes('.')) return false;
        const relName = s.field.split('.')[0];
        return (
          options.mongoFieldsExpanded?.relations?.some(
            (r) => r.propertyName === relName,
          ) ?? false
        );
      }) ?? false;

    const sortAfterJoins = hasRelationFilters || hasSortOnRelation;

    if (!options.mongoFieldsExpanded) {
      if (options.sort) {
        pipeline.push({ $sort: this.buildMongoSortSpec(options.sort) });
      }

      if (options.mongoCountOnly) {
        pipeline.push({ $count: 'count' });
      } else {
        if (options.offset) {
          pipeline.push({ $skip: options.offset });
        }
        if (
          options.limit !== undefined &&
          options.limit !== null &&
          options.limit > 0
        ) {
          pipeline.push({ $limit: options.limit });
        }
      }

      if (options.select) {
        const projection: any = {};
        for (const field of options.select) {
          projection[field] = 1;
        }
        pipeline.push({ $project: projection });
      }

      if (this.debugLog) {
        this.debugLog.push({
          type: 'MongoDB Aggregation Pipeline',
          collection: options.table,
          pipeline: JSON.parse(JSON.stringify(pipeline)),
        });
      }

      const results = await collection.aggregate(pipeline).toArray();

      if (options.mongoCountOnly) {
        return results;
      }

      return this.normalizeMongoResults(results);
    }

    const { scalarFields, relations } = options.mongoFieldsExpanded;

    if (!sortAfterJoins) {
      if (options.sort) {
        pipeline.push({ $sort: this.buildMongoSortSpec(options.sort) });
      }

      if (!options.mongoCountOnly) {
        if (options.offset) {
          pipeline.push({ $skip: options.offset });
        }
        if (
          options.limit !== undefined &&
          options.limit !== null &&
          options.limit > 0
        ) {
          pipeline.push({ $limit: options.limit });
        }
      }
    }

    for (const rel of relations) {
      const needsNestedPipeline =
        rel.nestedFields && rel.nestedFields.length > 0;
      const relationFilter = options.mongoRawFilter?.[rel.propertyName];

      if (needsNestedPipeline) {
        const nestedPipeline = await buildNestedLookupPipeline(
          this.metadata,
          rel.targetTable,
          rel.nestedFields,
          relationFilter,
        );

        if (rel.type === 'one' && nestedPipeline.length > 0) {
          nestedPipeline.push({ $limit: 1 });
        }

        pipeline.push({
          $lookup: {
            from: rel.targetTable,
            localField: rel.localField,
            foreignField: rel.foreignField,
            as: rel.propertyName,
            pipeline: nestedPipeline.length > 0 ? nestedPipeline : undefined,
          },
        });
      } else if (relationFilter) {
        const nestedPipeline = await buildNestedLookupPipeline(
          this.metadata,
          rel.targetTable,
          ['_id'],
          relationFilter,
        );

        if (rel.type === 'one' && nestedPipeline.length > 0) {
          nestedPipeline.push({ $limit: 1 });
        }

        pipeline.push({
          $lookup: {
            from: rel.targetTable,
            localField: rel.localField,
            foreignField: rel.foreignField,
            as: rel.propertyName,
            pipeline: nestedPipeline.length > 0 ? nestedPipeline : undefined,
          },
        });
      } else {
        pipeline.push({
          $lookup: {
            from: rel.targetTable,
            localField: rel.localField,
            foreignField: rel.foreignField,
            as: rel.propertyName,
          },
        });
      }

      if (rel.type === 'one') {
        pipeline.push({
          $unwind: {
            path: `$${rel.propertyName}`,
            preserveNullAndEmptyArrays: true,
          },
        });

        if (relationFilter) {
          const hasIsNullFilter =
            this.checkIfFilterContainsIsNull(relationFilter);
          if (!hasIsNullFilter) {
            pipeline.push({
              $match: {
                [rel.propertyName]: { $ne: null },
              },
            });
          }
        }
      }
    }

    if (sortAfterJoins) {
      if (options.sort) {
        pipeline.push({ $sort: this.buildMongoSortSpec(options.sort) });
      }

      if (!options.mongoCountOnly) {
        if (options.offset) {
          pipeline.push({ $skip: options.offset });
        }
        if (
          options.limit !== undefined &&
          options.limit !== null &&
          options.limit > 0
        ) {
          pipeline.push({ $limit: options.limit });
        }
      }
    }

    await addProjectionStage(
      this.metadata,
      pipeline,
      options.table,
      scalarFields,
      relations,
    );

    if (options.mongoCountOnly) {
      pipeline.push({ $count: 'count' });
    }

    if (this.debugLog) {
      this.debugLog.push({
        type: 'MongoDB Aggregation Pipeline',
        collection: options.table,
        pipeline: JSON.parse(JSON.stringify(pipeline)),
      });
    }

    this.lastBuiltPipeline = pipeline;
    const results = await collection.aggregate(pipeline).toArray();

    if (options.mongoCountOnly) {
      return results;
    }

    return this.normalizeMongoResults(results);
  }

  private buildMongoSortSpec(
    sort: Array<{ field: string; direction: 'asc' | 'desc' }>,
  ): Record<string, 1 | -1> {
    const spec: Record<string, 1 | -1> = {};
    for (const sortOpt of sort) {
      let mongoField = sortOpt.field;

      if (mongoField === 'id') mongoField = '_id';

      spec[mongoField] = sortOpt.direction === 'asc' ? 1 : -1;
    }
    return spec;
  }

  private checkIfFilterContainsIsNull(filter: any): boolean {
    if (!filter || typeof filter !== 'object') {
      return false;
    }

    if (filter === null) {
      return true;
    }

    if (Array.isArray(filter)) {
      return filter.some((item) => this.checkIfFilterContainsIsNull(item));
    }

    if ('_or' in filter && Array.isArray(filter._or)) {
      return filter._or.some((condition: any) =>
        this.checkIfFilterContainsIsNull(condition),
      );
    }

    if ('_and' in filter && Array.isArray(filter._and)) {
      return filter._and.some((condition: any) =>
        this.checkIfFilterContainsIsNull(condition),
      );
    }

    if ('_not' in filter) {
      return this.checkIfFilterContainsIsNull(filter._not);
    }

    for (const [key, value] of Object.entries(filter)) {
      if (value === null) {
        if (key === '_eq' || key === '$eq') {
          return true;
        }
        continue;
      }

      if (typeof value === 'object') {
        if (
          '_is_null' in value &&
          (value._is_null === true || value._is_null === 'true')
        ) {
          return true;
        }
        if ('_eq' in value && value._eq === null) {
          return true;
        }
        if ('$eq' in value && value.$eq === null) {
          return true;
        }
        if (this.checkIfFilterContainsIsNull(value)) {
          return true;
        }
      }
    }

    return false;
  }

  private normalizeMongoResults(results: any[]): any[] {
    return results.map((result) => this.normalizeMongoObject(result));
  }

  private normalizeMongoObject(obj: any): any {
    if (!obj || typeof obj !== 'object') {
      return obj;
    }

    if (obj instanceof ObjectId) {
      return obj.toString();
    }

    if (obj instanceof Date) {
      return obj.toISOString();
    }

    if (Array.isArray(obj)) {
      return obj.map((item) => this.normalizeMongoObject(item));
    }

    if (
      'buffer' in obj &&
      obj.buffer &&
      typeof obj.buffer === 'object' &&
      Object.keys(obj.buffer).length === 12
    ) {
      try {
        const bufferObj = obj.buffer as Record<string, number>;
        const bufferArray = Object.keys(bufferObj)
          .sort((a, b) => parseInt(a) - parseInt(b))
          .map((key) => bufferObj[key]);
        const objectId = new ObjectId(Buffer.from(bufferArray));
        return objectId.toString();
      } catch {}
    }

    const normalized: any = {};
    for (const [key, value] of Object.entries(obj)) {
      if (value instanceof ObjectId) {
        normalized[key] = value.toString();
      } else if (value instanceof Date) {
        normalized[key] = value.toISOString();
      } else if (
        value &&
        typeof value === 'object' &&
        !(value instanceof Buffer)
      ) {
        if (
          'buffer' in value &&
          value.buffer &&
          typeof value.buffer === 'object' &&
          Object.keys(value.buffer).length === 12
        ) {
          try {
            const bufferObj = value.buffer as Record<string, number>;
            const bufferArray = Object.keys(bufferObj)
              .sort((a, b) => parseInt(a) - parseInt(b))
              .map((key) => bufferObj[key]);
            const objectId = new ObjectId(Buffer.from(bufferArray));
            normalized[key] = objectId.toString();
          } catch {
            normalized[key] = this.normalizeMongoObject(value);
          }
        } else {
          normalized[key] = this.normalizeMongoObject(value);
        }
      } else {
        normalized[key] = value;
      }
    }

    return normalized;
  }
}
