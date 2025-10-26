import { Db, Collection } from 'mongodb';
import {
  QueryOptions,
  WhereCondition,
} from '../../../shared/types/query-builder.types';
import { hasLogicalOperators } from '../utils/shared/logical-operators.util';
import { whereToMongoFilter, convertLogicalFilterToMongo } from '../utils/mongo/filter-builder';
import { expandFieldsMongo } from '../utils/mongo/expand-fields';
import { buildNestedLookupPipeline, addProjectionStage } from '../utils/mongo/pipeline-builder';
import { applyMixedFilters } from '../utils/mongo/relation-filter';

export class MongoQueryExecutor {
  private debugLog: any[] = [];
  private readonly db: Db;
  private metadata: any;

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
    pipeline?: any[];
    metadata?: any;
  }): Promise<any> {
    this.metadata = options.metadata;
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
        queryOptions.fields = options.fields.split(',').map(f => f.trim());
      }
    }

    if (options.filter) {
      queryOptions.mongoRawFilter = options.filter;

      if (!hasLogicalOperators(options.filter)) {
        queryOptions.where = [];

        for (const [field, value] of Object.entries(options.filter)) {
          if (typeof value === 'object' && value !== null) {
            const firstKey = Object.keys(value)[0];
            const isOperator = firstKey?.startsWith('_') || ['eq', 'neq', 'gt', 'gte', 'lt', 'lte', 'in', 'like'].includes(firstKey);

            if (!isOperator) {
              continue;
            }

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
      } else {
        queryOptions.mongoLogicalFilter = convertLogicalFilterToMongo(this.metadata, options.filter, options.tableName);
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

    const metaParts = Array.isArray(options.meta)
      ? options.meta
      : (options.meta || '').split(',').map((x) => x.trim()).filter(Boolean);

    let totalCount = 0;
    let filterCount = 0;

    if (metaParts.includes('totalCount') || metaParts.includes('*')) {
      const collection = this.mongoService.collection(options.tableName);
      totalCount = await collection.countDocuments({});
    }

    const hasRelationFilters = queryOptions.mongoRawFilter &&
      Object.keys(queryOptions.mongoRawFilter).some(key => {
        const value = queryOptions.mongoRawFilter[key];
        if (typeof value === 'object' && value !== null) {
          const firstKey = Object.keys(value)[0];
          const isOperator = firstKey?.startsWith('_') || ['eq', 'neq', 'gt', 'gte', 'lt', 'lte', 'in', 'like'].includes(firstKey);
          return !isOperator; // Has relation filter
        }
        return false;
      });

    if (metaParts.includes('filterCount') || metaParts.includes('*')) {
      if (!hasRelationFilters) {
        const collection = this.mongoService.collection(options.tableName);
        let filter = {};

        if (queryOptions.where && queryOptions.where.length > 0) {
          filter = whereToMongoFilter(this.metadata, queryOptions.where, options.tableName);
        }

        filterCount = await collection.countDocuments(filter);
      }
    }

    const results = await this.selectLegacy(queryOptions);

    if (hasRelationFilters && (metaParts.includes('filterCount') || metaParts.includes('*'))) {
      queryOptions.mongoCountOnly = true;
      const countResults = await this.selectLegacy(queryOptions);
      filterCount = countResults.length > 0 ? countResults[0].count : 0;
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

  private async selectLegacy(options: QueryOptions): Promise<any[]> {
    if (options.fields && options.fields.length > 0) {
      const expanded = await expandFieldsMongo(this.metadata, options.table, options.fields);
      options.mongoFieldsExpanded = expanded; // Store for MongoDB usage
    }

    if (options.where) {
      options.where = options.where.map(condition => {
        if (!condition.field.includes('.')) {
          return {
            ...condition,
            field: `${options.table}.${condition.field}`,
          };
        }
        return condition;
      });
    }

    if (options.sort) {
      options.sort = options.sort.map(sortOpt => {
        if (!sortOpt.field.includes('.')) {
          return {
            ...sortOpt,
            field: `${options.table}.${sortOpt.field}`,
          };
        }
        return sortOpt;
      });
    }

    const collection = this.db.collection(options.table);

    if (options.pipeline) {
      if (this.debugLog && this.debugLog.length >= 0) {
        this.debugLog.push({
          type: 'MongoDB Custom Pipeline',
          collection: options.table,
          pipeline: JSON.parse(JSON.stringify(options.pipeline)),
        });
      }
      const results = await collection.aggregate(options.pipeline).toArray();
      return results;
    }

    return this.executeAggregationPipeline(collection, options);
  }

  private async executeAggregationPipeline(collection: Collection, options: QueryOptions): Promise<any[]> {
    const pipeline: any[] = [];

    // Check if we have relation filters that require lookups before limiting
    const hasRelationFilters = options.mongoRawFilter && this.metadata &&
      Object.keys(options.mongoRawFilter).some(key => {
        const tableMeta = this.metadata.tables?.get(options.table);
        if (!tableMeta) return false;
        const relation = tableMeta.relations?.find((r: any) => r.propertyName === key);
        return !!relation; // Is a relation field
      });

    if (options.mongoRawFilter && this.metadata) {
      const tableMeta = this.metadata.tables?.get(options.table);
      if (tableMeta) {
        await applyMixedFilters(this.metadata, pipeline, options.mongoRawFilter, options.table, tableMeta);
      }
    } else if (options.where) {
      const filter = whereToMongoFilter(this.metadata, options.where, options.table);
      pipeline.push({ $match: filter });
    } else if (options.mongoLogicalFilter) {
      pipeline.push({ $match: options.mongoLogicalFilter });
    }

    if (!options.mongoFieldsExpanded) {
      // OPTIMIZATION: Apply sort/limit BEFORE projection for simple queries
      if (options.sort) {
        const sortSpec: any = {};
        for (const sortOpt of options.sort) {
          let fieldName = sortOpt.field.includes('.') ? sortOpt.field.split('.').pop() : sortOpt.field;
          if (fieldName === 'id') {
            fieldName = '_id';
          }
          sortSpec[fieldName] = sortOpt.direction === 'asc' ? 1 : -1;
        }
        pipeline.push({ $sort: sortSpec });
      }

      if (options.mongoCountOnly) {
        pipeline.push({ $count: 'count' });
      } else {
        if (options.offset) {
          pipeline.push({ $skip: options.offset });
        }
        if (options.limit !== undefined && options.limit !== null && options.limit > 0) {
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

      if (this.debugLog && this.debugLog.length >= 0) {
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

      return results;
    }

    const { scalarFields, relations } = options.mongoFieldsExpanded;

    // OPTIMIZATION: If no relation filters, apply sort/limit BEFORE lookups
    // This reduces the number of documents that need to be joined
    if (!hasRelationFilters) {
      if (options.sort) {
        const sortSpec: any = {};
        for (const sortOpt of options.sort) {
          let fieldName = sortOpt.field.includes('.') ? sortOpt.field.split('.').pop() : sortOpt.field;
          if (fieldName === 'id') {
            fieldName = '_id';
          }
          sortSpec[fieldName] = sortOpt.direction === 'asc' ? 1 : -1;
        }
        pipeline.push({ $sort: sortSpec });
      }

      if (!options.mongoCountOnly) {
        if (options.offset) {
          pipeline.push({ $skip: options.offset });
        }
        if (options.limit !== undefined && options.limit !== null && options.limit > 0) {
          pipeline.push({ $limit: options.limit });
        }
      }
    }

    // Now apply lookups (on limited dataset if no relation filters)
    for (const rel of relations) {
      const needsNestedPipeline = rel.nestedFields && rel.nestedFields.length > 0;
      const relationFilter = options.mongoRawFilter?.[rel.propertyName];

      if (needsNestedPipeline) {
        const nestedPipeline = await buildNestedLookupPipeline(
          this.metadata,
          rel.targetTable,
          rel.nestedFields,
          relationFilter
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
            pipeline: nestedPipeline.length > 0 ? nestedPipeline : undefined
          }
        });
      } else if (relationFilter) {
        const nestedPipeline = await buildNestedLookupPipeline(
          this.metadata,
          rel.targetTable,
          ['_id'],
          relationFilter
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
            pipeline: nestedPipeline.length > 0 ? nestedPipeline : undefined
          }
        });
      } else {
        pipeline.push({
          $lookup: {
            from: rel.targetTable,
            localField: rel.localField,
            foreignField: rel.foreignField,
            as: rel.propertyName
          }
        });
      }

      if (rel.type === 'one') {
        pipeline.push({
          $unwind: {
            path: `$${rel.propertyName}`,
            preserveNullAndEmptyArrays: true
          }
        });

        if (relationFilter) {
          pipeline.push({
            $match: {
              [rel.propertyName]: { $ne: null }
            }
          });
        }
      }
    }

    // If we had relation filters, apply sort/limit AFTER lookups
    if (hasRelationFilters) {
      if (options.sort) {
        const sortSpec: any = {};
        for (const sortOpt of options.sort) {
          let fieldName = sortOpt.field.includes('.') ? sortOpt.field.split('.').pop() : sortOpt.field;
          if (fieldName === 'id') {
            fieldName = '_id';
          }
          sortSpec[fieldName] = sortOpt.direction === 'asc' ? 1 : -1;
        }
        pipeline.push({ $sort: sortSpec });
      }

      if (!options.mongoCountOnly) {
        if (options.offset) {
          pipeline.push({ $skip: options.offset });
        }
        if (options.limit !== undefined && options.limit !== null && options.limit > 0) {
          pipeline.push({ $limit: options.limit });
        }
      }
    }

    await addProjectionStage(this.metadata, pipeline, options.table, scalarFields, relations);

    if (options.mongoCountOnly) {
      pipeline.push({ $count: 'count' });
    }

    if (this.debugLog && this.debugLog.length >= 0) {
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

    return results;
  }

  private async addProjectionStage(
    pipeline: any[],
    options: QueryOptions,
    scalarFields: string[],
    relations: any[]
  ): Promise<void> {
    const baseMeta = this.metadata?.tables?.get(options.table);
    const allRelations = baseMeta?.relations || [];
    const allColumns = baseMeta?.columns || [];

    const unpopulatedRelations = allRelations.filter(rel =>
      !relations.some(r => r.propertyName === rel.propertyName)
    );

    const hasWildcard = scalarFields.length === allColumns.length ||
      (scalarFields.length === 0 && relations.length === 0);

    if (unpopulatedRelations.length > 0 || !hasWildcard) {
      const projectStage: any = { _id: 1 };

      for (const field of scalarFields) {
        projectStage[field] = 1;
      }

      for (const rel of relations) {
        projectStage[rel.propertyName] = 1;
      }

      for (const rel of unpopulatedRelations) {
        const isInverse = rel.type === 'one-to-many' ||
          (rel.type === 'many-to-many' && rel.mappedBy);

        if (isInverse) {
          continue; // Inverse relations not stored, skip mapping
        }

        const isArray = rel.type === 'many-to-many';

        if (isArray) {
          projectStage[rel.propertyName] = {
            $map: {
              input: `$${rel.propertyName}`,
              as: 'item',
              in: { _id: '$$item' }
            }
          };
        } else {
          projectStage[rel.propertyName] = {
            $cond: {
              if: { $ne: [`$${rel.propertyName}`, null] },
              then: { _id: `$${rel.propertyName}` },
              else: null
            }
          };
        }
      }

      pipeline.push({ $project: projectStage });
    }
  }

}
