import { Db, Collection } from 'mongodb';
import {
  QueryOptions,
  WhereCondition,
} from '../../../shared/types/query-builder.types';
import { hasLogicalOperators } from '../utils/build-where-clause';

export class MongoQueryExecutor {
  private debugLog: any[] = [];
  private readonly db: Db;

  constructor(
    private readonly mongoService: any,
    private readonly metadataCache: any,
  ) {
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
  }): Promise<any> {
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

    if (options.filter && !hasLogicalOperators(options.filter)) {
      queryOptions.where = [];

      queryOptions.mongoRawFilter = options.filter;

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
          filter = this.whereToMongoFilter(queryOptions.where);
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
      const expanded = await this.expandFieldsMongo(options.table, options.fields);
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
      const results = await collection.aggregate(options.pipeline).toArray();
      return this.transformMongoResults(results);
    }

    if (options.mongoFieldsExpanded) {
      return this.executeAggregationPipeline(collection, options);
    }

    return this.executeSimpleQuery(collection, options);
  }

  private async executeAggregationPipeline(collection: Collection, options: QueryOptions): Promise<any[]> {
    const { scalarFields, relations } = options.mongoFieldsExpanded!;
    const pipeline: any[] = [];

    if (options.where) {
      const filter = this.whereToMongoFilter(options.where);
      pipeline.push({ $match: filter });
    }

    for (const rel of relations) {
      const needsNestedPipeline = rel.nestedFields && rel.nestedFields.length > 0;

      if (needsNestedPipeline) {
        const nestedPipeline = await this.buildNestedLookupPipeline(rel.targetTable, rel.nestedFields);

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
      }
    }

    if (options.mongoRawFilter) {
      const relationMatchConditions: any = {};

      for (const [field, value] of Object.entries(options.mongoRawFilter)) {
        const wasLookedUp = relations.some(r => r.propertyName === field);

        if (wasLookedUp && typeof value === 'object' && value !== null) {
          this.buildRelationMatchCondition(field, value, relationMatchConditions);
        }
      }

      if (Object.keys(relationMatchConditions).length > 0) {
        pipeline.push({ $match: relationMatchConditions });
      }
    }

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

    await this.addProjectionStage(pipeline, options, scalarFields, relations);

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

    const results = await collection.aggregate(pipeline).toArray();

    if (options.mongoCountOnly) {
      return results;
    }

    return this.transformMongoResults(results);
  }

  private async addProjectionStage(
    pipeline: any[],
    options: QueryOptions,
    scalarFields: string[],
    relations: any[]
  ): Promise<void> {
    const baseMeta = await this.metadataCache.lookupTableByName(options.table);
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

  private async executeSimpleQuery(collection: Collection, options: QueryOptions): Promise<any[]> {
    const filter = options.where ? this.whereToMongoFilter(options.where) : {};
    let cursor = collection.find(filter);

    if (options.select) {
      const projection: any = {};
      for (const field of options.select) {
        projection[field] = 1;
      }
      cursor = cursor.project(projection);
    }

    if (options.sort) {
      const sortSpec: any = {};
      for (const sortOpt of options.sort) {
        sortSpec[sortOpt.field] = sortOpt.direction === 'asc' ? 1 : -1;
      }
      cursor = cursor.sort(sortSpec);
    }

    if (options.offset) {
      cursor = cursor.skip(options.offset);
    }

    if (options.limit !== undefined && options.limit !== null && options.limit > 0) {
      cursor = cursor.limit(options.limit);
    }

    const results = await cursor.toArray();
    return this.transformMongoResults(results);
  }

  private transformMongoResults(documents: any[]): any[] {
    return documents.map(doc => this.mongoService['mapDocument'](doc));
  }

  private whereToMongoFilter(conditions: WhereCondition[]): any {
    const filter: any = {};
    const { ObjectId } = require('mongodb');

    for (const condition of conditions) {
      let fieldName = condition.field.includes('.') ? condition.field.split('.').pop() : condition.field;

      if (fieldName === 'id') {
        fieldName = '_id';
      }

      let value = condition.value;
      if (fieldName === '_id' && typeof value === 'string') {
        try {
          value = new ObjectId(value);
        } catch (err) {
        }
      }

      switch (condition.operator) {
        case '=':
          filter[fieldName] = value;
          break;
        case '!=':
          filter[fieldName] = { $ne: value };
          break;
        case '>':
          filter[fieldName] = { $gt: value };
          break;
        case '<':
          filter[fieldName] = { $lt: value };
          break;
        case '>=':
          filter[fieldName] = { $gte: value };
          break;
        case '<=':
          filter[fieldName] = { $lte: value };
          break;
        case 'like':
          filter[fieldName] = { $regex: value.replace(/%/g, '.*'), $options: 'i' };
          break;
        case 'in':
          const inValues = fieldName === '_id'
            ? (value as any[]).map(v => typeof v === 'string' ? new ObjectId(v) : v)
            : value;
          filter[fieldName] = { $in: inValues };
          break;
        case 'not in':
          const ninValues = fieldName === '_id'
            ? (value as any[]).map(v => typeof v === 'string' ? new ObjectId(v) : v)
            : value;
          filter[fieldName] = { $nin: ninValues };
          break;
        case 'is null':
          filter[fieldName] = null;
          break;
        case 'is not null':
          filter[fieldName] = { $ne: null };
          break;
        case '_contains':
          const escapedContains = String(value).replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
          filter[fieldName] = { $regex: escapedContains, $options: 'i' };
          break;
        case '_starts_with':
          const escapedStarts = String(value).replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
          filter[fieldName] = { $regex: `^${escapedStarts}`, $options: 'i' };
          break;
        case '_ends_with':
          const escapedEnds = String(value).replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
          filter[fieldName] = { $regex: `${escapedEnds}$`, $options: 'i' };
          break;
        case '_between':
          if (Array.isArray(value) && value.length === 2) {
            filter[fieldName] = { $gte: value[0], $lte: value[1] };
          }
          break;
        case '_is_null':
          filter[fieldName] = value === true ? null : { $ne: null };
          break;
        case '_is_not_null':
          filter[fieldName] = value === true ? { $ne: null } : null;
          break;
      }
    }

    return filter;
  }

  private buildRelationMatchCondition(relationName: string, nestedFilter: any, output: any): void {
    for (const [field, value] of Object.entries(nestedFilter)) {
      if (typeof value === 'object' && value !== null) {
        for (const [op, val] of Object.entries(value)) {
          const fullFieldPath = `${relationName}.${field}`;

          switch (op) {
            case '_contains':
              const escapedContains = String(val).replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
              output[fullFieldPath] = { $regex: escapedContains, $options: 'i' };
              break;
            case '_starts_with':
              const escapedStarts = String(val).replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
              output[fullFieldPath] = { $regex: `^${escapedStarts}`, $options: 'i' };
              break;
            case '_ends_with':
              const escapedEnds = String(val).replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
              output[fullFieldPath] = { $regex: `${escapedEnds}$`, $options: 'i' };
              break;
            case '_eq':
              output[fullFieldPath] = val;
              break;
            case '_neq':
              output[fullFieldPath] = { $ne: val };
              break;
            case '_in':
              output[fullFieldPath] = { $in: val };
              break;
            case '_not_in':
              output[fullFieldPath] = { $nin: val };
              break;
            case '_gt':
              output[fullFieldPath] = { $gt: val };
              break;
            case '_gte':
              output[fullFieldPath] = { $gte: val };
              break;
            case '_lt':
              output[fullFieldPath] = { $lt: val };
              break;
            case '_lte':
              output[fullFieldPath] = { $lte: val };
              break;
            case '_is_null':
              output[fullFieldPath] = val === true ? null : { $ne: null };
              break;
            case '_is_not_null':
              output[fullFieldPath] = val === true ? { $ne: null } : null;
              break;
          }
        }
      }
    }
  }

  private async buildNestedLookupPipeline(
    tableName: string,
    nestedFields: string[]
  ): Promise<any[]> {
    const nestedExpanded = await this.expandFieldsMongo(tableName, nestedFields);
    const nestedPipeline: any[] = [];

    for (const nestedRel of nestedExpanded.relations) {
      const nestedNestedPipeline = nestedRel.nestedFields && nestedRel.nestedFields.length > 0
        ? await this.buildNestedLookupPipeline(nestedRel.targetTable, nestedRel.nestedFields)
        : [];

      nestedPipeline.push({
        $lookup: {
          from: nestedRel.targetTable,
          localField: nestedRel.localField,
          foreignField: nestedRel.foreignField,
          as: nestedRel.propertyName,
          pipeline: nestedNestedPipeline.length > 0 ? nestedNestedPipeline : undefined
        }
      });

      if (nestedRel.type === 'one') {
        nestedPipeline.push({
          $unwind: {
            path: `$${nestedRel.propertyName}`,
            preserveNullAndEmptyArrays: true
          }
        });
      }
    }

    const baseMeta = await this.metadataCache.lookupTableByName(tableName);
    const allRelations = baseMeta?.relations || [];

    const unpopulatedRelations = allRelations.filter(rel =>
      !nestedExpanded.relations.some(r => r.propertyName === rel.propertyName)
    );

    if (nestedExpanded.scalarFields.length > 0 || nestedExpanded.relations.length > 0 || unpopulatedRelations.length > 0) {
      const projection: any = { _id: 1 };

      for (const field of nestedExpanded.scalarFields) {
        projection[field] = 1;
      }

      for (const nestedRel of nestedExpanded.relations) {
        projection[nestedRel.propertyName] = 1;
      }

      const hasWildcard = nestedFields.includes('*');

      if (hasWildcard) {
        for (const rel of unpopulatedRelations) {
          const isInverse = rel.type === 'one-to-many' || (rel.type === 'many-to-many' && rel.mappedBy);
          if (isInverse) continue;

          const isArray = rel.type === 'many-to-many';

          if (isArray) {
            projection[rel.propertyName] = {
              $map: {
                input: `$${rel.propertyName}`,
                as: 'item',
                in: { _id: '$$item' }
              }
            };
          } else {
            projection[rel.propertyName] = {
              $cond: {
                if: { $ne: [`$${rel.propertyName}`, null] },
                then: { _id: `$${rel.propertyName}` },
                else: null
              }
            };
          }
        }
      }

      nestedPipeline.push({ $project: projection });
    }

    return nestedPipeline;
  }

  private async expandFieldsMongo(
    tableName: string,
    fields: string[]
  ): Promise<{
    scalarFields: string[];  // Regular fields to include
    relations: Array<{      // Relations to $lookup
      propertyName: string;
      targetTable: string;
      localField: string;
      foreignField: string;
      type: 'one' | 'many';
      nestedFields: string[]; // Fields to include from related table (can be nested like 'methods.*')
    }>;
  }> {
    if (!this.metadataCache) {
      return { scalarFields: [], relations: [] };
    }

    const baseMeta = await this.metadataCache.getTableMetadata(tableName);
    if (!baseMeta) {
      return { scalarFields: [], relations: [] };
    }

    const fieldsByRelation = new Map<string, string[]>();

    for (const field of fields) {
      if (field === '*') {
        if (!fieldsByRelation.has('')) {
          fieldsByRelation.set('', []);
        }
        fieldsByRelation.get('')!.push(field);
      } else if (field.includes('.')) {
        const parts = field.split('.');
        const relationName = parts[0];
        const remainingPath = parts.slice(1).join('.');

        if (!fieldsByRelation.has(relationName)) {
          fieldsByRelation.set(relationName, []);
        }
        fieldsByRelation.get(relationName)!.push(remainingPath);
      } else {
        const isRelation = baseMeta.relations?.some(r => r.propertyName === field);

        if (isRelation) {
          if (!fieldsByRelation.has(field)) {
            fieldsByRelation.set(field, ['_id']);
          }
        } else {
          if (!fieldsByRelation.has('')) {
            fieldsByRelation.set('', []);
          }
          fieldsByRelation.get('')!.push(field);
        }
      }
    }

    const scalarFields: string[] = [];
    const relations: Array<any> = [];

    const rootFields = fieldsByRelation.get('') || [];
    for (const field of rootFields) {
      if (field === '*') {
        if (baseMeta.columns) {
          for (const col of baseMeta.columns) {
            if (!scalarFields.includes(col.name)) {
              scalarFields.push(col.name);
            }
          }
        }

        if (baseMeta.relations) {
          for (const rel of baseMeta.relations) {
            if (!fieldsByRelation.has(rel.propertyName)) {
              fieldsByRelation.set(rel.propertyName, ['_id']);
            }
          }
        }
      } else {
        if (!scalarFields.includes(field)) {
          scalarFields.push(field);
        }
      }
    }

    for (const [relationName, nestedFields] of fieldsByRelation.entries()) {
      if (relationName === '') continue;

      const rel = baseMeta.relations?.find(r => r.propertyName === relationName);
      if (!rel) {
        continue;
      }

      let localField: string;
      let foreignField: string;
      let isInverse = false;

      if (rel.type === 'many-to-one' || rel.type === 'one-to-one') {
        localField = rel.propertyName;
        foreignField = '_id';
        isInverse = false;
      }
      else if (rel.type === 'one-to-many') {
        localField = '_id';
        foreignField = rel.inversePropertyName || rel.propertyName;
        isInverse = true;
      }
      else if (rel.type === 'many-to-many') {
        if (rel.mappedBy) {
          localField = '_id';
          foreignField = rel.mappedBy; // Owner field name in target table
          isInverse = true;
        } else {
          localField = rel.propertyName;
          foreignField = '_id';
          isInverse = false;
        }
      }

      const isToMany = rel.type === 'one-to-many' || rel.type === 'many-to-many';

      relations.push({
        propertyName: relationName,
        targetTable: rel.targetTableName,
        localField,
        foreignField,
        type: isToMany ? 'many' : 'one',
        isInverse,
        nestedFields: nestedFields
      });
    }

    return { scalarFields, relations };
  }
}
