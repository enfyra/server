import { Injectable, Optional, Inject, forwardRef } from '@nestjs/common';
import { Knex } from 'knex';
import { KnexService } from '../knex/knex.service';
import { MongoService } from '../mongo/services/mongo.service';
import { MetadataCacheService } from '../cache/services/metadata-cache.service';
import {
  DatabaseType,
  QueryOptions,
  WhereCondition,
  InsertOptions,
  UpdateOptions,
  DeleteOptions,
  CountOptions,
} from '../../shared/types/query-builder.types';
import { expandFieldsToJoinsAndSelect } from './utils/expand-fields';
import { buildWhereClause, hasLogicalOperators } from './utils/build-where-clause';
import { separateFilters, applyRelationFilters } from './utils/relation-filter.util';

/**
 * QueryBuilderService - Unified database query interface
 * Provides same syntax for both SQL and MongoDB
 * Converts unified query to appropriate database query
 */
@Injectable()
export class QueryBuilderService {
  private dbType: DatabaseType;
  private debugLog: any[] = [];

  constructor(
    @Optional() @Inject(forwardRef(() => KnexService))
    private readonly knexService: KnexService,
    @Optional() @Inject(forwardRef(() => MongoService))
    private readonly mongoService: MongoService,
    @Optional() @Inject(forwardRef(() => MetadataCacheService))
    private readonly metadataCache: MetadataCacheService,
  ) {
    this.dbType = (process.env.DB_TYPE as DatabaseType);
  }

  getDbType(): DatabaseType {
    return this.dbType;
  }

  private pushDebug(key: string, data: any): void {
    this.debugLog.push({ [key]: data });
  }

  /**
   * Convert unified WHERE conditions to Knex query
   */
  private applyWhereToKnex(query: any, conditions: WhereCondition[]): any {
    for (const condition of conditions) {
      switch (condition.operator) {
        case '=':
          query = query.where(condition.field, '=', condition.value);
          break;
        case '!=':
          query = query.where(condition.field, '!=', condition.value);
          break;
        case '>':
          query = query.where(condition.field, '>', condition.value);
          break;
        case '<':
          query = query.where(condition.field, '<', condition.value);
          break;
        case '>=':
          query = query.where(condition.field, '>=', condition.value);
          break;
        case '<=':
          query = query.where(condition.field, '<=', condition.value);
          break;
        case 'like':
          query = query.where(condition.field, 'like', condition.value);
          break;
        case 'in':
          query = query.whereIn(condition.field, condition.value);
          break;
        case 'not in':
          query = query.whereNotIn(condition.field, condition.value);
          break;
        case 'is null':
          query = query.whereNull(condition.field);
          break;
        case 'is not null':
          query = query.whereNotNull(condition.field);
          break;
      }
    }
    return query;
  }

  /**
   * Convert unified WHERE conditions to MongoDB filter
   */
  private whereToMongoFilter(conditions: WhereCondition[]): any {
    const { ObjectId } = require('mongodb');
    const filter: any = {};

    for (const condition of conditions) {
      // MongoDB field name normalization:
      // - 'id' -> '_id' (MongoDB primary key)
      // - 'sourceTableId' -> 'sourceTable' (relation field in relation_definition)
      // - 'targetTableId' -> 'targetTable' (relation field in relation_definition)
      let fieldName = condition.field;
      let fieldValue = condition.value;

      if (fieldName === 'id') {
        fieldName = '_id';
        // Convert string ID to ObjectId for MongoDB
        if (typeof fieldValue === 'string' && fieldValue.length === 24) {
          fieldValue = new ObjectId(fieldValue);
        }
        console.log(`[WHERE-TO-MONGO] Converting 'id' to '_id' for condition:`, condition);
      } else if (fieldName === 'sourceTableId') {
        fieldName = 'sourceTable';
        // Convert string ID to ObjectId for MongoDB relation fields
        if (typeof fieldValue === 'string' && fieldValue.length === 24) {
          fieldValue = new ObjectId(fieldValue);
        }
        console.log(`[WHERE-TO-MONGO] Converting 'sourceTableId' to 'sourceTable' for condition:`, condition, '-> ObjectId');
      } else if (fieldName === 'targetTableId') {
        fieldName = 'targetTable';
        // Convert string ID to ObjectId for MongoDB relation fields
        if (typeof fieldValue === 'string' && fieldValue.length === 24) {
          fieldValue = new ObjectId(fieldValue);
        }
        console.log(`[WHERE-TO-MONGO] Converting 'targetTableId' to 'targetTable' for condition:`, condition);
      }

      switch (condition.operator) {
        case '=':
          filter[fieldName] = fieldValue;
          break;
        case '!=':
          filter[fieldName] = { $ne: fieldValue };
          break;
        case '>':
          filter[fieldName] = { $gt: fieldValue };
          break;
        case '<':
          filter[fieldName] = { $lt: fieldValue };
          break;
        case '>=':
          filter[fieldName] = { $gte: fieldValue };
          break;
        case '<=':
          filter[fieldName] = { $lte: fieldValue };
          break;
        case 'like':
          filter[fieldName] = { $regex: fieldValue.replace(/%/g, '.*') };
          break;
        case 'in':
          filter[fieldName] = { $in: fieldValue };
          break;
        case 'not in':
          filter[fieldName] = { $nin: fieldValue };
          break;
        case 'is null':
          filter[fieldName] = null;
          break;
        case 'is not null':
          filter[fieldName] = { $ne: null };
          break;
      }
    }
    
    return filter;
  }

  /**
   * Expand MongoDB nested wildcard fields to $lookup joins
   * Example: 'mainTable.*' -> { table: 'table_definition', on: { local: 'mainTable', foreign: '_id' } }
   */
  private async expandMongoNestedFields(tableName: string, fields: string[]): Promise<any[]> {
    const joins: any[] = [];
    const metadata = await this.metadataCache.getTableMetadata(tableName);

    console.log(`[EXPAND-MONGO] Table: ${tableName}, Fields:`, fields);
    console.log(`[EXPAND-MONGO] Metadata relations:`, metadata?.relations?.map((r: any) => r.propertyName));

    if (!metadata) {
      console.log(`[EXPAND-MONGO] No metadata found`);
      return joins;
    }

    for (const field of fields) {
      if (!field.includes('.') || !field.includes('*')) {
        continue; // Not a nested wildcard
      }

      // Extract relation name from 'relationName.*'
      const relationName = field.split('.')[0];
      const relation = metadata.relations?.find((r: any) => r.propertyName === relationName);

      console.log(`[EXPAND-MONGO] Field: ${field}, RelName: ${relationName}, Found: ${!!relation}`);

      if (!relation) {
        continue; // Relation not found
      }

      console.log(`[EXPAND-MONGO] Relation object:`, JSON.stringify(relation, null, 2));
      console.log(`[EXPAND-MONGO] relation.targetTableName =`, relation.targetTableName);
      console.log(`[EXPAND-MONGO] relation.targetTable =`, relation.targetTable);

      // Build MongoDB $lookup join
      const join = {
        table: relation.targetTableName,
        on: {
          local: relationName,
          foreign: '_id',
        },
      };
      console.log(`[EXPAND-MONGO] Adding join:`, join);
      joins.push(join);
    }

    console.log(`[EXPAND-MONGO] Total joins: ${joins.length}`);
    return joins;
  }

  /**
   * Insert records (one or multiple)
   */
  async insert(options: InsertOptions): Promise<any> {
    if (this.dbType === 'mongodb') {
      const collection = this.mongoService.collection(options.table);
      if (Array.isArray(options.data)) {
        // Process nested relations for each record
        const processedData = await Promise.all(
          options.data.map(record => this.mongoService.processNestedRelations(options.table, record))
        );
        
        // Apply timestamps hook
        const dataWithTimestamps = this.mongoService.applyTimestamps(processedData);
        const result = await collection.insertMany(dataWithTimestamps as any[]);
        return Object.values(result.insertedIds).map((id, idx) => ({
          id: id.toString(),
          ...(dataWithTimestamps as any[])[idx],
        }));
      } else {
        return this.mongoService.insertOne(options.table, options.data);
      }
    }
    
    // SQL: Use KnexService.insertWithCascade for automatic relation handling
    if (Array.isArray(options.data)) {
      // Handle multiple records
      const results = [];
      for (const record of options.data) {
        const result = await this.knexService.insertWithCascade(options.table, record);
        results.push(result);
      }
      return results;
    } else {
      // Handle single record
      return await this.knexService.insertWithCascade(options.table, options.data);
    }
  }

  /**
   * SQL Query Executor - Executes queries with Directus/queryEngine-style parameters
   * This is the target method for SqlQueryEngine
   *
   * @param options - Query options in queryEngine format (tableName, fields, filter, sort, page, limit, meta, deep)
   * @returns {data, meta?} - Results wrapped in data property with optional metadata
   */
  async sqlExecutor(options: {
    tableName: string;
    fields?: string | string[];
    filter?: any;
    sort?: string | string[];
    page?: number;
    limit?: number;
    meta?: string;
    deep?: Record<string, any>;
    debugMode?: boolean;
  }): Promise<any> {
    // Reset debug log
    this.debugLog = [];

    // Convert queryEngine-style params to QueryOptions format
    const queryOptions: QueryOptions = {
      table: options.tableName,
    };

    // Convert fields
    if (options.fields) {
      if (Array.isArray(options.fields)) {
        queryOptions.fields = options.fields;
      } else if (typeof options.fields === 'string') {
        queryOptions.fields = options.fields.split(',').map(f => f.trim());
      }
    }

    // Store original filter for new buildWhereClause approach
    const originalFilter = options.filter;

    // Convert filter to where conditions (backward compatible with legacy approach)
    // If filter contains _and/_or/_not, we'll use buildWhereClause later
    if (options.filter && !hasLogicalOperators(options.filter)) {
      queryOptions.where = [];
      // Simple conversion for backward compatibility
      for (const [field, value] of Object.entries(options.filter)) {
        if (typeof value === 'object' && value !== null) {
          // Handle operators like {_eq: value}
          for (const [op, val] of Object.entries(value)) {
            // Convert operator: _eq -> =, _neq -> !=, _in -> in, _is_null -> is null, etc.
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
          // Direct equality
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

    // Separate main table sorts from relation sorts
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

    // Auto-expand `fields` into `join` + `select` if provided
    if (queryOptions.fields && queryOptions.fields.length > 0) {
      const expanded = await this.expandFields(queryOptions.table, queryOptions.fields, relationSorts);
      queryOptions.join = [...(queryOptions.join || []), ...expanded.joins];
      queryOptions.select = [...(queryOptions.select || []), ...expanded.select];
    }

    // Auto-prefix table name to where conditions if not already qualified
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

    // Use only main table sorts for the query
    queryOptions.sort = mainTableSorts;

    // Execute SQL query using Knex
    const knex = this.knexService.getKnex();
    let query: any = knex(queryOptions.table);

    // Parse meta requirements early
    const metaParts = Array.isArray(options.meta)
      ? options.meta
      : (options.meta || '').split(',').map((x) => x.trim()).filter(Boolean);

    const needsFilterCount = metaParts.includes('filterCount') || metaParts.includes('*');

    if (queryOptions.select) {
      // Convert subqueries to knex.raw to prevent double-escaping
      const selectItems = queryOptions.select.map(field => {
        // Detect if field contains subquery (starts with parenthesis)
        if (typeof field === 'string' && field.trim().startsWith('(')) {
          return knex.raw(field);
        }
        return field;
      });
      query = query.select(selectItems);
    }

    if (needsFilterCount) {
      query.select(knex.raw('COUNT(*) OVER() as __filter_count__'));
    }

    // Apply WHERE clause with relation filtering support
    // Skip relation filtering for system metadata tables to avoid infinite loop
    const isSystemTable = ['table_definition', 'column_definition', 'relation_definition', 'method_definition'].includes(queryOptions.table);

    if (originalFilter && (hasLogicalOperators(originalFilter) || Object.keys(originalFilter).length > 0)) {
      if (!isSystemTable) {
        // Try to get metadata for relation filtering
        const metadata = await this.metadataCache.getTableMetadata(queryOptions.table);

        if (metadata && metadata.relations && metadata.relations.length > 0) {
          // This table has relations - check if filter uses any relations
          const { hasRelations } = separateFilters(originalFilter, metadata);

          if (hasRelations) {
            // Debug: log metadata and filter
            this.pushDebug('table_metadata', {
              tableName: queryOptions.table,
              relations: metadata.relations,
            });
            this.pushDebug('original_filter', originalFilter);

            // Use applyRelationFilters which handles both field and relation filters with logical operators
            await applyRelationFilters(
              knex,
              query,
              originalFilter,  // Pass the full filter
              queryOptions.table,
              metadata,
              this.dbType,
              (tableName: string) => this.metadataCache.getTableMetadata(tableName),
            );
          } else {
            // No relation filters, use regular buildWhereClause
            query = buildWhereClause(query, originalFilter, queryOptions.table, this.dbType);
          }
        } else {
          // No metadata or no relations, use regular buildWhereClause
          query = buildWhereClause(query, originalFilter, queryOptions.table, this.dbType);
        }
      } else {
        // System tables: skip relation filtering to prevent infinite loop
        query = buildWhereClause(query, originalFilter, queryOptions.table, this.dbType);
      }
    } else if (queryOptions.where && queryOptions.where.length > 0) {
      // Use legacy applyWhereToKnex for simple filters (backward compatible)
      query = this.applyWhereToKnex(query, queryOptions.where);
    }

    if (queryOptions.join) {
      for (const joinOpt of queryOptions.join) {
        const joinMethod = `${joinOpt.type}Join` as 'innerJoin' | 'leftJoin' | 'rightJoin';
        query = query[joinMethod](joinOpt.table, joinOpt.on.local, joinOpt.on.foreign);
      }
    }

    if (queryOptions.sort) {
      for (const sortOpt of queryOptions.sort) {
        // Add table prefix if field doesn't contain dot (nested relation sort)
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

    // limit=0 means no limit (fetch all), undefined/null means use default
    if (queryOptions.limit !== undefined && queryOptions.limit !== null && queryOptions.limit > 0) {
      query = query.limit(queryOptions.limit);
    }

    // Add SQL to debug if debugMode is enabled
    if (options.debugMode) {
      this.pushDebug('sql', query.toString());
    }

    // Execute totalCount query separately if needed
    let totalCount = 0;

    if (metaParts.includes('totalCount') || metaParts.includes('*')) {
      // Total count (no filters)
      const totalQuery = knex(queryOptions.table);
      const totalResult = await totalQuery.count('* as count').first();
      totalCount = Number(totalResult?.count || 0);
    }

    // Execute main query (now includes __filter_count__ via window function if needed)
    const results = await query;

    let filterCount = 0;

    if (needsFilterCount && results.length > 0) {
      // Extract filterCount from first row (all rows have same value from window function)
      filterCount = Number(results[0].__filter_count__ || 0);

      // Clean up: Remove __filter_count__ column from all result rows
      results.forEach((row: any) => {
        delete row.__filter_count__;
      });
    }

    // Return in queryEngine format with optional meta and debug
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
      ...(options.debugMode ? { debug: this.debugLog } : {}),
    };
  }

  /**
   * Find multiple records - Router method
   * Routes to sqlExecutor() for SQL databases or handles MongoDB directly
   * Accepts queryEngine-style parameters (Directus format)
   */
  async select(options: {
    tableName: string;
    fields?: string | string[];
    filter?: any;
    sort?: string | string[];
    page?: number;
    limit?: number;
    meta?: string;
    deep?: Record<string, any>;
    debugMode?: boolean;
    pipeline?: any[]; // MongoDB aggregation pipeline (MongoDB only)
  }): Promise<any> {

    // For SQL databases, delegate to sqlExecutor
    if (this.dbType !== 'mongodb') {
      return this.sqlExecutor(options);
    }

    // For MongoDB, delegate to mongoExecutor
    return this.mongoExecutor(options);
  }

  /**
   * MongoDB Query Executor - Handles MongoDB query execution
   * Converts queryEngine-style params to MongoDB queries
   *
   * Note: MongoDB already handles _and/_or/_not via walkFilter in MongoQueryEngine
   * This method is primarily for backward compatibility with simple queries
   */
  async mongoExecutor(options: {
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
  }): Promise<any> {
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
    } else if (options.filter && hasLogicalOperators(options.filter)) {
      // For complex filters, MongoDB should use MongoQueryEngine with walkFilter
      // This is a fallback warning
      console.warn('[QueryBuilderService] Complex MongoDB filters with _and/_or/_not should use MongoQueryEngine directly');
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
    const results = await this.selectLegacy(queryOptions);
    return { data: results };
  }

  /**
   * Legacy select method - INTERNAL USE ONLY
   * Used by mongoExecutor and sqlExecutor internally
   * @private
   */
  private async selectLegacy(options: QueryOptions): Promise<any[]> {
    // Handle field expansion differently for SQL vs MongoDB
    if (this.dbType !== 'mongodb') {
      // SQL: Auto-expand `fields` into `join` + `select` using SQL subqueries
      if (options.fields && options.fields.length > 0) {
        const expanded = await this.expandFields(options.table, options.fields);
        options.join = [...(options.join || []), ...expanded.joins];
        options.select = [...(options.select || []), ...expanded.select];
      }
    } else {
      // MongoDB: Expand nested wildcard fields to $lookup pipeline
      if (options.fields && options.fields.length > 0) {
        const hasNestedWildcard = options.fields.some(f => f.includes('.') && f.includes('*'));
        console.log(`[SELECT-LEGACY-MONGO] Fields:`, options.fields);
        console.log(`[SELECT-LEGACY-MONGO] hasNestedWildcard:`, hasNestedWildcard);

        if (hasNestedWildcard) {
          // Build MongoDB-specific joins for nested wildcards
          console.log(`[SELECT-LEGACY] Calling expandMongoNestedFields...`);
          const mongoJoins = await this.expandMongoNestedFields(options.table, options.fields);
          console.log(`[SELECT-LEGACY] Got mongoJoins:`, mongoJoins);
          options.join = mongoJoins;
          options.select = options.fields;
          console.log(`[SELECT-LEGACY] Set options.join for MongoDB:`, options.join);
        } else {
          options.select = options.fields;
        }
      }
    }

    console.log(`[SELECT-LEGACY] After field expansion, options.join:`, options.join);

    // Auto-prefix table name to where conditions (SQL only)
    // MongoDB: Fields don't have table prefixes in documents
    if (this.dbType !== 'mongodb' && options.where) {
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

    // Auto-prefix table name to sort fields (SQL only)
    // MongoDB: Fields don't have table prefixes in documents
    if (this.dbType !== 'mongodb' && options.sort) {
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

    if (this.dbType === 'mongodb') {

      const collection = this.mongoService.collection(options.table);

      // Use custom pipeline if provided (e.g., from MongoQueryEngine)
      if (options.pipeline) {
        const results = await collection.aggregate(options.pipeline).toArray();
        return results.map(doc => this.mongoService['mapDocument'](doc));
      }

      // Use aggregation pipeline if joins are present
      console.log(`[MONGO-EXEC] Checking joins:`, options.join);
      if (options.join && options.join.length > 0) {
        console.log(`[MONGO-EXEC] Using aggregation pipeline with ${options.join.length} joins`);
        const pipeline: any[] = [];

        // $match stage
        if (options.where) {
          const filter = this.whereToMongoFilter(options.where);
          pipeline.push({ $match: filter });
        }

        // $lookup stages for joins
        for (const joinOpt of options.join) {
          // Extract base table name (remove alias)
          const tableName = joinOpt.table.split(' as ')[0];

          // Extract field names from dot notation
          const localField = joinOpt.on.local.split('.').pop();
          const foreignField = joinOpt.on.foreign.split('.').pop();

          // Use localField as alias (e.g., 'mainTable' not 'table_definition')
          // This ensures the populated object replaces the original field
          const alias = localField;

          pipeline.push({
            $lookup: {
              from: tableName,
              localField,
              foreignField,
              as: alias,
            },
          });

          // Unwind array to single object (for left join behavior)
          pipeline.push({
            $unwind: {
              path: `$${alias}`,
              preserveNullAndEmptyArrays: true,
            },
          });
        }

        // $project stage for select fields
        // Skip projection if wildcard '*' is present (select all fields)
        const hasRootWildcard = options.select?.some(f => f === '*');

        if (options.select && !hasRootWildcard) {
          const projection: any = {};
          for (const field of options.select) {
            if (field.includes('.*')) {
              // Keep nested object: 'mainTable.*' -> include all mainTable fields
              projection[field.replace('.*', '')] = 1;
            } else if (field.includes(' as ')) {
              const [source, alias] = field.split(' as ');
              projection[alias.trim()] = `$${source.trim()}`;
            } else {
              projection[field] = 1;
            }
          }
          pipeline.push({ $project: projection });
        }

        // $sort stage
        if (options.sort) {
          const sortSpec: any = {};
          for (const sortOpt of options.sort) {
            sortSpec[sortOpt.field] = sortOpt.direction === 'asc' ? 1 : -1;
          }
          pipeline.push({ $sort: sortSpec });
        }

        // $skip and $limit
        if (options.offset) {
          pipeline.push({ $skip: options.offset });
        }
        if (options.limit) {
          pipeline.push({ $limit: options.limit });
        }

        const results = await collection.aggregate(pipeline).toArray();
        return results.map(doc => this.mongoService['mapDocument'](doc));
      }

      // Simple query without joins
      const filter = options.where ? this.whereToMongoFilter(options.where) : {};
      let cursor = collection.find(filter);

      if (options.select) {
        // MongoDB: Skip projection if wildcard '*' is present (means select all)
        // Also skip nested wildcards like 'mainTable.*' for simple queries
        const hasWildcard = options.select.some(f => f === '*' || f.includes('.*'));

        if (!hasWildcard) {
          const projection: any = {};
          for (const field of options.select) {
            projection[field] = 1;
          }
          cursor = cursor.project(projection);
        } else {
        }
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

      // limit=0 means no limit (fetch all), undefined/null means use default
      if (options.limit !== undefined && options.limit !== null && options.limit > 0) {
        cursor = cursor.limit(options.limit);
      }

      const results = await cursor.toArray();
      return results.map(doc => this.mongoService['mapDocument'](doc));
    }

    // SQL (Knex)
    const knex = this.knexService.getKnex();
    let query: any = knex(options.table);

    if (options.select) {
      query = query.select(options.select);
    }

    if (options.where && options.where.length > 0) {
      query = this.applyWhereToKnex(query, options.where);
    }

    if (options.join) {
      for (const joinOpt of options.join) {
        const joinMethod = `${joinOpt.type}Join` as 'innerJoin' | 'leftJoin' | 'rightJoin';
        query = query[joinMethod](joinOpt.table, joinOpt.on.local, joinOpt.on.foreign);
      }
    }

    if (options.sort) {
      for (const sortOpt of options.sort) {
        // Add table prefix if field doesn't contain dot (nested relation sort)
        const sortField = sortOpt.field.includes('.')
          ? sortOpt.field
          : `${options.table}.${sortOpt.field}`;
        query = query.orderBy(sortField, sortOpt.direction);
      }
    }

    if (options.groupBy) {
      query = query.groupBy(options.groupBy);
    }

    if (options.offset) {
      query = query.offset(options.offset);
    }

    // limit=0 means no limit (fetch all), undefined/null means use default
    if (options.limit !== undefined && options.limit !== null && options.limit > 0) {
      query = query.limit(options.limit);
    }

    return query;
  }

  /**
   * Update records
   */
  async update(options: UpdateOptions): Promise<any> {
    if (this.dbType === 'mongodb') {
      // Process nested relations first
      const dataWithRelations = await this.mongoService.processNestedRelations(options.table, options.data);
      
      // Apply update timestamp
      const dataWithTimestamp = this.mongoService.applyUpdateTimestamp(dataWithRelations);
      
      const filter = this.whereToMongoFilter(options.where);
      const collection = this.mongoService.collection(options.table);
      await collection.updateMany(filter, { $set: dataWithTimestamp });
      return collection.find(filter).toArray();
    }
    
    // SQL: Use KnexService.updateWithCascade for automatic relation handling
    const knex = this.knexService.getKnex();
    let query: any = knex(options.table);
    
    if (options.where.length > 0) {
      query = this.applyWhereToKnex(query, options.where);
    }
    
    // Get records to update first
    const recordsToUpdate = await query.clone();
    
    // Update each record with cascade
    for (const record of recordsToUpdate) {
      await this.knexService.updateWithCascade(options.table, record.id, options.data);
    }
    
    if (options.returning) {
      return query.returning(options.returning);
    }
    
    return { affected: recordsToUpdate.length };
  }

  /**
   * Delete records
   */
  async delete(options: DeleteOptions): Promise<number> {
    if (this.dbType === 'mongodb') {
      const filter = this.whereToMongoFilter(options.where);
      const collection = this.mongoService.collection(options.table);
      const result = await collection.deleteMany(filter);
      return result.deletedCount;
    }
    
    const knex = this.knexService.getKnex();
    let query: any = knex(options.table);
    
    if (options.where.length > 0) {
      query = this.applyWhereToKnex(query, options.where);
    }
    
    return query.delete();
  }

  /**
   * Count records
   */
  async count(options: CountOptions): Promise<number> {
    if (this.dbType === 'mongodb') {
      const filter = options.where ? this.whereToMongoFilter(options.where) : {};
      return this.mongoService.count(options.table, filter);
    }
    
    const knex = this.knexService.getKnex();
    let query: any = knex(options.table);
    
    if (options.where && options.where.length > 0) {
      query = this.applyWhereToKnex(query, options.where);
    }
    
    const result = await query.count('* as count').first();
    return Number(result?.count || 0);
  }

  /**
   * Execute transaction
   */
  async transaction<T>(callback: (trx: any) => Promise<T>): Promise<T> {
    if (this.dbType === 'mongodb') {
      const session = this.mongoService.getClient().startSession();
      try {
        await session.startTransaction();
        const result = await callback(session);
        await session.commitTransaction();
        return result;
      } catch (error) {
        await session.abortTransaction();
        throw error;
      } finally {
        await session.endSession();
      }
    }
    
    const knex = this.knexService.getKnex();
    return knex.transaction(callback);
  }

  /**
   * Find one by ID
   */
  async findById(table: string, id: any): Promise<any> {
    if (this.dbType === 'mongodb') {
      return this.mongoService.findOne(table, { _id: id });
    }
    
    const knex = this.knexService.getKnex();
    return knex(table).where('id', id).first();
  }

  /**
   * Find one by conditions (simple object)
   */
  async findOneWhere(table: string, where: Record<string, any>): Promise<any> {
    if (this.dbType === 'mongodb') {
      // Normalize 'id' to '_id' and convert to ObjectId for MongoDB
      const { ObjectId } = require('mongodb');
      const normalizedWhere: any = {};
      
      for (const [key, value] of Object.entries(where)) {
        if (key === 'id' || key === '_id') {
          normalizedWhere._id = typeof value === 'string' ? new ObjectId(value) : value;
        } else {
          normalizedWhere[key] = value;
        }
      }
      
      return this.mongoService.findOne(table, normalizedWhere);
    }
    
    const knex = this.knexService.getKnex();
    return knex(table).where(where).first();
  }

  /**
   * Find many by conditions (simple object)
   */
  async findWhere(table: string, where: Record<string, any>): Promise<any[]> {
    if (this.dbType === 'mongodb') {
      // Normalize 'id' to '_id' and convert to ObjectId for MongoDB
      const { ObjectId } = require('mongodb');
      const normalizedWhere: any = {};
      
      for (const [key, value] of Object.entries(where)) {
        if (key === 'id' || key === '_id') {
          normalizedWhere._id = typeof value === 'string' ? new ObjectId(value) : value;
        } else {
          normalizedWhere[key] = value;
        }
      }
      
      const collection = this.mongoService.collection(table);
      const results = await collection.find(normalizedWhere).toArray();
      return results.map(doc => this.mongoService['mapDocument'](doc));
    }
    
    const knex = this.knexService.getKnex();
    return knex(table).where(where);
  }

  /**
   * Insert one and return with ID
   * Uses insertWithCascade to handle M2M and O2M relations
   */
  async insertAndGet(table: string, data: any): Promise<any> {
    if (this.dbType === 'mongodb') {
      return this.mongoService.insertOne(table, data);
    }

    // Use insertWithCascade for M2M/O2M relation handling
    const insertedId = await this.knexService.insertWithCascade(table, data);

    const knex = this.knexService.getKnex();
    const recordId = insertedId || data.id;

    // Query back the inserted record
    return knex(table).where('id', recordId).first();
  }

  /**
   * Update by ID
   */
  async updateById(table: string, id: any, data: any): Promise<any> {
    if (this.dbType === 'mongodb') {
      return this.mongoService.updateOne(table, id, data);
    }
    
    // SQL: Use KnexService.updateWithCascade for automatic relation handling
    await this.knexService.updateWithCascade(table, id, data);
    const knex = this.knexService.getKnex();
    return knex(table).where('id', id).first();
  }

  /**
   * Delete by ID
   */
  async deleteById(table: string, id: any): Promise<number> {
    if (this.dbType === 'mongodb') {
      const deleted = await this.mongoService.deleteOne(table, id);
      return deleted ? 1 : 0;
    }
    
    const knex = this.knexService.getKnex();
    return knex(table).where('id', id).delete();
  }

  /**
   * Execute raw query/command
   * SQL: knex.raw()
   * MongoDB: db.command()
   */
  async raw(query: string | any, bindings?: any): Promise<any> {
    if (this.dbType === 'mongodb') {
      // MongoDB: execute command
      const db = this.mongoService.getDb();
      if (typeof query === 'string') {
        // If string, treat as simple ping or eval
        if (query.toLowerCase().includes('select 1')) {
          return db.command({ ping: 1 });
        }
        throw new Error('String queries not supported for MongoDB. Use db.command() object instead.');
      }
      return db.command(query);
    }
    
    // SQL: execute raw query
    const knex = this.knexService.getKnex();
    return knex.raw(query, bindings);
  }

  /**
   * Get database connection (Knex for SQL, Db for MongoDB)
   * WARNING: Use with caution - code using this will need conditional logic for each DB type
   */
  getConnection(): any {
    if (this.dbType === 'mongodb') {
      return this.mongoService.getDb();
    }
    return this.knexService.getKnex();
  }

  /**
   * Get Knex instance (ONLY for SQL-specific code)
   * Throws error if MongoDB is being used
   */
  getKnex(): any {
    if (this.dbType === 'mongodb') {
      throw new Error('getKnex() is not available for MongoDB. Use getConnection() or unified methods.');
    }
    return this.knexService.getKnex();
  }

  /**
   * Get MongoDB Db instance (ONLY for MongoDB-specific code)
   * Throws error if SQL is being used
   */
  getMongoDb(): any {
    if (this.dbType !== 'mongodb') {
      throw new Error('getMongoDb() is not available for SQL. Use getConnection() or unified methods.');
    }
    return this.mongoService.getDb();
  }

  /**
   * Get database type
   */
  getDatabaseType(): DatabaseType {
    return this.dbType;
  }

  /**
   * Check if using MongoDB
   */
  isMongoDb(): boolean {
    return this.dbType === 'mongodb';
  }

  /**
   * Check if using SQL
   */
  isSql(): boolean {
    return ['mysql', 'postgres'].includes(this.dbType);
  }


  /**
   * Expand smart field list into explicit JOINs and SELECT
   * Private helper for auto-relation expansion
   */
  private async expandFields(
    tableName: string,
    fields: string[],
    sortOptions: Array<{ field: string; direction: 'asc' | 'desc' }> = []
  ): Promise<{
    joins: any[];
    select: string[];
  }> {
    if (!this.metadataCache) {
      // Metadata cache not available (e.g., during early bootstrap)
      // Fall back to simple field expansion
      return { joins: [], select: fields };
    }

    // Cache metadata ONCE to avoid repeated async calls
    const allMetadata = await this.metadataCache.getMetadata();

    // Metadata getter function (now synchronous, reads from cached result)
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
        console.warn(`[EXPAND-FIELDS] Failed to get metadata for table ${tName}:`, error.message);
        return null;
      }
    };

    try {
      const result = await expandFieldsToJoinsAndSelect(tableName, fields, metadataGetter, this.dbType, sortOptions);
      return result;
    } catch (error) {
      console.error(`[EXPAND-FIELDS] Field expansion failed: ${error.message}`);
      // Fall back to simple field expansion
      return { joins: [], select: fields };
    }
  }
}


