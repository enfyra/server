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
import { sqlExecutor as sqlExecutorFunction } from './sql-executor.function';
import { mongoExecutor as mongoExecutorFunction } from './mongo-executor.function';
import * as sqlCrud from './sql-crud.functions';
import * as mongoCrud from './mongo-crud.functions';

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

    if (!metadata) {
      return joins;
    }

    for (const field of fields) {
      if (!field.includes('.') || !field.includes('*')) {
        continue;
      }

      const relationName = field.split('.')[0];
      const relation = metadata.relations?.find((r: any) => r.propertyName === relationName);

      if (!relation) {
        continue;
      }

      joins.push({
        table: relation.targetTableName,
        on: {
          local: relationName,
          foreign: '_id',
        },
      });
    }

    return joins;
  }

  /**
   * Insert records (one or multiple)
   */
  async insert(options: InsertOptions): Promise<any> {
    if (this.dbType === 'mongodb') {
      return mongoCrud.mongoInsert(options, this.mongoService);
    }
    return sqlCrud.sqlInsert(options, this.knexService);
  }

  /**
   * Find multiple records - Router method
   * Routes to appropriate executor (SQL or MongoDB) based on database type
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

    // Router: delegate to appropriate executor based on database type
    if (this.dbType !== 'mongodb') {
      // SQL: Use sqlExecutorFunction
      const knex = this.knexService.getKnex();
      return sqlExecutorFunction(
        options,
        knex,
        this.expandFields.bind(this),
        (tableName: string) => this.metadataCache.getTableMetadata(tableName),
        this.dbType
      );
    }

    // MongoDB: Use mongoExecutorFunction
    return mongoExecutorFunction(
      options,
      this.selectLegacy.bind(this)
    );
  }

  /**
   * Legacy select method - INTERNAL USE ONLY
   * Used by mongoExecutorFunction internally
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

        if (hasNestedWildcard) {
          const mongoJoins = await this.expandMongoNestedFields(options.table, options.fields);
          options.join = mongoJoins;
          options.select = options.fields;
        } else {
          options.select = options.fields;
        }
      }
    }

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
      if (options.join && options.join.length > 0) {
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
      return mongoCrud.mongoUpdate(options, this.mongoService);
    }
    return sqlCrud.sqlUpdate(options, this.knexService);
  }

  /**
   * Delete records
   */
  async delete(options: DeleteOptions): Promise<number> {
    if (this.dbType === 'mongodb') {
      return mongoCrud.mongoDelete(options, this.mongoService);
    }
    return sqlCrud.sqlDelete(options, this.knexService);
  }

  /**
   * Count records
   */
  async count(options: CountOptions): Promise<number> {
    if (this.dbType === 'mongodb') {
      return mongoCrud.mongoCount(options, this.mongoService);
    }
    return sqlCrud.sqlCount(options, this.knexService);
  }

  /**
   * Execute transaction
   */
  async transaction<T>(callback: (trx: any) => Promise<T>): Promise<T> {
    if (this.dbType === 'mongodb') {
      return mongoCrud.mongoTransaction(callback, this.mongoService);
    }
    return sqlCrud.sqlTransaction(callback, this.knexService);
  }

  /**
   * Find one by ID
   */
  async findById(table: string, id: any): Promise<any> {
    if (this.dbType === 'mongodb') {
      return mongoCrud.mongoFindById(table, id, this.mongoService);
    }
    return sqlCrud.sqlFindById(table, id, this.knexService);
  }

  /**
   * Find one by conditions (simple object)
   */
  async findOneWhere(table: string, where: Record<string, any>): Promise<any> {
    if (this.dbType === 'mongodb') {
      return mongoCrud.mongoFindOneWhere(table, where, this.mongoService);
    }
    return sqlCrud.sqlFindOneWhere(table, where, this.knexService);
  }

  /**
   * Find many by conditions (simple object)
   */
  async findWhere(table: string, where: Record<string, any>): Promise<any[]> {
    if (this.dbType === 'mongodb') {
      return mongoCrud.mongoFindWhere(table, where, this.mongoService);
    }
    return sqlCrud.sqlFindWhere(table, where, this.knexService);
  }

  /**
   * Insert one and return with ID
   * Uses insertWithCascade to handle M2M and O2M relations
   */
  async insertAndGet(table: string, data: any): Promise<any> {
    if (this.dbType === 'mongodb') {
      return mongoCrud.mongoInsertAndGet(table, data, this.mongoService);
    }
    return sqlCrud.sqlInsertAndGet(table, data, this.knexService);
  }

  /**
   * Update by ID
   */
  async updateById(table: string, id: any, data: any): Promise<any> {
    if (this.dbType === 'mongodb') {
      return mongoCrud.mongoUpdateById(table, id, data, this.mongoService);
    }
    return sqlCrud.sqlUpdateById(table, id, data, this.knexService);
  }

  /**
   * Delete by ID
   */
  async deleteById(table: string, id: any): Promise<number> {
    if (this.dbType === 'mongodb') {
      return mongoCrud.mongoDeleteById(table, id, this.mongoService);
    }
    return sqlCrud.sqlDeleteById(table, id, this.knexService);
  }

  /**
   * Execute raw query/command
   * SQL: knex.raw()
   * MongoDB: db.command()
   */
  async raw(query: string | any, bindings?: any): Promise<any> {
    if (this.dbType === 'mongodb') {
      return mongoCrud.mongoRaw(query, this.mongoService);
    }
    return sqlCrud.sqlRaw(query, bindings, this.knexService);
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


