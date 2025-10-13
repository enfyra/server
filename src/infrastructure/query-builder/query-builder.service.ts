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

/**
 * QueryBuilderService - Unified database query interface
 * Provides same syntax for both SQL and MongoDB
 * Converts unified query to appropriate database query
 */
@Injectable()
export class QueryBuilderService {
  private dbType: DatabaseType;

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
    const filter: any = {};
    
    for (const condition of conditions) {
      switch (condition.operator) {
        case '=':
          filter[condition.field] = condition.value;
          break;
        case '!=':
          filter[condition.field] = { $ne: condition.value };
          break;
        case '>':
          filter[condition.field] = { $gt: condition.value };
          break;
        case '<':
          filter[condition.field] = { $lt: condition.value };
          break;
        case '>=':
          filter[condition.field] = { $gte: condition.value };
          break;
        case '<=':
          filter[condition.field] = { $lte: condition.value };
          break;
        case 'like':
          filter[condition.field] = { $regex: condition.value.replace(/%/g, '.*') };
          break;
        case 'in':
          filter[condition.field] = { $in: condition.value };
          break;
        case 'not in':
          filter[condition.field] = { $nin: condition.value };
          break;
        case 'is null':
          filter[condition.field] = null;
          break;
        case 'is not null':
          filter[condition.field] = { $ne: null };
          break;
      }
    }
    
    return filter;
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
    
    const knex = this.knexService.getKnex();
    
    // Postgres supports returning, MySQL doesn't
    if (this.dbType === 'postgres' && options.returning) {
      return knex(options.table).insert(options.data).returning(options.returning);
    }
    
    // MySQL: insert without returning
    return knex(options.table).insert(options.data);
  }

  /**
   * Find multiple records with unified query options
   */
  async select(options: QueryOptions): Promise<any[]> {
    // Auto-expand `fields` into `join` + `select` if provided
    if (options.fields && options.fields.length > 0) {
      const expanded = await this.expandFields(options.table, options.fields);
      options.join = [...(options.join || []), ...expanded.joins];
      options.select = [...(options.select || []), ...expanded.select];
    }

    // Auto-prefix table name to where conditions if not already qualified
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

    // Auto-prefix table name to sort fields if not already qualified
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
          const alias = joinOpt.table.includes(' as ') ? joinOpt.table.split(' as ')[1] : tableName;
          
          // Extract field names from dot notation
          const localField = joinOpt.on.local.split('.').pop(); // e.g., "route_definition.mainTableId" -> "mainTableId"
          const foreignField = joinOpt.on.foreign.split('.').pop(); // e.g., "mainTable.id" -> "id"
          
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
        if (options.select) {
          const projection: any = {};
          for (const field of options.select) {
            if (field.includes('.*')) {
              // Handle wildcard like "relation_definition.*"
              projection[field.replace('.*', '')] = 1;
            } else if (field.includes(' as ')) {
              // Handle aliases like "mainTable.id as mainTable_id"
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
      
      if (options.limit) {
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
        query = query.orderBy(sortOpt.field, sortOpt.direction);
      }
    }

    if (options.groupBy) {
      query = query.groupBy(options.groupBy);
    }

    if (options.offset) {
      query = query.offset(options.offset);
    }

    if (options.limit) {
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
    
    const knex = this.knexService.getKnex();
    let query: any = knex(options.table);
    
    if (options.where.length > 0) {
      query = this.applyWhereToKnex(query, options.where);
    }
    
    await query.update(options.data);
    
    if (options.returning) {
      return query.returning(options.returning);
    }
    
    return { affected: 1 };
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
   */
  async insertAndGet(table: string, data: any): Promise<any> {
    if (this.dbType === 'mongodb') {
      return this.mongoService.insertOne(table, data);
    }
    
    const knex = this.knexService.getKnex();
    const dbType = this.getDatabaseType();
    
    if (dbType === 'postgres') {
      const [result] = await knex(table).insert(data).returning('*');
      return result;
    } else {
      const [id] = await knex(table).insert(data);
      return knex(table).where('id', id).first();
    }
  }

  /**
   * Update by ID
   */
  async updateById(table: string, id: any, data: any): Promise<any> {
    if (this.dbType === 'mongodb') {
      return this.mongoService.updateOne(table, id, data);
    }
    
    const knex = this.knexService.getKnex();
    await knex(table).where('id', id).update(data);
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
   * Reload with metadata (SQL only - for Knex hooks)
   * MongoDB: No-op (doesn't need metadata reload)
   */
  async reloadWithMetadata(metadata: any): Promise<void> {
    if (this.dbType === 'mongodb') {
      // MongoDB doesn't need metadata reload
      return;
    }
    
    // SQL: Reload Knex with metadata for hooks and JSON parsing
    await this.knexService.reloadWithMetadata(metadata);
  }

  /**
   * Expand smart field list into explicit JOINs and SELECT
   * Private helper for auto-relation expansion
   */
  private async expandFields(tableName: string, fields: string[]): Promise<{
    joins: any[];
    select: string[];
  }> {
    if (!this.metadataCache) {
      // Metadata cache not available (e.g., during early bootstrap)
      // Fall back to simple field expansion
      return { joins: [], select: fields };
    }

    // Metadata getter function
    const metadataGetter = async (tName: string) => {
      try {
        const metadata = await this.metadataCache.getMetadata();
        const tableMeta = metadata.tables.get(tName);
        if (!tableMeta) return null;

        return {
          name: tableMeta.name,
          columns: tableMeta.columns || [],
          relations: tableMeta.relations || [],
        };
      } catch (error) {
        console.warn(`Failed to get metadata for table ${tName}:`, error.message);
        return null;
      }
    };

    try {
      return await expandFieldsToJoinsAndSelect(tableName, fields, metadataGetter);
    } catch (error) {
      console.error('Field expansion failed:', error.message);
      // Fall back to simple field expansion
      return { joins: [], select: fields };
    }
  }
}


