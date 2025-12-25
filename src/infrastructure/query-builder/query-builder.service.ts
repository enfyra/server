import { Injectable, Optional, Inject, forwardRef } from '@nestjs/common';
import { KnexService } from '../knex/knex.service';
import { MongoService } from '../mongo/services/mongo.service';
import { MetadataCacheService } from '../cache/services/metadata-cache.service';
import {
  DatabaseType,
  WhereCondition,
  InsertOptions,
  UpdateOptions,
  DeleteOptions,
  CountOptions,
} from '../../shared/types/query-builder.types';
import { expandFieldsToJoinsAndSelect } from './utils/sql/expand-fields';
import { MongoQueryExecutor } from './executors/mongo-query-executor';
import { SqlQueryExecutor } from './executors/sql-query-executor';

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

  async insert(options: InsertOptions): Promise<any> {
    if (this.dbType === 'mongodb') {
      const collection = this.mongoService.collection(options.table);
      if (Array.isArray(options.data)) {
        const processedData = await Promise.all(
          options.data.map(record => this.mongoService.processNestedRelations(options.table, record))
        );
        
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
    
    if (Array.isArray(options.data)) {
      const results = [];
      for (const record of options.data) {
        const result = await this.knexService.insertWithCascade(options.table, record);
        results.push(result);
      }
      return results;
    } else {
      return await this.knexService.insertWithCascade(options.table, options.data);
    }
  }

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
    debugLog?: any[];
    pipeline?: any[];
  }): Promise<any> {
    const metadata = this.metadataCache.getDirectMetadata();

    if (this.dbType === 'mongodb') {
      const executor = new MongoQueryExecutor(this.mongoService);
      return executor.execute({ ...options, metadata, dbType: this.dbType });
    }

    const executor = new SqlQueryExecutor(
      this.knexService.getKnex(),
      this.dbType as 'postgres' | 'mysql' | 'sqlite',
      this.knexService,
    );
    return executor.execute({ ...options, metadata });
  }


  async update(options: UpdateOptions): Promise<any> {
    if (this.dbType === 'mongodb') {
      const dataWithRelations = await this.mongoService.processNestedRelations(options.table, options.data);
      const dataWithoutHiddenNulls = await this.mongoService.stripHiddenNullFields(options.table, dataWithRelations);
      const dataWithTimestamp = this.mongoService.applyUpdateTimestamp(dataWithoutHiddenNulls);

      const filter: any = {};
      const { ObjectId } = require('mongodb');
      for (const condition of options.where) {
        let fieldName = condition.field.includes('.') ? condition.field.split('.').pop() : condition.field;
        if (fieldName === 'id') fieldName = '_id';

        let value = condition.value;
        if (fieldName === '_id' && typeof value === 'string') {
          try { value = new ObjectId(value); } catch (err) { }
        }

        switch (condition.operator) {
          case '=': filter[fieldName] = value; break;
          case '!=': filter[fieldName] = { $ne: value }; break;
          case '>': filter[fieldName] = { $gt: value }; break;
          case '<': filter[fieldName] = { $lt: value }; break;
          case '>=': filter[fieldName] = { $gte: value }; break;
          case '<=': filter[fieldName] = { $lte: value }; break;
          case 'in': filter[fieldName] = { $in: value }; break;
          case 'not in': filter[fieldName] = { $nin: value }; break;
        }
      }

      const collection = this.mongoService.collection(options.table);
      await collection.updateMany(filter, { $set: dataWithTimestamp });
      const results = await collection.find(filter).toArray();
      return results;
    }
    
    const knex = this.knexService.getKnex();
    let query: any = knex(options.table);
    
    if (options.where.length > 0) {
      query = this.applyWhereToKnex(query, options.where);
    }
    
    const recordsToUpdate = await query.clone();
    
    for (const record of recordsToUpdate) {
      await this.knexService.updateWithCascade(options.table, record.id, options.data);
    }
    
    if (options.returning) {
      return query.returning(options.returning);
    }
    
    return { affected: recordsToUpdate.length };
  }

  async delete(options: DeleteOptions): Promise<number> {
    if (this.dbType === 'mongodb') {
      const filter: any = {};
      const { ObjectId } = require('mongodb');
      for (const condition of options.where) {
        let fieldName = condition.field.includes('.') ? condition.field.split('.').pop() : condition.field;
        if (fieldName === 'id') fieldName = '_id';

        let value = condition.value;
        if (fieldName === '_id' && typeof value === 'string') {
          try { value = new ObjectId(value); } catch (err) { }
        }

        switch (condition.operator) {
          case '=': filter[fieldName] = value; break;
          case '!=': filter[fieldName] = { $ne: value }; break;
          case '>': filter[fieldName] = { $gt: value }; break;
          case '<': filter[fieldName] = { $lt: value }; break;
          case '>=': filter[fieldName] = { $gte: value }; break;
          case '<=': filter[fieldName] = { $lte: value }; break;
          case 'in': filter[fieldName] = { $in: value }; break;
          case 'not in': filter[fieldName] = { $nin: value }; break;
        }
      }

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

  async count(options: CountOptions): Promise<number> {
    if (this.dbType === 'mongodb') {
      const filter: any = {};
      if (options.where) {
        const { ObjectId } = require('mongodb');
        for (const condition of options.where) {
          let fieldName = condition.field.includes('.') ? condition.field.split('.').pop() : condition.field;
          if (fieldName === 'id') fieldName = '_id';

          let value = condition.value;
          if (fieldName === '_id' && typeof value === 'string') {
            try { value = new ObjectId(value); } catch (err) { }
          }

          switch (condition.operator) {
            case '=': filter[fieldName] = value; break;
            case '!=': filter[fieldName] = { $ne: value }; break;
            case '>': filter[fieldName] = { $gt: value }; break;
            case '<': filter[fieldName] = { $lt: value }; break;
            case '>=': filter[fieldName] = { $gte: value }; break;
            case '<=': filter[fieldName] = { $lte: value }; break;
            case 'in': filter[fieldName] = { $in: value }; break;
            case 'not in': filter[fieldName] = { $nin: value }; break;
          }
        }
      }
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
    
    return this.knexService.transaction(callback);
  }

  async findById(table: string, id: any): Promise<any> {
    if (this.dbType === 'mongodb') {
      return this.mongoService.findOne(table, { _id: id });
    }
    
    const knex = this.knexService.getKnex();
    return knex(table).where('id', id).first();
  }

  async findOneWhere(table: string, where: Record<string, any>): Promise<any> {
    if (this.dbType === 'mongodb') {
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

  async findWhere(table: string, where: Record<string, any>): Promise<any[]> {
    if (this.dbType === 'mongodb') {
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
      return results;
    }
    
    const knex = this.knexService.getKnex();
    return knex(table).where(where);
  }

  async insertAndGet(table: string, data: any): Promise<any> {
    if (this.dbType === 'mongodb') {
      return this.mongoService.insertOne(table, data);
    }

    const insertedId = await this.knexService.insertWithCascade(table, data);

    const knex = this.knexService.getKnex();
    const recordId = insertedId || data.id;

    return knex(table).where('id', recordId).first();
  }

  async updateById(table: string, id: any, data: any): Promise<any> {
    if (this.dbType === 'mongodb') {
      return this.mongoService.updateOne(table, id, data);
    }
    
    await this.knexService.updateWithCascade(table, id, data);
    const knex = this.knexService.getKnex();
    return knex(table).where('id', id).first();
  }

  async deleteById(table: string, id: any): Promise<number> {
    if (this.dbType === 'mongodb') {
      const deleted = await this.mongoService.deleteOne(table, id);
      return deleted ? 1 : 0;
    }
    
    const knex = this.knexService.getKnex();
    return knex(table).where('id', id).delete();
  }

  async raw(query: string | any, bindings?: any): Promise<any> {
    if (this.dbType === 'mongodb') {
      const db = this.mongoService.getDb();
      if (typeof query === 'string') {
        if (query.toLowerCase().includes('select 1')) {
          return db.command({ ping: 1 });
        }
        throw new Error('String queries not supported for MongoDB. Use db.command() object instead.');
      }
      return db.command(query);
    }
    
    const knex = this.knexService.getKnex();
    return knex.raw(query, bindings);
  }

  getConnection(): any {
    if (this.dbType === 'mongodb') {
      return this.mongoService.getDb();
    }
    return this.knexService.getKnex();
  }

  getKnex(): any {
    if (this.dbType === 'mongodb') {
      throw new Error('getKnex() is not available for MongoDB. Use getConnection() or unified methods.');
    }
    return this.knexService.getKnex();
  }

  getMongoDb(): any {
    if (this.dbType !== 'mongodb') {
      throw new Error('getMongoDb() is not available for SQL. Use getConnection() or unified methods.');
    }
    return this.mongoService.getDb();
  }

  getDatabaseType(): DatabaseType {
    return this.dbType;
  }

  isMongoDb(): boolean {
    return this.dbType === 'mongodb';
  }

  isSql(): boolean {
    return ['mysql', 'postgres'].includes(this.dbType);
  }


  private async expandFieldsToSelect(
    tableName: string,
    fields: string[],
    sortOptions: Array<{ field: string; direction: 'asc' | 'desc' }> = []
  ): Promise<string[]> {
    if (!this.metadataCache) {
      return fields;
    }

    const allMetadata = await this.metadataCache.getMetadata();

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
        return null;
      }
    };

    try {
      const result = await expandFieldsToJoinsAndSelect(tableName, fields, metadataGetter, this.dbType, sortOptions);
      return result.select;
    } catch (error) {
      return fields;
    }
  }
}


