import { Injectable, Logger, OnModuleInit, OnModuleDestroy, forwardRef, Inject } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { Knex, knex } from 'knex';
import { RelationHandlerService } from './services/relation-handler.service';
import { MetadataCacheService } from '../cache/services/metadata-cache.service';
import { applyRelations } from './utils/query-with-relations';
import { ExtendedKnex } from '../../shared/utils/knex-extended.types';

@Injectable()
export class KnexService implements OnModuleInit, OnModuleDestroy {
  private knexInstance: Knex;
  private readonly logger = new Logger(KnexService.name);
  private columnTypesMap: Map<string, Map<string, string>> = new Map();
  private currentMetadata: any = null;
  
  // Hook registry
  private hooks: {
    beforeInsert: Array<(tableName: string, data: any) => any>;
    afterInsert: Array<(tableName: string, result: any) => any>;
    beforeUpdate: Array<(tableName: string, data: any) => any>;
    afterUpdate: Array<(tableName: string, result: any) => any>;
    beforeDelete: Array<(tableName: string, criteria: any) => any>;
    afterDelete: Array<(tableName: string, result: any) => any>;
    beforeSelect: Array<(qb: any, tableName: string) => any>;
    afterSelect: Array<(tableName: string, result: any) => any>;
  } = {
    beforeInsert: [],
    afterInsert: [],
    beforeUpdate: [],
    afterUpdate: [],
    beforeDelete: [],
    afterDelete: [],
    beforeSelect: [],
    afterSelect: [],
  };

  constructor(
    private readonly configService: ConfigService,
    @Inject(forwardRef(() => RelationHandlerService))
    private readonly relationHandler: RelationHandlerService,
    @Inject(forwardRef(() => MetadataCacheService))
    private readonly metadataCacheService: MetadataCacheService,
  ) {}

  async onModuleInit() {
    this.logger.log('🔌 Initializing Knex connection with hooks...');
    
    const DB_TYPE = this.configService.get<string>('DB_TYPE') || 'mysql';
    const DB_HOST = this.configService.get<string>('DB_HOST') || 'localhost';
    const DB_PORT = this.configService.get<number>('DB_PORT') || (DB_TYPE === 'postgres' ? 5432 : 3306);
    const DB_USERNAME = this.configService.get<string>('DB_USERNAME') || 'root';
    const DB_PASSWORD = this.configService.get<string>('DB_PASSWORD') || '';
    const DB_NAME = this.configService.get<string>('DB_NAME') || 'enfyra';

    this.knexInstance = knex({
      client: DB_TYPE === 'postgres' ? 'pg' : 'mysql2',
      connection: {
        host: DB_HOST,
        port: DB_PORT,
        user: DB_USERNAME,
        password: DB_PASSWORD,
        database: DB_NAME,
      },
      pool: {
        min: 2,
        max: 10,
      },
      acquireConnectionTimeout: 10000,
      debug: false,
    });

    // Register default hooks (replaces postProcessResponse)
    this.registerDefaultHooks();

    // Test connection
    try {
      await this.knexInstance.raw('SELECT 1');
      this.logger.log('✅ Knex connection established with timestamp hooks');
    } catch (error) {
      this.logger.error('❌ Failed to establish Knex connection:', error);
      throw error;
    }
  }

  /**
   * Register default hooks (timestamps, relations, JSON parsing, etc.)
   */
  private registerDefaultHooks() {
    // Hook 1: Transform relations to FK before insert
    this.addHook('beforeInsert', (tableName, data) => {
      if (Array.isArray(data)) {
        return data.map(record => this.transformRelationsToFK(tableName, record));
      } else {
        return this.transformRelationsToFK(tableName, data);
      }
    });

    // Hook 2: Add timestamps on insert
    this.addHook('beforeInsert', (tableName, data) => {
      const now = this.knexInstance.fn.now();
      if (Array.isArray(data)) {
        return data.map(record => ({
          ...record,
          createdAt: record.createdAt !== undefined ? record.createdAt : now,
          updatedAt: record.updatedAt !== undefined ? record.updatedAt : now,
        }));
      } else {
        return {
          ...data,
          createdAt: data.createdAt !== undefined ? data.createdAt : now,
          updatedAt: data.updatedAt !== undefined ? data.updatedAt : now,
        };
      }
    });

    // Hook 3: Transform relations to FK before update
    this.addHook('beforeUpdate', (tableName, data) => {
      return this.transformRelationsToFK(tableName, data);
    });

    // Hook 4: Strip createdAt and non-updatable fields on update
    this.addHook('beforeUpdate', (tableName, data) => {
      const { createdAt, updatedAt, ...updateData } = data;
      return this.stripNonUpdatableFields(tableName, updateData);
    });

    // Hook 5: Auto update updatedAt timestamp
    this.addHook('beforeUpdate', (tableName, data) => {
      return {
        ...data,
        updatedAt: this.knexInstance.fn.now(),
      };
    });

    // Hook 6: Parse JSON fields after select
    this.addHook('afterSelect', (tableName, result) => {
      return this.autoParseJsonFields(result, { table: tableName });
    });

    this.logger.log('🪝 Default hooks registered: relations, timestamps, JSON parsing');
  }

  addHook(event: keyof typeof this.hooks, handler: any): void {
    if (!this.hooks[event]) throw new Error(`Unknown hook event: ${event}`);
    this.hooks[event].push(handler);
  }

  removeHook(event: keyof typeof this.hooks, handler: any): void {
    const index = this.hooks[event].indexOf(handler);
    if (index > -1) this.hooks[event].splice(index, 1);
  }

  private async runHooks(event: keyof typeof this.hooks, ...args: any[]): Promise<any> {
    let result = args[args.length - 1];
    for (const hook of this.hooks[event]) {
      result = await Promise.resolve(hook.apply(null, args));
      args[args.length - 1] = result;
    }
    return result;
  }

  /**
   * Wrap query builder to intercept CRUD methods with hooks
   */
  private wrapQueryBuilder(qb: any, knexInstance: Knex): any {
    const self = this;
    const originalInsert = qb.insert;
    const originalUpdate = qb.update;
    const originalDelete = qb.delete || qb.del;
    const originalSelect = qb.select;
    const originalThen = qb.then;
    const tableName = qb._single?.table;

    // Store metadata for auto-join detection
    qb._relationMetadata = null;
    qb._joinedRelations = new Set();

    // Hook insert
    qb.insert = async function(data: any, ...rest: any[]) {
      const processedData = await self.runHooks('beforeInsert', tableName, data);
      const result = await originalInsert.call(this, processedData, ...rest);
      return self.runHooks('afterInsert', tableName, result);
    };

    // Hook update  
    qb.update = async function(data: any, ...rest: any[]) {
      const processedData = await self.runHooks('beforeUpdate', tableName, data);
      const result = await originalUpdate.call(this, processedData, ...rest);
      return self.runHooks('afterUpdate', tableName, result);
    };

    // Hook delete
    qb.delete = qb.del = async function(...args: any[]) {
      await self.runHooks('beforeDelete', tableName, args);
      const result = await originalDelete.call(this, ...args);
      return self.runHooks('afterDelete', tableName, result);
    };

    // Hook select - Auto-alias relation fields
    qb.select = function(...fields: any[]) {
      const flatFields = fields.flat();
      const processedFields: string[] = [];
      
      // Process each field
      for (const field of flatFields) {
        if (typeof field === 'string') {
          // Check if field is relation.column format
          const parts = field.split('.');
          if (parts.length >= 2 && this._joinedRelations.has(parts[0])) {
            // Relation field - add alias: mainTable.id → mainTable.id as mainTable_id
            const relationName = parts[0];
            const columnName = parts[1];
            processedFields.push(`${relationName}.${columnName} as ${relationName}_${columnName}`);
          } else {
            // Regular field
            processedFields.push(field);
          }
        } else {
          processedFields.push(field);
        }
      }
      
      return originalSelect.call(this, ...processedFields);
    };

    // Hook then (runs before query execution)
    qb.then = function(onFulfilled: any, onRejected: any) {
      // Run beforeSelect hooks
      self.runHooks('beforeSelect', this, tableName);
      
      // Execute original query and run afterSelect hooks
      return originalThen.call(this, async (result: any) => {
        let processedResult = await self.runHooks('afterSelect', tableName, result);
        
        // Auto-nest joined relation data (like TypeORM)
        if (this._joinedRelations.size > 0) {
          const { nestJoinedData } = require('./utils/nest-joined-data');
          const relations = Array.from(this._joinedRelations);
          processedResult = nestJoinedData(processedResult, relations, tableName);
        }
        
        return onFulfilled ? onFulfilled(processedResult) : processedResult;
      }, onRejected);
    };

    // Add relations helper (like TypeORM)
    qb.relations = function(relationNames: string[], metadataGetter?: (tableName: string) => any) {
      if (!relationNames || relationNames.length === 0) {
        return this;
      }
      
      // Use provided metadataGetter or fallback to injected MetadataCacheService
      const getter = metadataGetter || ((tbl: string) => self.metadataCacheService?.getTableMetadata(tbl));
      
      // Apply joins immediately
      applyRelations(this, tableName, relationNames, getter);
      
      // Mark relations as joined
      relationNames.forEach(r => this._joinedRelations.add(r.split('.')[0]));
      
      return this;
    };

    return qb;
  }

  /**
   * Transform relation objects to FK values
   * Example: { ec: { id: 3 } } → { ecId: 3 }
   */
  private transformRelationsToFK(tableName: string, data: any): any {
    if (!tableName || !this.currentMetadata) {
      return data;
    }

    const tableMeta = this.currentMetadata.tables?.get?.(tableName) || 
                      this.currentMetadata.tablesList?.find((t: any) => t.name === tableName);
    
    if (!tableMeta || !tableMeta.relations) {
      return data;
    }

    const transformed = { ...data };

    for (const relation of tableMeta.relations) {
      // Only transform M2O and O2O (they have FK columns)
      if (!['many-to-one', 'one-to-one'].includes(relation.type)) {
        continue;
      }

      const relName = relation.propertyName;
      const fkColumn = relation.foreignKeyColumn || `${relName}Id`;

      // If relation field exists in data
      if (relName in transformed) {
        const relValue = transformed[relName];
        
        if (relValue === null) {
          // Null relation → set FK to null
          transformed[fkColumn] = null;
          delete transformed[relName];
        } else if (typeof relValue === 'object' && relValue.id !== undefined) {
          // Object with id: { id: 3 } → 3
          transformed[fkColumn] = relValue.id;
          delete transformed[relName];
        } else if (typeof relValue === 'number' || typeof relValue === 'string') {
          // Direct ID value: 3 → ecId: 3
          transformed[fkColumn] = relValue;
          delete transformed[relName];
        }
        // Otherwise (array, invalid), remove relation field
        else {
          delete transformed[relName];
        }
      }
    }

    // Remove O2M and M2M relation fields (no FK columns)
    for (const relation of tableMeta.relations) {
      if (['one-to-many', 'many-to-many'].includes(relation.type)) {
        const relName = relation.propertyName;
        if (relName in transformed) {
          delete transformed[relName];
        }
      }
    }

    return transformed;
  }

  /**
   * Remove fields that have isUpdatable = false in metadata
   */
  private stripNonUpdatableFields(tableName: string, data: any): any {
    if (!tableName || !this.currentMetadata) {
      return data;
    }

    const tableMeta = this.currentMetadata.tables?.get?.(tableName) || 
                      this.currentMetadata.tablesList?.find((t: any) => t.name === tableName);
    
    if (!tableMeta || !tableMeta.columns) {
      return data;
    }

    const stripped = { ...data };
    
    for (const column of tableMeta.columns) {
      if (column.isUpdatable === false && column.name in stripped) {
        delete stripped[column.name];
      }
    }

    return stripped;
  }

  async onModuleDestroy() {
    this.logger.log('🔌 Destroying Knex connection...');
    if (this.knexInstance) {
      await this.knexInstance.destroy();
      this.logger.log('✅ Knex connection destroyed');
    }
  }

  /**
   * Get the Knex instance for querying
   * Returns a proxy that wraps query builders with hooks and custom methods
   */
  getKnex(): ExtendedKnex {
    if (!this.knexInstance) {
      throw new Error('Knex instance not initialized. Call onModuleInit first.');
    }
    
    // Return a proxy that intercepts all knex calls and wraps query builders
    const self = this;
    return new Proxy(this.knexInstance, {
      get(target, prop) {
        const value = target[prop];
        
        // If accessing a method that might return a query builder, wrap it
        if (typeof value === 'function') {
          // Special handling for methods that return query builders
          if (prop === 'table' || prop === 'from' || prop === 'queryBuilder') {
            return function(...args: any[]) {
              const qb = value.apply(target, args);
              return self.wrapQueryBuilder(qb, target);
            };
          }
          
          // Bind other methods to the target but don't wrap
          return value.bind(target);
        }
        
        return value;
      },
      apply(target, thisArg, args: [string]) {
        // Intercept knex(tableName) calls
        const qb = Reflect.apply(target, thisArg, args);
        return self.wrapQueryBuilder(qb, target);
      },
    }) as ExtendedKnex;
  }

  async raw(sql: string, bindings?: any[]): Promise<any> {
    return await this.knexInstance.raw(sql, bindings);
  }

  async hasTable(tableName: string): Promise<boolean> {
    return await this.knexInstance.schema.hasTable(tableName);
  }

  async hasColumn(tableName: string, columnName: string): Promise<boolean> {
    return await this.knexInstance.schema.hasColumn(tableName, columnName);
  }

  async getTableNames(): Promise<string[]> {
    const DB_TYPE = this.configService.get<string>('DB_TYPE') || 'mysql';
    
    if (DB_TYPE === 'postgres') {
      const result = await this.knexInstance.raw(`
        SELECT tablename 
        FROM pg_tables 
        WHERE schemaname = 'public'
      `);
      return result.rows.map((row: any) => row.tablename);
    } else {
      const result = await this.knexInstance.raw(`
        SELECT TABLE_NAME 
        FROM information_schema.TABLES 
        WHERE TABLE_SCHEMA = DATABASE()
      `);
      return result[0].map((row: any) => row.TABLE_NAME);
    }
  }
  
  /**
   * Insert with auto UUID generation and timestamps
   */
  async insertWithAutoUUID(tableName: string, data: any | any[]): Promise<any> {
    const records = Array.isArray(data) ? data : [data];
    const tableColumns = this.columnTypesMap.get(tableName);
    const now = this.knexInstance.fn.now();
    
    if (tableColumns) {
      const { randomUUID } = await import('crypto');
      // Auto-generate UUID for UUID columns that are null/undefined
      for (const record of records) {
        for (const [colName, colType] of tableColumns.entries()) {
          if (colType === 'uuid' && (record[colName] === null || record[colName] === undefined)) {
            record[colName] = randomUUID();
          }
        }
        
        // Auto-add timestamps (runtime behavior, not metadata-driven)
        if (record.createdAt === undefined) {
          record.createdAt = now;
        }
          record.updatedAt = now;
        
      }
    }
    
    return await this.knexInstance(tableName).insert(Array.isArray(data) ? records : records[0]);
  }

  async transaction(callback: (trx: Knex.Transaction) => Promise<any>): Promise<any> {
    return await this.knexInstance.transaction(callback);
  }

  /**
   * Reload Knex connection with metadata for auto-parsing JSON fields
   */
  async reloadWithMetadata(metadata: any): Promise<void> {
    this.logger.log('🔄 Reloading Knex connection with metadata for auto-parse...');

    // Store metadata for relation handling
    this.currentMetadata = metadata;

    // Build column types map from metadata
    this.columnTypesMap.clear();
    for (const table of metadata.tablesList || []) {
      const tableMap = new Map<string, string>();
      for (const col of table.columns || []) {
        tableMap.set(col.name, col.type);
      }
      this.columnTypesMap.set(table.name, tableMap);
    }

    // Close old connection
    if (this.knexInstance) {
      await this.knexInstance.destroy();
    }

    // Create new connection with postProcessResponse
    const DB_TYPE = this.configService.get<string>('DB_TYPE') || 'mysql';
    const DB_HOST = this.configService.get<string>('DB_HOST') || 'localhost';
    const DB_PORT = this.configService.get<number>('DB_PORT') || (DB_TYPE === 'postgres' ? 5432 : 3306);
    const DB_USERNAME = this.configService.get<string>('DB_USERNAME') || 'root';
    const DB_PASSWORD = this.configService.get<string>('DB_PASSWORD') || '';
    const DB_NAME = this.configService.get<string>('DB_NAME') || 'enfyra';

    this.knexInstance = knex({
      client: DB_TYPE === 'postgres' ? 'pg' : 'mysql2',
      connection: {
        host: DB_HOST,
        port: DB_PORT,
        user: DB_USERNAME,
        password: DB_PASSWORD,
        database: DB_NAME,
      },
      pool: {
        min: 2,
        max: 10,
      },
      acquireConnectionTimeout: 10000,
      debug: false,
      postProcessResponse: (result, queryContext) => {
        return this.autoParseJsonFields(result, queryContext);
      },
    });

    this.logger.log(`✅ Knex reloaded with auto-parse for ${this.columnTypesMap.size} tables`);
  }

  /**
   * Auto-parse JSON fields based on column types from metadata
   */
  private autoParseJsonFields(result: any, queryContext?: any): any {
    if (!result) return result;

    // Get table name from query context
    const tableName = queryContext?.table || queryContext?.__knexQueryUid?.split('.')[0];

    // If no table name or no metadata for this table, return as-is
    if (!tableName || !this.columnTypesMap.has(tableName)) {
      return result;
    }

    // Get column types for this table
    const columnTypes = this.columnTypesMap.get(tableName)!;

    // Handle array of records
    if (Array.isArray(result)) {
      return result.map(record => this.parseRecord(record, columnTypes));
    }

    // Handle single record
    if (typeof result === 'object' && !Buffer.isBuffer(result)) {
      return this.parseRecord(result, columnTypes);
    }

    return result;
  }

  /**
   * Parse JSON fields in a single record
   */
  private parseRecord(record: any, columnTypes: Map<string, string>): any {
    if (!record || typeof record !== 'object') {
      return record;
    }

    const parsed = { ...record };

    // Parse JSON fields only
    for (const [fieldName, fieldType] of columnTypes) {
      if ((fieldType === 'simple-json' || fieldType === 'json') && 
          parsed[fieldName] && 
          typeof parsed[fieldName] === 'string') {
        try {
          parsed[fieldName] = JSON.parse(parsed[fieldName]);
        } catch (e) {
          // Keep as string if parse fails
        }
      }
    }

    return parsed;
  }

  /**
   * Insert with cascade - handles TypeORM-like relation behavior
   * Automatically transforms relation objects and handles junction tables
   */
  async insertWithCascade(tableName: string, data: any): Promise<any> {
    // Auto-add timestamps (runtime behavior, not metadata-driven)
    const now = this.knexInstance.fn.now();
    if (data.createdAt === undefined) {
      data.createdAt = now;
    }
    if (data.updatedAt === undefined) {
      data.updatedAt = now;
    }

    if (!this.currentMetadata) {
      this.logger.warn('No metadata loaded - falling back to regular insert');
      return await this.knexInstance(tableName).insert(data);
    }

    return await this.relationHandler.insertWithCascade(
      this.knexInstance,
      tableName,
      data,
      this.currentMetadata,
    );
  }

  /**
   * Update with cascade - handles TypeORM-like relation behavior
   * Automatically transforms relation objects and handles junction tables
   */
  async updateWithCascade(tableName: string, recordId: any, data: any): Promise<void> {
    // Remove createdAt from update data (should never be updated)
    const { createdAt, ...updateData } = data;
    
    // Auto-update updatedAt timestamp (runtime behavior, not metadata-driven)
    if (updateData.updatedAt === undefined) {
      updateData.updatedAt = this.knexInstance.fn.now();
    }

    if (!this.currentMetadata) {
      this.logger.warn('No metadata loaded - falling back to regular update');
      await this.knexInstance(tableName).where('id', recordId).update(updateData);
      return;
    }

    return await this.relationHandler.updateWithCascade(
      this.knexInstance,
      tableName,
      recordId,
      updateData,
      this.currentMetadata,
    );
  }

  /**
   * Preprocess data to transform relations (without insert/update)
   * Useful when you want to handle the insert/update yourself
   */
  preprocessData(tableName: string, data: any) {
    if (!this.currentMetadata) {
      return { cleanData: data, manyToManyRelations: [], oneToManyRelations: [] };
    }

    return this.relationHandler.preprocessData(tableName, data, this.currentMetadata);
  }



}
