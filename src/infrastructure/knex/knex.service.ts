import { Injectable, Logger, OnModuleInit, OnModuleDestroy, forwardRef, Inject } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { Knex, knex } from 'knex';
import { MetadataCacheService } from '../cache/services/metadata-cache.service';
import { ExtendedKnex } from './types/knex-extended.types';
import { parseBooleanFields } from '../query-builder/utils/sql/parse-boolean-fields';
import { stringifyRecordJsonFields } from './utils/json-parser';
import { KnexEntityManager } from './entity-manager';
import { CascadeHandler } from './utils/cascade-handler';
import { FieldStripper } from './utils/field-stripper';
import { RelationTransformer } from './utils/relation-transformer';

@Injectable()
export class KnexService implements OnModuleInit, OnModuleDestroy {
  private knexInstance: Knex;
  private readonly logger = new Logger(KnexService.name);
  private columnTypesMap: Map<string, Map<string, string>> = new Map();
  private dbType: string;

  // Helper classes
  private cascadeHandler: CascadeHandler;
  private fieldStripper: FieldStripper;
  private relationTransformer: RelationTransformer;

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
    @Inject(forwardRef(() => MetadataCacheService))
    private readonly metadataCacheService: MetadataCacheService,
  ) {}

  async onModuleInit() {
    const DB_TYPE = this.configService.get<string>('DB_TYPE') || 'mysql';
    this.dbType = DB_TYPE;

    // Skip Knex initialization if using MongoDB
    if (DB_TYPE === 'mongodb') {
      this.logger.log('Skipping Knex initialization (DB_TYPE=mongodb)');
      return;
    }

    
    this.logger.log('🔌 Initializing Knex connection with hooks...');
    
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
        // Disable automatic date parsing - return dates as strings
        typeCast: function (field: any, next: any) {
          if (field.type === 'DATE' || field.type === 'DATETIME' || field.type === 'TIMESTAMP') {
            // Return raw string instead of Date object
            return field.string();
          }
          return next();
        },
      },
      pool: {
        min: 2,
        max: 10,
      },
      acquireConnectionTimeout: 10000,
      debug: false,
    });

    // Initialize helper classes
    this.cascadeHandler = new CascadeHandler(
      this.knexInstance,
      this.metadataCacheService,
      this.logger,
    );
    this.fieldStripper = new FieldStripper(this.metadataCacheService);
    this.relationTransformer = new RelationTransformer(
      this.metadataCacheService,
      this.logger,
    );

    // Register default hooks (replaces postProcessResponse)
    this.registerDefaultHooks();

    // Verify connection
    try {
      await this.knexInstance.raw('SELECT 1');
      this.logger.log('Knex connection established with timestamp hooks');
    } catch (error) {
      this.logger.error('Failed to establish Knex connection:', error);
      throw error;
    }
  }

  private registerDefaultHooks() {
    // Store M2M and O2M data in a Map with tableName as key (since we process one insert at a time)
    const cascadeContextMap = new Map<string, any>();

    this.addHook('beforeInsert', (tableName, data) => {
      // Store original M2M and O2M data for afterInsert hook
      // Extract relations BEFORE transformRelationsToFK deletes them
      const originalRelationData: any = {};

      if (typeof data === 'object' && !Array.isArray(data)) {
        for (const key in data) {
          if (Array.isArray(data[key])) {
            // Might be M2M or O2M relation - store it
            originalRelationData[key] = data[key];
          }
        }
      }

      // Store relation data for afterInsert hook
      cascadeContextMap.set(tableName, {
        relationData: originalRelationData
      });

      if (Array.isArray(data)) {
        return data.map(record => this.transformRelationsToFK(tableName, record));
      }
      return this.transformRelationsToFK(tableName, data);
    });

    this.addHook('beforeInsert', (tableName, data) => {
      if (Array.isArray(data)) {
        return data.map(record => this.stripUnknownColumns(tableName, record));
      }
      return this.stripUnknownColumns(tableName, data);
    });


    this.addHook('beforeInsert', async (tableName, data) => {
      const tableMetadata = await this.metadataCacheService.getTableMetadata(tableName);
      if (!tableMetadata) return data;

      if (Array.isArray(data)) {
        return data.map(record => stringifyRecordJsonFields(record, tableMetadata));
      }
      return stringifyRecordJsonFields(data, tableMetadata);
    });

    this.addHook('beforeInsert', async (tableName, data) => {
      if (await this.isJunctionTable(tableName)) return data;

      const now = this.knexInstance.raw('CURRENT_TIMESTAMP');
      if (Array.isArray(data)) {
        return data.map(record => {
          const { createdAt, updatedAt, created_at, updated_at, CreatedAt, UpdatedAt, ...cleanRecord } = record;
          return { ...cleanRecord, createdAt: now, updatedAt: now };
        });
      }
      const { createdAt, updatedAt, created_at, updated_at, CreatedAt, UpdatedAt, ...cleanData } = data;
      return { ...cleanData, createdAt: now, updatedAt: now };
    });

    this.addHook('afterInsert', async (tableName, result) => {
      await this.handleCascadeRelations(tableName, result, cascadeContextMap);
      return result;
    });

    this.addHook('beforeUpdate', async (tableName, data) => {
      // Store original O2M data and recordId for afterUpdate hook
      const originalRelationData: any = {};
      let recordId = data.id;

      if (typeof data === 'object' && !Array.isArray(data)) {
        for (const key in data) {
          if (Array.isArray(data[key])) {
            // Might be O2M relation - store it
            originalRelationData[key] = data[key];
          }
        }
      }

      cascadeContextMap.set(tableName, { relationData: originalRelationData, recordId });

      await this.syncManyToManyRelations(tableName, data);
      return this.transformRelationsToFK(tableName, data);
    });

    this.addHook('beforeUpdate', (tableName, data) => {
      return this.stripUnknownColumns(tableName, data);
    });

    this.addHook('beforeUpdate', async (tableName, data) => {
      const tableMetadata = await this.metadataCacheService.getTableMetadata(tableName);
      if (!tableMetadata) return data;
      return stringifyRecordJsonFields(data, tableMetadata);
    });

    this.addHook('beforeUpdate', (tableName, data) => {
      const { createdAt, updatedAt, created_at, updated_at, CreatedAt, UpdatedAt, ...updateData } = data;
      return this.stripNonUpdatableFields(tableName, updateData);
    });

    this.addHook('beforeUpdate', async (tableName, data) => {
      if (await this.isJunctionTable(tableName)) return data;
      return { ...data, updatedAt: this.knexInstance.raw('CURRENT_TIMESTAMP') };
    });

    this.addHook('afterUpdate', async (tableName: string, result: any) => {
      const context = cascadeContextMap.get(tableName);
      if (!context) {
        this.logger.log(`[afterUpdate] No cascade context found for table: ${tableName}`);
        return result;
      }

      const { recordId } = context;
      await this.handleCascadeRelations(tableName, recordId, cascadeContextMap);
      return result;
    });

    this.addHook('afterSelect', (tableName, result) => {
      return this.autoParseJsonFields(result, { table: tableName });
    });

    this.addHook('afterSelect', (tableName, result) => {
      return parseBooleanFields(result);
    });

    this.logger.log('🪝 Default hooks registered');
  }

  private async handleCascadeRelations(tableName: string, recordId: any, cascadeContextMap: Map<string, any>): Promise<void> {
    return this.cascadeHandler.handleCascadeRelations(tableName, recordId, cascadeContextMap);
  }

  private async isJunctionTable(tableName: string): Promise<boolean> {
    // Query metadata to check if this table is a junction table
    const metadata = await this.metadataCacheService.getMetadata();
    if (!metadata) return false;

    const tables = Array.from(metadata.tables?.values?.() || []) || metadata.tablesList || [];
    for (const table of tables) {
      if (!table.relations) continue;
      for (const rel of table.relations) {
        if (rel.type === 'many-to-many' && rel.junctionTableName === tableName) {
          return true;
        }
      }
    }
    return false;
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

  private wrapQueryBuilder(qb: any): any {
    const self = this;
    const originalInsert = qb.insert;
    const originalUpdate = qb.update;
    const originalDelete = qb.delete || qb.del;
    const originalThen = qb.then;
    const tableName = qb._single?.table;

    qb.insert = async function(data: any, ...rest: any[]) {
      const processedData = await self.runHooks('beforeInsert', tableName, data);
      const result = await originalInsert.call(this, processedData, ...rest);
      return self.runHooks('afterInsert', tableName, result);
    };

    qb.update = async function(data: any, ...rest: any[]) {
      const processedData = await self.runHooks('beforeUpdate', tableName, data);
      const result = await originalUpdate.call(this, processedData, ...rest);
      return self.runHooks('afterUpdate', tableName, result);
    };

    qb.delete = qb.del = async function(...args: any[]) {
      await self.runHooks('beforeDelete', tableName, args);
      const result = await originalDelete.call(this, ...args);
      return self.runHooks('afterDelete', tableName, result);
    };

    qb.then = function(onFulfilled: any, onRejected: any) {
      self.runHooks('beforeSelect', this, tableName);

      return originalThen.call(this, async (result: any) => {
        let processedResult = await self.runHooks('afterSelect', tableName, result);
        return onFulfilled ? onFulfilled(processedResult) : processedResult;
      }, onRejected);
    };

    return qb;
  }

  private async transformRelationsToFK(tableName: string, data: any): Promise<any> {
    return this.relationTransformer.transformRelationsToFK(tableName, data);
  }

  private async syncManyToManyRelations(tableName: string, data: any): Promise<void> {
    return this.cascadeHandler.syncManyToManyRelations(tableName, data);
  }

  private async stripUnknownColumns(tableName: string, data: any): Promise<any> {
    return this.fieldStripper.stripUnknownColumns(tableName, data);
  }

  private async stripNonUpdatableFields(tableName: string, data: any): Promise<any> {
    return this.fieldStripper.stripNonUpdatableFields(tableName, data);
  }

  async onModuleDestroy() {
    this.logger.log('🔌 Destroying Knex connection...');
    if (this.knexInstance) {
      await this.knexInstance.destroy();
      this.logger.log('Knex connection destroyed');
    }
  }

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
              return self.wrapQueryBuilder(qb);
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
        return self.wrapQueryBuilder(qb);
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

  async insertWithCascade(tableName: string, data: any): Promise<any> {
    // Create EntityManager with knexInstance (no transaction)
    const manager = new KnexEntityManager(
      this.knexInstance,
      this.hooks,
      this.dbType,
      this.logger,
      this,
    );

    return await manager.insert(tableName, data);
  }

  async updateWithCascade(tableName: string, recordId: any, data: any): Promise<void> {
    // Create EntityManager with knexInstance (no transaction)
    const manager = new KnexEntityManager(
      this.knexInstance,
      this.hooks,
      this.dbType,
      this.logger,
      this,
    );

    return await manager.update(tableName, recordId, data);
  }

}
