import { Injectable, Logger, OnModuleInit, OnModuleDestroy, forwardRef, Inject, Optional } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { Knex, knex } from 'knex';
import { AsyncLocalStorage } from 'async_hooks';
import { MetadataCacheService } from '../cache/services/metadata-cache.service';
import { ExtendedKnex } from './types/knex-extended.types';
import { stringifyRecordJsonFields } from './utils/json-parser';
import { KnexEntityManager } from './entity-manager';
import { CascadeHandler } from './utils/cascade-handler';
import { FieldStripper } from './utils/field-stripper';
import { RelationTransformer } from './utils/relation-transformer';
import { parseDatabaseUri } from './utils/uri-parser';
import { ReplicationManager } from './services/replication-manager.service';

@Injectable()
export class KnexService implements OnModuleInit, OnModuleDestroy {
  private knexInstance: Knex;
  private readonly logger = new Logger(KnexService.name);
  private columnTypesMap: Map<string, Map<string, string>> = new Map();
  private dbType: string;
  private readonly knexContext = new AsyncLocalStorage<Knex | Knex.Transaction>();

  private cascadeHandler: CascadeHandler;
  private fieldStripper: FieldStripper;
  private relationTransformer: RelationTransformer;

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
    @Optional() @Inject(forwardRef(() => ReplicationManager))
    private readonly replicationManager?: ReplicationManager,
  ) {}

  async onModuleInit() {
    const DB_TYPE = this.configService.get<string>('DB_TYPE') || 'mysql';
    this.dbType = DB_TYPE;

    if (DB_TYPE === 'mongodb') {
      this.logger.log('Skipping Knex initialization (DB_TYPE=mongodb)');
      return;
    }

    
    this.logger.log('🔌 Initializing Knex connection with hooks...');
    
    if (this.replicationManager) {
      let retries = 50;
      while (retries > 0) {
        try {
          const masterKnex = this.replicationManager.getMasterKnex();
          if (masterKnex) {
            this.knexInstance = masterKnex;
            this.logger.log('Using replication manager for connection routing');
            break;
          }
        } catch (error) {
        }
        await new Promise(resolve => setTimeout(resolve, 100));
        retries--;
      }
      
      if (!this.knexInstance) {
        this.logger.warn('ReplicationManager not ready after waiting, falling back to direct connection');
      }
    }
    
    if (!this.knexInstance) {
      const DB_URI = this.configService.get<string>('DB_URI');
      let connectionConfig: {
        host: string;
        port: number;
        user: string;
        password: string;
        database: string;
      };

      if (DB_URI) {
        const parsed = parseDatabaseUri(DB_URI);
        connectionConfig = {
          host: parsed.host,
          port: parsed.port,
          user: parsed.user,
          password: parsed.password,
          database: parsed.database,
        };
        this.logger.log(`Using database URI: ${DB_URI.replace(/:[^:@]+@/, ':****@')}`);
      } else {
        connectionConfig = {
          host: this.configService.get<string>('DB_HOST') || 'localhost',
          port: this.configService.get<number>('DB_PORT') || (DB_TYPE === 'postgres' ? 5432 : 3306),
          user: this.configService.get<string>('DB_USERNAME') || 'root',
          password: this.configService.get<string>('DB_PASSWORD') || '',
          database: this.configService.get<string>('DB_NAME') || 'enfyra',
        };
        this.logger.warn('Using legacy DB_HOST/DB_PORT/DB_USERNAME/DB_PASSWORD/DB_NAME format. Consider migrating to DB_URI format.');
      }

      const poolMinSize = parseInt(this.configService.get<string>('DB_POOL_MIN_SIZE') || '2');
      const poolMaxSize = parseInt(this.configService.get<string>('DB_POOL_MAX_SIZE') || '10');

      this.knexInstance = knex({
        client: DB_TYPE === 'postgres' ? 'pg' : 'mysql2',
        connection: {
          host: connectionConfig.host,
          port: connectionConfig.port,
          user: connectionConfig.user,
          password: connectionConfig.password,
          database: connectionConfig.database,
          typeCast: (field: any, next: any) => {
            if (field.type === 'DATE' || field.type === 'DATETIME' || field.type === 'TIMESTAMP') {
              return field.string();
            }
            return next();
          },
        },
        pool: {
          min: poolMinSize,
          max: poolMaxSize,
        },
        acquireConnectionTimeout: parseInt(this.configService.get<string>('DB_ACQUIRE_TIMEOUT') || '10000'),
        debug: false,
      });
    }

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

    this.registerDefaultHooks();

    try {
      await this.knexInstance.raw('SELECT 1');
      this.logger.log('Knex connection established with timestamp hooks');
    } catch (error) {
      this.logger.error('Failed to establish Knex connection:', error);
      throw error;
    }
  }

  private registerDefaultHooks() {
    const cascadeContextMap = new Map<string, any>();

    this.addHook('beforeInsert', async (tableName, data) => {
      const tableMetadata = await this.metadataCacheService.getTableMetadata(tableName);
      if (!tableMetadata) return data;

      const pkColumn = tableMetadata.columns.find(col => col.isPrimary);
      if (!pkColumn || pkColumn.type !== 'uuid') return data;

      const uuid = await import('uuid');
      const pkName = pkColumn.name;

      if (Array.isArray(data)) {
        return data.map(record => {
          if (!record[pkName] || record[pkName] === null || record[pkName] === undefined) {
            return { ...record, [pkName]: uuid.v7() };
          }
          return record;
        });
      }

      if (!data[pkName] || data[pkName] === null || data[pkName] === undefined) {
        return { ...data, [pkName]: uuid.v7() };
      }

      return data;
    });

    this.addHook('beforeInsert', (tableName, data) => {
      const originalRelationData: any = {};

      if (typeof data === 'object' && !Array.isArray(data)) {
        for (const key in data) {
          const value = data[key];
          if (Array.isArray(value)) {
            originalRelationData[key] = value;
          } else if (value && typeof value === 'object' && !Buffer.isBuffer(value) && !(value instanceof Date)) {
            originalRelationData[key] = value;
          }
        }
      }

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
      const originalRelationData: any = {};
      let recordId = data.id;

      if (typeof data === 'object' && !Array.isArray(data)) {
        for (const key in data) {
          const value = data[key];
          if (Array.isArray(value)) {
            originalRelationData[key] = value;
          } else if (value && typeof value === 'object' && !Buffer.isBuffer(value) && !(value instanceof Date)) {
            originalRelationData[key] = value;
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
      const tableMetadata = await this.metadataCacheService.getTableMetadata(tableName);
      if (!tableMetadata || !tableMetadata.columns) return data;

      const filteredData = { ...data };
      
      for (const column of tableMetadata.columns) {
        if (column.isHidden === true && column.name in filteredData && filteredData[column.name] === null) {
          delete filteredData[column.name];
        }
      }

      return filteredData;
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

    this.addHook('afterSelect', async (tableName, result) => {
      await this.loadColumnTypesForTable(tableName);
      return await this.autoParseJsonFields(result, { table: tableName });
    });

    this.addHook('afterSelect', (tableName, result) => {
      if (this.dbType !== 'mysql' || result == null) return result;

      const meta = this.metadataCacheService.getDirectMetadata?.();
      if (!meta) return result;

      const booleanMap = new Map<string, Set<string>>();
      const relationMap = new Map<string, Map<string, string>>();

      const tables: any[] =
        Array.from(meta.tables?.values?.() || []) ||
        (meta.tablesList || []);

      for (const t of tables) {
        const tName = t.name || t.tableName || t;
        if (!tName) continue;

        if (!booleanMap.has(tName)) {
          const set = new Set<string>();
          for (const c of t.columns || []) {
            if (c?.type === 'boolean') set.add(c.name);
          }
          booleanMap.set(tName, set);
        }

        if (!relationMap.has(tName)) {
          const rels = new Map<string, string>();
          for (const r of t.relations || []) {
            const prop = r?.propertyName;
            const target = r?.targetTableName || r?.targetTable;
            if (prop && target) rels.set(prop, target);
          }
          relationMap.set(tName, rels);
        }
      }

      const coerce = (val: any) => (val === 0 || val === 1) ? val === 1 : val;

      const walk = (node: any, currentTable: string): any => {
        if (node == null) return node;
        if (Array.isArray(node)) return node.map(item => walk(item, currentTable));
        if (typeof node !== 'object' || Buffer.isBuffer(node) || node instanceof Date) return node;

        const bSet = booleanMap.get(currentTable) || new Set<string>();
        const rels = relationMap.get(currentTable) || new Map<string, string>();

        const out: any = { ...node };
        for (const key in out) {
          if (key === 'createdAt' || key === 'updatedAt') continue;
          const value = out[key];
          if (bSet.has(key)) {
            out[key] = coerce(value);
          } else if (value && typeof value === 'object') {
            const targetTable = rels.get(key);
            out[key] = walk(value, targetTable || currentTable);
          }
        }
        return out;
      };

      return walk(result, tableName);
    });

    this.logger.log('🪝 Default hooks registered');
  }

  private async handleCascadeRelations(tableName: string, recordId: any, cascadeContextMap: Map<string, any>): Promise<void> {
    const connection = this.knexContext.getStore() || this.knexInstance;
    return this.cascadeHandler.handleCascadeRelations(tableName, recordId, cascadeContextMap, connection);
  }

  private async isJunctionTable(tableName: string): Promise<boolean> {
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

  private async getBooleanFieldsForTable(tableName: string): Promise<Set<string>> {
    const booleanFields = new Set<string>();

    const metadata = this.metadataCacheService.getDirectMetadata();
    if (!metadata) return booleanFields;

    const tableMetadata = await this.metadataCacheService.lookupTableByName(tableName);
    if (!tableMetadata) return booleanFields;

    if (tableMetadata.columns) {
      for (const column of tableMetadata.columns) {
        if (column.type === 'boolean') {
          booleanFields.add(column.name);
        }
      }
    }

    return booleanFields;
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

  private wrapQueryBuilder(qb: any, currentKnex?: Knex): any {
    const self = this;
    const originalInsert = qb.insert;
    const originalUpdate = qb.update;
    const originalDelete = qb.delete || qb.del;
    const originalThen = qb.then;
    const tableName = qb._single?.table;

    const getMasterQueryBuilder = () => {
      if (!self.replicationManager) {
        return qb;
      }
      
      const masterKnex = self.getKnexForWrite();
      if (currentKnex === masterKnex) {
        return qb;
      }

      const newQb = masterKnex(tableName);
      return newQb;
    };

    qb.insert = async function(data: any, ...rest: any[]) {
      const masterQb = getMasterQueryBuilder();
      const processedData = await self.runHooks('beforeInsert', tableName, data);
      const result = await originalInsert.call(masterQb, processedData, ...rest);
      return self.runHooks('afterInsert', tableName, result);
    };

    qb.update = async function(data: any, ...rest: any[]) {
      const masterQb = getMasterQueryBuilder();
      const processedData = await self.runHooks('beforeUpdate', tableName, data);
      const result = await originalUpdate.call(masterQb, processedData, ...rest);
      return self.runHooks('afterUpdate', tableName, result);
    };

    qb.delete = qb.del = async function(...args: any[]) {
      const masterQb = getMasterQueryBuilder();
      await self.runHooks('beforeDelete', tableName, args);
      const result = await originalDelete.call(masterQb, ...args);
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

    const self = this;
    const baseKnex = this.knexInstance;

    return new Proxy(baseKnex, {
      get(target, prop) {
        const value = target[prop];

        if (typeof value === 'function') {
          if (prop === 'table' || prop === 'from' || prop === 'queryBuilder') {
            return function(...args: any[]) {
              const knexInstance = self.getKnexForRead();
              const qb = value.apply(knexInstance, args);
              return self.wrapQueryBuilder(qb, knexInstance);
            };
          }

          if (prop === 'raw') {
            return function(sql: string, bindings?: any) {
              const sqlUpper = sql.trim().toUpperCase();
              const isReadQuery = sqlUpper.startsWith('SELECT') || 
                                   sqlUpper.startsWith('SHOW') || 
                                   sqlUpper.startsWith('DESCRIBE') ||
                                   sqlUpper.startsWith('EXPLAIN');
              
              const knexInstance = isReadQuery ? self.getKnexForRead() : self.getKnexForWrite();
              return value.apply(knexInstance, arguments);
            };
          }

          return value.bind(target);
        }

        return value;
      },
      apply(target, thisArg, args: [string]) {
        const knexInstance = self.getKnexForRead();
        const qb = Reflect.apply(target, knexInstance, args);
        return self.wrapQueryBuilder(qb, knexInstance);
      },
    }) as ExtendedKnex;
  }

  private getKnexForRead(): Knex {
    if (this.replicationManager) {
      return this.replicationManager.getReplicaKnex();
    }
    return this.knexInstance;
  }

  private getKnexForWrite(): Knex {
    if (this.replicationManager) {
      return this.replicationManager.getMasterKnex();
    }
    return this.knexInstance;
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
      const uuid = await import('uuid');
      for (const record of records) {
        for (const [colName, colType] of tableColumns.entries()) {
          if (colType === 'uuid' && (record[colName] === null || record[colName] === undefined)) {
            record[colName] = uuid.v7();
          }
        }

        if (record.createdAt === undefined) {
          record.createdAt = now;
        }
          record.updatedAt = now;

      }
    }
    
    return await this.knexInstance(tableName).insert(Array.isArray(data) ? records : records[0]);
  }

  async transaction(callback: (trx: Knex.Transaction) => Promise<any>): Promise<any> {
    const knexInstance = this.getKnexForWrite();
    return await knexInstance.transaction(async (trx) => {
      return this.knexContext.run(trx, async () => callback(trx));
    });
  }


  async parseResult(result: any, tableName: string, runHooks: boolean = true): Promise<any> {
    if (!result || !tableName) {
      return result;
    }

    if (runHooks) {
      await this.runHooks('beforeSelect', null, tableName);
    }

    if (!this.columnTypesMap.has(tableName)) {
      await this.loadColumnTypesForTable(tableName);
    }

    if (!this.columnTypesMap.has(tableName)) {
      return result;
    }

    const columnTypes = this.columnTypesMap.get(tableName)!;

    let parsed: any;
    if (Array.isArray(result)) {
      parsed = await Promise.all(result.map(record => this.parseRecord(record, columnTypes, tableName)));
    } else if (typeof result === 'object' && !Buffer.isBuffer(result)) {
      parsed = await this.parseRecord(result, columnTypes, tableName);
    } else {
      parsed = result;
    }

    if (runHooks) {
      parsed = await this.runHooks('afterSelect', tableName, parsed);
    }

    return parsed;
  }

  private async autoParseJsonFields(result: any, queryContext?: any): Promise<any> {
    if (!result) {
      return result;
    }

    const tableName = queryContext?.table || queryContext?.__knexQueryUid?.split('.')[0];

    if (!tableName || !this.columnTypesMap.has(tableName)) {
      return result;
    }

    const columnTypes = this.columnTypesMap.get(tableName)!;

    if (Array.isArray(result)) {
      return Promise.all(result.map(record => this.parseRecord(record, columnTypes, tableName)));
    }

    if (typeof result === 'object' && !Buffer.isBuffer(result)) {
      return this.parseRecord(result, columnTypes, tableName);
    }

    return result;
  }

  private async loadColumnTypesForTable(tableName: string): Promise<void> {
    if (this.columnTypesMap.has(tableName)) {
      return;
    }

    try {
      const tableDef = await this.knexInstance('table_definition')
        .where('name', tableName)
        .first();

      if (!tableDef) {
        return;
      }

      const tableIdColumn = this.dbType === 'postgres' ? 'tableId' : 'tableId';
      const columns = await this.knexInstance('column_definition')
        .where(tableIdColumn, tableDef.id)
        .select('name', 'type');

      const columnTypes = new Map<string, string>();
      for (const col of columns) {
        columnTypes.set(col.name, col.type);
      }

      this.columnTypesMap.set(tableName, columnTypes);
    } catch (error) {
      this.logger.error(`[loadColumnTypesForTable] Error loading columnTypes for ${tableName}:`, error);
    }
  }

  private async parseRecord(record: any, columnTypes: Map<string, string>, tableName?: string): Promise<any> {
    if (!record || typeof record !== 'object') {
      return record;
    }

    const parsed = { ...record };

    for (const [fieldName, fieldType] of columnTypes) {
      if (fieldType === 'simple-json') {
        const fieldValue = parsed[fieldName];
        
        if (fieldValue === null || fieldValue === undefined) {
          continue;
        }

        if (typeof fieldValue === 'string') {
          if (fieldValue.trim() === '') {
            continue;
          }
          
        try {
            parsed[fieldName] = JSON.parse(fieldValue);
        } catch (e) {
          }
        }
      }
    }

    if (tableName) {
      for (const [key, value] of Object.entries(parsed)) {
        if (value && typeof value === 'object' && !Array.isArray(value) && !Buffer.isBuffer(value)) {
          const valueAny = value as any;
          if (valueAny.id !== undefined || valueAny.createdAt !== undefined) {
            const nestedTableName = await this.getTargetTableNameFromRelation(key, tableName);
            if (nestedTableName) {
              await this.loadColumnTypesForTable(nestedTableName);
              const nestedColumnTypes = this.columnTypesMap.get(nestedTableName);
              if (nestedColumnTypes) {
                parsed[key] = await this.parseRecord(value, nestedColumnTypes, nestedTableName);
              }
            }
          }
        } else if (Array.isArray(value) && value.length > 0 && typeof value[0] === 'object') {
          const firstItem = value[0] as any;
          if (firstItem.id !== undefined) {
            const nestedTableName = await this.getTargetTableNameFromRelation(key, tableName);
            if (nestedTableName) {
              await this.loadColumnTypesForTable(nestedTableName);
              const nestedColumnTypes = this.columnTypesMap.get(nestedTableName);
              if (nestedColumnTypes) {
                parsed[key] = await Promise.all(value.map(item => this.parseRecord(item, nestedColumnTypes, nestedTableName)));
              }
            }
          }
        }
      }
    }

    return parsed;
  }

  private async getTargetTableNameFromRelation(relationName: string, parentTableName: string): Promise<string | null> {
    if (relationName.endsWith('_definition')) {
      return relationName;
    }
    if (relationName.endsWith('s')) {
      const singular = relationName.slice(0, -1);
      if (singular.endsWith('_definition')) {
        return singular;
      }
    }

    try {
      const tableMetadata = await this.metadataCacheService.getTableMetadata?.(parentTableName);
      if (tableMetadata && tableMetadata.relations) {
        const relation = tableMetadata.relations.find((r: any) => r.propertyName === relationName);
        if (relation && relation.targetTable) {
          return relation.targetTable;
        }
      }

      const tableDef = await this.knexInstance('table_definition')
        .where('name', parentTableName)
        .first();

      if (!tableDef) {
        return null;
      }

      const relationDef = await this.knexInstance('relation_definition')
        .where('sourceTableId', tableDef.id)
        .where('propertyName', relationName)
        .first();

      if (!relationDef) {
        return null;
      }

      const targetTableDef = await this.knexInstance('table_definition')
        .where('id', relationDef.targetTableId)
        .first();

      if (!targetTableDef) {
        return null;
      }

      return targetTableDef.name;
    } catch (error) {
      this.logger.error(`[getTargetTableNameFromRelation] Error getting target table for relation ${relationName} from ${parentTableName}:`, error);
      return null;
    }
  }

  async insertWithCascade(tableName: string, data: any, trx?: Knex | Knex.Transaction): Promise<any> {
    const connection = trx || this.knexContext.getStore() || this.knexInstance;
    const manager = new KnexEntityManager(
      connection,
      this.hooks,
      this.dbType,
      this.logger,
      this,
    );

    return await this.knexContext.run(connection, async () => manager.insert(tableName, data));
  }

  async updateWithCascade(tableName: string, recordId: any, data: any, trx?: Knex | Knex.Transaction): Promise<void> {
    const connection = trx || this.knexContext.getStore() || this.knexInstance;
    const manager = new KnexEntityManager(
      connection,
      this.hooks,
      this.dbType,
      this.logger,
      this,
    );

    return await this.knexContext.run(connection, async () => manager.update(tableName, recordId, data));
  }

}
