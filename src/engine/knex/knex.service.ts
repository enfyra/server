import { Logger } from '../../shared/logger';
import { Knex, knex } from 'knex';
import { AsyncLocalStorage } from 'async_hooks';
import type { Cradle } from '../../container';
import { ExtendedKnex } from './types/knex-extended.types';
import { KnexEntityManager } from './entity-manager';
import { FieldStripper } from './utils/field-stripper';
import { parseDatabaseUri } from './utils/uri-parser';
import { DatabaseConfigService } from '../../shared/services';
import { ReplicationManager } from './services/replication-manager.service';
import { KnexHookManagerService } from './services/knex-hook-manager.service';
import { LifecycleAware } from '../../shared/interfaces/lifecycle-aware.interface';
import {
  SQL_ACQUIRE_TIMEOUT_MS,
  SQL_BOOTSTRAP_POOL_MAX_TOTAL,
  SQL_BOOTSTRAP_POOL_MIN,
} from '../../shared/utils/auto-scaling.constants';
import { EnvService } from '../../shared/services';

export class KnexService implements LifecycleAware {
  private knexInstance: Knex;
  private readonly logger = new Logger(KnexService.name);
  private columnTypesMap: Map<string, Map<string, string>> = new Map();
  private dbType: string;
  private readonly knexContext = new AsyncLocalStorage<
    Knex | Knex.Transaction
  >();
  private readonly cascadeContext = new AsyncLocalStorage<Map<string, any>>();
  private readonly policyServiceContext = new AsyncLocalStorage<{
    check: (
      tableName: string,
      operation: 'create' | 'update' | 'delete',
      data: any,
    ) => Promise<void>;
  }>();
  private readonly fieldPermissionContext = new AsyncLocalStorage<{
    check: (
      tableName: string,
      action: 'create' | 'update',
      data: any,
    ) => Promise<void>;
  }>();

  private fieldStripper: FieldStripper;
  private hookManager: KnexHookManagerService;

  private readonly databaseConfigService: DatabaseConfigService;
  private readonly envService: EnvService;
  private readonly knexHookManagerService: KnexHookManagerService;
  private readonly replicationManager?: ReplicationManager;
  private readonly lazyRef: Cradle;

  constructor(deps: {
    databaseConfigService: DatabaseConfigService;
    knexHookManagerService: KnexHookManagerService;
    replicationManager?: ReplicationManager;
    lazyRef: Cradle;
    envService: EnvService;
  }) {
    this.databaseConfigService = deps.databaseConfigService;
    this.knexHookManagerService = deps.knexHookManagerService;
    this.replicationManager = deps.replicationManager;
    this.lazyRef = deps.lazyRef;
    this.envService = deps.envService;
  }

  async init(): Promise<void> {
    const start = Date.now();
    const DB_TYPE = this.databaseConfigService.getDbType();
    this.dbType = DB_TYPE;

    if (this.databaseConfigService.isMongoDb()) {
      return;
    }

    if (this.replicationManager) {
      let retries = 50;
      while (retries > 0) {
        try {
          const masterKnex = this.replicationManager.getMasterKnex();
          if (masterKnex) {
            this.knexInstance = masterKnex;
            break;
          }
        } catch (error) {}
        await new Promise((resolve) => setTimeout(resolve, 100));
        retries--;
      }

      if (!this.knexInstance) {
        this.logger.warn(
          'ReplicationManager not ready after waiting, falling back to direct connection',
        );
      }
    }

    if (!this.knexInstance) {
      const DB_URI = this.envService.get('DB_URI');
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
      } else {
        connectionConfig = {
          host: 'localhost',
          port: this.databaseConfigService.isPostgres() ? 5432 : 3306,
          user: 'root',
          password: '',
          database: 'enfyra',
        };
      }

      this.knexInstance = knex({
        client: this.databaseConfigService.isPostgres() ? 'pg' : 'mysql2',
        connection: {
          host: connectionConfig.host,
          port: connectionConfig.port,
          user: connectionConfig.user,
          password: connectionConfig.password,
          database: connectionConfig.database,
          typeCast: (field: any, next: any) => {
            if (
              field.type === 'DATE' ||
              field.type === 'DATETIME' ||
              field.type === 'TIMESTAMP'
            ) {
              return field.string();
            }
            if (field.type === 'TINY' && field.length === 1) {
              const val = field.string();
              return val === null ? null : val === '1';
            }
            return next();
          },
        },
        pool: {
          min: SQL_BOOTSTRAP_POOL_MIN,
          max: SQL_BOOTSTRAP_POOL_MAX_TOTAL,
        },
        acquireConnectionTimeout: SQL_ACQUIRE_TIMEOUT_MS,
        debug: false,
      });
    }

    this.fieldStripper = new FieldStripper(
      this.lazyRef.metadataCacheService || (null as any),
    );
    this.hookManager = this.knexHookManagerService;

    this.hookManager.initialize(
      this.dbType,
      this.knexInstance,
      this.knexContext,
      this.cascadeContext,
      this.policyServiceContext,
      this.fieldPermissionContext,
      () => this.getActiveKnex(),
      (tableName, data) => this.stripUnknownColumns(tableName, data),
      (tableName, data) => this.stripNonUpdatableFields(tableName, data),
      (tableName, data, trx) => this.insertWithCascade(tableName, data, trx),
      (tableName, recordId, data, trx) =>
        this.updateWithCascade(tableName, recordId, data, trx),
    );

    try {
      await this.knexInstance.raw('SELECT 1');
      this.logger.log(`Connected in ${Date.now() - start}ms`);
    } catch (error) {
      this.logger.error('Failed to establish Knex connection:', error);
      throw error;
    }
  }

  async onDestroy(): Promise<void> {
    this.logger.log('Destroying Knex connection...');
    if (this.knexInstance) {
      await this.knexInstance.destroy();
      this.logger.log('Knex connection destroyed');
    }
  }

  private getActiveKnex(): Knex | Knex.Transaction {
    return this.knexContext.getStore() || this.knexInstance;
  }

  private getActiveCascadeMap(): Map<string, any> {
    return this.cascadeContext.getStore() || new Map();
  }

  private async isJunctionTable(tableName: string): Promise<boolean> {
    const metadata = await this.lazyRef.metadataCacheService?.getMetadata();
    if (!metadata) return false;

    const tables =
      Array.from(metadata.tables?.values?.() || []) ||
      metadata.tablesList ||
      [];
    for (const table of tables) {
      if (!table.relations) continue;
      for (const rel of table.relations) {
        if (
          rel.type === 'many-to-many' &&
          rel.junctionTableName === tableName
        ) {
          return true;
        }
      }
    }
    return false;
  }

  private async getBooleanFieldsForTable(
    tableName: string,
  ): Promise<Set<string>> {
    const booleanFields = new Set<string>();

    const metadata = this.lazyRef.metadataCacheService.getDirectMetadata();
    if (!metadata) return booleanFields;

    const tableMetadata =
      await this.lazyRef.metadataCacheService.lookupTableByName(tableName);
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

  private async runHooks(
    event:
      | 'beforeInsert'
      | 'afterInsert'
      | 'beforeUpdate'
      | 'afterUpdate'
      | 'beforeDelete'
      | 'afterDelete'
      | 'beforeSelect'
      | 'afterSelect',
    ...args: any[]
  ): Promise<any> {
    return this.hookManager.runHooks(event, ...args);
  }

  private wrapQueryBuilder(qb: any, currentKnex?: Knex): any {
    return this.hookManager.wrapQueryBuilder(
      qb,
      currentKnex,
      () => this.getKnexForWrite(),
      this.knexContext,
      this.cascadeContext,
    );
  }

  private async stripUnknownColumns(
    tableName: string,
    data: any,
  ): Promise<any> {
    return this.fieldStripper.stripUnknownColumns(tableName, data);
  }

  private async stripNonUpdatableFields(
    tableName: string,
    data: any,
  ): Promise<any> {
    return this.fieldStripper.stripNonUpdatableFields(tableName, data);
  }

  coordinatesPoolViaReplication(): boolean {
    return (
      !!this.replicationManager &&
      !!this.knexInstance &&
      this.knexInstance === this.replicationManager.getMasterKnex()
    );
  }

  applyCoordinatedPoolMax(poolMax: number): void {
    if (!this.knexInstance || this.dbType === 'mongodb') {
      return;
    }
    if (this.coordinatesPoolViaReplication()) {
      this.logger.warn(
        'applyCoordinatedPoolMax called but replication is active; use ReplicationManager.applyCoordinatedTotalPoolMax instead',
      );
      return;
    }
    const pool = this.knexInstance.client.pool;
    const used = typeof pool?.numUsed === 'function' ? pool.numUsed() : '?';
    const free = typeof pool?.numFree === 'function' ? pool.numFree() : '?';
    const pending =
      typeof pool?.numPendingAcquires === 'function'
        ? pool.numPendingAcquires()
        : '?';
    this.logger.debug(
      `Pool before resize: used=${used} free=${free} pending=${pending}`,
    );
    const p = pool as { min?: number; max: number };
    const nextMax = Math.max(1, Math.trunc(poolMax));
    const nextMin = Math.max(1, Math.min(2, nextMax));
    p.min = nextMin;
    p.max = nextMax;
    this.logger.log(`SQL pool coordinated: max=${nextMax} min=${nextMin}`);
  }

  getKnex(): ExtendedKnex {
    if (!this.knexInstance) {
      throw new Error('Knex instance not initialized. Call init first.');
    }

    const baseKnex = this.knexInstance;
    const getKnexForRead = () => this.getKnexForRead();
    const getKnexForWrite = () => this.getKnexForWrite();
    const wrapQueryBuilder = (qb: any, knexInstance: any) =>
      this.wrapQueryBuilder(qb, knexInstance);

    return new Proxy(baseKnex, {
      get(target, prop) {
        const value = target[prop];

        if (typeof value === 'function') {
          if (prop === 'table' || prop === 'from' || prop === 'queryBuilder') {
            return function (...args: any[]) {
              const knexInstance = getKnexForRead();
              const qb = value.apply(knexInstance, args);
              return wrapQueryBuilder(qb, knexInstance);
            };
          }

          if (prop === 'raw') {
            return function (sql: string, ...rawArgs: any[]) {
              const sqlUpper = sql.trim().toUpperCase();
              const isReadQuery =
                sqlUpper.startsWith('SELECT') ||
                sqlUpper.startsWith('SHOW') ||
                sqlUpper.startsWith('DESCRIBE') ||
                sqlUpper.startsWith('EXPLAIN');

              const knexInstance = isReadQuery
                ? getKnexForRead()
                : getKnexForWrite();
              return value.apply(knexInstance, [sql, ...rawArgs]);
            };
          }

          return value.bind(target);
        }

        return value;
      },
      apply(target, thisArg, args: [string]) {
        const knexInstance = getKnexForRead();
        const qb = Reflect.apply(target, knexInstance, args);
        return wrapQueryBuilder(qb, knexInstance);
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
    if (this.databaseConfigService.isPostgres()) {
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
          if (
            colType === 'uuid' &&
            (record[colName] === null || record[colName] === undefined)
          ) {
            record[colName] = uuid.v7();
          }
        }

        if (record.createdAt === undefined) {
          record.createdAt = now;
        }
        record.updatedAt = now;
      }
    }

    return await this.knexInstance(tableName).insert(
      Array.isArray(data) ? records : records[0],
    );
  }

  async transaction(
    callback: (trx: Knex.Transaction) => Promise<any>,
  ): Promise<any> {
    const knexInstance = this.getKnexForWrite();
    return await knexInstance.transaction(async (trx) => {
      return this.knexContext.run(trx, async () => callback(trx));
    });
  }

  async parseResult(
    result: any,
    tableName: string,
    runHooks: boolean = true,
  ): Promise<any> {
    if (!result || !tableName) {
      return result;
    }

    if (runHooks) {
      await this.hookManager.runHooks('beforeSelect', null, tableName);
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
      parsed = await Promise.all(
        result.map((record) =>
          this.parseRecord(record, columnTypes, tableName),
        ),
      );
    } else if (typeof result === 'object' && !Buffer.isBuffer(result)) {
      parsed = await this.parseRecord(result, columnTypes, tableName);
    } else {
      parsed = result;
    }

    if (runHooks) {
      parsed = await this.hookManager.runHooks(
        'afterSelect',
        tableName,
        parsed,
      );
    }

    return parsed;
  }

  private async autoParseJsonFields(
    result: any,
    queryContext?: any,
  ): Promise<any> {
    if (!result) {
      return result;
    }

    const tableName =
      queryContext?.table || queryContext?.__knexQueryUid?.split('.')[0];

    if (!tableName || !this.columnTypesMap.has(tableName)) {
      return result;
    }

    const columnTypes = this.columnTypesMap.get(tableName)!;

    if (Array.isArray(result)) {
      return Promise.all(
        result.map((record) =>
          this.parseRecord(record, columnTypes, tableName),
        ),
      );
    }

    if (typeof result === 'object' && !Buffer.isBuffer(result)) {
      return this.parseRecord(result, columnTypes, tableName);
    }

    return result;
  }

  private columnTypesLoading = new Map<string, Promise<void>>();

  private async loadColumnTypesForTable(tableName: string): Promise<void> {
    if (this.columnTypesMap.has(tableName)) {
      return;
    }

    const existing = this.columnTypesLoading.get(tableName);
    if (existing) return existing;

    const promise = (async () => {
      try {
        const tableDef = await this.knexInstance('table_definition')
          .where('name', tableName)
          .first();

        if (!tableDef) {
          return;
        }

        const columns = await this.knexInstance('column_definition')
          .where('tableId', tableDef.id)
          .select('name', 'type');

        const columnTypes = new Map<string, string>();
        for (const col of columns) {
          columnTypes.set(col.name, col.type);
        }

        this.columnTypesMap.set(tableName, columnTypes);
      } catch (error) {
        this.logger.error(
          `[loadColumnTypesForTable] Error loading columnTypes for ${tableName}:`,
          error,
        );
      } finally {
        this.columnTypesLoading.delete(tableName);
      }
    })();

    this.columnTypesLoading.set(tableName, promise);
    return promise;
  }

  private async parseRecord(
    record: any,
    columnTypes: Map<string, string>,
    tableName?: string,
  ): Promise<any> {
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
          } catch (e) {}
        }
      }
    }

    if (tableName) {
      for (const [key, value] of Object.entries(parsed)) {
        if (
          value &&
          typeof value === 'object' &&
          !Array.isArray(value) &&
          !Buffer.isBuffer(value)
        ) {
          const valueAny = value as any;
          if (valueAny.id !== undefined || valueAny.createdAt !== undefined) {
            const nestedTableName = await this.getTargetTableNameFromRelation(
              key,
              tableName,
            );
            if (nestedTableName) {
              await this.loadColumnTypesForTable(nestedTableName);
              const nestedColumnTypes =
                this.columnTypesMap.get(nestedTableName);
              if (nestedColumnTypes) {
                parsed[key] = await this.parseRecord(
                  value,
                  nestedColumnTypes,
                  nestedTableName,
                );
              }
            }
          }
        } else if (
          Array.isArray(value) &&
          value.length > 0 &&
          typeof value[0] === 'object'
        ) {
          const firstItem = value[0] as any;
          if (firstItem.id !== undefined) {
            const nestedTableName = await this.getTargetTableNameFromRelation(
              key,
              tableName,
            );
            if (nestedTableName) {
              await this.loadColumnTypesForTable(nestedTableName);
              const nestedColumnTypes =
                this.columnTypesMap.get(nestedTableName);
              if (nestedColumnTypes) {
                parsed[key] = await Promise.all(
                  value.map((item) =>
                    this.parseRecord(item, nestedColumnTypes, nestedTableName),
                  ),
                );
              }
            }
          }
        }
      }
    }

    return parsed;
  }

  private async getTargetTableNameFromRelation(
    relationName: string,
    parentTableName: string,
  ): Promise<string | null> {
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
      const tableMetadata =
        await this.lazyRef.metadataCacheService.getTableMetadata?.(
          parentTableName,
        );
      if (tableMetadata && tableMetadata.relations) {
        const relation = tableMetadata.relations.find(
          (r: any) => r.propertyName === relationName,
        );
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
      this.logger.warn(
        `[getTargetTableNameFromRelation] Failed to resolve relation '${relationName}' from '${parentTableName}'`,
      );
      return null;
    }
  }

  async runWithPolicy<T>(
    policyCheck: (
      tableName: string,
      operation: 'create' | 'update' | 'delete',
      data: any,
    ) => Promise<void>,
    callback: () => Promise<T>,
  ): Promise<T> {
    return this.policyServiceContext.run({ check: policyCheck }, callback);
  }

  async runWithFieldPermissionCheck<T>(
    checker: (
      tableName: string,
      action: 'create' | 'update',
      data: any,
    ) => Promise<void>,
    callback: () => Promise<T>,
  ): Promise<T> {
    return this.fieldPermissionContext.run({ check: checker }, callback);
  }

  async insertWithCascade(
    tableName: string,
    data: any,
    trx?: Knex | Knex.Transaction,
  ): Promise<any> {
    const connection = trx || this.knexContext.getStore() || this.knexInstance;
    const cascadeMap = this.cascadeContext.getStore() || new Map<string, any>();
    const existingPolicy = this.policyServiceContext.getStore() || null;
    const manager = new KnexEntityManager(
      connection,
      this.hookManager.getHooks(),
      this.dbType,
    );

    const run = () =>
      this.knexContext.run(connection, () =>
        this.cascadeContext.run(cascadeMap, () =>
          manager.insert(tableName, data),
        ),
      );

    if (existingPolicy) {
      return await this.policyServiceContext.run(existingPolicy, run);
    }
    return await run();
  }

  async updateWithCascade(
    tableName: string,
    recordId: any,
    data: any,
    trx?: Knex | Knex.Transaction,
  ): Promise<void> {
    const connection = trx || this.knexContext.getStore() || this.knexInstance;
    const cascadeMap = this.cascadeContext.getStore() || new Map<string, any>();
    const existingPolicy = this.policyServiceContext.getStore() || null;
    const manager = new KnexEntityManager(
      connection,
      this.hookManager.getHooks(),
      this.dbType,
    );

    const run = () =>
      this.knexContext.run(connection, () =>
        this.cascadeContext.run(cascadeMap, () =>
          manager.update(tableName, recordId, data),
        ),
      );

    if (existingPolicy) {
      return await this.policyServiceContext.run(existingPolicy, run);
    }
    return await run();
  }
}
