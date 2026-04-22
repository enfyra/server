import { Logger } from '../../../shared/logger';
import { Knex } from 'knex';
import { AsyncLocalStorage } from 'async_hooks';
import { MetadataCacheService } from '../../cache/services/metadata-cache.service';
import { CascadeHandler } from '../utils/cascade-handler';
import { FieldStripper } from '../utils/field-stripper';
import { RelationTransformer } from '../utils/relation-transformer';
import { stringifyRecordJsonFields } from '../utils/json-parser';
import { ReplicationManager } from './replication-manager.service';
import { getForeignKeyColumnName } from '../utils/sql-schema-naming.util';

export class KnexHookManagerService {
  private readonly logger = new Logger(KnexHookManagerService.name);
  private dbType: string;
  private knexContext: AsyncLocalStorage<Knex | Knex.Transaction>;
  private cascadeContext: AsyncLocalStorage<Map<string, any>>;

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

  private cascadeHandler: CascadeHandler;
  private fieldStripper: FieldStripper;
  private relationTransformer: RelationTransformer;
  private readonly metadataCacheService: MetadataCacheService;
  private readonly replicationManager?: ReplicationManager;

  constructor(deps: {
    metadataCacheService: MetadataCacheService;
    replicationManager?: ReplicationManager;
  }) {
    this.metadataCacheService = deps.metadataCacheService;
    this.replicationManager = deps.replicationManager;
  }

  initialize(
    dbType: string,
    knexInstance: Knex,
    knexContext: AsyncLocalStorage<Knex | Knex.Transaction>,
    cascadeContext: AsyncLocalStorage<Map<string, any>>,
    policyContext: AsyncLocalStorage<{
      check: (
        tableName: string,
        operation: 'create' | 'update' | 'delete',
        data: any,
      ) => Promise<void>;
    }>,
    fieldPermissionContext: AsyncLocalStorage<{
      check: (
        tableName: string,
        action: 'create' | 'update',
        data: any,
      ) => Promise<void>;
    }>,
    getActiveKnex: () => Knex | Knex.Transaction,
    stripUnknownColumns: (tableName: string, data: any) => any,
    stripNonUpdatableFields: (tableName: string, data: any) => any,
    insertWithCascade: (
      tableName: string,
      data: any,
      trx?: Knex | Knex.Transaction,
    ) => any,
    updateWithCascade: (
      tableName: string,
      recordId: any,
      data: any,
      trx?: Knex | Knex.Transaction,
    ) => any,
  ) {
    this.dbType = dbType;
    this.knexContext = knexContext;
    this.cascadeContext = cascadeContext;

    this.cascadeHandler = new CascadeHandler(
      knexInstance,
      this.metadataCacheService,
      this.logger,
      stripUnknownColumns,
      stripNonUpdatableFields,
      insertWithCascade,
      updateWithCascade,
      () => policyContext.getStore() || null,
      () => fieldPermissionContext.getStore() || null,
    );

    this.fieldStripper = new FieldStripper(this.metadataCacheService);
    this.relationTransformer = new RelationTransformer(
      this.metadataCacheService,
      this.logger,
    );

    this.registerDefaultHooks(
      knexInstance,
      knexContext,
      cascadeContext,
      policyContext,
      getActiveKnex,
    );
  }

  registerDefaultHooks(
    knexInstance: Knex,
    knexContext: AsyncLocalStorage<Knex | Knex.Transaction>,
    cascadeContext: AsyncLocalStorage<Map<string, any>>,
    policyContext: AsyncLocalStorage<{
      check: (
        tableName: string,
        operation: 'create' | 'update' | 'delete',
        data: any,
      ) => Promise<void>;
    }>,
    getActiveKnex: () => Knex | Knex.Transaction,
  ) {
    this.addHook('beforeInsert', async (tableName, data) => {
      const tableMetadata =
        await this.metadataCacheService.getTableMetadata(tableName);
      if (!tableMetadata) return data;

      const pkColumn = tableMetadata.columns.find((col) => col.isPrimary);
      if (!pkColumn || pkColumn.type !== 'uuid') return data;

      const uuid = await import('uuid');
      const pkName = pkColumn.name;

      if (Array.isArray(data)) {
        return data.map((record) => {
          if (
            !record[pkName] ||
            record[pkName] === null ||
            record[pkName] === undefined
          ) {
            return { ...record, [pkName]: uuid.v7() };
          }
          return record;
        });
      }

      if (
        !data[pkName] ||
        data[pkName] === null ||
        data[pkName] === undefined
      ) {
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
          } else if (
            value &&
            typeof value === 'object' &&
            !Buffer.isBuffer(value) &&
            !(value instanceof Date)
          ) {
            originalRelationData[key] = value;
          }
        }
      }

      const activeCascadeMap = cascadeContext.getStore() || new Map();
      activeCascadeMap.set(tableName, {
        relationData: originalRelationData,
      });

      if (Array.isArray(data)) {
        return data.map((record) =>
          this.transformRelationsToFK(tableName, record),
        );
      }
      return this.transformRelationsToFK(tableName, data);
    });

    this.addHook('beforeInsert', (tableName, data) => {
      if (Array.isArray(data)) {
        return data.map((record) =>
          this.fieldStripper.stripUnknownColumns(tableName, record),
        );
      }
      return this.fieldStripper.stripUnknownColumns(tableName, data);
    });

    this.addHook('beforeInsert', async (tableName, data) => {
      const tableMetadata =
        await this.metadataCacheService.getTableMetadata(tableName);
      if (!tableMetadata) return data;

      if (Array.isArray(data)) {
        return data.map((record) =>
          stringifyRecordJsonFields(record, tableMetadata),
        );
      }
      return stringifyRecordJsonFields(data, tableMetadata);
    });

    this.addHook('beforeInsert', async (tableName, data) => {
      if (await this.isJunctionTable(tableName)) return data;

      const tableMetadata =
        await this.metadataCacheService.getTableMetadata(tableName);
      if (!tableMetadata?.columns) return data;

      const hasCreatedAt = tableMetadata.columns.some(
        (c: any) => c.name === 'createdAt',
      );
      const hasUpdatedAt = tableMetadata.columns.some(
        (c: any) => c.name === 'updatedAt',
      );

      if (!hasCreatedAt && !hasUpdatedAt) return data;

      const now = getActiveKnex().raw('CURRENT_TIMESTAMP');
      if (Array.isArray(data)) {
        return data.map((record) => {
          const {
            createdAt: _createdAt,
            updatedAt: _updatedAt,
            created_at: _created_at,
            updated_at: _updated_at,
            CreatedAt: _CreatedAt,
            UpdatedAt: _UpdatedAt,
            ...cleanRecord
          } = record;
          const result: any = { ...cleanRecord };
          if (hasCreatedAt) result.createdAt = now;
          if (hasUpdatedAt) result.updatedAt = now;
          return result;
        });
      }
      const {
        createdAt: _createdAt,
        updatedAt: _updatedAt,
        created_at: _created_at,
        updated_at: _updated_at,
        CreatedAt: _CreatedAt,
        UpdatedAt: _UpdatedAt,
        ...cleanData
      } = data;
      const result: any = { ...cleanData };
      if (hasCreatedAt) result.createdAt = now;
      if (hasUpdatedAt) result.updatedAt = now;
      return result;
    });

    this.addHook('afterInsert', async (tableName, result) => {
      await this.handleCascadeRelations(tableName, result);
      return result;
    });

    this.addHook('beforeUpdate', async (tableName, data) => {
      const originalRelationData: any = {};
      const recordId = data.id;

      if (typeof data === 'object' && !Array.isArray(data)) {
        for (const key in data) {
          const value = data[key];
          if (Array.isArray(value)) {
            originalRelationData[key] = value;
          } else if (
            value &&
            typeof value === 'object' &&
            !Buffer.isBuffer(value) &&
            !(value instanceof Date)
          ) {
            originalRelationData[key] = value;
          }
        }
      }

      const activeCascadeMap = cascadeContext.getStore() || new Map();
      activeCascadeMap.set(tableName, {
        relationData: originalRelationData,
        recordId,
      });

      await this.syncManyToManyRelations(tableName, data);
      return this.transformRelationsToFK(tableName, data);
    });

    this.addHook('beforeUpdate', async (tableName, data) => {
      if (!data?.id) return data;

      const tableMetadata =
        await this.metadataCacheService.getTableMetadata(tableName);
      if (!tableMetadata?.uniques || !tableMetadata?.relations) return data;

      const uniques = Array.isArray(tableMetadata.uniques)
        ? tableMetadata.uniques
        : Object.values(tableMetadata.uniques || {});

      for (const relation of tableMetadata.relations) {
        if (!['one-to-one', 'many-to-one'].includes(relation.type)) continue;
        if (relation.isInverse || relation.mappedBy) continue;

        const fkColumn =
          relation.foreignKeyColumn ||
          getForeignKeyColumnName(relation.propertyName);
        if (!fkColumn || !(fkColumn in data) || data[fkColumn] == null)
          continue;

        const hasUnique = uniques.some((u: any) => {
          const cols = Array.isArray(u) ? u : [u];
          return cols.some((col: string) => {
            const colFk = col.endsWith('Id')
              ? col
              : getForeignKeyColumnName(col);
            return colFk === fkColumn || col === relation.propertyName;
          });
        });

        if (!hasUnique) continue;

        await getActiveKnex()(tableName)
          .where(fkColumn, data[fkColumn])
          .whereNot('id', data.id)
          .update({ [fkColumn]: null });
      }

      return data;
    });

    this.addHook('beforeUpdate', (tableName, data) => {
      return this.fieldStripper.stripUnknownColumns(tableName, data);
    });

    this.addHook('beforeUpdate', async (tableName, data) => {
      const tableMetadata =
        await this.metadataCacheService.getTableMetadata(tableName);
      if (!tableMetadata) return data;
      return stringifyRecordJsonFields(data, tableMetadata);
    });

    this.addHook('beforeUpdate', (tableName, data) => {
      const {
        createdAt: _createdAt,
        updatedAt: _updatedAt,
        created_at: _created_at,
        updated_at: _updated_at,
        CreatedAt: _CreatedAt,
        UpdatedAt: _UpdatedAt,
        ...updateData
      } = data;
      return this.fieldStripper.stripNonUpdatableFields(tableName, updateData);
    });

    this.addHook('beforeUpdate', async (tableName, data) => {
      const tableMetadata =
        await this.metadataCacheService.getTableMetadata(tableName);
      if (!tableMetadata || !tableMetadata.columns) return data;

      const filteredData = { ...data };

      for (const column of tableMetadata.columns) {
        if (
          column.isPublished === false &&
          column.name in filteredData &&
          filteredData[column.name] === null
        ) {
          delete filteredData[column.name];
        }
      }

      return filteredData;
    });

    this.addHook('beforeUpdate', async (tableName, data) => {
      if (await this.isJunctionTable(tableName)) return data;
      const tableMetadata =
        await this.metadataCacheService.getTableMetadata(tableName);
      if (!tableMetadata?.columns) return data;

      const hasUpdatedAt = tableMetadata.columns.some(
        (c: any) => c.name === 'updatedAt',
      );
      if (!hasUpdatedAt) return data;

      return {
        ...data,
        updatedAt: getActiveKnex().raw('CURRENT_TIMESTAMP'),
      };
    });

    this.addHook('afterUpdate', async (tableName: string, result: any) => {
      const activeCascadeMap = cascadeContext.getStore() || new Map();
      const context = activeCascadeMap.get(tableName);
      if (!context) {
        return result;
      }

      const { recordId } = context;
      await this.handleCascadeRelations(tableName, recordId);
      return result;
    });
  }

  addHook(event: keyof typeof this.hooks, handler: any): void {
    if (!this.hooks[event]) throw new Error(`Unknown hook event: ${event}`);
    this.hooks[event].push(handler);
  }

  removeHook(event: keyof typeof this.hooks, handler: any): void {
    const index = this.hooks[event].indexOf(handler);
    if (index > -1) this.hooks[event].splice(index, 1);
  }

  async runHooks(event: keyof typeof this.hooks, ...args: any[]): Promise<any> {
    let result = args[args.length - 1];
    for (const hook of this.hooks[event]) {
      result = await Promise.resolve(hook.apply(null, args));
      args[args.length - 1] = result;
    }
    return result;
  }

  wrapQueryBuilder(
    qb: any,
    currentKnex: Knex,
    getKnexForWrite: () => Knex,
    knexContext: AsyncLocalStorage<Knex | Knex.Transaction>,
    cascadeContext: AsyncLocalStorage<Map<string, any>>,
  ): any {
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

      const masterKnex = getKnexForWrite();
      if (currentKnex === masterKnex) {
        return qb;
      }

      const newQb = masterKnex(tableName);
      return newQb;
    };

    const ensureTransaction = async <T>(run: () => Promise<T>): Promise<T> => {
      const activeKnex = knexContext.getStore() || currentKnex;
      if ('commit' in activeKnex) {
        return run();
      }
      return activeKnex.transaction(async (trx) => {
        const { getIoAbortSignal } =
          await import('../../executor-engine/services/isolated-executor.service');
        const signal = getIoAbortSignal();
        if (signal) {
          const onAbort = () => {
            if (!trx.isCompleted()) trx.rollback().catch(() => {});
          };
          if (signal.aborted) throw new Error('Operation aborted');
          signal.addEventListener('abort', onAbort, { once: true });
        }
        return knexContext.run(trx, run);
      });
    };

    qb.insert = async function (data: any, ...rest: any[]) {
      const masterQb = getMasterQueryBuilder();
      const cascadeMap = cascadeContext.getStore() || new Map<string, any>();
      const runInsert = () =>
        cascadeContext.run(cascadeMap, async () => {
          const processedData = await self.runHooks(
            'beforeInsert',
            tableName,
            data,
          );
          const result = await originalInsert.call(
            masterQb,
            processedData,
            ...rest,
          );
          return self.runHooks('afterInsert', tableName, result);
        });
      return ensureTransaction(runInsert);
    };

    qb.update = async function (data: any, ...rest: any[]) {
      const masterQb = getMasterQueryBuilder();
      const cascadeMap = cascadeContext.getStore() || new Map<string, any>();
      const runUpdate = () =>
        cascadeContext.run(cascadeMap, async () => {
          const processedData = await self.runHooks(
            'beforeUpdate',
            tableName,
            data,
          );
          const result = await originalUpdate.call(
            masterQb,
            processedData,
            ...rest,
          );
          return self.runHooks('afterUpdate', tableName, result);
        });
      return ensureTransaction(runUpdate);
    };

    qb.delete = qb.del = async function (...args: any[]) {
      const masterQb = getMasterQueryBuilder();
      const runDelete = async () => {
        await self.runHooks('beforeDelete', tableName, args);
        const result = await originalDelete.call(masterQb, ...args);
        return self.runHooks('afterDelete', tableName, result);
      };
      return ensureTransaction(runDelete);
    };

    qb.then = function (onFulfilled: any, onRejected: any) {
      self.runHooks('beforeSelect', this, tableName);

      return originalThen.call(
        this,
        async (result: any) => {
          const processedResult = await self.runHooks(
            'afterSelect',
            tableName,
            result,
          );
          return onFulfilled ? onFulfilled(processedResult) : processedResult;
        },
        onRejected,
      );
    };

    return qb;
  }

  getHooks() {
    return this.hooks;
  }

  private async handleCascadeRelations(
    tableName: string,
    recordId: any,
  ): Promise<void> {
    const connection = this.knexContext.getStore();
    const cascadeMap = this.cascadeContext.getStore() || new Map();
    return this.cascadeHandler.handleCascadeRelations(
      tableName,
      recordId,
      cascadeMap,
      connection,
    );
  }

  private async isJunctionTable(tableName: string): Promise<boolean> {
    const metadata = await this.metadataCacheService.getMetadata();
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

  private async transformRelationsToFK(
    tableName: string,
    data: any,
  ): Promise<any> {
    return this.relationTransformer.transformRelationsToFK(tableName, data);
  }

  private async syncManyToManyRelations(
    tableName: string,
    data: any,
  ): Promise<void> {
    return this.cascadeHandler.syncManyToManyRelations(tableName, data);
  }
}
