import { Logger } from '../../../shared/logger';
import { Knex } from 'knex';
import type { MetadataCacheService } from '../../cache/services/metadata-cache.service';
import { stringifyRecordJsonFields } from '../utils/json-parser';
import { isMetadataTable } from '../../../shared/utils/cache-events.constants';
export type HookEvent =
  | 'beforeInsert'
  | 'afterInsert'
  | 'beforeUpdate'
  | 'afterUpdate'
  | 'beforeDelete'
  | 'afterDelete'
  | 'beforeSelect'
  | 'afterSelect';
export interface HookRegistry {
  beforeInsert: Array<(tableName: string, data: any) => any>;
  afterInsert: Array<(tableName: string, result: any) => any>;
  beforeUpdate: Array<(tableName: string, data: any) => any>;
  afterUpdate: Array<(tableName: string, result: any) => any>;
  beforeDelete: Array<(tableName: string, criteria: any) => any>;
  afterDelete: Array<(tableName: string, result: any) => any>;
  beforeSelect: Array<(qb: any, tableName: string) => any>;
  afterSelect: Array<(tableName: string, result: any) => any>;
}
export class KnexHookRegistry {
  private hooks: HookRegistry = {
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
    private knexInstance: Knex,
    private metadataCacheService: MetadataCacheService,
    private logger: Logger,
    private stripUnknownColumns: (tableName: string, data: any) => Promise<any>,
    private stripNonUpdatableFields: (
      tableName: string,
      data: any,
    ) => Promise<any>,
    private transformRelationsToFK: (
      tableName: string,
      data: any,
    ) => Promise<any>,
    private syncManyToManyRelations: (
      tableName: string,
      data: any,
    ) => Promise<void>,
    private handleCascadeRelations: (
      tableName: string,
      recordId: any,
      cascadeContextMap: Map<string, any>,
    ) => Promise<void>,
    private autoParseJsonFields: (result: any, options: any) => any,
    private isJunctionTable: (tableName: string) => Promise<boolean>,
    private getPolicyContext?: () => {
      check: (
        tableName: string,
        operation: 'create' | 'update' | 'delete',
        data: any,
      ) => Promise<void>;
    } | null,
  ) {}
  getHooks(): HookRegistry {
    return this.hooks;
  }
  addHook(event: HookEvent, handler: any): void {
    if (!this.hooks[event]) throw new Error(`Unknown hook event: ${event}`);
    this.hooks[event].push(handler);
  }
  removeHook(event: HookEvent, handler: any): void {
    const index = this.hooks[event].indexOf(handler);
    if (index > -1) this.hooks[event].splice(index, 1);
  }
  async runHooks(event: HookEvent, ...args: any[]): Promise<any> {
    let result = args[args.length - 1];
    for (const hook of this.hooks[event]) {
      result = await Promise.resolve(hook.apply(null, args));
      args[args.length - 1] = result;
    }
    return result;
  }
  registerDefaultHooks(cascadeContextMap: Map<string, any>): void {
    this.addHook('beforeInsert', async (tableName, data) => {
      const relationData: any = {};
      if (typeof data === 'object' && !Array.isArray(data)) {
        for (const key in data) {
          if (Array.isArray(data[key])) {
            relationData[key] = data[key];
          }
        }
      }
      cascadeContextMap.set(tableName, relationData);
      await this.syncManyToManyRelations(tableName, data);
      return this.transformRelationsToFK(tableName, data);
    });
    this.addHook('beforeInsert', (tableName, data) => {
      return this.stripUnknownColumns(tableName, data);
    });
    this.addHook('beforeInsert', async (tableName, data) => {
      const tableMetadata =
        await this.metadataCacheService.getTableMetadata(tableName);
      if (!tableMetadata) return data;
      return stringifyRecordJsonFields(data, tableMetadata);
    });
    this.addHook('beforeInsert', async (tableName, data) => {
      if (await this.isJunctionTable(tableName)) return data;
      const tableMetadata = await this.metadataCacheService.getTableMetadata(tableName);
      if (!tableMetadata || !tableMetadata.columns) return data;

      const hasCreatedAt = tableMetadata.columns.some((c: any) => c.name === 'createdAt');
      const hasUpdatedAt = tableMetadata.columns.some((c: any) => c.name === 'updatedAt');

      if (!hasCreatedAt && !hasUpdatedAt) return data;

      const now = this.knexInstance.raw('CURRENT_TIMESTAMP');
      if (Array.isArray(data)) {
        return data.map((record) => {
          const {
            createdAt,
            updatedAt,
            created_at,
            updated_at,
            CreatedAt,
            UpdatedAt,
            ...cleanRecord
          } = record;
          const result: any = { ...cleanRecord };
          if (hasCreatedAt) result.createdAt = now;
          if (hasUpdatedAt) result.updatedAt = now;
          return result;
        });
      }
      const {
        createdAt,
        updatedAt,
        created_at,
        updated_at,
        CreatedAt,
        UpdatedAt,
        ...cleanData
      } = data;
      const result: any = { ...cleanData };
      if (hasCreatedAt) result.createdAt = now;
      if (hasUpdatedAt) result.updatedAt = now;
      return result;
    });
    this.addHook('afterInsert', async (tableName, result) => {
      await this.handleCascadeRelations(tableName, result, cascadeContextMap);
      return result;
    });
    this.addHook('beforeUpdate', async (tableName, data) => {
      const originalRelationData: any = {};
      const recordId = data.id;
      if (typeof data === 'object' && !Array.isArray(data)) {
        for (const key in data) {
          if (Array.isArray(data[key])) {
            originalRelationData[key] = data[key];
          }
        }
      }
      cascadeContextMap.set(tableName, {
        relationData: originalRelationData,
        recordId,
      });
      await this.syncManyToManyRelations(tableName, data);
      return this.transformRelationsToFK(tableName, data);
    });
    this.addHook('beforeUpdate', (tableName, data) => {
      return this.stripUnknownColumns(tableName, data);
    });
    this.addHook('beforeUpdate', async (tableName, data) => {
      const tableMetadata =
        await this.metadataCacheService.getTableMetadata(tableName);
      if (!tableMetadata) return data;
      return stringifyRecordJsonFields(data, tableMetadata);
    });
    this.addHook('beforeUpdate', (tableName, data) => {
      const {
        createdAt,
        updatedAt,
        created_at,
        updated_at,
        CreatedAt,
        UpdatedAt,
        ...updateData
      } = data;
      return this.stripNonUpdatableFields(tableName, updateData);
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
      const tableMetadata = await this.metadataCacheService.getTableMetadata(tableName);
      if (!tableMetadata || !tableMetadata.columns) return data;

      const hasUpdatedAt = tableMetadata.columns.some((c: any) => c.name === 'updatedAt');
      if (!hasUpdatedAt) return data;

      return { ...data, updatedAt: this.knexInstance.raw('CURRENT_TIMESTAMP') };
    });
    this.addHook('afterUpdate', async (tableName: string, result: any) => {
      const context = cascadeContextMap.get(tableName);
      if (!context) {
        this.logger.debug(
          `[afterUpdate] No cascade context found for table: ${tableName}`,
        );
        return result;
      }
      const { recordId } = context;
      await this.handleCascadeRelations(tableName, recordId, cascadeContextMap);
      return result;
    });
    this.addHook('afterDelete', async (tableName: string, result: any) => {
      if (result == null) {
        return result;
      }
      let metadata: any;
      try {
        metadata = await this.metadataCacheService.getTableMetadata(tableName);
      } catch (err) {
        this.logger.error(
          `[afterDelete] Failed to get metadata for ${tableName}: ${err}`,
        );
        return result;
      }
      if (!metadata || !metadata.relations) {
        return result;
      }
      const relations = Array.isArray(metadata.relations)
        ? metadata.relations
        : Object.values(metadata.relations || {});
      const oneToOneCascadeRelations = relations.filter(
        (relation: any) =>
          relation.type === 'one-to-one' &&
          relation.onDelete === 'CASCADE' &&
          relation.mappedBy,
      );
      if (oneToOneCascadeRelations.length === 0) {
        return result;
      }
      const ids = Array.isArray(result) ? result : [result];
      for (const relation of oneToOneCascadeRelations) {
        const targetTableName =
          relation.targetTableName || relation.targetTable;
        if (!targetTableName) {
          continue;
        }
        const isInverse = relation.isInverse;
        const foreignKeyColumn = relation.foreignKeyColumn;
        if (!foreignKeyColumn && !isInverse) {
          continue;
        }
        if (!isInverse) {
          if (isMetadataTable(targetTableName)) continue;
          const policyCtx = this.getPolicyContext?.();
          if (policyCtx) {
            await policyCtx.check(targetTableName, 'delete', { ids });
          }
          let targetMeta: any;
          try {
            targetMeta =
              await this.metadataCacheService.getTableMetadata(targetTableName);
          } catch (err) {
            this.logger.error(
              `[afterDelete] Failed to get target metadata for ${targetTableName}: ${err}`,
            );
            continue;
          }
          const hasIsSystem = targetMeta?.columns?.some(
            (col: any) => col.name === 'isSystem',
          );
          const qb = this.knexInstance(targetTableName).whereIn(
            foreignKeyColumn,
            ids,
          );
          if (hasIsSystem) qb.andWhere('isSystem', false);
          try {
            await qb.delete();
          } catch (err) {
            this.logger.error(
              `[afterDelete] Cascade delete failed for ${targetTableName}: ${err}`,
            );
          }
        } else {
          if (isMetadataTable(tableName)) continue;
          const policyCtx = this.getPolicyContext?.();
          if (policyCtx) {
            await policyCtx.check(tableName, 'delete', { ids });
          }
          let sourceMeta: any;
          try {
            sourceMeta =
              await this.metadataCacheService.getTableMetadata(tableName);
          } catch (err) {
            this.logger.error(
              `[afterDelete] Failed to get source metadata for ${tableName}: ${err}`,
            );
            continue;
          }
          const hasIsSystem = sourceMeta?.columns?.some(
            (col: any) => col.name === 'isSystem',
          );
          const qb = this.knexInstance(tableName).whereIn(
            foreignKeyColumn,
            ids,
          );
          if (hasIsSystem) qb.andWhere('isSystem', false);
          try {
            await qb.delete();
          } catch (err) {
            this.logger.error(
              `[afterDelete] Cascade delete failed for ${tableName} (inverse): ${err}`,
            );
          }
        }
      }
      return result;
    });
    this.addHook('afterSelect', (tableName, result) => {
      return this.autoParseJsonFields(result, { table: tableName });
    });
    this.logger.log('🪝 Default hooks registered');
  }
}
