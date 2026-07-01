import { DatabaseConfigService } from '../../../shared/services';
import { Logger } from '../../../shared/logger';
import type { Cradle } from '../../../container';
import {
  getJunctionTableName,
  getForeignKeyColumnName,
  getJunctionColumnNames,
} from '@enfyra/kernel';
import { IMetadataCache } from '../../../domain/shared/interfaces/metadata-cache.interface';
import { TCacheInvalidationPayload } from '../../../shared/types/cache.types';
import { ObjectId } from 'mongodb';
import { RedisRuntimeCacheStore } from './redis-runtime-cache-store.service';
import { EventEmitter2 } from 'eventemitter2';
import { CACHE_EVENTS } from '../../../shared/utils/cache-events.constants';
import { normalizeMongoPrimaryKeyColumn } from '../../../modules/table-management/utils/mongo-primary-key.util';
import { logMemory } from '../../../shared/utils/memory-log.util';
import { normalizeJsonFieldValue } from '../../../shared/utils/json-field-normalizer.util';

const COLOR = '\x1b[36m';
const RESET = '\x1b[0m';

export interface EnfyraMetadata {
  tables: Map<string, any>;
  tablesList: any[];
  version: number;
  timestamp: Date;
}

export class MetadataCacheService implements IMetadataCache {
  private readonly logger = new Logger(`${COLOR}MetadataCache${RESET}`);
  private inMemoryCache: EnfyraMetadata | null = null;
  private isLoading: boolean = false;
  private loadingPromise: Promise<void> | null = null;
  private readonly dbType: string;
  private readonly lazyRef: Cradle;
  private readonly redisRuntimeCacheStore?: RedisRuntimeCacheStore;
  private sharedCacheLoaded = false;
  private sharedRefreshLockValue: string | null = null;
  private systemReady = false;

  constructor(deps: {
    databaseConfigService: DatabaseConfigService;
    lazyRef: Cradle;
    redisRuntimeCacheStore?: RedisRuntimeCacheStore;
    eventEmitter?: EventEmitter2;
  }) {
    this.dbType = deps.databaseConfigService.getDbType();
    this.lazyRef = deps.lazyRef;
    this.redisRuntimeCacheStore = deps.redisRuntimeCacheStore;
    deps.eventEmitter?.once(CACHE_EVENTS.SYSTEM_READY, () => {
      this.systemReady = true;
      if (this.usesSharedRuntimeCache()) {
        this.inMemoryCache = null;
      }
    });
  }

  async reload(): Promise<void> {
    if (this.isLoading && this.loadingPromise) {
      return this.loadingPromise;
    }

    this.isLoading = true;
    this.loadingPromise = (async () => {
      try {
        logMemory(this.logger, 'metadata reload start');
        const metadata = await this.loadFreshMetadataForReload();
        logMemory(this.logger, 'metadata reload data loaded', {
          tables: metadata.tablesList.length,
        });
        await this.setLoadedMetadata(metadata);

        this.logger.log(
          `Loaded ${metadata.tablesList.length} table definitions`,
        );
        logMemory(this.logger, 'metadata reload done', {
          tables: metadata.tablesList.length,
        });
      } catch (error) {
        await this.releaseActiveSharedLock();
        this.logger.error('Failed to reload metadata cache:', error);
        throw error;
      } finally {
        this.isLoading = false;
        this.loadingPromise = null;
      }
    })();

    return this.loadingPromise;
  }

  async partialReload(payload: TCacheInvalidationPayload): Promise<void> {
    if (this.isLoading && this.loadingPromise) {
      await this.loadingPromise;
    }
    try {
      const start = Date.now();
      logMemory(this.logger, 'metadata partial reload start', {
        table: payload.table,
        scope: payload.scope,
        ids: payload.ids?.length ?? 0,
        affectedTables: payload.affectedTables?.length ?? 0,
      });
      if (this.usesSharedRuntimeCache()) {
        const lockValue =
          await this.redisRuntimeCacheStore!.acquireRefreshLockWithWait(
            'metadata',
          );
        if (!lockValue) {
          throw new Error('Metadata shared cache refresh lock timed out');
        }
        this.sharedRefreshLockValue = lockValue;
        const snapshot =
          await this.redisRuntimeCacheStore!.getSnapshot<EnfyraMetadata>(
            'metadata',
          );
        if (!snapshot) {
          await this.releaseActiveSharedLock();
          await this.reload();
          return;
        }
        this.inMemoryCache = snapshot.data;
      }
      await this.applyPartialUpdate(payload);
      if (this.usesSharedRuntimeCache()) {
        await this.redisRuntimeCacheStore!.setSnapshot(
          'metadata',
          this.inMemoryCache!,
        );
        this.sharedCacheLoaded = true;
      }
      this.logger.log(
        `Partial reload (${payload.ids?.length ?? 0} tables) in ${Date.now() - start}ms`,
      );
      logMemory(this.logger, 'metadata partial reload done', {
        table: payload.table,
        scope: payload.scope,
        ids: payload.ids?.length ?? 0,
        affectedTables: payload.affectedTables?.length ?? 0,
        durationMs: Date.now() - start,
      });
    } catch (error) {
      await this.releaseActiveSharedLock();
      this.logger.warn(
        `Partial reload failed, falling back to full: ${(error as Error).message}`,
      );
      await this.reload();
    } finally {
      await this.releaseActiveSharedLock();
      if (this.usesSharedRuntimeCache()) {
        this.inMemoryCache = null;
      }
    }
  }

  private applyRelationMappedByDerivedFields(
    relations: any[],
    relationIdMap: Map<string, any>,
    isMongoDB: boolean,
  ): void {
    for (const rel of relations) {
      const rawRef = isMongoDB ? rel.mappedBy : rel.mappedById;
      rel.mappedByRelationId =
        rawRef != null && rawRef !== '' ? String(rawRef) : null;
      if (rawRef) {
        const owningRelation = relationIdMap.get(String(rawRef));
        rel.mappedBy = owningRelation?.propertyName || null;
      } else {
        rel.mappedBy = null;
      }
    }
  }

  private async applyPartialUpdate(
    payload: TCacheInvalidationPayload,
  ): Promise<void> {
    if (!this.inMemoryCache) {
      throw new Error('Cache not initialized, cannot partial reload');
    }

    const isMongoDB = this.dbType === 'mongodb';
    const tableIds = payload.ids || [];

    let tables: any[] = [];
    if (tableIds.length > 0) {
      tables = await this.loadTablesByIds(tableIds, isMongoDB);
    }

    if (tables.length === 0 && !payload.affectedTables?.length) {
      const deletedIds = tableIds.filter(
        (tid) =>
          !tables.some(
            (t) => String(DatabaseConfigService.getRecordId(t)) === String(tid),
          ),
      );
      if (deletedIds.length > 0) {
        const namesToRemove = new Set<string>();
        for (const tid of deletedIds) {
          for (const t of this.inMemoryCache.tablesList) {
            if (String(DatabaseConfigService.getRecordId(t)) === String(tid)) {
              namesToRemove.add(t.name);
            }
          }
        }
        for (const name of namesToRemove) {
          this.inMemoryCache.tables.delete(name);
        }
        this.inMemoryCache.tablesList = this.inMemoryCache.tablesList.filter(
          (t: any) => !namesToRemove.has(t.name),
        );
        this.inMemoryCache.version = Date.now();
        this.inMemoryCache.timestamp = new Date();
      }
      return;
    }

    const allTableIds = [...tableIds];
    const affectedTableNames = new Set(payload.affectedTables || []);

    if (affectedTableNames.size > 0) {
      const affectedTables = await this.loadTablesByNames([
        ...affectedTableNames,
      ]);
      for (const t of affectedTables) {
        tables.push(t);
        const tid = String(DatabaseConfigService.getRecordId(t));
        allTableIds.push(tid);
      }
    }

    const uniqueTableIds = [...new Set(allTableIds.map(String))];

    const [columnsResult, relationsResult] = await Promise.all([
      this.loadColumnsByTableIds(uniqueTableIds, isMongoDB),
      this.loadRelationsBySourceTableIds(uniqueTableIds, isMongoDB),
    ]);

    const allRelations = relationsResult;
    const relationIdMap = new Map<string, any>();
    for (const rel of allRelations) {
      const relId = String(DatabaseConfigService.getRecordId(rel));
      relationIdMap.set(relId, rel);
    }

    const existingRelations = this.inMemoryCache.tablesList.flatMap(
      (t: any) => t.relations || [],
    );
    const reloadedSourceTableIds = new Set(uniqueTableIds);
    for (const rel of existingRelations) {
      const relId = String(DatabaseConfigService.getRecordId(rel));
      if (!relationIdMap.has(relId)) {
        relationIdMap.set(relId, rel);
        const sourceId = String(
          isMongoDB ? rel.sourceTable : rel.sourceTableId,
        );
        if (!reloadedSourceTableIds.has(sourceId)) {
          allRelations.push(rel);
        }
      }
    }

    this.applyRelationMappedByDerivedFields(
      allRelations,
      relationIdMap,
      isMongoDB,
    );

    const columnsByTable = new Map<string, any[]>();
    for (const col of columnsResult) {
      const key = String(isMongoDB ? col.table : col.tableId);
      if (!columnsByTable.has(key)) columnsByTable.set(key, []);
      columnsByTable.get(key)!.push(col);
    }

    const relationsBySource = new Map<string, any[]>();
    for (const rel of allRelations) {
      const key = String(isMongoDB ? rel.sourceTable : rel.sourceTableId);
      if (!relationsBySource.has(key)) relationsBySource.set(key, []);
      relationsBySource.get(key)!.push(rel);
    }

    const globalTableIdToName = new Map<string, string>();
    for (const t of this.inMemoryCache.tablesList) {
      const tid = String(DatabaseConfigService.getRecordId(t));
      globalTableIdToName.set(tid, t.name);
    }
    for (const t of tables) {
      const tid = String(DatabaseConfigService.getRecordId(t));
      globalTableIdToName.set(tid, t.name);
    }

    for (const table of tables) {
      const tableIdValue = String(DatabaseConfigService.getRecordId(table));
      const namesToRemove = new Set<string>();
      const rename = payload.tableRenames?.find(
        (item) => String(item.id) === tableIdValue,
      );
      if (rename?.oldName && rename.oldName !== table.name) {
        namesToRemove.add(rename.oldName);
      }
      for (const cached of this.inMemoryCache.tablesList) {
        const cachedId = String(DatabaseConfigService.getRecordId(cached));
        if (cachedId === tableIdValue && cached.name !== table.name) {
          namesToRemove.add(cached.name);
        }
      }
      for (const name of namesToRemove) {
        this.inMemoryCache.tables.delete(name);
      }
      if (namesToRemove.size > 0) {
        this.inMemoryCache.tablesList = this.inMemoryCache.tablesList.filter(
          (t: any) => !namesToRemove.has(t.name),
        );
      }

      const metadata = this.buildTableMetadata(
        table,
        columnsByTable,
        relationsBySource,
        globalTableIdToName,
        isMongoDB,
      );
      if (!metadata) continue;

      const existingIndex = this.inMemoryCache.tablesList.findIndex(
        (t: any) =>
          String(DatabaseConfigService.getRecordId(t)) === tableIdValue ||
          t.name === table.name,
      );
      if (existingIndex >= 0) {
        this.inMemoryCache.tablesList[existingIndex] = metadata;
      } else {
        this.inMemoryCache.tablesList.push(metadata);
      }
      this.inMemoryCache.tables.set(table.name, metadata);
    }

    const deletedTableIds = new Set(uniqueTableIds);
    for (const table of tables) {
      const tid = String(DatabaseConfigService.getRecordId(table));
      deletedTableIds.delete(tid);
    }
    if (deletedTableIds.size > 0) {
      const namesToRemove = new Set<string>();
      for (const tid of deletedTableIds) {
        const name = globalTableIdToName.get(tid);
        if (name) namesToRemove.add(name);
      }
      for (const name of namesToRemove) {
        this.inMemoryCache.tables.delete(name);
      }
      this.inMemoryCache.tablesList = this.inMemoryCache.tablesList.filter(
        (t: any) => !namesToRemove.has(t.name),
      );
    }

    this.inMemoryCache.version = Date.now();
    this.inMemoryCache.timestamp = new Date();
  }

  private buildTableMetadata(
    table: any,
    columnsByTable: Map<string, any[]>,
    relationsBySource: Map<string, any[]>,
    tableIdToName: Map<string, string>,
    isMongoDB: boolean,
  ): any | null {
    try {
      let uniques = [];
      let indexes = [];
      if (table.uniques) {
        if (typeof table.uniques === 'string') {
          try {
            uniques = JSON.parse(table.uniques);
          } catch (e) {
            this.logger.warn(`Failed to parse uniques for table ${table.name}`);
          }
        } else if (Array.isArray(table.uniques)) {
          uniques = table.uniques;
        }
      }
      if (table.indexes) {
        if (typeof table.indexes === 'string') {
          try {
            indexes = JSON.parse(table.indexes);
          } catch (e) {
            this.logger.warn(`Failed to parse indexes for table ${table.name}`);
          }
        } else if (Array.isArray(table.indexes)) {
          indexes = table.indexes;
        }
      }

      const tableIdValue = String(DatabaseConfigService.getRecordId(table));
      const explicitColumns = columnsByTable.get(tableIdValue) || [];

      const parsedExplicitColumns = explicitColumns.map((col: any) => {
        const column = isMongoDB
          ? normalizeMongoPrimaryKeyColumn({ ...col })
          : { ...col };
        column.metadata = normalizeJsonFieldValue(column.metadata);
        if (col.options && typeof col.options === 'string') {
          try {
            column.options = normalizeJsonFieldValue(col.options);
          } catch (e) {}
        }
        if (col.defaultValue && typeof col.defaultValue === 'string') {
          try {
            column.defaultValue = normalizeJsonFieldValue(col.defaultValue);
          } catch (e) {}
        }
        const booleanFields = [
          'isPrimary',
          'isGenerated',
          'isNullable',
          'isSystem',
          'isUpdatable',
          'isPublished',
          'isEncrypted',
        ];
        for (const field of booleanFields) {
          if (column[field] !== undefined && column[field] !== null) {
            column[field] = column[field] === 1 || column[field] === true;
          }
        }
        return column;
      });

      const relationsData = relationsBySource.get(tableIdValue) || [];
      const allRelations = Array.from(relationsBySource.values()).flat();
      const relations: any[] = [];
      for (const rel of relationsData) {
        const relBooleanFields = [
          'isNullable',
          'isSystem',
          'isUpdatable',
          'isPublished',
        ];
        for (const field of relBooleanFields) {
          if (rel[field] !== undefined && rel[field] !== null) {
            rel[field] = rel[field] === 1 || rel[field] === true;
          }
        }

        const targetTableIdValue = String(
          isMongoDB ? rel.targetTable : rel.targetTableId,
        );
        const targetTableName = tableIdToName.get(targetTableIdValue);

        const relationMetadata: any = {
          ...rel,
          metadata: normalizeJsonFieldValue(rel.metadata),
          sourceTableName: table.name,
          targetTableName: targetTableName || rel.targetTableName,
        };

        if (rel.type === 'one-to-many') {
          relationMetadata.isInverse = true;
        } else if (rel.type === 'many-to-many' && rel.mappedBy) {
          relationMetadata.isInverse = true;
        } else if (rel.type === 'one-to-one') {
          if (rel.mappedBy) {
            relationMetadata.isInverse = true;
          } else {
            relationMetadata.isInverse = false;
          }
        } else {
          relationMetadata.isInverse = false;
        }

        if (rel.type === 'many-to-one') {
          relationMetadata.foreignKeyColumn = isMongoDB
            ? rel.foreignKeyColumn || rel.propertyName
            : rel.foreignKeyColumn || getForeignKeyColumnName(rel.propertyName);
          relationMetadata.referencedColumn = rel.referencedColumn || 'id';
          relationMetadata.constraintName = rel.constraintName || null;
        }
        if (rel.type === 'one-to-one') {
          if (relationMetadata.isInverse) {
            const mappedByRelationId = isMongoDB
              ? rel.mappedByRelationId
              : rel.mappedById;
            const owningRel = allRelations.find(
              (candidate: any) =>
                String(DatabaseConfigService.getRecordId(candidate)) ===
                String(mappedByRelationId),
            );
            relationMetadata.foreignKeyColumn = isMongoDB
              ? rel.foreignKeyColumn ||
                owningRel?.foreignKeyColumn ||
                rel.mappedBy
              : owningRel?.foreignKeyColumn ||
                getForeignKeyColumnName(rel.mappedBy || rel.propertyName);
            relationMetadata.referencedColumn =
              owningRel?.referencedColumn || 'id';
            relationMetadata.constraintName = owningRel?.constraintName || null;
          } else {
            relationMetadata.foreignKeyColumn = isMongoDB
              ? rel.foreignKeyColumn || rel.propertyName
              : rel.foreignKeyColumn ||
                getForeignKeyColumnName(rel.propertyName);
            relationMetadata.referencedColumn = rel.referencedColumn || 'id';
            relationMetadata.constraintName = rel.constraintName || null;
          }
        }
        if (rel.type === 'one-to-many') {
          if (!rel.mappedBy) {
            this.logger.error(
              `O2M relation '${rel.propertyName}' in table '${table.name}' missing mappedBy`,
            );
            throw new Error(
              `One-to-many relation '${rel.propertyName}' in table '${table.name}' MUST have mappedBy`,
            );
          }
          const mappedByRelationId = isMongoDB
            ? rel.mappedByRelationId
            : rel.mappedById;
          const owningRel = allRelations.find(
            (candidate: any) =>
              String(DatabaseConfigService.getRecordId(candidate)) ===
              String(mappedByRelationId),
          );
          relationMetadata.foreignKeyColumn = isMongoDB
            ? rel.foreignKeyColumn ||
              owningRel?.foreignKeyColumn ||
              rel.mappedBy
            : owningRel?.foreignKeyColumn ||
              getForeignKeyColumnName(rel.mappedBy || rel.propertyName);
          relationMetadata.referencedColumn =
            owningRel?.referencedColumn || 'id';
          relationMetadata.constraintName = owningRel?.constraintName || null;
        }
        if (rel.type === 'many-to-many') {
          relationMetadata.junctionTableName =
            rel.junctionTableName ||
            getJunctionTableName(
              table.name,
              rel.propertyName,
              relationMetadata.targetTableName,
            );
          const { sourceColumn, targetColumn } = getJunctionColumnNames(
            table.name,
            rel.propertyName,
            relationMetadata.targetTableName,
          );
          relationMetadata.junctionSourceColumn =
            rel.junctionSourceColumn || sourceColumn;
          relationMetadata.junctionTargetColumn =
            rel.junctionTargetColumn || targetColumn;
        }

        relations.push(relationMetadata);
      }

      const combinedColumns = [...parsedExplicitColumns];

      if (!isMongoDB) {
        this.ensureSqlSystemColumns(combinedColumns);

        for (const rel of relations) {
          if (
            !['many-to-one', 'one-to-one'].includes(rel.type) ||
            rel.isInverse
          ) {
            continue;
          }

          const fkColumn = rel.foreignKeyColumn;
          const explicitFkColumn = combinedColumns.find(
            (col) => col.name === fkColumn,
          );
          if (explicitFkColumn) {
            if (rel.isUpdatable === false) {
              explicitFkColumn.isUpdatable = false;
            }
            explicitFkColumn.isForeignKey = true;
            explicitFkColumn.relationPropertyName = rel.propertyName;
            continue;
          }

          combinedColumns.push(
            this.buildForeignKeyColumn(rel, columnsByTable, tableIdToName),
          );
        }
      }

      const tableData: any = { ...table };
      tableData.metadata = normalizeJsonFieldValue(tableData.metadata);
      for (const key in tableData) {
        if (tableData[key] !== undefined && tableData[key] !== null) {
          if (tableData[key] === 1 || tableData[key] === true)
            tableData[key] = true;
          else if (tableData[key] === 0 || tableData[key] === false)
            tableData[key] = false;
        }
      }

      return {
        ...tableData,
        uniques,
        indexes,
        columns: combinedColumns,
        relations,
      };
    } catch (error) {
      this.logger.error(
        `Failed to load metadata for table ${table.name}:`,
        (error as Error).message,
      );
      return null;
    }
  }

  private ensureSqlSystemColumns(columns: any[]): void {
    this.ensureColumn(columns, {
      name: 'createdAt',
      type: 'datetime',
      isPrimary: false,
      isGenerated: false,
      isNullable: true,
      isSystem: true,
      isUpdatable: false,
      isPublished: true,
      isEncrypted: false,
      defaultValue: 'now',
    });
    this.ensureColumn(columns, {
      name: 'updatedAt',
      type: 'datetime',
      isPrimary: false,
      isGenerated: false,
      isNullable: true,
      isSystem: true,
      isUpdatable: false,
      isPublished: true,
      isEncrypted: false,
      defaultValue: 'now',
    });
  }

  private ensureColumn(columns: any[], column: any): void {
    const existing = columns.find((col) => col.name === column.name);
    if (existing) {
      if (column.isSystem === true) existing.isSystem = true;
      if (column.isUpdatable === false) existing.isUpdatable = false;
      if (column.isEncrypted === true) existing.isEncrypted = true;
      return;
    }
    columns.push(column);
  }

  private buildForeignKeyColumn(
    relation: any,
    columnsByTable: Map<string, any[]>,
    tableIdToName: Map<string, string>,
  ): any {
    const targetTableId =
      relation.targetTableId != null
        ? String(relation.targetTableId)
        : [...tableIdToName.entries()].find(
            ([, name]) => name === relation.targetTableName,
          )?.[0];
    const targetColumns = targetTableId
      ? columnsByTable.get(String(targetTableId)) || []
      : [];
    const targetPrimaryColumn = targetColumns.find((col: any) => col.isPrimary);

    return {
      name: relation.foreignKeyColumn,
      type: targetPrimaryColumn?.type || 'int',
      isPrimary: false,
      isGenerated: false,
      isNullable: relation.isNullable !== false,
      isSystem: false,
      isUpdatable: relation.isUpdatable !== false,
      isPublished: relation.isPublished !== false,
      defaultValue: null,
      options: targetPrimaryColumn?.options ?? null,
      isForeignKey: true,
      relationPropertyName: relation.propertyName,
      description: `FK column for ${relation.propertyName} relation`,
    };
  }

  private async loadMetadataFromDb(): Promise<EnfyraMetadata> {
    const isMongoDB = this.dbType === 'mongodb';

    const [tables, allColumns, allRelations] = await Promise.all([
      this.loadAllTables(),
      this.loadAllColumns(isMongoDB),
      this.loadAllRelations(isMongoDB),
    ]);

    const relationIdMap = new Map<string, any>();
    for (const rel of allRelations) {
      const relId = String(DatabaseConfigService.getRecordId(rel));
      relationIdMap.set(relId, rel);
    }
    this.applyRelationMappedByDerivedFields(
      allRelations,
      relationIdMap,
      isMongoDB,
    );

    const columnsByTable = new Map<string, any[]>();
    for (const col of allColumns) {
      const key = String(isMongoDB ? col.table : col.tableId);
      if (!columnsByTable.has(key)) columnsByTable.set(key, []);
      columnsByTable.get(key)!.push(col);
    }

    const relationsBySource = new Map<string, any[]>();
    for (const rel of allRelations) {
      const key = String(isMongoDB ? rel.sourceTable : rel.sourceTableId);
      if (!relationsBySource.has(key)) relationsBySource.set(key, []);
      relationsBySource.get(key)!.push(rel);
    }

    const tableIdToName = new Map<string, string>();
    for (const t of tables) {
      const id = String(DatabaseConfigService.getRecordId(t));
      tableIdToName.set(id, t.name);
    }

    const tablesList: any[] = [];
    const tablesMap = new Map<string, any>();

    for (const table of tables) {
      const metadata = this.buildTableMetadata(
        table,
        columnsByTable,
        relationsBySource,
        tableIdToName,
        isMongoDB,
      );
      if (!metadata) continue;
      tablesList.push(metadata);
      tablesMap.set(table.name, metadata);
    }

    return {
      tables: tablesMap,
      tablesList,
      version: Date.now(),
      timestamp: new Date(),
    };
  }

  async getMetadata(): Promise<EnfyraMetadata> {
    if (this.usesSharedRuntimeCache()) {
      if (this.inMemoryCache) return this.inMemoryCache;
      const snapshot =
        await this.redisRuntimeCacheStore!.getSnapshot<EnfyraMetadata>(
          'metadata',
        );
      if (snapshot) {
        this.sharedCacheLoaded = true;
        return this.normalizeMetadataSnapshot(snapshot.data);
      }
      return await this.loadAndCacheMetadata();
    }
    if (this.inMemoryCache) return this.inMemoryCache;
    return await this.loadAndCacheMetadata();
  }

  private initialLoadPromise: Promise<EnfyraMetadata> | null = null;

  private async loadAndCacheMetadata(): Promise<EnfyraMetadata> {
    if (this.initialLoadPromise) return this.initialLoadPromise;
    this.initialLoadPromise = (async () => {
      try {
        const metadata = await this.loadFreshMetadataForReload();
        await this.setLoadedMetadata(metadata);
        return metadata;
      } finally {
        this.initialLoadPromise = null;
      }
    })();
    return this.initialLoadPromise;
  }

  async getTableMetadata(tableName: string): Promise<any | null> {
    const metadata = await this.getMetadata();
    return metadata.tables.get(tableName) || null;
  }

  async getAllTablesMetadata(): Promise<any[]> {
    const metadata = await this.getMetadata();
    return metadata.tablesList;
  }

  async lookupTableByName(tableName: string): Promise<any | null> {
    return this.getTableMetadata(tableName);
  }

  async lookupTableById(tableId: number | string): Promise<any | null> {
    const metadata = await this.getMetadata();
    return (
      metadata.tablesList.find(
        (t) => t.id === tableId || t.id === Number(tableId),
      ) || null
    );
  }

  async clearMetadataCache(): Promise<void> {
    this.inMemoryCache = null;
    this.sharedCacheLoaded = false;
  }

  getDirectMetadata(): EnfyraMetadata {
    return this.inMemoryCache!;
  }

  isLoaded(): boolean {
    return this.usesSharedRuntimeCache()
      ? this.sharedCacheLoaded
      : this.inMemoryCache !== null;
  }

  usesSharedRuntimeCache(): boolean {
    return this.redisRuntimeCacheStore?.isEnabled() === true;
  }

  async syncFromSharedCache(timeoutMs = 10000): Promise<void> {
    if (!this.usesSharedRuntimeCache()) {
      await this.reload();
      return;
    }
    const snapshot =
      await this.redisRuntimeCacheStore!.waitForSnapshot<EnfyraMetadata>(
        'metadata',
        timeoutMs,
      );
    if (!snapshot) {
      throw new Error('Metadata shared cache is unavailable');
    }
    this.sharedCacheLoaded = true;
    this.inMemoryCache = null;
  }

  private async setLoadedMetadata(metadata: EnfyraMetadata): Promise<void> {
    metadata = this.normalizeMetadataSnapshot(metadata);
    if (this.usesSharedRuntimeCache()) {
      await this.redisRuntimeCacheStore!.setSnapshot('metadata', metadata);
      this.sharedCacheLoaded = true;
      this.inMemoryCache = this.systemReady ? null : metadata;
      await this.releaseActiveSharedLock();
      return;
    }
    this.inMemoryCache = metadata;
  }

  private normalizeMetadataSnapshot(metadata: EnfyraMetadata): EnfyraMetadata {
    for (const table of metadata.tablesList ?? []) {
      table.metadata = normalizeJsonFieldValue(table.metadata);
      for (const column of table.columns ?? []) {
        column.metadata = normalizeJsonFieldValue(column.metadata);
        column.options = normalizeJsonFieldValue(column.options);
        column.defaultValue = normalizeJsonFieldValue(column.defaultValue);
      }
      for (const relation of table.relations ?? []) {
        relation.metadata = normalizeJsonFieldValue(relation.metadata);
      }
    }

    if (!(metadata.tables instanceof Map)) {
      metadata.tables = new Map(
        (metadata.tablesList ?? [])
          .filter((table: any) => table?.name)
          .map((table: any) => [table.name, table]),
      );
    } else {
      for (const table of metadata.tablesList ?? []) {
        if (table?.name) metadata.tables.set(table.name, table);
      }
    }

    return metadata;
  }

  private async loadFreshMetadataForReload(): Promise<EnfyraMetadata> {
    if (!this.usesSharedRuntimeCache()) {
      return this.loadMetadataFromDb();
    }

    const lockValue =
      await this.redisRuntimeCacheStore!.acquireRefreshLock('metadata');
    if (!lockValue) {
      const snapshot =
        await this.redisRuntimeCacheStore!.waitForSnapshot<EnfyraMetadata>(
          'metadata',
        );
      if (snapshot) return this.normalizeMetadataSnapshot(snapshot.data);
    }

    try {
      return await this.loadMetadataFromDb();
    } finally {
      this.sharedRefreshLockValue = lockValue;
    }
  }

  private async releaseActiveSharedLock(): Promise<void> {
    if (!this.usesSharedRuntimeCache() || !this.sharedRefreshLockValue) return;
    const lockValue = this.sharedRefreshLockValue;
    this.sharedRefreshLockValue = null;
    await this.redisRuntimeCacheStore!.releaseRefreshLock(
      'metadata',
      lockValue,
    );
  }

  private async loadTablesByIds(
    ids: (string | number)[],
    isMongoDB: boolean,
  ): Promise<any[]> {
    if (isMongoDB && this.lazyRef.mongoService) {
      const collection = this.lazyRef.mongoService
        .getDb()
        .collection('enfyra_table');
      const docs = await collection
        .find({ _id: { $in: ids.map((id) => new ObjectId(id as string)) } })
        .toArray();
      return docs;
    } else if (this.lazyRef.knexService) {
      const pkField = DatabaseConfigService.getPkField();
      const rows = await this.lazyRef.knexService
        .getKnex({ skipMetadataHooks: true })
        .table('enfyra_table')
        .whereIn(pkField, ids);
      return rows;
    }
    return [];
  }

  private async loadTablesByNames(names: string[]): Promise<any[]> {
    if (this.dbType === 'mongodb' && this.lazyRef.mongoService) {
      const collection = this.lazyRef.mongoService
        .getDb()
        .collection('enfyra_table');
      const docs = await collection.find({ name: { $in: names } }).toArray();
      return docs;
    } else if (this.lazyRef.knexService) {
      const rows = await this.lazyRef.knexService
        .getKnex({ skipMetadataHooks: true })
        .table('enfyra_table')
        .whereIn('name', names);
      return rows;
    }
    return [];
  }

  private async loadColumnsByTableIds(
    tableIds: string[],
    isMongoDB: boolean,
  ): Promise<any[]> {
    if (isMongoDB && this.lazyRef.mongoService) {
      const collection = this.lazyRef.mongoService
        .getDb()
        .collection('enfyra_column');
      const docs = await collection
        .find({ table: { $in: tableIds.map((id) => new ObjectId(id)) } })
        .toArray();
      return docs;
    } else if (this.lazyRef.knexService) {
      const rows = await this.lazyRef.knexService
        .getKnex({ skipMetadataHooks: true })
        .table('enfyra_column')
        .whereIn('tableId', tableIds);
      return rows;
    }
    return [];
  }

  private async loadRelationsBySourceTableIds(
    tableIds: string[],
    isMongoDB: boolean,
  ): Promise<any[]> {
    if (isMongoDB && this.lazyRef.mongoService) {
      const collection = this.lazyRef.mongoService
        .getDb()
        .collection('enfyra_relation');
      const docs = await collection
        .find({ sourceTable: { $in: tableIds.map((id) => new ObjectId(id)) } })
        .toArray();
      return docs;
    } else if (this.lazyRef.knexService) {
      const rows = await this.lazyRef.knexService
        .getKnex({ skipMetadataHooks: true })
        .table('enfyra_relation')
        .whereIn('sourceTableId', tableIds);
      return rows;
    }
    return [];
  }

  private async loadAllTables(): Promise<any[]> {
    if (this.dbType === 'mongodb' && this.lazyRef.mongoService) {
      const collection = this.lazyRef.mongoService
        .getDb()
        .collection('enfyra_table');
      return await collection.find({}).toArray();
    } else if (this.lazyRef.knexService) {
      return await this.lazyRef.knexService
        .getKnex({ skipMetadataHooks: true })
        .table('enfyra_table')
        .select();
    }
    return [];
  }

  private async loadAllColumns(isMongoDB: boolean): Promise<any[]> {
    if (isMongoDB && this.lazyRef.mongoService) {
      const collection = this.lazyRef.mongoService
        .getDb()
        .collection('enfyra_column');
      return await collection.find({}).toArray();
    } else if (this.lazyRef.knexService) {
      return await this.lazyRef.knexService
        .getKnex({ skipMetadataHooks: true })
        .table('enfyra_column')
        .select();
    }
    return [];
  }

  private async loadAllRelations(isMongoDB: boolean): Promise<any[]> {
    if (isMongoDB && this.lazyRef.mongoService) {
      const collection = this.lazyRef.mongoService
        .getDb()
        .collection('enfyra_relation');
      return await collection.find({}).toArray();
    } else if (this.lazyRef.knexService) {
      return await this.lazyRef.knexService
        .getKnex({ skipMetadataHooks: true })
        .table('enfyra_relation')
        .select();
    }
    return [];
  }
}
