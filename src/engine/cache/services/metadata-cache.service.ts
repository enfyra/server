import { DatabaseConfigService } from '../../../shared/services/database-config.service';
import { Logger } from '../../../shared/logger';
import type { Cradle } from '../../../container';
import { DatabaseSchemaService } from '../../knex/services/database-schema.service';
import {
  getJunctionTableName,
  getForeignKeyColumnName,
  getJunctionColumnNames,
} from '../../../domain/query-dsl/utils/sql-schema-naming.util';
import { IMetadataCache } from '../../../domain/shared/interfaces/metadata-cache.interface';
import { TCacheInvalidationPayload } from '../../../shared/types/cache.types';
import { ObjectId } from 'mongodb';

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
  private readonly databaseSchemaService: DatabaseSchemaService;
  private readonly dbType: string;
  private readonly lazyRef: Cradle;

  constructor(deps: {
    databaseSchemaService: DatabaseSchemaService;
    databaseConfigService: DatabaseConfigService;
    lazyRef: Cradle;
  }) {
    this.databaseSchemaService = deps.databaseSchemaService;
    this.dbType = deps.databaseConfigService.getDbType();
    this.lazyRef = deps.lazyRef;
  }

  async reload(): Promise<void> {
    if (this.isLoading && this.loadingPromise) {
      return this.loadingPromise;
    }

    this.isLoading = true;
    this.loadingPromise = (async () => {
      try {
        const metadata = await this.loadMetadataFromDb();
        this.inMemoryCache = metadata;

        this.logger.log(
          `Loaded ${metadata.tablesList.length} table definitions`,
        );
      } catch (error) {
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
      await this.applyPartialUpdate(payload);
      this.logger.log(
        `Partial reload (${payload.ids?.length ?? 0} tables) in ${Date.now() - start}ms`,
      );
    } catch (error) {
      this.logger.warn(
        `Partial reload failed, falling back to full: ${(error as Error).message}`,
      );
      await this.reload();
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

    let schemasForTables: Map<string, any> | null = null;
    if (!isMongoDB) {
      const tableNames = tables.map((t: any) => t.name);
      schemasForTables =
        await this.databaseSchemaService.getTableSchemas(tableNames);
    }

    const allRelations = relationsResult;
    const relationIdMap = new Map<string, any>();
    for (const rel of allRelations) {
      const relId = String(DatabaseConfigService.getRecordId(rel));
      relationIdMap.set(relId, rel);
    }

    const existingRelations = this.inMemoryCache.tablesList.flatMap(
      (t: any) => t.relations || [],
    );
    for (const rel of existingRelations) {
      const relId = String(DatabaseConfigService.getRecordId(rel));
      if (!relationIdMap.has(relId)) {
        relationIdMap.set(relId, rel);
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
      const metadata = this.buildTableMetadata(
        table,
        columnsByTable,
        relationsBySource,
        globalTableIdToName,
        schemasForTables,
        isMongoDB,
      );
      if (!metadata) continue;

      const existingIndex = this.inMemoryCache.tablesList.findIndex(
        (t: any) => t.name === table.name,
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
    schemasForTables: Map<string, any> | null,
    isMongoDB: boolean,
  ): any | null {
    try {
      let actualSchema = null;
      if (!isMongoDB) {
        actualSchema = schemasForTables?.get(table.name);
        if (!actualSchema) {
          this.logger.warn(
            `Table ${table.name} not found in database, skipping...`,
          );
          return null;
        }
      }

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
        const column = { ...col };
        if (col.options && typeof col.options === 'string') {
          try {
            column.options = JSON.parse(col.options);
          } catch (e) {}
        }
        if (col.defaultValue && typeof col.defaultValue === 'string') {
          try {
            column.defaultValue = JSON.parse(col.defaultValue);
          } catch (e) {}
        }
        const booleanFields = [
          'isPrimary',
          'isGenerated',
          'isNullable',
          'isSystem',
          'isUpdatable',
          'isPublished',
        ];
        for (const field of booleanFields) {
          if (column[field] !== undefined && column[field] !== null) {
            column[field] = column[field] === 1 || column[field] === true;
          }
        }
        return column;
      });

      const relationsData = relationsBySource.get(tableIdValue) || [];
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
            const fkColumn = isMongoDB
              ? rel.propertyName
              : getForeignKeyColumnName(rel.propertyName);
            const hasFkColumn = actualSchema?.columns?.some(
              (col: any) => col.name === fkColumn,
            );
            relationMetadata.isInverse = !hasFkColumn;
          }
        } else {
          relationMetadata.isInverse = false;
        }

        if (rel.type === 'many-to-one') {
          relationMetadata.foreignKeyColumn = isMongoDB
            ? rel.propertyName
            : getForeignKeyColumnName(rel.propertyName);
        }
        if (rel.type === 'one-to-one') {
          if (relationMetadata.isInverse) {
            relationMetadata.foreignKeyColumn = isMongoDB
              ? rel.mappedBy
              : getForeignKeyColumnName(rel.mappedBy);
          } else {
            relationMetadata.foreignKeyColumn = isMongoDB
              ? rel.propertyName
              : getForeignKeyColumnName(rel.propertyName);
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
          relationMetadata.foreignKeyColumn = isMongoDB
            ? rel.mappedBy
            : getForeignKeyColumnName(rel.mappedBy);
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

      if (!isMongoDB && actualSchema) {
        for (const rel of relations) {
          if (['many-to-one', 'one-to-one'].includes(rel.type)) {
            const fkColumn = rel.foreignKeyColumn;
            const existsInExplicit = parsedExplicitColumns.some(
              (col) => col.name === fkColumn,
            );
            if (!existsInExplicit) {
              const actualFkColumn = actualSchema.columns.find(
                (col) => col.name === fkColumn,
              );
              if (actualFkColumn) {
                combinedColumns.push({
                  ...actualFkColumn,
                  isForeignKey: true,
                  relationPropertyName: rel.propertyName,
                  isUpdatable: rel.isUpdatable !== false,
                  description: `FK column for ${rel.propertyName} relation`,
                });
              }
            } else {
              const explicitFkColumn = combinedColumns.find(
                (col) => col.name === fkColumn,
              );
              if (explicitFkColumn && rel.isUpdatable === false) {
                explicitFkColumn.isUpdatable = false;
              }
            }
          }
        }

        const hasCreatedAt = combinedColumns.some(
          (col) => col.name === 'createdAt',
        );
        const hasUpdatedAt = combinedColumns.some(
          (col) => col.name === 'updatedAt',
        );
        if (!hasCreatedAt) {
          const actualCreatedAt = actualSchema.columns.find(
            (col) => col.name === 'createdAt',
          );
          if (actualCreatedAt)
            combinedColumns.push({
              ...actualCreatedAt,
              isSystem: true,
              isUpdatable: false,
            });
        }
        if (!hasUpdatedAt) {
          const actualUpdatedAt = actualSchema.columns.find(
            (col) => col.name === 'updatedAt',
          );
          if (actualUpdatedAt)
            combinedColumns.push({
              ...actualUpdatedAt,
              isSystem: true,
              isUpdatable: false,
            });
        }
      }

      const tableData: any = { ...table };
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

  private async loadMetadataFromDb(): Promise<EnfyraMetadata> {
    const isMongoDB = this.dbType === 'mongodb';

    const [tables, allColumns, allRelations] = await Promise.all([
      this.loadAllTables(),
      this.loadAllColumns(isMongoDB),
      this.loadAllRelations(isMongoDB),
    ]);

    let allSchemas: Map<string, any> | null = null;
    if (!isMongoDB) {
      allSchemas = await this.databaseSchemaService.getAllTableSchemas();
    }

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
        allSchemas,
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
    if (this.inMemoryCache) return this.inMemoryCache;
    return await this.loadAndCacheMetadata();
  }

  private initialLoadPromise: Promise<EnfyraMetadata> | null = null;

  private async loadAndCacheMetadata(): Promise<EnfyraMetadata> {
    if (this.initialLoadPromise) return this.initialLoadPromise;
    this.initialLoadPromise = (async () => {
      try {
        const metadata = await this.loadMetadataFromDb();
        this.inMemoryCache = metadata;
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
  }

  getDirectMetadata(): EnfyraMetadata {
    return this.inMemoryCache;
  }

  isLoaded(): boolean {
    return this.inMemoryCache !== null;
  }

  private async loadTablesByIds(
    ids: (string | number)[],
    isMongoDB: boolean,
  ): Promise<any[]> {
    if (isMongoDB && this.lazyRef.mongoService) {
      const collection = this.lazyRef.mongoService
        .getDb()
        .collection('table_definition');
      const docs = await collection
        .find({ _id: { $in: ids.map((id) => new ObjectId(id as string)) } })
        .toArray();
      return docs;
    } else if (this.lazyRef.knexService) {
      const pkField = DatabaseConfigService.getPkField();
      const rows = await this.lazyRef.knexService
        .getKnex()
        .table('table_definition')
        .whereIn(pkField, ids);
      return rows;
    }
    return [];
  }

  private async loadTablesByNames(names: string[]): Promise<any[]> {
    if (this.dbType === 'mongodb' && this.lazyRef.mongoService) {
      const collection = this.lazyRef.mongoService
        .getDb()
        .collection('table_definition');
      const docs = await collection.find({ name: { $in: names } }).toArray();
      return docs;
    } else if (this.lazyRef.knexService) {
      const rows = await this.lazyRef.knexService
        .getKnex()
        .table('table_definition')
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
        .collection('column_definition');
      const docs = await collection
        .find({ table: { $in: tableIds.map((id) => new ObjectId(id)) } })
        .toArray();
      return docs;
    } else if (this.lazyRef.knexService) {
      const rows = await this.lazyRef.knexService
        .getKnex()
        .table('column_definition')
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
        .collection('relation_definition');
      const docs = await collection
        .find({ sourceTable: { $in: tableIds.map((id) => new ObjectId(id)) } })
        .toArray();
      return docs;
    } else if (this.lazyRef.knexService) {
      const rows = await this.lazyRef.knexService
        .getKnex()
        .table('relation_definition')
        .whereIn('sourceTableId', tableIds);
      return rows;
    }
    return [];
  }

  private async loadAllTables(): Promise<any[]> {
    if (this.dbType === 'mongodb' && this.lazyRef.mongoService) {
      const collection = this.lazyRef.mongoService
        .getDb()
        .collection('table_definition');
      return await collection.find({}).toArray();
    } else if (this.lazyRef.knexService) {
      return await this.lazyRef.knexService
        .getKnex()
        .table('table_definition')
        .select();
    }
    return [];
  }

  private async loadAllColumns(isMongoDB: boolean): Promise<any[]> {
    if (isMongoDB && this.lazyRef.mongoService) {
      const collection = this.lazyRef.mongoService
        .getDb()
        .collection('column_definition');
      return await collection.find({}).toArray();
    } else if (this.lazyRef.knexService) {
      return await this.lazyRef.knexService
        .getKnex()
        .table('column_definition')
        .select();
    }
    return [];
  }

  private async loadAllRelations(isMongoDB: boolean): Promise<any[]> {
    if (isMongoDB && this.lazyRef.mongoService) {
      const collection = this.lazyRef.mongoService
        .getDb()
        .collection('relation_definition');
      return await collection.find({}).toArray();
    } else if (this.lazyRef.knexService) {
      return await this.lazyRef.knexService
        .getKnex()
        .table('relation_definition')
        .select();
    }
    return [];
  }
}
