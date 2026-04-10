import {
  Injectable,
  Logger,
  Inject,
  forwardRef,
} from '@nestjs/common';
import { QueryBuilderService } from '../../query-builder/query-builder.service';
import { DatabaseSchemaService } from '../../knex/services/database-schema.service';
import {
  getJunctionTableName,
  getForeignKeyColumnName,
  getJunctionColumnNames,
} from '../../knex/utils/sql-schema-naming.util';
import { ENFYRA_ADMIN_WEBSOCKET_NAMESPACE } from '../../../shared/utils/constant';
import { TCacheInvalidationPayload } from '../../../shared/types/cache.types';
import { DynamicWebSocketGateway } from '../../../modules/websocket/gateway/dynamic-websocket.gateway';

const COLOR = '\x1b[36m';
const RESET = '\x1b[0m';

export interface EnfyraMetadata {
  tables: Map<string, any>;
  tablesList: any[];
  version: number;
  timestamp: Date;
}

@Injectable()
export class MetadataCacheService {
  private readonly logger = new Logger(`${COLOR}MetadataCache${RESET}`);
  private inMemoryCache: EnfyraMetadata | null = null;
  private isLoading: boolean = false;
  private loadingPromise: Promise<void> | null = null;

  constructor(
    @Inject(forwardRef(() => QueryBuilderService))
    private readonly queryBuilder: QueryBuilderService,
    private readonly databaseSchemaService: DatabaseSchemaService,
    private readonly websocketGateway: DynamicWebSocketGateway,
  ) {}

  async reload(): Promise<void> {
    if (this.isLoading && this.loadingPromise) {
      return this.loadingPromise;
    }

    this.isLoading = true;
    this.loadingPromise = (async () => {
      try {
        try {
          this.websocketGateway.emitToNamespace(
            ENFYRA_ADMIN_WEBSOCKET_NAMESPACE,
            '$system:metadata:reload',
            { status: 'pending' },
          );
        } catch {}

        const metadata = await this.loadMetadataFromDb();
        this.inMemoryCache = metadata;

        try {
          this.websocketGateway.emitToNamespace(
            ENFYRA_ADMIN_WEBSOCKET_NAMESPACE,
            '$system:metadata:reload',
            { status: 'done' },
          );
        } catch {}

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
        `Partial reload failed, falling back to full: ${error.message}`,
      );
      await this.reload();
    }
  }

  private async applyPartialUpdate(
    payload: TCacheInvalidationPayload,
  ): Promise<void> {
    if (!this.inMemoryCache) {
      throw new Error('Cache not initialized, cannot partial reload');
    }

    const isMongoDB = this.queryBuilder.isMongoDb();
    const tableIds = payload.ids || [];

    let tables: any[] = [];
    if (tableIds.length > 0) {
      const tablesResult = await this.queryBuilder.select({
        tableName: 'table_definition',
        filter: { id: { _in: tableIds } },
      });
      tables = tablesResult.data;
    }

    if (tables.length === 0 && !payload.affectedTables?.length) {
      return;
    }

    const allTableIds = [...tableIds];
    const affectedTableNames = new Set(payload.affectedTables || []);

    if (affectedTableNames.size > 0) {
      const affectedResult = await this.queryBuilder.select({
        tableName: 'table_definition',
        filter: { name: { _in: [...affectedTableNames] } },
      });
      for (const t of affectedResult.data) {
        tables.push(t);
        const tid = isMongoDB ? String(t._id) : t.id;
        allTableIds.push(tid);
      }
    }

    const uniqueTableIds = [...new Set(allTableIds.map(String))];

    const [columnsResult, relationsResult] = await Promise.all([
      this.queryBuilder.select({
        tableName: 'column_definition',
        filter: isMongoDB
          ? { table: { _in: uniqueTableIds } }
          : { tableId: { _in: uniqueTableIds } },
      }),
      this.queryBuilder.select({
        tableName: 'relation_definition',
        filter: isMongoDB
          ? { sourceTable: { _in: uniqueTableIds } }
          : { sourceTableId: { _in: uniqueTableIds } },
      }),
    ]);

    let schemasForTables: Map<string, any> | null = null;
    if (!isMongoDB) {
      const tableNames = tables.map((t: any) => t.name);
      schemasForTables =
        await this.databaseSchemaService.getTableSchemas(tableNames);
    }

    const allRelations = relationsResult.data;
    const relationIdMap = new Map<string, any>();
    for (const rel of allRelations) {
      const relId = isMongoDB ? String(rel._id) : String(rel.id);
      relationIdMap.set(relId, rel);
    }

    const existingRelations = this.inMemoryCache.tablesList.flatMap(
      (t: any) => t.relations || [],
    );
    for (const rel of existingRelations) {
      const relId = isMongoDB ? String(rel._id) : String(rel.id);
      if (!relationIdMap.has(relId)) {
        relationIdMap.set(relId, rel);
      }
    }

    for (const rel of allRelations) {
      const mappedById = isMongoDB ? rel.mappedBy : rel.mappedById;
      if (mappedById) {
        const owningRelation = relationIdMap.get(String(mappedById));
        rel.mappedBy = owningRelation?.propertyName || null;
      } else {
        rel.mappedBy = null;
      }
    }

    const columnsByTable = new Map<string, any[]>();
    for (const col of columnsResult.data) {
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
      const tid = String(isMongoDB ? t._id : t.id);
      globalTableIdToName.set(tid, t.name);
    }
    for (const t of tables) {
      const tid = String(isMongoDB ? t._id : t.id);
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
      const tid = String(isMongoDB ? table._id : table.id);
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

    try {
      this.websocketGateway.emitToNamespace(
        ENFYRA_ADMIN_WEBSOCKET_NAMESPACE,
        '$system:metadata:reload',
        { status: 'done' },
      );
    } catch {}
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
          try { uniques = JSON.parse(table.uniques); } catch (e) {
            this.logger.warn(`Failed to parse uniques for table ${table.name}`);
          }
        } else if (Array.isArray(table.uniques)) {
          uniques = table.uniques;
        }
      }
      if (table.indexes) {
        if (typeof table.indexes === 'string') {
          try { indexes = JSON.parse(table.indexes); } catch (e) {
            this.logger.warn(`Failed to parse indexes for table ${table.name}`);
          }
        } else if (Array.isArray(table.indexes)) {
          indexes = table.indexes;
        }
      }

      const tableIdValue = String(isMongoDB ? table._id : table.id);
      const explicitColumns = columnsByTable.get(tableIdValue) || [];

      const parsedExplicitColumns = explicitColumns.map((col: any) => {
        const column = { ...col };
        if (col.options && typeof col.options === 'string') {
          try { column.options = JSON.parse(col.options); } catch (e) {}
        }
        if (col.defaultValue && typeof col.defaultValue === 'string') {
          try { column.defaultValue = JSON.parse(col.defaultValue); } catch (e) {}
        }
        const booleanFields = ['isPrimary', 'isGenerated', 'isNullable', 'isSystem', 'isUpdatable', 'isPublished'];
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
        const relBooleanFields = ['isNullable', 'isSystem', 'isUpdatable', 'isPublished'];
        for (const field of relBooleanFields) {
          if (rel[field] !== undefined && rel[field] !== null) {
            rel[field] = rel[field] === 1 || rel[field] === true;
          }
        }

        const targetTableIdValue = String(isMongoDB ? rel.targetTable : rel.targetTableId);
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
            const fkColumn = isMongoDB ? rel.propertyName : getForeignKeyColumnName(rel.propertyName);
            const hasFkColumn = actualSchema?.columns?.some((col: any) => col.name === fkColumn);
            relationMetadata.isInverse = !hasFkColumn;
          }
        } else {
          relationMetadata.isInverse = false;
        }

        if (rel.type === 'many-to-one') {
          relationMetadata.foreignKeyColumn = isMongoDB ? rel.propertyName : getForeignKeyColumnName(rel.propertyName);
        }
        if (rel.type === 'one-to-one') {
          if (relationMetadata.isInverse) {
            relationMetadata.foreignKeyColumn = isMongoDB ? rel.mappedBy : getForeignKeyColumnName(rel.mappedBy);
          } else {
            relationMetadata.foreignKeyColumn = isMongoDB ? rel.propertyName : getForeignKeyColumnName(rel.propertyName);
          }
        }
        if (rel.type === 'one-to-many') {
          if (!rel.mappedBy) {
            this.logger.error(`O2M relation '${rel.propertyName}' in table '${table.name}' missing mappedBy`);
            throw new Error(`One-to-many relation '${rel.propertyName}' in table '${table.name}' MUST have mappedBy`);
          }
          relationMetadata.foreignKeyColumn = isMongoDB ? rel.mappedBy : getForeignKeyColumnName(rel.mappedBy);
        }
        if (rel.type === 'many-to-many') {
          relationMetadata.junctionTableName = rel.junctionTableName || getJunctionTableName(table.name, rel.propertyName, relationMetadata.targetTableName);
          const { sourceColumn, targetColumn } = getJunctionColumnNames(table.name, rel.propertyName, relationMetadata.targetTableName);
          relationMetadata.junctionSourceColumn = rel.junctionSourceColumn || sourceColumn;
          relationMetadata.junctionTargetColumn = rel.junctionTargetColumn || targetColumn;
        }

        relations.push(relationMetadata);
      }

      const combinedColumns = [...parsedExplicitColumns];

      if (!isMongoDB && actualSchema) {
        for (const rel of relations) {
          if (['many-to-one', 'one-to-one'].includes(rel.type)) {
            const fkColumn = rel.foreignKeyColumn;
            const existsInExplicit = parsedExplicitColumns.some((col) => col.name === fkColumn);
            if (!existsInExplicit) {
              const actualFkColumn = actualSchema.columns.find((col) => col.name === fkColumn);
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
              const explicitFkColumn = combinedColumns.find((col) => col.name === fkColumn);
              if (explicitFkColumn && rel.isUpdatable === false) {
                explicitFkColumn.isUpdatable = false;
              }
            }
          }
        }

        const hasCreatedAt = combinedColumns.some((col) => col.name === 'createdAt');
        const hasUpdatedAt = combinedColumns.some((col) => col.name === 'updatedAt');
        if (!hasCreatedAt) {
          const actualCreatedAt = actualSchema.columns.find((col) => col.name === 'createdAt');
          if (actualCreatedAt) combinedColumns.push({ ...actualCreatedAt, isSystem: true, isUpdatable: false });
        }
        if (!hasUpdatedAt) {
          const actualUpdatedAt = actualSchema.columns.find((col) => col.name === 'updatedAt');
          if (actualUpdatedAt) combinedColumns.push({ ...actualUpdatedAt, isSystem: true, isUpdatable: false });
        }
      }

      const tableData: any = { ...table };
      for (const key in tableData) {
        if (tableData[key] !== undefined && tableData[key] !== null) {
          if (tableData[key] === 1 || tableData[key] === true) tableData[key] = true;
          else if (tableData[key] === 0 || tableData[key] === false) tableData[key] = false;
        }
      }

      return { ...tableData, uniques, indexes, columns: combinedColumns, relations };
    } catch (error) {
      this.logger.error(`Failed to load metadata for table ${table.name}:`, error.message);
      return null;
    }
  }

  private async loadMetadataFromDb(): Promise<EnfyraMetadata> {
    const isMongoDB = this.queryBuilder.isMongoDb();

    const [tablesResult, allColumnsResult, allRelationsResult] =
      await Promise.all([
        this.queryBuilder.select({ tableName: 'table_definition' }),
        this.queryBuilder.select({ tableName: 'column_definition' }),
        this.queryBuilder.select({ tableName: 'relation_definition' }),
      ]);
    const tables = tablesResult.data;

    let allSchemas: Map<string, any> | null = null;
    if (!isMongoDB) {
      allSchemas = await this.databaseSchemaService.getAllTableSchemas();
    }

    const allRelations = allRelationsResult.data;
    const relationIdMap = new Map<string, any>();
    for (const rel of allRelations) {
      const relId = isMongoDB ? String(rel._id) : String(rel.id);
      relationIdMap.set(relId, rel);
    }
    for (const rel of allRelations) {
      const mappedById = isMongoDB ? rel.mappedBy : rel.mappedById;
      if (mappedById) {
        const owningRelation = relationIdMap.get(String(mappedById));
        rel.mappedBy = owningRelation?.propertyName || null;
      } else {
        rel.mappedBy = null;
      }
    }

    const columnsByTable = new Map<string, any[]>();
    for (const col of allColumnsResult.data) {
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
      const id = String(isMongoDB ? t._id : t.id);
      tableIdToName.set(id, t.name);
    }

    const tablesList: any[] = [];
    const tablesMap = new Map<string, any>();

    for (const table of tables) {
      const metadata = this.buildTableMetadata(table, columnsByTable, relationsBySource, tableIdToName, allSchemas, isMongoDB);
      if (!metadata) continue;
      tablesList.push(metadata);
      tablesMap.set(table.name, metadata);
    }

    return { tables: tablesMap, tablesList, version: Date.now(), timestamp: new Date() };
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
    return metadata.tablesList.find((t) => t.id === tableId || t.id === Number(tableId)) || null;
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
}
