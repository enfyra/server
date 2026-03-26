import { Injectable, Logger, OnApplicationBootstrap, OnModuleInit, Inject, forwardRef } from '@nestjs/common';
import { OnEvent, EventEmitter2 } from '@nestjs/event-emitter';
import { QueryBuilderService } from '../../query-builder/query-builder.service';
import { RedisPubSubService } from './redis-pubsub.service';
import { InstanceService } from '../../../shared/services/instance.service';
import { DatabaseSchemaService } from '../../knex/services/database-schema.service';
import { getJunctionTableName, getForeignKeyColumnName, getJunctionColumnNames } from '../../knex/utils/naming-helpers';
import { METADATA_CACHE_SYNC_EVENT_KEY } from '../../../shared/utils/constant';
import { CACHE_EVENTS, CACHE_IDENTIFIERS, shouldReloadCache } from '../../../shared/utils/cache-events.constants';
import { ObjectId } from 'mongodb';

const COLOR = '\x1b[36m';
const RESET = '\x1b[0m';

export interface EnfyraMetadata {
  tables: Map<string, any>;
  tablesList: any[];
  version: number;
  timestamp: Date;
}

@Injectable()
export class MetadataCacheService implements OnApplicationBootstrap, OnModuleInit {
  private readonly logger = new Logger(`${COLOR}MetadataCache${RESET}`);
  private inMemoryCache: EnfyraMetadata | null = null;
  private isLoading: boolean = false;
  private loadingPromise: Promise<void> | null = null;
  private messageHandler: ((channel: string, message: string) => void) | null = null;

  constructor(
    @Inject(forwardRef(() => QueryBuilderService))
    private readonly queryBuilder: QueryBuilderService,
    private readonly redisPubSubService: RedisPubSubService,
    private readonly instanceService: InstanceService,
    private readonly databaseSchemaService: DatabaseSchemaService,
    private readonly eventEmitter: EventEmitter2,
  ) {}

  async onModuleInit() {
    this.subscribe();

  }

  async onApplicationBootstrap() {
    try {
      const start = Date.now();
      await this.reload();
      this.logger.log(`Loaded ${this.inMemoryCache?.tablesList?.length || 0} table definitions in ${Date.now() - start}ms`);
      this.eventEmitter.emit(CACHE_EVENTS.METADATA_LOADED);
    } catch (error) {
      this.logger.error('MetadataCacheService initialization failed:', error);
      throw error;
    }
  }

  private subscribe() {
    if (this.messageHandler) {
      return;
    }

    this.messageHandler = async (channel: string, message: string) => {
      if (channel === METADATA_CACHE_SYNC_EVENT_KEY) {
        try {
          const payload = JSON.parse(message);
          const myInstanceId = this.instanceService.getInstanceId();

          if (payload.instanceId === myInstanceId) {
            return;
          }

          if (payload.type === 'RELOAD_SIGNAL') {
            this.logger.log(`Received reload signal from instance ${payload.instanceId.slice(0, 8)}..., reloading from DB`);
            this.forceReloadFromDb();
          }
        } catch (error) {
          this.logger.error('Failed to parse metadata cache sync message:', error);
        }
      }
    };

    this.redisPubSubService.subscribeWithHandler(
      METADATA_CACHE_SYNC_EVENT_KEY,
      this.messageHandler
    );
  }

  private async forceReloadFromDb(): Promise<void> {
    try {
      const metadata = await this.loadMetadataFromDb();
      this.inMemoryCache = metadata;
      this.logger.log('Metadata reloaded from DB (forced)');
    } catch (error) {
      this.logger.error('Failed to force reload metadata:', error);
    }
  }

  /**
   * Listen for cache invalidation events.
   * When a table that affects metadata is modified, reload the cache.
   * The reload() method handles Redis Pub/Sub to sync other instances.
   */
  @OnEvent(CACHE_EVENTS.INVALIDATE)
  async handleCacheInvalidation(payload: { tableName: string; action: string }) {
    if (shouldReloadCache(payload.tableName, CACHE_IDENTIFIERS.METADATA)) {
      this.logger.log(`Cache invalidation event received for table: ${payload.tableName}`);
      await this.reload();
    }
  }

  private async loadMetadataFromDb(): Promise<EnfyraMetadata> {
    const isMongoDB = this.queryBuilder.isMongoDb();

    const tablesResult = await this.queryBuilder.select({ tableName: 'table_definition' });
    const tables = tablesResult.data;

    const tablesList: any[] = [];
    const tablesMap = new Map<string, any>();

    for (const table of tables) {
      try {
        let actualSchema = null;
        if (!isMongoDB) {
          actualSchema = await this.databaseSchemaService.getActualTableSchema(table.name);
          if (!actualSchema) {
            this.logger.warn(`Table ${table.name} not found in database, skipping...`);
            continue;
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

        const tableIdField = isMongoDB ? 'table' : 'tableId';
        const tableIdValue = isMongoDB
          ? (typeof table._id === 'string' ? new ObjectId(table._id) : table._id)
          : table.id;

        const columnsResult = await this.queryBuilder.select({
          tableName: 'column_definition',
          filter: { [tableIdField]: { _eq: tableIdValue } }
        });
        const explicitColumns = columnsResult.data;

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

          const booleanFields = ['isPrimary', 'isGenerated', 'isNullable', 'isSystem', 'isUpdatable', 'isHidden'];
          for (const field of booleanFields) {
            if (column[field] !== undefined && column[field] !== null) {
              column[field] = column[field] === 1 || column[field] === true;
            }
          }

          return column;
        });

        const sourceTableIdField = isMongoDB ? 'sourceTable' : 'sourceTableId';

        const relationsResult = await this.queryBuilder.select({
          tableName: 'relation_definition',
          filter: { [sourceTableIdField]: { _eq: tableIdValue } }
        });
        const relationsData = relationsResult.data;

        const relations: any[] = [];
        for (const rel of relationsData) {
          const relBooleanFields = ['isNullable', 'isSystem', 'isUpdatable'];
          for (const field of relBooleanFields) {
            if (rel[field] !== undefined && rel[field] !== null) {
              rel[field] = rel[field] === 1 || rel[field] === true;
            }
          }

          const targetTableIdField = isMongoDB ? '_id' : 'id';
          const targetTableIdValue = isMongoDB
            ? (typeof rel.targetTable === 'string' ? new ObjectId(rel.targetTable) : rel.targetTable)
            : rel.targetTableId;

          const targetTableResult = await this.queryBuilder.select({
            tableName: 'table_definition',
            filter: { [targetTableIdField]: { _eq: targetTableIdValue } }
          });
          const targetTable = targetTableResult.data;

          const relationMetadata: any = {
            ...rel,
            sourceTableName: table.name,
            targetTableName: targetTable[0]?.name || rel.targetTableName,
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
            relationMetadata.foreignKeyColumn = isMongoDB
              ? rel.propertyName
              : getForeignKeyColumnName(rel.propertyName);
          }

          if (rel.type === 'one-to-one') {
            if (relationMetadata.isInverse) {
              relationMetadata.foreignKeyColumn = isMongoDB
                ? rel.inversePropertyName
                : getForeignKeyColumnName(rel.inversePropertyName);
            } else {
              relationMetadata.foreignKeyColumn = isMongoDB
                ? rel.propertyName
                : getForeignKeyColumnName(rel.propertyName);
            }
          }

          if (rel.type === 'one-to-many') {
            if (!rel.inversePropertyName) {
              this.logger.error(`O2M relation '${rel.propertyName}' in table '${table.name}' missing inversePropertyName`);
              throw new Error(`One-to-many relation '${rel.propertyName}' in table '${table.name}' MUST have inversePropertyName`);
            }

            relationMetadata.foreignKeyColumn = isMongoDB
              ? rel.inversePropertyName
              : getForeignKeyColumnName(rel.inversePropertyName);
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
              const existsInExplicit = parsedExplicitColumns.some(col => col.name === fkColumn);

              if (!existsInExplicit) {
                const actualFkColumn = actualSchema.columns.find(col => col.name === fkColumn);
                if (actualFkColumn) {
                  combinedColumns.push({
                    ...actualFkColumn,
                    isForeignKey: true,
                    relationPropertyName: rel.propertyName,
                    isUpdatable: rel.isUpdatable !== false,
                    description: `FK column for ${rel.propertyName} relation`
                  });
                }
              } else {
                const explicitFkColumn = combinedColumns.find(col => col.name === fkColumn);
                if (explicitFkColumn && rel.isUpdatable === false) {
                  explicitFkColumn.isUpdatable = false;
                }
              }
            }
          }

          const hasCreatedAt = combinedColumns.some(col => col.name === 'createdAt');
          const hasUpdatedAt = combinedColumns.some(col => col.name === 'updatedAt');

          if (!hasCreatedAt) {
            const actualCreatedAt = actualSchema.columns.find(col => col.name === 'createdAt');
            if (actualCreatedAt) {
              combinedColumns.push({
                ...actualCreatedAt,
                isSystem: true,
                isUpdatable: false
              });
            }
          }

          if (!hasUpdatedAt) {
            const actualUpdatedAt = actualSchema.columns.find(col => col.name === 'updatedAt');
            if (actualUpdatedAt) {
              combinedColumns.push({
                ...actualUpdatedAt,
                isSystem: true,
                isUpdatable: false
              });
            }
          }
        }

        const tableData: any = { ...table };

        for (const key in tableData) {
          if (tableData[key] !== undefined && tableData[key] !== null) {
            if (tableData[key] === 1 || tableData[key] === true) {
              tableData[key] = true;
            } else if (tableData[key] === 0 || tableData[key] === false) {
              tableData[key] = false;
            }
          }
        }

        const metadata: any = {
          ...tableData,
          uniques,
          indexes,
          columns: combinedColumns,
          relations,
        };

        tablesList.push(metadata);
        tablesMap.set(table.name, metadata);

      } catch (error) {
        this.logger.error(`Failed to load metadata for table ${table.name}:`, error.message);
      }
    }

    this.generateInverseRelations(tablesList, tablesMap);

    const result = {
      tables: tablesMap,
      tablesList,
      version: Date.now(),
      timestamp: new Date(),
    };

    return result;
  }

  private generateInverseRelations(tablesList: any[], tablesMap: Map<string, any>): void {
    for (const table of tablesList) {
      for (const relation of table.relations || []) {
        if (!relation.inversePropertyName) {
          continue;
        }

        const targetTableName = relation.targetTableName || relation.targetTable;
        const targetTable = tablesMap.get(targetTableName);

        if (!targetTable) {
          continue;
        }

        const inverseExists = targetTable.relations?.some(
          (r: any) => r.propertyName === relation.inversePropertyName
        );

        if (inverseExists) {
          continue;
        }

        let inverseType = 'one-to-many';
        if (relation.type === 'one-to-many') {
          inverseType = 'many-to-one';
        } else if (relation.type === 'many-to-one') {
          inverseType = 'one-to-many';
        } else if (relation.type === 'one-to-one') {
          inverseType = 'one-to-one';
        } else if (relation.type === 'many-to-many') {
          inverseType = 'many-to-many';
        }

        const inverseRelation: any = {
          propertyName: relation.inversePropertyName,
          type: inverseType,
          targetTable: table.name,
          targetTableName: table.name,
          sourceTableName: targetTableName,
          inversePropertyName: relation.propertyName,
          isNullable: true,
          isSystem: relation.isSystem || false,
          isGenerated: true,
          isInverse: true,
          onDelete: relation.onDelete,
        };

        if (inverseType === 'many-to-one') {
          const isMongoDB = this.queryBuilder.isMongoDb();
          inverseRelation.foreignKeyColumn = relation.foreignKeyColumn || (isMongoDB
            ? relation.inversePropertyName
            : getForeignKeyColumnName(relation.inversePropertyName));
        }

        if (inverseType === 'one-to-many') {
          const isMongoDB = this.queryBuilder.isMongoDb();
          inverseRelation.foreignKeyColumn = isMongoDB
            ? relation.propertyName
            : getForeignKeyColumnName(relation.propertyName);
        }

        if (inverseType === 'one-to-one') {
          inverseRelation.mappedBy = relation.propertyName;
          inverseRelation.isInverse = true;
        }

        if (inverseType === 'many-to-many') {
          inverseRelation.junctionTableName = relation.junctionTableName;
          inverseRelation.junctionSourceColumn = relation.junctionTargetColumn;
          inverseRelation.junctionTargetColumn = relation.junctionSourceColumn;
          if (relation.propertyName) {
            inverseRelation.mappedBy = relation.propertyName;
          }
        }

        if (!targetTable.relations) {
          targetTable.relations = [];
        }
        targetTable.relations.push(inverseRelation);
      }
    }
  }

  async getMetadata(): Promise<EnfyraMetadata> {
    if (this.isLoading && this.loadingPromise) {
      await this.loadingPromise;
    }
    if (this.inMemoryCache) {
      return this.inMemoryCache;
    }
    return await this.loadAndCacheMetadata();
  }

  private async loadAndCacheMetadata(): Promise<EnfyraMetadata> {
    const metadata = await this.loadMetadataFromDb();
    this.inMemoryCache = metadata;
    return metadata;
  }

  async reload(): Promise<void> {
    if (this.isLoading && this.loadingPromise) {
      return this.loadingPromise;
    }

    this.isLoading = true;
    this.loadingPromise = (async () => {
      try {
        await this.publishReloadSignal();

        const metadata = await this.loadMetadataFromDb();

        this.inMemoryCache = metadata;

        this.logger.log('Metadata reloaded from database');
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

  private async publishReloadSignal(): Promise<void> {
    try {
      const payload = {
        instanceId: this.instanceService.getInstanceId(),
        type: 'RELOAD_SIGNAL',
        timestamp: Date.now(),
      };

      await this.redisPubSubService.publish(
        METADATA_CACHE_SYNC_EVENT_KEY,
        JSON.stringify(payload),
      );
    } catch (error) {
      this.logger.error('Failed to publish reload signal:', error);
    }
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
    const table = metadata.tablesList.find(t => t.id === tableId || t.id === Number(tableId));
    return table || null;
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