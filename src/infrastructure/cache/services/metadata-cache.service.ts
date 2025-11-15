import { Injectable, Logger, OnApplicationBootstrap, OnModuleInit, Inject, forwardRef } from '@nestjs/common';
import { QueryBuilderService } from '../../query-builder/query-builder.service';
import { CacheService } from './cache.service';
import { RedisPubSubService } from './redis-pubsub.service';
import { InstanceService } from '../../../shared/services/instance.service';
import { DatabaseSchemaService } from '../../knex/services/database-schema.service';
import { getJunctionTableName, getForeignKeyColumnName, getJunctionColumnNames } from '../../knex/utils/naming-helpers';
import {
  METADATA_CACHE_KEY,
  METADATA_CACHE_SYNC_EVENT_KEY,
  METADATA_RELOAD_LOCK_KEY,
  REDIS_TTL,
} from '../../../shared/utils/constant';
import { ObjectId } from 'mongodb';

export interface EnfyraMetadata {
  tables: Map<string, any>;
  tablesList: any[];
  version: number;
  timestamp: Date;
}

@Injectable()
export class MetadataCacheService implements OnApplicationBootstrap, OnModuleInit {
  private readonly logger = new Logger(MetadataCacheService.name);
  private inMemoryCache: EnfyraMetadata | null = null; // In-memory cache to avoid Redis calls
  private messageHandler: ((channel: string, message: string) => void) | null = null;

  constructor(
    @Inject(forwardRef(() => QueryBuilderService))
    private readonly queryBuilder: QueryBuilderService,
    private readonly cacheService: CacheService,
    private readonly redisPubSubService: RedisPubSubService,
    private readonly instanceService: InstanceService,
    private readonly databaseSchemaService: DatabaseSchemaService,
  ) {}

  async onModuleInit() {
    this.subscribe();

  }

  async onApplicationBootstrap() {
    try {
      await this.reload();
      this.logger.log('MetadataCacheService initialization completed');
    } catch (error) {
      this.logger.error('MetadataCacheService initialization failed:', error);
      throw error;
    }
  }

  private subscribe() {
    const sub = this.redisPubSubService.sub;
    if (!sub) {
      this.logger.warn('Redis subscription not available for metadata cache sync');
      return;
    }

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

          this.logger.log(`Received metadata cache sync from instance ${payload.instanceId.slice(0, 8)}...`);

          const metadata: EnfyraMetadata = {
            tables: new Map(Object.entries(payload.metadata.tables)),
            tablesList: payload.metadata.tablesList,
            version: payload.metadata.version,
            timestamp: new Date(payload.metadata.timestamp),
          };

          this.inMemoryCache = metadata;
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
        if (table.uniques && typeof table.uniques === 'string') {
          try {
            uniques = JSON.parse(table.uniques);
          } catch (e) {
            this.logger.warn(`Failed to parse uniques for table ${table.name}`);
          }
        }
        if (table.indexes && typeof table.indexes === 'string') {
          try {
            indexes = JSON.parse(table.indexes);
          } catch (e) {
            this.logger.warn(`Failed to parse indexes for table ${table.name}`);
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
          const relBooleanFields = ['isNullable', 'isSystem'];
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
                    description: `FK column for ${rel.propertyName} relation`
                  });
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
          uniques: uniques.length > 0 ? uniques : (actualSchema?.uniques || []),
          indexes: indexes.length > 0 ? indexes : (actualSchema?.indexes || []),
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
    const instanceId = this.instanceService.getInstanceId();

    try {
      const acquired = await this.cacheService.acquire(
        METADATA_RELOAD_LOCK_KEY,
        instanceId,
        REDIS_TTL.RELOAD_LOCK_TTL
      );

      if (!acquired) {
        return;
      }

      try {
        const metadata = await this.loadMetadataFromDb();
        await this.publish(metadata);
        this.inMemoryCache = metadata;
      } finally {
        await this.cacheService.release(METADATA_RELOAD_LOCK_KEY, instanceId);
      }
    } catch (error) {
      this.logger.error('Failed to reload metadata cache:', error);
      throw error;
    }
  }

  private async publish(metadata: EnfyraMetadata): Promise<void> {
    try {
      const payload = {
        instanceId: this.instanceService.getInstanceId(),
        metadata: {
          tables: Object.fromEntries(metadata.tables),
          tablesList: metadata.tablesList,
          version: metadata.version,
          timestamp: metadata.timestamp,
        },
        timestamp: Date.now(),
      };

      await this.redisPubSubService.publish(
        METADATA_CACHE_SYNC_EVENT_KEY,
        JSON.stringify(payload),
      );
    } catch (error) {
      this.logger.error('Failed to publish metadata cache sync:', error);
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
}