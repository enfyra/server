import { Injectable, Logger, OnApplicationBootstrap, OnModuleInit, Inject, forwardRef } from '@nestjs/common';
import { ObjectId } from 'mongodb';
import { QueryBuilderService } from '../../query-builder/query-builder.service';
import { CacheService } from './cache.service';
import { RedisPubSubService } from './redis-pubsub.service';
import { InstanceService } from '../../../shared/services/instance.service';
import { getJunctionTableName, getForeignKeyColumnName } from '../../../shared/utils/naming-helpers';
import { 
  METADATA_CACHE_KEY, 
  METADATA_CACHE_SYNC_EVENT_KEY,
  METADATA_RELOAD_LOCK_KEY,
  REDIS_TTL,
} from '../../../shared/utils/constant';

export interface EnfyraMetadata {
  tables: Map<string, any>;
  tablesList: any[];
  version: number;
  timestamp: Date;
}

@Injectable()
export class MetadataCacheService implements OnApplicationBootstrap, OnModuleInit {
  private readonly logger = new Logger(MetadataCacheService.name);

  constructor(
    @Inject(forwardRef(() => QueryBuilderService))
    private readonly queryBuilder: QueryBuilderService,
    private readonly cacheService: CacheService,
    private readonly redisPubSubService: RedisPubSubService,
    private readonly instanceService: InstanceService,
  ) {}

  async onModuleInit() {
    this.subscribeToMetadataCacheSync();
  }

  async onApplicationBootstrap() {
    await this.reloadMetadataCache();
  }

  private subscribeToMetadataCacheSync() {
    const sub = this.redisPubSubService.sub;
    if (!sub) {
      this.logger.warn('Redis subscription not available for metadata cache sync');
      return;
    }

    sub.subscribe(METADATA_CACHE_SYNC_EVENT_KEY);
    
    sub.on('message', async (channel: string, message: string) => {
      if (channel === METADATA_CACHE_SYNC_EVENT_KEY) {
        try {
          const payload = JSON.parse(message);
          const myInstanceId = this.instanceService.getInstanceId();
          
          if (payload.instanceId === myInstanceId) {
            this.logger.debug('⏭️  Skipping metadata cache sync from self');
            return;
          }

          this.logger.log(`📥 Received metadata cache sync from instance ${payload.instanceId.slice(0, 8)}...`);
          
          const metadata: EnfyraMetadata = {
            tables: new Map(Object.entries(payload.metadata.tables)),
            tablesList: payload.metadata.tablesList,
            version: payload.metadata.version,
            timestamp: new Date(payload.metadata.timestamp),
          };

          await this.cacheService.set(
            METADATA_CACHE_KEY,
            JSON.stringify({
              tables: Object.fromEntries(metadata.tables),
              tablesList: metadata.tablesList,
              version: metadata.version,
              timestamp: metadata.timestamp,
            }),
            0,
          );

          await this.queryBuilder.reloadWithMetadata(metadata);
          this.logger.log(`✅ Metadata cache synced: ${metadata.tablesList.length} tables`);
        } catch (error) {
          this.logger.error('Failed to parse metadata cache sync message:', error);
        }
      }
    });
  }

  private async loadMetadataFromDb(): Promise<EnfyraMetadata> {
    const isMongoDB = this.queryBuilder.isMongoDb();

    const [tables, allColumns, allRelations] = await Promise.all([
      this.queryBuilder.select({ table: 'table_definition' }),
      this.queryBuilder.select({ table: 'column_definition' }),
      this.queryBuilder.select({ table: 'relation_definition' }),
    ]);

    const columnsByTable = new Map<any, any[]>();
    for (const col of allColumns) {
      const tableKey = isMongoDB ? col.table?.toString() : col.tableId;
      if (!columnsByTable.has(tableKey)) {
        columnsByTable.set(tableKey, []);
      }
      columnsByTable.get(tableKey).push(col);
    }

    const relationsByTable = new Map<any, any[]>();
    for (const rel of allRelations) {
      const tableKey = isMongoDB ? rel.sourceTable?.toString() : rel.sourceTableId;
      if (!relationsByTable.has(tableKey)) {
        relationsByTable.set(tableKey, []);
      }
      relationsByTable.get(tableKey).push(rel);
    }

    const tablesList: any[] = [];
    const tablesMap = new Map<string, any>();
    const tablesById = new Map<any, any>();
    
    for (const table of tables) {
      const tableKey = isMongoDB ? table._id?.toString() : table.id;
      tablesById.set(tableKey, table);
    }

    for (const table of tables) {
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

      const tableKey = isMongoDB ? table._id?.toString() : table.id;
      const columnsData = columnsByTable.get(tableKey) || [];

      // Parse JSON fields (options, defaultValue) and boolean fields - ALWAYS parse
      const columns = columnsData.map((col: any) => {
        const column = { ...col };

        // Parse options (always try if string)
        if (col.options && typeof col.options === 'string') {
          try {
            column.options = JSON.parse(col.options);
          } catch (e) {
            // Keep as string if parse fails
          }
        }

        // Parse defaultValue (always try if string)
        if (col.defaultValue && typeof col.defaultValue === 'string') {
          try {
            column.defaultValue = JSON.parse(col.defaultValue);
          } catch (e) {
            // Keep as string if parse fails
          }
        }

        // Parse boolean fields (MySQL returns 1/0, convert to true/false)
        const booleanFields = ['isPrimary', 'isGenerated', 'isNullable', 'isSystem', 'isUpdatable', 'isHidden'];
        for (const field of booleanFields) {
          if (column[field] !== undefined && column[field] !== null) {
            column[field] = column[field] === 1 || column[field] === true;
          }
        }

        return column;
      });
      
      // Manually inject timestamp columns (exist in physical DB, not in column_definition)
      const hasCreatedAt = columns.some((col: any) => col.name === 'createdAt');
      const hasUpdatedAt = columns.some((col: any) => col.name === 'updatedAt');
      
      if (!hasCreatedAt) {
        columns.push({
          name: 'createdAt',
          type: 'timestamp',
          isPrimary: false,
          isGenerated: true,
          isNullable: false,
          isSystem: true,
          isUpdatable: false,
          isHidden: false,
          tableId: table.id,
        });
      }
      
      if (!hasUpdatedAt) {
        columns.push({
          name: 'updatedAt',
          type: 'timestamp',
          isPrimary: false,
          isGenerated: true,
          isNullable: false,
          isSystem: true,
          isUpdatable: false,
          isHidden: false,
          tableId: table.id,
        });
      }

      const relationsData = relationsByTable.get(tableKey) || [];
      const relations: any[] = [];
      for (const rel of relationsData) {
        // Parse boolean fields for relation
        const relBooleanFields = ['isNullable', 'isSystem'];
        for (const field of relBooleanFields) {
          if (rel[field] !== undefined && rel[field] !== null) {
            rel[field] = rel[field] === 1 || rel[field] === true;
          }
        }

        const targetTableKey = isMongoDB ? rel.targetTable?.toString() : rel.targetTableId;
        const targetTable = tablesById.get(targetTableKey);

        const relationMetadata: any = {
          ...rel,
          sourceTableName: table.name,
          targetTableName: targetTable?.name || rel.targetTableName,
        };

        if (rel.type === 'many-to-one' || rel.type === 'one-to-one') {
          relationMetadata.foreignKeyColumn = isMongoDB ? rel.propertyName : getForeignKeyColumnName(rel.propertyName);
        }

        if (rel.type === 'many-to-many') {
          relationMetadata.junctionTableName = rel.junctionTableName;
          relationMetadata.junctionSourceColumn = rel.junctionSourceColumn;
          relationMetadata.junctionTargetColumn = rel.junctionTargetColumn;
        }

        relations.push(relationMetadata);
      }

      // Parse boolean fields for table (MySQL returns 1/0, convert to true/false)
      const tableData = { ...table };
      if (tableData.isSystem !== undefined && tableData.isSystem !== null) {
        tableData.isSystem = tableData.isSystem === 1 || tableData.isSystem === true;
      }

      // Lưu toàn bộ raw data từ table + columns + relations
      const metadata: any = {
        ...tableData,
        uniques,
        indexes,
        columns,
        relations,
      };

      tablesList.push(metadata);
      tablesMap.set(table.name, metadata);
    }

    // Generate inverse relations (for MongoDB and SQL consistency)
    // This handles O2M relations that are inverse of M2O relations
    for (const table of tablesList) {
      for (const relation of table.relations || []) {
        // Only process relations with inversePropertyName
        if (!relation.inversePropertyName) continue;
        
        const targetTableName = relation.targetTableName || relation.targetTable;
        const targetTable = tablesMap.get(targetTableName);
        
        if (!targetTable) continue;
        
        // Check if inverse relation already exists
        const inverseExists = targetTable.relations?.some(
          (r: any) => r.propertyName === relation.inversePropertyName
        );
        
        if (!inverseExists) {
          // Generate inverse relation
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
            isGenerated: true, // Mark as auto-generated
          };
          
          // Add foreign key column for M2O
          if (inverseType === 'many-to-one') {
            // Inverse M2O from O2M: FK is based on the ORIGINAL M2O's propertyName
            // e.g., route_definition.hooks (O2M) → hook_definition.route (M2O) → FK is 'routeId'
            // So we use the inverse's propertyName (which is the original M2O's propertyName)
            inverseRelation.foreignKeyColumn = relation.foreignKeyColumn || getForeignKeyColumnName(relation.inversePropertyName);
          }
          
          // Add junction table info for M2M
          if (inverseType === 'many-to-many') {
            inverseRelation.junctionTableName = relation.junctionTableName;
            inverseRelation.junctionSourceColumn = relation.junctionTargetColumn;
            inverseRelation.junctionTargetColumn = relation.junctionSourceColumn;
          }
          
          // Add inverse relation to target table
          if (!targetTable.relations) {
            targetTable.relations = [];
          }
          targetTable.relations.push(inverseRelation);
        }
      }
    }

    return {
      tables: tablesMap,
      tablesList,
      version: Date.now(),
      timestamp: new Date(),
    };
  }

  /**
   * Get metadata from cache (no TTL - cache forever until reload)
   */
  async getMetadata(): Promise<EnfyraMetadata> {
    const cachedData = await this.cacheService.get<any>(METADATA_CACHE_KEY);

    if (cachedData) {
      // Redis converts Map to plain object, so we need to rebuild it
      if (cachedData.tables && !(cachedData.tables instanceof Map)) {
        cachedData.tables = new Map(Object.entries(cachedData.tables));
      }
      return cachedData;
    }

    // If not in cache, load from DB
    this.logger.log('📦 Metadata not in cache, loading from DB...');
    return await this.loadAndCacheMetadata();
  }

  /**
   * Load metadata from DB and cache it (no TTL)
   */
  private async loadAndCacheMetadata(): Promise<EnfyraMetadata> {
    const loadStart = Date.now();
    const metadata = await this.loadMetadataFromDb();
    const loadTime = Date.now() - loadStart;

    this.logger.log(
      `📦 Loaded ${metadata.tablesList.length} tables from DB in ${loadTime}ms`,
    );

    // Convert Map to plain object for Redis serialization
    const cacheData = {
      tablesList: metadata.tablesList,
      tables: Object.fromEntries(metadata.tables), // Map → Object
    };

    // Cache with no TTL (cache forever until manually cleared/reloaded)
    await this.cacheService.set(METADATA_CACHE_KEY, cacheData, 0);

    return metadata;
  }

  /**
   * Manually reload metadata cache
   */
  async reloadMetadataCache(): Promise<void> {
    const instanceId = this.instanceService.getInstanceId();
    
    try {
      const acquired = await this.cacheService.acquire(
        METADATA_RELOAD_LOCK_KEY, 
        instanceId, 
        REDIS_TTL.RELOAD_LOCK_TTL
      );
      
      if (!acquired) {
        this.logger.log('🔒 Another instance is reloading metadata, waiting for broadcast...');
        return;
      }

      this.logger.log(`🔓 Acquired metadata reload lock (instance ${instanceId.slice(0, 8)})`);
      
      try {
        await this.performReload();
      } finally {
        await this.cacheService.release(METADATA_RELOAD_LOCK_KEY, instanceId);
        this.logger.log('🔓 Released metadata reload lock');
      }
    } catch (error) {
      this.logger.error('❌ Failed to reload metadata cache:', error);
      throw error;
    }
  }

  private async performReload(): Promise<void> {
    this.logger.log('🔄 Reloading metadata cache...');
    
    await this.clearMetadataCache();
    const metadata = await this.loadAndCacheMetadata();
    
    this.logger.log(`✅ Metadata cache reloaded - ${metadata.tablesList.length} tables`);
    
    await this.queryBuilder.reloadWithMetadata(metadata);
    await this.publishMetadataCacheSync(metadata);
  }

  private async publishMetadataCacheSync(metadata: EnfyraMetadata): Promise<void> {
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

      this.logger.log(`📤 Published metadata cache to other instances (${metadata.tablesList.length} tables)`);
    } catch (error) {
      this.logger.error('Failed to publish metadata cache sync:', error);
    }
  }

  /**
   * Get metadata for a specific table
   */
  async getTableMetadata(tableName: string): Promise<any | null> {
    const metadata = await this.getMetadata();
    return metadata.tables.get(tableName) || null;
  }

  /**
   * Get all tables metadata
   */
  async getAllTablesMetadata(): Promise<any[]> {
    const metadata = await this.getMetadata();
    return metadata.tablesList;
  }

  /**
   * Clear metadata cache
   */
  async clearMetadataCache(): Promise<void> {
    await this.cacheService.deleteKey(METADATA_CACHE_KEY);
    this.logger.log('🗑️ Metadata cache cleared');
  }
}

