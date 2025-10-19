import { Logger } from '@nestjs/common';
import { Knex } from 'knex';
import type { MetadataCacheService } from '../../cache/services/metadata-cache.service';

export class CascadeHandler {
  constructor(
    private knexInstance: Knex,
    private metadataCacheService: MetadataCacheService,
    private logger: Logger,
  ) {}

  /**
   * Handle cascade relations for both INSERT and UPDATE
   * Logic: For each relation item with ID -> update its FK to point to parent
   *        For each relation item without ID -> create new with FK pointing to parent
   */
  async handleCascadeRelations(tableName: string, recordId: any, cascadeContextMap: Map<string, any>): Promise<void> {
    const contextData = cascadeContextMap.get(tableName);
    if (!contextData) {
      this.logger.log(`⚠️ [handleCascadeRelations] No context for table: ${tableName}`);
      return;
    }

    const originalRelationData = contextData.relationData || contextData;

    this.logger.log(`🔍 [handleCascadeRelations] Table: ${tableName}, RecordId: ${recordId}, Relation keys: ${Object.keys(originalRelationData).join(', ')}`);

    const metadata = await this.metadataCacheService.getMetadata();
    const tableMetadata = metadata.tables?.get?.(tableName) || metadata.tablesList?.find((t: any) => t.name === tableName);

    if (!tableMetadata?.relations) {
      this.logger.log(`   No relations in metadata`);
      cascadeContextMap.delete(tableName);
      return;
    }

    for (const relation of tableMetadata.relations) {
      const relName = relation.propertyName;

      if (!(relName in originalRelationData)) {
        continue;
      }

      const relValue = originalRelationData[relName];
      if (!Array.isArray(relValue) || relValue.length === 0) {
        continue;
      }

      if (relation.type === 'many-to-many') {
        // Handle M2M: sync junction table using transaction
        this.logger.log(`   Processing M2M relation: ${relName} with ${relValue.length} items`);

        const junctionTable = relation.junctionTableName;
        const sourceColumn = relation.junctionSourceColumn;
        const targetColumn = relation.junctionTargetColumn;

        if (!junctionTable || !sourceColumn || !targetColumn) {
          this.logger.warn(`     Missing M2M metadata`);
          continue;
        }

        const ids = relValue
          .map(item => (typeof item === 'object' && 'id' in item ? item.id : item))
          .filter(id => id != null);

        this.logger.log(`     Junction: ${junctionTable}, IDs: [${ids.join(', ')}]`);

        // Clear existing junction records
        await this.knexInstance(junctionTable)
          .where(sourceColumn, recordId)
          .delete();

        // Insert new junction records
        if (ids.length > 0) {
          const junctionRecords = ids.map(targetId => ({
            [sourceColumn]: recordId,
            [targetColumn]: targetId,
          }));

          await this.knexInstance(junctionTable).insert(junctionRecords);
          this.logger.log(`     ✅ Synced ${junctionRecords.length} M2M junction records`);
        }

      } else if (relation.type === 'one-to-many') {
        // Handle O2M: compare old list vs new list, set FK = NULL for removed items
        this.logger.log(`   Processing O2M relation: ${relName} with ${relValue.length} items`);

        const targetTableName = relation.targetTableName || relation.targetTable;
        const foreignKeyColumn = relation.foreignKeyColumn;

        if (!targetTableName || !foreignKeyColumn) {
          this.logger.warn(`     Missing O2M metadata`);
          continue;
        }

        this.logger.log(`     Target: ${targetTableName}, FK: ${foreignKeyColumn}`);

        // Get existing items that point to this parent
        const existingItems = await this.knexInstance(targetTableName)
          .where(foreignKeyColumn, recordId)
          .select('id');

        const existingIds = existingItems.map((item: any) => item.id);
        const incomingIds = relValue.filter((item: any) => item.id).map((item: any) => item.id);

        this.logger.log(`     Existing IDs: [${existingIds.join(', ')}]`);
        this.logger.log(`     Incoming IDs: [${incomingIds.join(', ')}]`);

        // Items that are no longer in the new list -> SET FK = NULL
        const idsToRemove = existingIds.filter(id => !incomingIds.includes(id));

        if (idsToRemove.length > 0) {
          this.logger.log(`     Setting FK = NULL for removed items: [${idsToRemove.join(', ')}]`);

          await this.knexInstance(targetTableName)
            .whereIn('id', idsToRemove)
            .update({ [foreignKeyColumn]: null });
        }

        // Process incoming items
        let updateCount = 0;
        let createCount = 0;

        for (const item of relValue) {
          if (item.id) {
            // Item has ID -> UPDATE its FK to point to parent
            this.logger.log(`     Updating item id=${item.id}, set ${foreignKeyColumn}=${recordId}`);

            await this.knexInstance(targetTableName)
              .where('id', item.id)
              .update({ [foreignKeyColumn]: recordId });

            updateCount++;
          } else {
            // Item has no ID -> CREATE new with FK pointing to parent
            const newItem = {
              ...item,
              [foreignKeyColumn]: recordId,
            };

            this.logger.log(`     Creating new item with ${foreignKeyColumn}=${recordId}`);
            await this.knexInstance(targetTableName).insert(newItem);

            createCount++;
          }
        }

        this.logger.log(`     ✅ O2M complete: ${idsToRemove.length} removed (FK=NULL), ${updateCount} updated, ${createCount} created`);
      }
    }

    cascadeContextMap.delete(tableName);
  }

  /**
   * Sync M2M relations before insert/update
   * This removes M2M arrays from data and stores them in cascadeContextMap
   */
  async syncManyToManyRelations(tableName: string, data: any): Promise<void> {
    if (!data || typeof data !== 'object') return;
    if (Array.isArray(data)) {
      for (const item of data) {
        await this.syncManyToManyRelations(tableName, item);
      }
      return;
    }

    const metadata = await this.metadataCacheService.getMetadata();
    const tableMeta = metadata.tables?.get?.(tableName) ||
                      metadata.tablesList?.find((t: any) => t.name === tableName);

    if (!tableMeta || !tableMeta.relations) return;

    for (const relation of tableMeta.relations) {
      if (relation.type !== 'many-to-many') continue;

      const propertyName = relation.propertyName;
      if (propertyName in data) {
        // Remove M2M data from the main data object
        // It will be handled in afterInsert/afterUpdate hook
        delete data[propertyName];
      }
    }
  }
}
