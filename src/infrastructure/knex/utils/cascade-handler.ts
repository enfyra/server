import { Logger } from '@nestjs/common';
import { Knex } from 'knex';
import type { MetadataCacheService } from '../../cache/services/metadata-cache.service';

/**
 * Cascade rules:
 * - Many-to-many: sync junction table by replacing links with provided ids
 * - One-to-many: null-out removed children, update FK for existing ids, insert new children
 * - Many-to-one: clear FK when null, link existing ids/values, create related row when object lacks id
 * - One-to-one (owner side): link existing id or create related entity then update parent FK
 */
export class CascadeHandler {
  constructor(
    private knexInstance: Knex,
    private metadataCacheService: MetadataCacheService,
    private logger: Logger,
  ) {}

  async handleCascadeRelations(
    tableName: string,
    recordId: any,
    cascadeContextMap: Map<string, any>,
    knexOrTrx?: Knex | Knex.Transaction,
  ): Promise<void> {
    const knex = knexOrTrx || this.knexInstance;
    const contextData = cascadeContextMap.get(tableName);
    if (!contextData) {
      this.logger.log(`[handleCascadeRelations] No context for table: ${tableName}`);
      return;
    }

    const originalRelationData = contextData.relationData || contextData;

    this.logger.log(`[handleCascadeRelations] Table: ${tableName}, RecordId: ${recordId}, Relation keys: ${Object.keys(originalRelationData).join(', ')}`);

    const metadata = await this.metadataCacheService.getMetadata();
    const tableMetadata = metadata.tables?.get?.(tableName) || metadata.tablesList?.find((t: any) => t.name === tableName);

    if (!tableMetadata?.relations) {
      this.logger.log(`   No relations in metadata`);
      cascadeContextMap.delete(tableName);
      return;
    }

    const relations = Array.isArray(tableMetadata.relations)
      ? tableMetadata.relations
      : Object.values(tableMetadata.relations || {});

    if (relations.length === 0) {
      this.logger.log(`   No relations to process`);
      cascadeContextMap.delete(tableName);
      return;
    }

    for (const relation of relations) {
      const relName = relation.propertyName;

      if (!(relName in originalRelationData)) {
        continue;
      }

      const relValue = originalRelationData[relName];

      if (relation.type === 'many-to-one') {
        const foreignKeyColumn = relation.foreignKeyColumn || `${relName}Id`;
        let targetTableName = relation.targetTableName || relation.targetTable;

        if (!targetTableName) {
          targetTableName = await this.resolveTargetTableName(relName, tableName);
        }

        if (!foreignKeyColumn) {
          this.logger.warn(`   Missing FK column for ${relName}`);
          continue;
        }

        if (!targetTableName) {
          this.logger.warn(`   Unable to resolve target table for ${relName}`);
          continue;
        }

        const assignForeignKey = async (value: any) => {
          await knex(tableName)
            .where('id', recordId)
            .update({ [foreignKeyColumn]: value });
        };

        if (relValue == null) {
          this.logger.log(`   Clearing ${foreignKeyColumn} on ${tableName}#${recordId}`);
          await assignForeignKey(null);
          continue;
        }

        if (typeof relValue === 'number' || typeof relValue === 'string') {
          this.logger.log(`   Assigning primitive ${foreignKeyColumn}=${relValue}`);
          await assignForeignKey(relValue);
          continue;
        }

        const valueObject = Array.isArray(relValue) ? relValue[0] : relValue;

        if (valueObject && typeof valueObject === 'object') {
          if (valueObject.id != null) {
            this.logger.log(`   Linking existing ${relName} id=${valueObject.id}`);
            await assignForeignKey(valueObject.id);
            continue;
          }

          this.logger.log(`   Creating new ${relName} for ${tableName}#${recordId}`);
          const newId = await this.insertRecordAndGetId(targetTableName, valueObject, knex);
          if (newId == null) {
            this.logger.warn(`   Failed to capture new ${relName} id`);
            continue;
          }

          await assignForeignKey(newId);
          this.logger.log(`   Linked new ${relName} id=${newId}`);
          continue;
        }

        this.logger.warn(`   Unsupported value for ${relName}`);
        continue;
      }

      if (relation.type === 'one-to-one') {
        if (!relValue || (Array.isArray(relValue) && relValue.length === 0)) {
          continue;
        }
      } else {
        if (!Array.isArray(relValue)) {
          continue;
        }
      }

      if (relation.type === 'many-to-many') {
        this.logger.log(`   Processing M2M relation: ${relName} with ${relValue.length} items`);

        const junctionTable = relation.junctionTableName;
        const sourceColumn = relation.junctionSourceColumn;
        const targetColumn = relation.junctionTargetColumn;
        let targetTableName = relation.targetTableName || relation.targetTable;
        if (!targetTableName) {
          targetTableName = await this.resolveTargetTableName(relName, tableName);
        }

        if (!junctionTable || !sourceColumn || !targetColumn) {
          this.logger.warn(`     Missing M2M metadata`);
          continue;
        }

        if (!targetTableName) {
          this.logger.warn(`     Missing target table for M2M relation ${relName}`);
          continue;
        }

        const ids: any[] = [];
        for (const item of relValue) {
          if (item == null) continue;
          if (typeof item === 'object') {
            if ('id' in item && item.id != null) {
              ids.push(item.id);
            } else {
              this.logger.log(`     Creating related record for ${relName}`);
              const newId = await this.insertRecordAndGetId(targetTableName, item, knex);
              if (newId != null) {
                ids.push(newId);
              } else {
                this.logger.warn(`     Failed to create related record for ${relName}`);
              }
            }
          } else {
            ids.push(item);
          }
        }

        this.logger.log(`     Junction: ${junctionTable}, IDs: [${ids.join(', ')}]`);

        await knex(junctionTable)
          .where(sourceColumn, recordId)
          .delete();

        if (ids.length > 0) {
          const junctionRecords = ids.map(targetId => ({
            [sourceColumn]: recordId,
            [targetColumn]: targetId,
          }));

          await knex(junctionTable).insert(junctionRecords);
          this.logger.log(`     Synced ${junctionRecords.length} M2M junction records`);
        } else {
          this.logger.log(`     Cleared all M2M links for ${relName}`);
        }

      } else if (relation.type === 'one-to-many') {
        this.logger.log(`   Processing O2M relation: ${relName} with ${relValue.length} items`);

        const targetTableName = relation.targetTableName || relation.targetTable;
        const foreignKeyColumn = relation.foreignKeyColumn;

        if (!targetTableName || !foreignKeyColumn) {
          this.logger.warn(`     Missing O2M metadata`);
          continue;
        }

        this.logger.log(`     Target: ${targetTableName}, FK: ${foreignKeyColumn}`);

        const existingItems = await knex(targetTableName)
          .where(foreignKeyColumn, recordId)
          .select('id');

        const existingIds = existingItems.map((item: any) => item.id);
        const incomingIds = relValue.filter((item: any) => item?.id).map((item: any) => item.id);

        this.logger.log(`     Existing IDs: [${existingIds.join(', ')}]`);
        this.logger.log(`     Incoming IDs: [${incomingIds.join(', ')}]`);

        const idsToRemove = existingIds.filter(id => !incomingIds.includes(id));

        if (idsToRemove.length > 0) {
          this.logger.log(`     Setting FK = NULL for removed items: [${idsToRemove.join(', ')}]`);

          await knex(targetTableName)
            .whereIn('id', idsToRemove)
            .update({ [foreignKeyColumn]: null });
        }

        let updateCount = 0;
        let createCount = 0;

        for (const item of relValue) {
          if (item?.id) {
            this.logger.log(`     Updating item id=${item.id}, set ${foreignKeyColumn}=${recordId}`);

            await knex(targetTableName)
              .where('id', item.id)
              .update({ [foreignKeyColumn]: recordId });

            updateCount++;
          } else {
            const newItem = {
              ...item,
              [foreignKeyColumn]: recordId,
            };

            this.logger.log(`     Creating new item with ${foreignKeyColumn}=${recordId}`);
            await knex(targetTableName).insert(newItem);

            createCount++;
          }
        }

        this.logger.log(`     O2M complete: ${idsToRemove.length} removed (FK=NULL), ${updateCount} updated, ${createCount} created`);
      } else if (relation.type === 'one-to-one') {
        this.logger.log(`   Processing O2O relation: ${relName}`);

        const targetTableName = relation.targetTableName || relation.targetTable;
        const foreignKeyColumn = relation.foreignKeyColumn;
        const isInverse = relation.isInverse;

        if (!targetTableName || !foreignKeyColumn) {
          this.logger.warn(`     Missing O2O metadata`);
          continue;
        }

        this.logger.log(`     Target: ${targetTableName}, FK: ${foreignKeyColumn}`);

        const items = (Array.isArray(relValue) ? relValue : [relValue]).filter((item: any) => item != null);

        if (isInverse) {
          if (items.length === 0) {
            await knex(targetTableName)
              .where(foreignKeyColumn, recordId)
              .update({ [foreignKeyColumn]: null });

            this.logger.log(`     Cleared inverse O2O links for ${tableName}#${recordId}`);
            continue;
          }

          const linkedOwnerIds: any[] = [];

          for (const rawItem of items) {
            const item = typeof rawItem === 'object' ? rawItem : { id: rawItem };
            if (!item || typeof item !== 'object') {
              continue;
            }

            let ownerId = item.id ?? null;
            if (ownerId == null) {
              ownerId = await this.insertRecordAndGetId(targetTableName, item, knex);
            }

            if (ownerId == null) {
              this.logger.warn(`     Unable to resolve owner id for inverse O2O ${relName}`);
              continue;
            }

            linkedOwnerIds.push(ownerId);

            await knex(targetTableName)
              .where('id', ownerId)
              .update({ [foreignKeyColumn]: recordId });

            this.logger.log(`     Linked inverse owner ${ownerId} -> ${recordId}`);
          }

          if (linkedOwnerIds.length > 0) {
            await knex(targetTableName)
              .where(foreignKeyColumn, recordId)
              .whereNotIn('id', linkedOwnerIds)
              .update({ [foreignKeyColumn]: null });
          }

          this.logger.log(`     Inverse O2O complete: ${linkedOwnerIds.length} owners linked`);
          continue;
        }

        for (const item of items) {
          if (!item || typeof item !== 'object') {
            continue;
          }

          if (item.id != null) {
            this.logger.log(`     Linking to existing item id=${item.id}, set ${foreignKeyColumn}=${item.id}`);

            await knex(tableName)
              .where('id', recordId)
              .update({ [foreignKeyColumn]: item.id });
          } else {
            this.logger.log(`     Creating new related entity in ${targetTableName}`);

            const newRelatedId = await this.insertRecordAndGetId(targetTableName, item, knex);

            if (newRelatedId == null) {
              this.logger.warn(`     Failed to create related entity for ${relName}`);
              continue;
            }

            this.logger.log(`     Created related entity with id=${newRelatedId}, updating parent ${foreignKeyColumn}=${newRelatedId}`);

            await knex(tableName)
              .where('id', recordId)
              .update({ [foreignKeyColumn]: newRelatedId });
          }
        }

        this.logger.log(`     O2O complete: cascade created/linked related entity`);
      }
    }

    cascadeContextMap.delete(tableName);
  }

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
        delete data[propertyName];
      }
    }
  }

  private async resolveTargetTableName(relationName: string, parentTableName: string): Promise<string | null> {
    try {
      const metadata = await this.metadataCacheService.getMetadata();
      const tableMeta =
        metadata.tables?.get?.(parentTableName) ||
        metadata.tablesList?.find((t: any) => t.name === parentTableName);

        const relationMeta = Array.isArray(tableMeta?.relations)
          ? tableMeta?.relations?.find((rel: any) => rel.propertyName === relationName)
          : Object.values(tableMeta?.relations || {}).find((rel: any) => rel.propertyName === relationName);

      if (relationMeta?.targetTableName || relationMeta?.targetTable) {
        return relationMeta.targetTableName || relationMeta.targetTable;
      }

      const parentTable = await this.knexInstance('table_definition')
        .where('name', parentTableName)
        .first('id');

      if (!parentTable?.id) {
        return null;
      }

      const relationDef = await this.knexInstance('relation_definition')
        .where('sourceTableId', parentTable.id)
        .where('propertyName', relationName)
        .first('targetTableId');

      if (!relationDef?.targetTableId) {
        return null;
      }

      const targetTable = await this.knexInstance('table_definition')
        .where('id', relationDef.targetTableId)
        .first('name');

      return targetTable?.name || null;
    } catch (error) {
      this.logger.warn(
        `[resolveTargetTableName] Failed for ${relationName} of ${parentTableName}`,
        error,
      );
      return null;
    }
  }

  private async insertRecordAndGetId(
    targetTableName: string,
    data: any,
    knexOrTrx?: Knex | Knex.Transaction,
  ): Promise<any> {
    if (!targetTableName || !data) return null;

    const newRecord = { ...data };
    delete newRecord.id;

    const knex = knexOrTrx || this.knexInstance;
    const clientName = (knex?.client as any)?.config?.client || '';

    if (clientName.includes('pg')) {
      const result = await knex(targetTableName).insert(newRecord).returning('id');
      const inserted = Array.isArray(result) ? result[0] : result;
      if (inserted == null) return null;
      return typeof inserted === 'object' ? inserted.id ?? Object.values(inserted)[0] : inserted;
    }

    const result = await knex(targetTableName).insert(newRecord);
    let newId = Array.isArray(result) ? result[0] : result;

    if (newId == null && newRecord.id != null) {
      newId = newRecord.id;
    }

    if (newId == null) {
      const fallback = await knex(targetTableName).orderBy('id', 'desc').first('id');
      newId = fallback?.id;
    }

    return newId ?? null;
  }
}
