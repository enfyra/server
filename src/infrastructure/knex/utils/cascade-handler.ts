import { Logger } from '@nestjs/common';
import { Knex } from 'knex';
import type { MetadataCacheService } from '../../cache/services/metadata-cache.service';
import { getForeignKeyColumnName } from './sql-schema-naming.util';

/**
 * Cascade rules:
 * - Many-to-many: sync junction table by replacing links with provided ids
 * - One-to-many: null-out removed children, update FK for existing ids, insert new children
 * - Many-to-one: clear FK when null, link existing ids/values, create related row when object lacks id
 * - One-to-one (owner side): clear existing FK holder if unique constraint, then link existing id or create related entity
 *
 * Supports recursive cascade - nested relations are processed through insertWithCascade/updateWithCascade
 */
export class CascadeHandler {
  constructor(
    private knexInstance: Knex,
    private metadataCacheService: MetadataCacheService,
    private logger: Logger,
    private stripUnknownColumns?: (tableName: string, data: any) => Promise<any>,
    private stripNonUpdatableFields?: (tableName: string, data: any) => Promise<any>,
    private insertWithCascade?: (tableName: string, data: any, trx?: Knex | Knex.Transaction) => Promise<any>,
    private updateWithCascade?: (tableName: string, recordId: any, data: any, trx?: Knex | Knex.Transaction) => Promise<void>,
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
      return;
    }

    const originalRelationData = contextData.relationData || contextData;

    const metadata = await this.metadataCacheService.getMetadata();
    const tableMetadata = metadata.tables?.get?.(tableName) || metadata.tablesList?.find((t: any) => t.name === tableName);

    if (!tableMetadata?.relations) {
      cascadeContextMap.delete(tableName);
      return;
    }

    const relations = Array.isArray(tableMetadata.relations)
      ? tableMetadata.relations
      : Object.values(tableMetadata.relations || {});

    if (relations.length === 0) {
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
        const foreignKeyColumn = relation.foreignKeyColumn || getForeignKeyColumnName(relName);
        let targetTableName = relation.targetTableName || relation.targetTable;

        if (!targetTableName) {
          targetTableName = await this.resolveTargetTableName(relName, tableName);
        }

        if (!foreignKeyColumn) {
          this.logger.warn(`Missing FK column for ${relName}`);
          continue;
        }

        if (!targetTableName) {
          this.logger.warn(`Unable to resolve target table for ${relName}`);
          continue;
        }

        const assignForeignKey = async (value: any) => {
          await knex(tableName)
            .where('id', recordId)
            .update({ [foreignKeyColumn]: value });
        };

        if (relValue == null) {
          await assignForeignKey(null);
          continue;
        }

        if (typeof relValue === 'number' || typeof relValue === 'string') {
          await assignForeignKey(relValue);
          continue;
        }

        const valueObject = Array.isArray(relValue) ? relValue[0] : relValue;

        if (valueObject && typeof valueObject === 'object') {
          if (valueObject.id != null) {
            await assignForeignKey(valueObject.id);
            continue;
          }

          const newId = await this.insertRecordAndGetId(targetTableName, valueObject, knex);
          if (newId == null) {
            this.logger.warn(`Failed to capture new ${relName} id`);
            continue;
          }

          await assignForeignKey(newId);
          continue;
        }

        this.logger.warn(`Unsupported value for ${relName}`);
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
        const junctionTable = relation.junctionTableName;
        const sourceColumn = relation.junctionSourceColumn;
        const targetColumn = relation.junctionTargetColumn;
        let targetTableName = relation.targetTableName || relation.targetTable;
        if (!targetTableName) {
          targetTableName = await this.resolveTargetTableName(relName, tableName);
        }

        if (!junctionTable || !sourceColumn || !targetColumn) {
          this.logger.warn(`Missing M2M metadata`);
          continue;
        }

        if (!targetTableName) {
          this.logger.warn(`Missing target table for M2M relation ${relName}`);
          continue;
        }

        const ids: any[] = [];
        for (const item of relValue) {
          if (item == null) continue;
          if (typeof item === 'object') {
            if ('id' in item && item.id != null) {
              ids.push(item.id);
            } else {
              const newId = await this.insertRecordAndGetId(targetTableName, item, knex);
              if (newId != null) {
                ids.push(newId);
              } else {
                this.logger.warn(`Failed to create related record for ${relName}`);
              }
            }
          } else {
            ids.push(item);
          }
        }

        await knex(junctionTable)
          .where(sourceColumn, recordId)
          .delete();

        if (ids.length > 0) {
          const junctionRecords = ids.map(targetId => ({
            [sourceColumn]: recordId,
            [targetColumn]: targetId,
          }));

          await knex(junctionTable).insert(junctionRecords);
        }

      } else if (relation.type === 'one-to-many') {
        const targetTableName = relation.targetTableName || relation.targetTable;
        const foreignKeyColumn = relation.foreignKeyColumn;

        if (!targetTableName || !foreignKeyColumn) {
          this.logger.warn(`Missing O2M metadata`);
          continue;
        }

        const existingItems = await knex(targetTableName)
          .where(foreignKeyColumn, recordId)
          .select('id');

        const existingIds = existingItems.map((item: any) => String(item.id));
        const incomingIds = relValue.filter((item: any) => item?.id).map((item: any) => String(item.id));

        const idsToRemove = existingIds.filter(id => !incomingIds.includes(id));

        if (idsToRemove.length > 0) {
          await knex(targetTableName)
            .whereIn('id', idsToRemove)
            .update({ [foreignKeyColumn]: null });
        }

        for (const item of relValue) {
          if (item?.id) {
            await knex(targetTableName)
              .where('id', item.id)
              .update({ [foreignKeyColumn]: recordId });
          } else {
            const newItem = {
              ...item,
              [foreignKeyColumn]: recordId,
            };

            await this.insertRecordAndGetId(targetTableName, newItem, knex);
          }
        }

      } else if (relation.type === 'one-to-one') {
        const targetTableName = relation.targetTableName || relation.targetTable;
        const foreignKeyColumn = relation.foreignKeyColumn;
        const isInverse = relation.isInverse;

        if (!targetTableName || !foreignKeyColumn) {
          this.logger.warn(`Missing O2O metadata`);
          continue;
        }

        const items = (Array.isArray(relValue) ? relValue : [relValue]).filter((item: any) => item != null);

        if (isInverse) {
          if (items.length === 0) {
            await knex(targetTableName)
              .where(foreignKeyColumn, recordId)
              .update({ [foreignKeyColumn]: null });
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
              this.logger.warn(`Unable to resolve owner id for inverse O2O ${relName}`);
              continue;
            }

            linkedOwnerIds.push(ownerId);

            await knex(targetTableName)
              .where('id', ownerId)
              .update({ [foreignKeyColumn]: recordId });
          }

          if (linkedOwnerIds.length > 0) {
            await knex(targetTableName)
              .where(foreignKeyColumn, recordId)
              .whereNotIn('id', linkedOwnerIds)
              .update({ [foreignKeyColumn]: null });
          }

          continue;
        }

        for (const item of items) {
          if (!item || typeof item !== 'object') {
            continue;
          }

          if (item.id != null) {
            await knex(tableName)
              .where('id', recordId)
              .update({ [foreignKeyColumn]: item.id });
          } else {
            const newRelatedId = await this.insertRecordAndGetId(targetTableName, item, knex);

            if (newRelatedId == null) {
              this.logger.warn(`Failed to create related entity for ${relName}`);
              continue;
            }

            await knex(tableName)
              .where('id', recordId)
              .update({ [foreignKeyColumn]: newRelatedId });
          }
        }
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

    let newRecord = { ...data };
    delete newRecord.id;

    if (this.insertWithCascade) {
      const result = await this.insertWithCascade(targetTableName, newRecord, knexOrTrx);
      if (result && result.id) {
        return result.id;
      }
      if (result && result._id) {
        return result._id;
      }
      if (typeof result === 'number' || typeof result === 'string') {
        return result;
      }
    }

    if (this.stripUnknownColumns) {
      newRecord = await this.stripUnknownColumns(targetTableName, newRecord);
    }

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
      try {
        const rawResult = await knex.raw('SELECT LAST_INSERT_ID() as lastId');
        const row = rawResult?.[0]?.[0] || rawResult?.rows?.[0];
        if (row?.lastId) newId = row.lastId;
      } catch {
        const fallback = await knex(targetTableName).orderBy('id', 'desc').first('id');
        newId = fallback?.id;
      }
    }

    return newId ?? null;
  }

  private async prepareUpdateData(
    targetTableName: string,
    data: any,
  ): Promise<any> {
    if (!data || typeof data !== 'object') return data;

    let updateData = { ...data };

    if (this.stripUnknownColumns) {
      updateData = await this.stripUnknownColumns(targetTableName, updateData);
    }

    if (this.stripNonUpdatableFields) {
      updateData = await this.stripNonUpdatableFields(targetTableName, updateData);
    }

    return updateData;
  }
}