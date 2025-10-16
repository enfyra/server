import { Injectable, Logger } from '@nestjs/common';
import { Knex } from 'knex';

/**
 * RelationHandlerService - Handle TypeORM-like cascade behavior in Knex
 * Processes relation fields before insert/update and handles junction tables
 */
@Injectable()
export class RelationHandlerService {
  private readonly logger = new Logger(RelationHandlerService.name);

  /**
   * Pre-process data before insert/update
   * Transforms relation objects to FK IDs and extracts M2M/O2M relations
   */
  preprocessData(
    tableName: string,
    data: any,
    metadata: any,
  ): {
    cleanData: any;
    manyToManyRelations: Array<{ relationName: string; ids: any[] }>;
    oneToManyRelations: Array<{ relationName: string; items: any[] }>;
  } {
    const cleanData = { ...data };
    const manyToManyRelations: Array<{ relationName: string; ids: any[] }> = [];
    const oneToManyRelations: Array<{ relationName: string; items: any[] }> = [];

    const tableMetadata = metadata.tables?.get?.(tableName) || metadata.tablesList?.find((t: any) => t.name === tableName);
    if (!tableMetadata?.relations) {
      return { cleanData, manyToManyRelations, oneToManyRelations };
    }

    for (const relation of tableMetadata.relations) {
      const relationName = relation.propertyName;

      if (!(relationName in data)) {
        continue;
      }

      const relationValue = data[relationName];

      switch (relation.type) {
        case 'many-to-one':
        case 'one-to-one': {
          // Transform: { user: { id: 1 } } => { userId: 1 }
          if (relationValue && typeof relationValue === 'object' && 'id' in relationValue) {
            const fkColumn = relation.foreignKeyColumn || `${relationName}Id`;
            cleanData[fkColumn] = relationValue.id;
          } else if (relationValue === null) {
            const fkColumn = relation.foreignKeyColumn || `${relationName}Id`;
            cleanData[fkColumn] = null;
          }
          delete cleanData[relationName];
          break;
        }

        case 'many-to-many': {
          // Extract IDs for junction table insertion
          if (Array.isArray(relationValue)) {
            const ids = relationValue
              .map(item => (typeof item === 'object' && 'id' in item ? item.id : item))
              .filter(id => id != null);
            
            if (ids.length > 0) {
              manyToManyRelations.push({
                relationName,
                ids,
              });
            }
          }
          delete cleanData[relationName];
          break;
        }

        case 'one-to-many': {
          // Extract items for child table updates
          if (Array.isArray(relationValue)) {
            oneToManyRelations.push({
              relationName,
              items: relationValue,
            });
          }
          delete cleanData[relationName];
          break;
        }
      }
    }

    return { cleanData, manyToManyRelations, oneToManyRelations };
  }

  /**
   * Handle many-to-many relations after insert/update
   */
  async handleManyToManyRelations(
    knex: Knex,
    tableName: string,
    recordId: any,
    manyToManyRelations: Array<{ relationName: string; ids: any[] }>,
    metadata: any,
  ): Promise<void> {
    const tableMetadata = metadata.tables?.get?.(tableName) || metadata.tablesList?.find((t: any) => t.name === tableName);
    if (!tableMetadata) {
      this.logger.warn(`⚠️  No metadata found for table ${tableName}`);
      return;
    }

    for (const { relationName, ids } of manyToManyRelations) {
      const relation = tableMetadata.relations.find((r: any) => r.propertyName === relationName);
      if (!relation || !relation.junctionTableName) {
        this.logger.warn(`⚠️  Relation ${relationName} not found or no junction table`);
        continue;
      }

      // Check for null values
      if (!recordId) {
        this.logger.error(`❌ RecordId is null for M2M relation ${relationName}`);
        continue;
      }

      // Clear existing junction records
      await knex(relation.junctionTableName)
        .where(relation.junctionSourceColumn, recordId)
        .delete();

      // Insert new junction records
      if (ids.length > 0) {
        const junctionRecords = ids.map(targetId => {
          if (!targetId) {
            this.logger.warn(`⚠️  TargetId is null for M2M relation ${relationName}`);
            return null;
          }
          return {
            [relation.junctionSourceColumn]: recordId,
            [relation.junctionTargetColumn]: targetId,
          };
        }).filter(record => record !== null);

        if (junctionRecords.length > 0) {
          await knex(relation.junctionTableName).insert(junctionRecords);
        }
      }
    }
  }

  /**
   * Handle one-to-many relations after insert/update
   */
  async handleOneToManyRelations(
    knex: Knex,
    tableName: string,
    recordId: any,
    oneToManyRelations: Array<{ relationName: string; items: any[] }>,
    metadata: any,
  ): Promise<void> {
    const tableMetadata = metadata.tables?.get?.(tableName) || metadata.tablesList?.find((t: any) => t.name === tableName);
    if (!tableMetadata) {
      return;
    }

    for (const { relationName, items } of oneToManyRelations) {
      const relation = tableMetadata.relations.find((r: any) => r.propertyName === relationName);
      if (!relation) {
        continue;
      }

      // For O2M relations, FK column is {inversePropertyName}Id in target table
      if (!relation.inversePropertyName) {
        this.logger.warn(`⚠️  O2M relation ${relationName} missing inversePropertyName`);
        continue;
      }

      // O2M naming convention: FK column = {inversePropertyName}Id
      const fkColumn = `${relation.inversePropertyName}Id`;

      // Get existing children IDs to compare
      const existingChildren = await knex(relation.targetTableName)
        .select('id')
        .where(fkColumn, recordId);
      const existingIds = existingChildren.map(child => child.id);

      // Get new children IDs from input
      const newIds = items.filter(item => item.id).map(item => item.id);

      // Remove children that are no longer in the list
      const idsToRemove = existingIds.filter(id => !newIds.includes(id));
      if (idsToRemove.length > 0) {
        await knex(relation.targetTableName)
          .whereIn('id', idsToRemove)
          .update({ [fkColumn]: null });
      }

      // Update/insert children
      for (const item of items) {
        const childData = { ...item, [fkColumn]: recordId };

        if (item.id) {
          // Update existing child
          await knex(relation.targetTableName)
            .where('id', item.id)
            .update(childData);
        } else {
          // Insert new child
          await knex(relation.targetTableName).insert(childData);
        }
      }
    }
  }

  /**
   * Full cascade insert - handles all relation types
   */
  async insertWithCascade(
    knex: Knex,
    tableName: string,
    data: any,
    metadata: any,
  ): Promise<any> {
    const { cleanData, manyToManyRelations, oneToManyRelations } = 
      this.preprocessData(tableName, data, metadata);

    // Insert main record
    const [insertedId] = await knex(tableName).insert(cleanData);
    const recordId = insertedId || cleanData.id;

    // Handle relations
    await this.handleManyToManyRelations(
      knex,
      tableName,
      recordId,
      manyToManyRelations,
      metadata,
    );

    // Handle one-to-many relations
    if (oneToManyRelations.length > 0) {
      await this.handleOneToManyRelations(
        knex,
        tableName,
        recordId,
        oneToManyRelations,
        metadata,
      );
    }

    return recordId;
  }

  /**
   * Full cascade update - handles all relation types
   */
  async updateWithCascade(
    knex: Knex,
    tableName: string,
    recordId: any,
    data: any,
    metadata: any,
  ): Promise<void> {
    const { cleanData, manyToManyRelations, oneToManyRelations } = 
      this.preprocessData(tableName, data, metadata);

    // Update main record
    if (Object.keys(cleanData).length > 0) {
      await knex(tableName).where('id', recordId).update(cleanData);
    }

    // Handle relations
    await this.handleManyToManyRelations(
      knex,
      tableName,
      recordId,
      manyToManyRelations,
      metadata,
    );

    // Handle one-to-many relations
    if (oneToManyRelations.length > 0) {
      await this.handleOneToManyRelations(
        knex,
        tableName,
        recordId,
        oneToManyRelations,
        metadata,
      );
    }
  }
}


