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

    this.logger.log(`🔍 Transform relations for ${tableName}:`, { 
      inputData: data, 
      hasMetadata: !!metadata,
      metadataTables: metadata?.tables ? 'Map' : metadata?.tablesList ? 'Array' : 'None'
    });

    const tableMetadata = metadata.tables?.get?.(tableName) || metadata.tablesList?.find((t: any) => t.name === tableName);
    if (!tableMetadata?.relations) {
      this.logger.log(`⚠️  No relations found for table ${tableName}`);
      return { cleanData, manyToManyRelations, oneToManyRelations };
    }

    this.logger.log(`🔍 Found ${tableMetadata.relations.length} relations for ${tableName}:`, 
      tableMetadata.relations.map(r => ({ name: r.propertyName, type: r.type })));

    for (const relation of tableMetadata.relations) {
      const relationName = relation.propertyName;
      
      if (!(relationName in data)) {
        this.logger.log(`🔍 Relation ${relationName} not in input data`);
        continue;
      }

      const relationValue = data[relationName];
      this.logger.log(`🔍 Processing relation ${relationName} (${relation.type}):`, relationValue);

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
            this.logger.log(`🔍 Found O2M relation ${relationName} with ${relationValue.length} items`);
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

    this.logger.log(`🔍 Final cleanData for ${tableName}:`, cleanData);
    this.logger.log(`🔍 Relations to process:`, { 
      manyToMany: manyToManyRelations.length, 
      oneToMany: oneToManyRelations.length 
    });

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

    this.logger.log(`🔍 Handle M2M relations for ${tableName}:`, manyToManyRelations.map(r => ({ relationName: r.relationName, idsCount: r.ids.length })));

    for (const { relationName, ids } of manyToManyRelations) {
      const relation = tableMetadata.relations.find((r: any) => r.propertyName === relationName);
      if (!relation || !relation.junctionTableName) {
        this.logger.warn(`⚠️  Relation ${relationName} not found or no junction table`);
        continue;
      }

      this.logger.log(`🔍 M2M relation ${relationName}:`, {
        junctionTable: relation.junctionTableName,
        sourceColumn: relation.junctionSourceColumn,
        targetColumn: relation.junctionTargetColumn,
        recordId,
        targetIds: ids
      });

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
        
        this.logger.debug(
          `🔗 M2M: Linked ${ids.length} ${relationName} to ${tableName}#${recordId}`,
        );
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
      this.logger.warn(`⚠️  No metadata found for table ${tableName}`);
      return;
    }

    this.logger.log(`🔍 Handle O2M relations for ${tableName}:`, oneToManyRelations.map(r => ({ relationName: r.relationName, itemsCount: r.items.length })));

    for (const { relationName, items } of oneToManyRelations) {
      const relation = tableMetadata.relations.find((r: any) => r.propertyName === relationName);
      if (!relation) {
        this.logger.warn(`⚠️  Relation ${relationName} not found in ${tableName} metadata`);
        continue;
      }

      this.logger.log(`🔍 Found relation ${relationName}:`, { type: relation.type, targetTable: relation.targetTableName, inverseProperty: relation.inversePropertyName });

      // Find the inverse relation to get the FK column name
      const targetTableMetadata = metadata.tables?.get?.(relation.targetTableName) || 
        metadata.tablesList?.find((t: any) => t.name === relation.targetTableName);
      
      if (!targetTableMetadata) {
        this.logger.warn(`⚠️  Target table ${relation.targetTableName} not found in metadata`);
        continue;
      }

      const inverseRelation = targetTableMetadata.relations.find(
        (r: any) => r.propertyName === relation.inversePropertyName && 
                   r.targetTableName === tableName
      );

      if (!inverseRelation?.foreignKeyColumn) {
        this.logger.warn(`⚠️  Inverse relation not found or no FK column:`, { 
          relationName: relation.inversePropertyName, 
          targetTable: tableName,
          availableRelations: targetTableMetadata.relations.map(r => ({ propertyName: r.propertyName, targetTable: r.targetTableName }))
        });
        continue;
      }

      const fkColumn = inverseRelation.foreignKeyColumn;
      this.logger.log(`🔍 Using FK column: ${fkColumn} for relation ${relationName}`);

      // Get existing children IDs to compare
      const existingChildren = await knex(relation.targetTableName)
        .select('id')
        .where(fkColumn, recordId);
      const existingIds = existingChildren.map(child => child.id);
      this.logger.log(`🔍 Existing children IDs:`, existingIds);

      // Get new children IDs from input
      const newIds = items.filter(item => item.id).map(item => item.id);
      this.logger.log(`🔍 New children IDs:`, newIds);

      // Remove children that are no longer in the list
      const idsToRemove = existingIds.filter(id => !newIds.includes(id));
      if (idsToRemove.length > 0) {
        this.logger.log(`🔍 Removing children:`, idsToRemove);
        await knex(relation.targetTableName)
          .whereIn('id', idsToRemove)
          .update({ [fkColumn]: null });
      }

      // Update/insert children
      this.logger.log(`🔍 Processing ${items.length} child items`);
      for (const item of items) {
        const childData = { ...item, [fkColumn]: recordId };
        this.logger.log(`🔍 Processing child item:`, { id: item.id, data: childData });

        if (item.id) {
          // Update existing child
          await knex(relation.targetTableName)
            .where('id', item.id)
            .update(childData);
          this.logger.log(`✅ Updated existing child ${item.id}`);
        } else {
          // Insert new child
          const [insertedId] = await knex(relation.targetTableName).insert(childData);
          this.logger.log(`✅ Inserted new child with ID: ${insertedId}`);
        }
      }

      this.logger.log(
        `🔗 O2M: Updated ${items.length} ${relationName} for ${tableName}#${recordId}`,
      );
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
      this.logger.log(
        `🔗 O2M: Processing ${oneToManyRelations.length} O2M relations for ${tableName}#${recordId}`,
      );
      this.logger.log(`🔍 O2M relations:`, oneToManyRelations.map(r => ({ name: r.relationName, count: r.items.length })));
      
      await this.handleOneToManyRelations(
        knex,
        tableName,
        recordId,
        oneToManyRelations,
        metadata,
      );
    } else {
      this.logger.log(`🔍 No O2M relations to process for ${tableName}#${recordId}`);
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
      this.logger.log(
        `🔗 O2M: Processing ${oneToManyRelations.length} O2M relations for ${tableName}#${recordId}`,
      );
      this.logger.log(`🔍 O2M relations:`, oneToManyRelations.map(r => ({ name: r.relationName, count: r.items.length })));
      
      await this.handleOneToManyRelations(
        knex,
        tableName,
        recordId,
        oneToManyRelations,
        metadata,
      );
    } else {
      this.logger.log(`🔍 No O2M relations to process for ${tableName}#${recordId}`);
    }
  }
}


