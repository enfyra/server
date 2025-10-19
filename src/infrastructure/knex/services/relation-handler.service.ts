import { Injectable, Logger } from '@nestjs/common';
import { Knex } from 'knex';

@Injectable()
export class RelationHandlerService {
  private readonly logger = new Logger(RelationHandlerService.name);

  /**
   * Recursively clean nested objects - remove relation objects at all levels
   */
  private cleanNestedObject(obj: any, tableName: string, metadata: any, depth: number = 0): any {
    if (depth > 10) {
      this.logger.warn(`⚠️ Max recursion depth (10) reached for table ${tableName}`);
      return obj;
    }

    if (typeof obj !== 'object' || obj === null) {
      return obj;
    }

    if (Array.isArray(obj)) {
      return obj.map(item => this.cleanNestedObject(item, tableName, metadata, depth + 1));
    }

    this.logger.log(`🧹 [cleanNestedObject] Table: ${tableName}, Depth: ${depth}, Keys: ${Object.keys(obj).join(', ')}`);

    const tableMetadata = metadata.tables?.get?.(tableName) || metadata.tablesList?.find((t: any) => t.name === tableName);
    if (!tableMetadata?.relations) {
      this.logger.log(`   No relations found for ${tableName}`);
      return obj;
    }

    const cleanObj = { ...obj };

    for (const relation of tableMetadata.relations) {
      const relationName = relation.propertyName;

      if (!(relationName in cleanObj)) {
        continue;
      }

      const relationValue = cleanObj[relationName];

      switch (relation.type) {
        case 'many-to-one':
        case 'one-to-one': {
          // Convert relation object to FK value
          if (relationValue && typeof relationValue === 'object' && 'id' in relationValue && relation.foreignKeyColumn) {
            cleanObj[relation.foreignKeyColumn] = relationValue.id;
          } else if (relationValue === null && relation.foreignKeyColumn) {
            cleanObj[relation.foreignKeyColumn] = null;
          }
          delete cleanObj[relationName];
          break;
        }

        case 'many-to-many':
        case 'one-to-many': {
          // These should not be in nested objects being inserted/updated
          // Delete them to avoid trying to insert relation arrays
          delete cleanObj[relationName];
          break;
        }
      }
    }

    this.logger.log(`   After cleaning relations: ${Object.keys(cleanObj).join(', ')}`);
    return cleanObj;
  }

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
          if (!relation.foreignKeyColumn) {
            throw new Error(`${relation.type} relation '${relationName}' in table '${tableName}' missing foreignKeyColumn in metadata`);
          }

          if (relationValue && typeof relationValue === 'object' && 'id' in relationValue) {
            cleanData[relation.foreignKeyColumn] = relationValue.id;
          } else if (relationValue === null) {
            cleanData[relation.foreignKeyColumn] = null;
          }
          delete cleanData[relationName];
          break;
        }

        case 'many-to-many': {
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
          if (Array.isArray(relationValue)) {
            // Recursively clean nested items at all levels
            const targetTable = relation.targetTableName || relation.targetTable;
            if (!targetTable) {
              this.logger.warn(`⚠️ O2M relation '${relationName}' missing targetTableName, skipping cleaning`);
              oneToManyRelations.push({
                relationName,
                items: relationValue,
              });
            } else {
              const cleanedItems = relationValue.map(item =>
                this.cleanNestedObject(item, targetTable, metadata)
              );

              oneToManyRelations.push({
                relationName,
                items: cleanedItems,
              });
            }
          }
          delete cleanData[relationName];
          break;
        }
      }
    }

    return { cleanData, manyToManyRelations, oneToManyRelations };
  }

  async handleManyToManyRelations(
    knex: Knex,
    tableName: string,
    recordId: any,
    manyToManyRelations: Array<{ relationName: string; ids: any[] }>,
    metadata: any,
  ): Promise<void> {
    this.logger.log(`🔗 [handleManyToManyRelations] Table: ${tableName}, RecordId: ${recordId}`);
    this.logger.log(`   Relations to process: ${manyToManyRelations.length}`);

    const tableMetadata = metadata.tables?.get?.(tableName) || metadata.tablesList?.find((t: any) => t.name === tableName);
    if (!tableMetadata) {
      this.logger.warn(`⚠️  No metadata found for table ${tableName}`);
      return;
    }

    for (const { relationName, ids } of manyToManyRelations) {
      this.logger.log(`   Processing M2M: ${relationName} with ${ids.length} IDs: [${ids.join(', ')}]`);

      const relation = tableMetadata.relations.find((r: any) => r.propertyName === relationName);
      if (!relation) {
        throw new Error(`M2M relation '${relationName}' not found in table '${tableName}' metadata`);
      }
      if (!relation.junctionTableName) {
        throw new Error(`M2M relation '${relationName}' in table '${tableName}' missing junctionTableName in metadata`);
      }
      if (!relation.junctionSourceColumn) {
        throw new Error(`M2M relation '${relationName}' in table '${tableName}' missing junctionSourceColumn in metadata`);
      }
      if (!relation.junctionTargetColumn) {
        throw new Error(`M2M relation '${relationName}' in table '${tableName}' missing junctionTargetColumn in metadata`);
      }

      this.logger.log(`   Junction: ${relation.junctionTableName} (${relation.junctionSourceColumn}, ${relation.junctionTargetColumn})`);

      if (!recordId) {
        throw new Error(`RecordId is null for M2M relation '${relationName}' in table '${tableName}'`);
      }

      await knex(relation.junctionTableName)
        .where(relation.junctionSourceColumn, recordId)
        .delete();

      if (ids.length > 0) {
        const junctionRecords = ids.map(targetId => {
          if (!targetId) {
            throw new Error(`TargetId is null in M2M relation '${relationName}' for table '${tableName}'`);
          }
          return {
            [relation.junctionSourceColumn]: recordId,
            [relation.junctionTargetColumn]: targetId,
          };
        });

        await knex(relation.junctionTableName).insert(junctionRecords);

        this.logger.log(`   ✅ Inserted ${junctionRecords.length} junction records`);
      } else {
        this.logger.log(`   ⏭️  No IDs to insert`);
      }
    }
  }

  async handleOneToManyRelations(
    knex: Knex,
    tableName: string,
    recordId: any,
    oneToManyRelations: Array<{ relationName: string; items: any[] }>,
    metadata: any,
  ): Promise<void> {
    const tableMetadata = metadata.tables?.get?.(tableName) || metadata.tablesList?.find((t: any) => t.name === tableName);
    if (!tableMetadata) {
      throw new Error(`Metadata not found for table '${tableName}'`);
    }

    for (const { relationName, items } of oneToManyRelations) {
      const relation = tableMetadata.relations.find((r: any) => r.propertyName === relationName);
      if (!relation) {
        throw new Error(`O2M relation '${relationName}' not found in table '${tableName}' metadata`);
      }

      if (!relation.foreignKeyColumn) {
        throw new Error(`O2M relation '${relationName}' in table '${tableName}' missing foreignKeyColumn in metadata`);
      }

      if (!relation.targetTableName) {
        throw new Error(`O2M relation '${relationName}' in table '${tableName}' missing targetTableName in metadata`);
      }

      const fkColumn = relation.foreignKeyColumn;

      const existingChildren = await knex(relation.targetTableName)
        .select('id')
        .where(fkColumn, recordId);
      const existingIds = existingChildren.map(child => child.id);

      const newIds = items.filter(item => item.id).map(item => item.id);

      const idsToRemove = existingIds.filter(id => !newIds.includes(id));
      if (idsToRemove.length > 0) {
        await knex(relation.targetTableName)
          .whereIn('id', idsToRemove)
          .update({ [fkColumn]: null });
      }

      for (const item of items) {
        const childData = { ...item, [fkColumn]: recordId };

        if (item.id) {
          await knex(relation.targetTableName)
            .where('id', item.id)
            .update(childData);
        } else {
          await knex(relation.targetTableName).insert(childData);
        }
      }
    }
  }

  async insertWithCascade(
    knex: Knex,
    tableName: string,
    data: any,
    metadata: any,
    dbType: string,
  ): Promise<any> {
    this.logger.log(`🔍 [insertWithCascade] Table: ${tableName}`);
    this.logger.log(`   Data keys: ${Object.keys(data).join(', ')}`);

    const { cleanData, manyToManyRelations, oneToManyRelations } =
      this.preprocessData(tableName, data, metadata);

    this.logger.log(`   Clean data keys: ${Object.keys(cleanData).join(', ')}`);
    this.logger.log(`   M2M relations count: ${manyToManyRelations.length}`);
    if (manyToManyRelations.length > 0) {
      for (const m2m of manyToManyRelations) {
        this.logger.log(`     - ${m2m.relationName}: ${m2m.ids.length} IDs`);
      }
    }
    this.logger.log(`   O2M relations count: ${oneToManyRelations.length}`);

    let insertedId: any;

    if (dbType === 'pg' || dbType === 'postgres') {
      const result = await knex(tableName).insert(cleanData).returning('id');
      insertedId = result[0]?.id || result[0];
    } else {
      const result = await knex(tableName).insert(cleanData);
      insertedId = Array.isArray(result) ? result[0] : result;
    }

    const recordId = insertedId || cleanData.id;

    this.logger.log(`   ✅ Inserted record ID: ${recordId}`);

    await this.handleManyToManyRelations(
      knex,
      tableName,
      recordId,
      manyToManyRelations,
      metadata,
    );

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

  async updateWithCascade(
    knex: Knex,
    tableName: string,
    recordId: any,
    data: any,
    metadata: any,
    dbType: string,
  ): Promise<void> {
    const { cleanData, manyToManyRelations, oneToManyRelations } =
      this.preprocessData(tableName, data, metadata);

    if (Object.keys(cleanData).length > 0) {
      await knex(tableName).where('id', recordId).update(cleanData);
    }

    await this.handleManyToManyRelations(
      knex,
      tableName,
      recordId,
      manyToManyRelations,
      metadata,
    );

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


