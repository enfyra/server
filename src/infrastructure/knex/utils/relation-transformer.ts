import { Logger } from '@nestjs/common';
import type { MetadataCacheService } from '../../cache/services/metadata-cache.service';

export class RelationTransformer {
  constructor(
    private metadataCacheService: MetadataCacheService,
    private logger: Logger,
  ) {}

  /**
   * Transform relation objects to foreign key values
   * Extracts M2M, O2M, and O2O cascade data into special properties
   */
  async transformRelationsToFK(tableName: string, data: any): Promise<any> {
    if (!tableName) return data;

    const metadata = await this.metadataCacheService.getMetadata();
    const tableMeta = metadata.tables?.get?.(tableName) ||
                      metadata.tablesList?.find((t: any) => t.name === tableName);

    if (!tableMeta || !tableMeta.relations) {
      return data;
    }

    const transformed = { ...data };
    const manyToManyRelations: Array<{ relationName: string; ids: any[] }> = [];
    const oneToManyRelations: Array<{ relationName: string; items: any[] }> = [];
    const oneToOneRelations: Array<{ relationName: string; item: any; foreignKeyColumn: string; targetTable: string }> = [];

    for (const relation of tableMeta.relations) {
      const relName = relation.propertyName;

      if (!(relName in transformed)) {
        continue;
      }

      const relValue = transformed[relName];

      switch (relation.type) {
        case 'many-to-one': {
          const fkColumn = relation.foreignKeyColumn || `${relName}Id`;

          if (relValue === null) {
            transformed[fkColumn] = null;
          } else if (typeof relValue === 'object' && relValue.id !== undefined) {
            transformed[fkColumn] = relValue.id;
          } else if (typeof relValue === 'number' || typeof relValue === 'string') {
            transformed[fkColumn] = relValue;
          } else {
            this.logger.warn(`[RelationTransformer] Unexpected relation value format for ${relName}: ${typeof relValue}, value: ${JSON.stringify(relValue)}`);
          }

          delete transformed[relName];
          break;
        }

        case 'one-to-one': {
          const isInverse = relation.isInverse || relation.mappedBy;

          if (isInverse) {
            delete transformed[relName];
            break;
          }

          const fkColumn = relation.foreignKeyColumn || `${relName}Id`;

          if (relValue === null) {
            transformed[fkColumn] = null;
          } else if (typeof relValue === 'object') {
            if (relValue.id !== undefined) {
              transformed[fkColumn] = relValue.id;
            } else {
              const targetTable = relation.targetTableName || relation.targetTable;
              if (targetTable) {
                const cleanedItem = await this.cleanNestedRelations(relValue, targetTable, metadata);
                oneToOneRelations.push({
                  relationName: relName,
                  item: cleanedItem,
                  foreignKeyColumn: fkColumn,
                  targetTable,
                });
              }
            }
          } else if (typeof relValue === 'number' || typeof relValue === 'string') {
            transformed[fkColumn] = relValue;
          }

          delete transformed[relName];
          break;
        }

        case 'many-to-many': {
          // Extract IDs from M2M relation array
          if (Array.isArray(relValue)) {
            const ids = relValue
              .map(item => (typeof item === 'object' && 'id' in item ? item.id : item))
              .filter(id => id != null);

            if (ids.length > 0) {
              manyToManyRelations.push({
                relationName: relName,
                ids,
              });
            }
          }
          delete transformed[relName];
          break;
        }

        case 'one-to-many': {
          // Recursively clean nested O2M items
          if (Array.isArray(relValue)) {
            const targetTable = relation.targetTableName || relation.targetTable;
            if (targetTable) {
              const cleanedItems = await Promise.all(
                relValue.map(async item => this.cleanNestedRelations(item, targetTable, metadata))
              );

              oneToManyRelations.push({
                relationName: relName,
                items: cleanedItems,
              });
            } else {
              this.logger.warn(`O2M relation '${relName}' missing targetTableName, skipping cleaning`);
            }
          }
          delete transformed[relName];
          break;
        }
      }
    }

    // Store M2M, O2M, and O2O data in special properties for insertWithCascade to use
    if (manyToManyRelations.length > 0) {
      transformed._m2mRelations = manyToManyRelations;
    }
    if (oneToManyRelations.length > 0) {
      transformed._o2mRelations = oneToManyRelations;
    }
    if (oneToOneRelations.length > 0) {
      transformed._o2oRelations = oneToOneRelations;
    }

    return transformed;
  }

  /**
   * Recursively clean nested objects - remove relation objects at all levels
   * This is used for O2M cascade inserts/updates
   */
  async cleanNestedRelations(obj: any, tableName: string, metadata: any, depth: number = 0): Promise<any> {
    if (depth > 10) {
      this.logger.warn(`Max recursion depth (10) reached for table ${tableName}`);
      return obj;
    }

    if (typeof obj !== 'object' || obj === null) {
      return obj;
    }

    if (Array.isArray(obj)) {
      return Promise.all(obj.map(item => this.cleanNestedRelations(item, tableName, metadata, depth + 1)));
    }

    const tableMetadata = metadata.tables?.get?.(tableName) || metadata.tablesList?.find((t: any) => t.name === tableName);
    if (!tableMetadata?.relations) {
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
          delete cleanObj[relationName];
          break;
        }
      }
    }

    return cleanObj;
  }
}
