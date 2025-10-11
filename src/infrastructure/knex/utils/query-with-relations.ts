import { Knex } from 'knex';

/**
 * Apply relations to Knex query (like TypeORM relations option)
 * Auto-detects relation type and applies correct JOIN
 * 
 * @example
 * const query = knex('relation_definition')
 *   .where({ sourceTableId: 1 });
 * 
 * applyRelations(query, 'relation_definition', ['targetTable'], metadataGetter);
 * // → LEFT JOIN table_definition ON relation_definition.targetTableId = table_definition.id
 */
export function applyRelations(
  query: Knex.QueryBuilder,
  tableName: string,
  relations: string[],
  metadataGetter: (tableName: string) => any,
): Knex.QueryBuilder {
  if (!relations || relations.length === 0) {
    return query;
  }

  const metadata = metadataGetter(tableName);
  if (!metadata) {
    console.warn(`[applyRelations] Metadata not found for table: ${tableName}`);
    return query;
  }

  let result = query;

  for (const relationPath of relations) {
    // Parse nested relations (e.g., 'mainTable.columns')
    const parts = relationPath.split('.');
    let currentTableName = tableName;
    let currentMetadata = metadata;
    let parentAlias = tableName;

    for (let i = 0; i < parts.length; i++) {
      const relationName = parts[i];
      const relation = currentMetadata.relations?.find(
        (r: any) => r.propertyName === relationName
      );

      if (!relation) {
        console.warn(
          `[applyRelations] Relation '${relationName}' not found in ${currentTableName}`
        );
        break;
      }

      const alias = i === 0 ? relationName : `${parts.slice(0, i + 1).join('_')}`;
      const targetTableName = relation.targetTableName;

      // Apply JOIN based on relation type
      if (relation.type === 'many-to-one' || relation.type === 'one-to-one') {
        // M2O/O2O: Direct FK join
        const fkColumn = relation.foreignKeyColumn;
        result = result.leftJoin(
          `${targetTableName} as ${alias}`,
          `${parentAlias}.${fkColumn}`,
          `${alias}.id`
        );
      } else if (relation.type === 'one-to-many') {
        // O2M: Inverse FK join
        const targetMetadata = metadataGetter(targetTableName);
        if (!targetMetadata) {
          console.warn(`[applyRelations] Metadata not found for target: ${targetTableName}`);
          break;
        }

        // Find inverse relation
        const inverseRelation = targetMetadata.relations?.find(
          (r: any) => 
            r.targetTableName === currentTableName && 
            r.inversePropertyName === relationName
        );

        if (!inverseRelation) {
          console.warn(
            `[applyRelations] Inverse relation not found for O2M: ${currentTableName}.${relationName}`
          );
          break;
        }

        const inverseFkColumn = inverseRelation.foreignKeyColumn;
        result = result.leftJoin(
          `${targetTableName} as ${alias}`,
          `${alias}.${inverseFkColumn}`,
          `${parentAlias}.id`
        );
      } else if (relation.type === 'many-to-many') {
        // M2M: Join via junction table
        const junctionTable = relation.junctionTableName;
        const junctionAlias = `${alias}_junction`;
        const junctionSourceCol = relation.junctionSourceColumn;
        const junctionTargetCol = relation.junctionTargetColumn;

        if (!junctionTable || !junctionSourceCol || !junctionTargetCol) {
          console.warn(
            `[applyRelations] M2M junction info missing for: ${currentTableName}.${relationName}`
          );
          break;
        }

        result = result
          .leftJoin(
            `${junctionTable} as ${junctionAlias}`,
            `${junctionAlias}.${junctionSourceCol}`,
            `${parentAlias}.id`
          )
          .leftJoin(
            `${targetTableName} as ${alias}`,
            `${alias}.id`,
            `${junctionAlias}.${junctionTargetCol}`
          );
      }

      // Move to next level for nested relations
      parentAlias = alias;
      currentTableName = targetTableName;
      currentMetadata = metadataGetter(targetTableName);
      
      if (!currentMetadata) {
        console.warn(`[applyRelations] Metadata not found for nested: ${targetTableName}`);
        break;
      }
    }
  }

  return result;
}

/**
 * Select relation fields with proper aliases
 * 
 * @example
 * selectRelationFields('relation_definition', ['targetTable'], ['id', 'name'])
 * // → ['targetTable.id as targetTable_id', 'targetTable.name as targetTable_name']
 */
export function selectRelationFields(
  tableName: string,
  relations: string[],
  fields: string[] | '*' = '*',
  metadataGetter?: (tableName: string) => any,
): string[] {
  if (!relations || relations.length === 0) {
    return [];
  }

  const selects: string[] = [];

  for (const relationPath of relations) {
    const alias = relationPath.replace('.', '_');
    
    if (fields === '*') {
      // Select all scalar fields from relation
      if (metadataGetter) {
        const parts = relationPath.split('.');
        let currentTableName = tableName;
        
        for (const part of parts) {
          const metadata = metadataGetter(currentTableName);
          const relation = metadata?.relations?.find((r: any) => r.propertyName === part);
          
          if (relation) {
            currentTableName = relation.targetTableName;
          }
        }
        
        const targetMetadata = metadataGetter(currentTableName);
        if (targetMetadata?.columns) {
          for (const col of targetMetadata.columns) {
            selects.push(`${alias}.${col.name} as ${alias}_${col.name}`);
          }
        }
      } else {
        // Fallback: just select id
        selects.push(`${alias}.id as ${alias}_id`);
      }
    } else {
      // Select specific fields
      for (const field of fields) {
        selects.push(`${alias}.${field} as ${alias}_${field}`);
      }
    }
  }

  return selects;
}

