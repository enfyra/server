import { JoinOption } from '../../../shared/types/query-builder.types';
import { getForeignKeyColumnName } from '../../../shared/utils/naming-helpers';

interface FieldExpansionResult {
  joins: JoinOption[];
  select: string[];
}

interface TableMetadata {
  name: string;
  columns: Array<{ name: string; type: string }>;
  relations: Array<{
    propertyName: string;
    type: 'many-to-one' | 'one-to-many' | 'one-to-one' | 'many-to-many';
    targetTableName: string;
    foreignKeyColumn?: string;
    junctionTableName?: string;
    junctionSourceColumn?: string;
    junctionTargetColumn?: string;
  }>;
}

/**
 * Expand smart field list into explicit JOINs and SELECT
 * 
 * Examples:
 * - '*' → All scalar columns from main table
 * - 'mainTable.*' → JOIN table_definition, select all its columns
 * - 'handlers.*' → JOIN route_handler_definition (O2M)
 * - 'handlers.method.*' → JOIN route_handler_definition + method_definition (nested)
 * - 'publishedMethods.*' → JOIN junction + method_definition (M2M)
 * 
 * @param tableName Base table name
 * @param fields Field list to expand
 * @param metadataGetter Function to get table metadata
 */
export async function expandFieldsToJoinsAndSelect(
  tableName: string,
  fields: string[],
  metadataGetter: (tableName: string) => Promise<TableMetadata | null>,
): Promise<FieldExpansionResult> {
  const joins: JoinOption[] = [];
  const select: string[] = [];
  const joinedTables = new Set<string>(); // Track to avoid duplicate joins

  // Get metadata for base table
  const baseMeta = await metadataGetter(tableName);
  if (!baseMeta) {
    throw new Error(`Metadata not found for table: ${tableName}`);
  }

  for (const field of fields) {
    if (field === '*') {
      // Select all scalar columns from base table
      for (const col of baseMeta.columns) {
        select.push(`${tableName}.${col.name}`);
      }
      continue;
    }

    // Parse field path: "relation.nestedRelation.field" or "relation.*"
    const parts = field.split('.');
    
    if (parts.length === 1) {
      // Simple column: "columnName"
      select.push(`${tableName}.${parts[0]}`);
      continue;
    }

    // Relation path: expand recursively
    await expandRelationPath(
      tableName,
      baseMeta,
      parts,
      tableName,
      joins,
      select,
      joinedTables,
      metadataGetter,
    );
  }

  return { joins, select };
}

/**
 * Recursively expand a relation path into JOINs and SELECT
 */
async function expandRelationPath(
  currentTable: string,
  currentMeta: TableMetadata,
  pathParts: string[],
  rootTable: string,
  joins: JoinOption[],
  select: string[],
  joinedTables: Set<string>,
  metadataGetter: (tableName: string) => Promise<TableMetadata | null>,
  parentAlias?: string,
): Promise<void> {
  if (pathParts.length === 0) return;

  const [relationName, ...restParts] = pathParts;
  const isWildcard = restParts.length === 1 && restParts[0] === '*';
  const isLeaf = restParts.length === 0 && relationName === '*';

  if (isLeaf) {
    // "relation.*" at leaf - select all columns from current level
    for (const col of currentMeta.columns) {
      const tableRef = parentAlias || currentTable;
      select.push(`${tableRef}.${col.name}`);
    }
    return;
  }

  // Find relation in metadata
  const relation = currentMeta.relations?.find(r => r.propertyName === relationName);
  
  if (!relation) {
    // Not a relation, might be a column
    if (restParts.length === 0) {
      const tableRef = parentAlias || currentTable;
      select.push(`${tableRef}.${relationName}`);
    }
    return;
  }

  // Determine alias for this relation
  const alias = parentAlias 
    ? `${parentAlias}_${relationName}` 
    : relationName;

  const targetTable = relation.targetTableName;
  const targetMeta = await metadataGetter(targetTable);
  
  if (!targetMeta) {
    console.warn(`Metadata not found for target table: ${targetTable}`);
    return;
  }

  // Build JOIN based on relation type
  const localTableRef = parentAlias || currentTable;
  
  switch (relation.type) {
    case 'many-to-one':
    case 'one-to-one': {
      // M2O/O2O: Direct FK join
      const fkColumn = relation.foreignKeyColumn || getForeignKeyColumnName(targetTable);
      const joinKey = `${localTableRef}:${alias}`;
      
      if (!joinedTables.has(joinKey)) {
        joins.push({
          type: 'left',
          table: `${targetTable} as ${alias}`,
          on: {
            local: `${localTableRef}.${fkColumn}`,
            foreign: `${alias}.id`,
          },
        });
        joinedTables.add(joinKey);
      }

      // If wildcard or nested, continue expansion
      if (isWildcard) {
        // Select all columns from target table
        for (const col of targetMeta.columns) {
          select.push(`${alias}.${col.name} as ${alias}_${col.name}`);
        }
      } else if (restParts.length > 0) {
        // Nested relation: recurse
        await expandRelationPath(
          targetTable,
          targetMeta,
          restParts,
          rootTable,
          joins,
          select,
          joinedTables,
          metadataGetter,
          alias,
        );
      }
      break;
    }

    case 'one-to-many': {
      // O2M: Inverse FK join (target table has FK to source)
      // Need to find the inverse M2O relation in target table
      let fkColumn: string;
      
      if (relation.foreignKeyColumn) {
        // Use explicit FK from metadata
        fkColumn = relation.foreignKeyColumn;
      } else {
        // Find the M2O relation in target table that points back to current table
        const inverseRelation = targetMeta.relations?.find(
          r => r.type === 'many-to-one' && r.targetTableName === currentTable
        );
        
        if (inverseRelation?.foreignKeyColumn) {
          fkColumn = inverseRelation.foreignKeyColumn;
        } else if (inverseRelation) {
          // Use inverse relation's propertyName + 'Id'
          // e.g., hook has M2O 'route' → FK is 'routeId'
          fkColumn = `${inverseRelation.propertyName}Id`;
        } else {
          // Last resort fallback: use currentTable name (without _definition suffix) + 'Id'
          // e.g., 'route_definition' → 'routeId'
          const tableName = currentTable.replace('_definition', '');
          fkColumn = `${tableName}Id`;
        }
      }
      
      const joinKey = `${localTableRef}:${alias}`;
      
      if (!joinedTables.has(joinKey)) {
        joins.push({
          type: 'left',
          table: `${targetTable} as ${alias}`,
          on: {
            local: `${localTableRef}.id`,
            foreign: `${alias}.${fkColumn}`,
          },
        });
        joinedTables.add(joinKey);
      }

      // If wildcard, select all columns
      if (isWildcard) {
        for (const col of targetMeta.columns) {
          select.push(`${alias}.${col.name} as ${alias}_${col.name}`);
        }
      } else if (restParts.length > 0) {
        // Nested relation
        await expandRelationPath(
          targetTable,
          targetMeta,
          restParts,
          rootTable,
          joins,
          select,
          joinedTables,
          metadataGetter,
          alias,
        );
      }
      break;
    }

    case 'many-to-many': {
      // M2M: Join junction table + target table
      const junctionTable = relation.junctionTableName;
      const junctionSourceCol = relation.junctionSourceColumn || getForeignKeyColumnName(currentTable);
      const junctionTargetCol = relation.junctionTargetColumn || getForeignKeyColumnName(targetTable);
      
      if (!junctionTable) {
        console.warn(`M2M relation ${relationName} missing junctionTableName`);
        return;
      }

      const junctionAlias = `${alias}_junction`;
      const junctionJoinKey = `${localTableRef}:${junctionAlias}`;
      const targetJoinKey = `${junctionAlias}:${alias}`;

      // Join junction table
      if (!joinedTables.has(junctionJoinKey)) {
        joins.push({
          type: 'left',
          table: `${junctionTable} as ${junctionAlias}`,
          on: {
            local: `${localTableRef}.id`,
            foreign: `${junctionAlias}.${junctionSourceCol}`,
          },
        });
        joinedTables.add(junctionJoinKey);
      }

      // Join target table
      if (!joinedTables.has(targetJoinKey)) {
        joins.push({
          type: 'left',
          table: `${targetTable} as ${alias}`,
          on: {
            local: `${junctionAlias}.${junctionTargetCol}`,
            foreign: `${alias}.id`,
          },
        });
        joinedTables.add(targetJoinKey);
      }

      // If wildcard, select all columns from target
      if (isWildcard) {
        for (const col of targetMeta.columns) {
          select.push(`${alias}.${col.name} as ${alias}_${col.name}`);
        }
      } else if (restParts.length > 0) {
        // Nested relation
        await expandRelationPath(
          targetTable,
          targetMeta,
          restParts,
          rootTable,
          joins,
          select,
          joinedTables,
          metadataGetter,
          alias,
        );
      }
      break;
    }
  }
}

