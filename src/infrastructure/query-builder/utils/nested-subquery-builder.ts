import { DatabaseType } from '../../../shared/types/query-builder.types';
import { getForeignKeyColumnName } from '../../../shared/utils/naming-helpers';

interface TableMetadata {
  name: string;
  columns: Array<{ name: string; type: string }>;
  relations: Array<{
    propertyName: string;
    type: 'many-to-one' | 'one-to-many' | 'one-to-one' | 'many-to-many';
    targetTableName: string;
    inversePropertyName?: string;
    foreignKeyColumn?: string;
    junctionTableName?: string;
    junctionSourceColumn?: string;
    junctionTargetColumn?: string;
  }>;
}

/**
 * Recursively build nested subquery for a relation
 * Supports multi-level nesting like hooks.methods.*
 */
export async function buildNestedSubquery(
  parentTable: string,
  parentMeta: TableMetadata,
  relationName: string,
  nestedFields: string[], // e.g., ['*', 'methods.*'] for hooks relation
  dbType: DatabaseType,
  metadataGetter: (tableName: string) => Promise<TableMetadata | null>,
  sortOptions: Array<{ field: string; direction: 'asc' | 'desc' }> = [],
  nestingLevel: number = 0, // Track nesting depth for unique aliases
): Promise<string | null> {
  // Find the relation in parent metadata
  const relation = parentMeta.relations?.find(r => r.propertyName === relationName);
  if (!relation) {
    console.warn(`[NESTED-SUBQUERY] Relation ${relationName} not found in ${parentTable}`);
    return null;
  }

  // Get target table metadata
  const targetMeta = await metadataGetter(relation.targetTableName);
  if (!targetMeta) {
    console.warn(`[NESTED-SUBQUERY] Target metadata not found for ${relation.targetTableName}`);
    return null;
  }

  // Group nested fields to identify sub-relations
  // e.g., ['*', 'methods.*'] => root: ['*'], methods: ['*']
  const rootFields: string[] = [];
  const subRelations = new Map<string, string[]>();

  for (const field of nestedFields) {
    if (field === '*' || !field.includes('.')) {
      rootFields.push(field);
    } else {
      const parts = field.split('.');
      const subRelName = parts[0];
      const remaining = parts.slice(1).join('.');

      if (!subRelations.has(subRelName)) {
        subRelations.set(subRelName, []);
      }
      subRelations.get(subRelName)!.push(remaining);
    }
  }

  // Generate unique aliases for each nesting level
  // Level 0: currentAlias=c, parentAlias=c (but we use parentRef=tableName for root)
  // Level 1: currentAlias=c1, parentAlias=c
  // Level 2: currentAlias=c2, parentAlias=c1
  const currentAlias = nestingLevel === 0 ? 'c' : `c${nestingLevel}`;
  const parentAlias = nestingLevel <= 1 ? 'c' : `c${nestingLevel - 1}`;

  // Build columns for JSON_OBJECT
  const columns: string[] = [];

  // Add scalar columns
  if (rootFields.includes('*')) {
    // Wildcard: add all columns (except FK columns for M2O/O2O relations)
    const fkColumnsToOmit = new Set<string>();
    for (const rel of targetMeta.relations || []) {
      if (rel.type === 'many-to-one' || rel.type === 'one-to-one') {
        const fkCol = rel.foreignKeyColumn || getForeignKeyColumnName(rel.targetTableName);
        if (fkCol) fkColumnsToOmit.add(fkCol);
      }
    }
    for (const col of targetMeta.columns) {
      if (fkColumnsToOmit.has(col.name)) continue;
      columns.push(`'${col.name}', ${currentAlias}.${"`"}${col.name}${"`"}`);
    }

    // Auto-add all relations with only 'id' field when wildcard is used
    for (const rel of targetMeta.relations || []) {
      if (!subRelations.has(rel.propertyName)) {
        // Relation not explicitly requested, auto-add with id only
        subRelations.set(rel.propertyName, ['id']);
      }
    }
  } else {
    // Specific fields: add only requested columns
    for (const field of rootFields) {
      if (!field.includes('.')) {
        // Verify column exists in metadata
        const col = targetMeta.columns.find(c => c.name === field);
        if (col) {
          columns.push(`'${col.name}', ${currentAlias}.${"`"}${col.name}${"`"}`);
        }
      }
    }
  }

  // Add nested relation subqueries
  for (const [subRelName, subFields] of subRelations.entries()) {
    const nestedSubquery = await buildNestedSubquery(
      relation.targetTableName,
      targetMeta,
      subRelName,
      subFields,
      dbType,
      metadataGetter,
      sortOptions.filter(s => s.field.startsWith(`${relationName}.${subRelName}`)),
      nestingLevel + 1, // Increment nesting level
    );

    if (nestedSubquery) {
      columns.push(`'${subRelName}', ${nestedSubquery}`);
    }
  }

  if (columns.length === 0) {
    return null;
  }

  // Find sort for this relation
  const relSort = sortOptions.find(s => s.field === relationName || s.field.startsWith(relationName + '.'));
  const sortField = relSort?.field.split('.').pop() || '';

  // Build the complete subquery based on relation type
  const jsonObject = `JSON_OBJECT(${columns.join(',')})`;

  // Determine next level alias for FROM clause
  const nextAlias = nestingLevel === 0 ? 'c' : `c${nestingLevel}`;

  // For root-level queries (nestingLevel 0), use parent table name directly
  // For nested queries, use parent alias
  const parentRef = nestingLevel === 0 ? parentTable : parentAlias;

  if (relation.type === 'many-to-one' || relation.type === 'one-to-one') {
    // M2O/O2O: return single object
    const fkColumn = relation.foreignKeyColumn || `${relationName}Id`;
    return `(select ${jsonObject} from ${relation.targetTableName} ${nextAlias} where ${nextAlias}.id = ${parentRef}.${"`"}${fkColumn}${"`"} limit 1)`;
  } else if (relation.type === 'one-to-many') {
    // O2M: return array of objects
    // O2M naming convention: FK column = {inversePropertyName}Id
    let fkColumn = relation.foreignKeyColumn;

    if (!fkColumn) {
      if (relation.inversePropertyName) {
        // Use naming convention: {inversePropertyName}Id
        fkColumn = getForeignKeyColumnName(relation.inversePropertyName);
        console.log(`[NESTED-SUBQUERY] O2M relation ${relationName} - calculated FK column: ${fkColumn} from inversePropertyName: ${relation.inversePropertyName}`);
      } else {
        console.warn(`[NESTED-SUBQUERY] O2M relation ${relationName} missing both foreignKeyColumn and inversePropertyName in metadata`);
        return null;
      }
    }

    const orderClause = sortField ? ` order by ${nextAlias}.${"`"}${sortField}${"`"} ${relSort!.direction.toUpperCase()}` : '';
    return `(select ifnull(JSON_ARRAYAGG(${jsonObject}), JSON_ARRAY()) from ${relation.targetTableName} ${nextAlias} where ${nextAlias}.${"`"}${fkColumn}${"`"} = ${parentRef}.id${orderClause})`;
  } else if (relation.type === 'many-to-many') {
    // M2M: return array via junction
    const junctionTable = relation.junctionTableName;
    if (!junctionTable) {
      console.warn(`[NESTED-SUBQUERY] M2M relation ${relationName} missing junctionTableName`);
      return null;
    }

    const junctionSourceCol = relation.junctionSourceColumn || getForeignKeyColumnName(parentTable);
    const junctionTargetCol = relation.junctionTargetColumn || getForeignKeyColumnName(relation.targetTableName);

    return `(select ifnull(JSON_ARRAYAGG(${jsonObject}), JSON_ARRAY()) from ${junctionTable} j join ${relation.targetTableName} ${nextAlias} on j.${"`"}${junctionTargetCol}${"`"} = ${nextAlias}.id where j.${"`"}${junctionSourceCol}${"`"} = ${parentRef}.id)`;
  }

  return null;
}
