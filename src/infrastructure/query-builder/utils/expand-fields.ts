import { JoinOption, DatabaseType } from '../../../shared/types/query-builder.types';
import { getForeignKeyColumnName } from '../../knex/utils/naming-helpers';
import { buildNestedSubquery } from './nested-subquery-builder';
import { quoteIdentifier } from '../../knex/utils/migration/sql-dialect';

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
 * Expand smart field list into subqueries with SELECT
 */
export async function expandFieldsToJoinsAndSelect(
  tableName: string,
  fields: string[],
  metadataGetter: (tableName: string) => Promise<TableMetadata | null>,
  dbType: DatabaseType,
  sortOptions: Array<{ field: string; direction: 'asc' | 'desc' }> = [],
  listTables?: () => Promise<string[]>,
): Promise<FieldExpansionResult> {
  // Helper: find sort for a relation path
  function findSortForPath(pathPrefix: string): { field: string; direction: 'asc' | 'desc' } | null {
    const found = sortOptions.find(s => s.field.startsWith(pathPrefix + '.') || s.field === pathPrefix);
    if (!found) return null;
    return found;
  }

  const joins: JoinOption[] = [];
  const select: string[] = [];

  // Get metadata for base table
  const baseMeta = await metadataGetter(tableName);
  if (!baseMeta) {
    throw new Error(`Metadata not found for table: ${tableName}`);
  }

  // Group fields by parent relation to detect nested structures
  // Example: ['hooks.*', 'hooks.methods.*'] => { 'hooks': ['*', 'methods.*'] }
  const fieldsByRelation = new Map<string, string[]>();

  for (const field of fields) {
    if (field === '*' || !field.includes('.')) {
      // Root-level fields
      if (!fieldsByRelation.has('')) {
        fieldsByRelation.set('', []);
      }
      fieldsByRelation.get('')!.push(field);
    } else {
      // Relation fields
      const parts = field.split('.');
      const relationName = parts[0];
      const remainingPath = parts.slice(1).join('.');

      if (!fieldsByRelation.has(relationName)) {
        fieldsByRelation.set(relationName, []);
      }
      fieldsByRelation.get(relationName)!.push(remainingPath);
    }
  }

  // Process root-level fields first
  const rootFields = fieldsByRelation.get('') || [];
  for (const field of rootFields) {
    if (field === '*') {
      // Root wildcard: add all scalar columns from base table
      const fkColumnsToOmit = new Set<string>();
      for (const rel of baseMeta.relations || []) {
        if (rel.type === 'many-to-one' || rel.type === 'one-to-one') {
          const fkCol = rel.foreignKeyColumn || getForeignKeyColumnName(rel.targetTableName);
          if (fkCol) fkColumnsToOmit.add(fkCol);
        }
      }
      for (const col of baseMeta.columns) {
        if (fkColumnsToOmit.has(col.name)) continue;
        select.push(`${tableName}.${col.name}`);
      }

      // Auto-add all relations with only 'id' field
      for (const rel of baseMeta.relations || []) {
        if (!fieldsByRelation.has(rel.propertyName)) {
          // Relation not explicitly requested, auto-add with id only
          fieldsByRelation.set(rel.propertyName, ['id']);
        }
      }

      continue;
    }

    if (field.includes('.')) {
      // Simple column with dot notation (shouldn't happen, but handle it)
      select.push(`${tableName}.${field}`);
      continue;
    }

    // Regular scalar column
    select.push(`${tableName}.${field}`);
  }

  // Process relation fields (non-root)
  for (const [relationName, nestedFields] of fieldsByRelation.entries()) {
    if (relationName === '') continue; // Skip root fields, already processed

    // Build nested subquery with all nested fields
    const subquery = await buildNestedSubquery(
      tableName,
      baseMeta,
      relationName,
      nestedFields,
      dbType,
      metadataGetter,
      sortOptions,
    );

    if (subquery) {
      select.push(`${subquery} as ${quoteIdentifier(relationName, dbType)}`);
    }
  }

  return { joins, select };
}
