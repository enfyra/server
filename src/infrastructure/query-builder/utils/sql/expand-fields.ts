import { DatabaseType } from '../../../../shared/types/query-builder.types';
import { getForeignKeyColumnName } from '../../../knex/utils/naming-helpers';
import { buildNestedSubquery } from './nested-subquery-builder';
import { quoteIdentifier } from '../../../knex/utils/migration/sql-dialect';

interface FieldExpansionResult {
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

export async function expandFieldsToJoinsAndSelect(
  tableName: string,
  fields: string[],
  metadataGetter: (tableName: string) => Promise<TableMetadata | null>,
  dbType: DatabaseType,
  sortOptions: Array<{ field: string; direction: 'asc' | 'desc' }> = [],
  listTables?: () => Promise<string[]>,
): Promise<FieldExpansionResult> {
  function findSortForPath(pathPrefix: string): { field: string; direction: 'asc' | 'desc' } | null {
    const found = sortOptions.find(s => s.field.startsWith(pathPrefix + '.') || s.field === pathPrefix);
    if (!found) return null;
    return found;
  }

  const select: string[] = [];

  const baseMeta = await metadataGetter(tableName);
  if (!baseMeta) {
    return { select: [] };
  }

  const fieldsByRelation = new Map<string, string[]>();

  for (const field of fields) {
    if (field === '*' || !field.includes('.')) {
      if (!fieldsByRelation.has('')) {
        fieldsByRelation.set('', []);
      }
      fieldsByRelation.get('')!.push(field);
    } else {
      const parts = field.split('.');
      const relationName = parts[0];
      const remainingPath = parts.slice(1).join('.');

      if (!fieldsByRelation.has(relationName)) {
        fieldsByRelation.set(relationName, []);
      }
      fieldsByRelation.get(relationName)!.push(remainingPath);
    }
  }

  const rootFields = fieldsByRelation.get('') || [];
  for (const field of rootFields) {
    if (field === '*') {
      const fkColumnsToOmit = new Set<string>();
      for (const rel of baseMeta.relations || []) {
        if (rel.type === 'many-to-one' || (rel.type === 'one-to-one' && !(rel as any).isInverse)) {
          const fkCol = rel.foreignKeyColumn || getForeignKeyColumnName(rel.targetTableName);
          if (fkCol) fkColumnsToOmit.add(fkCol);
        }
      }
      const addedColumnNames = new Set<string>();
      for (const col of baseMeta.columns) {
        if (fkColumnsToOmit.has(col.name)) continue;
        select.push(`${tableName}.${col.name}`);
        addedColumnNames.add(col.name);
      }

      for (const rel of baseMeta.relations || []) {
        if (!fieldsByRelation.has(rel.propertyName) && !addedColumnNames.has(rel.propertyName)) {
          fieldsByRelation.set(rel.propertyName, ['id']);
        }
      }

      continue;
    }

    if (field.includes('.')) {
      select.push(`${tableName}.${field}`);
      continue;
    }

    const matchingColumn = baseMeta.columns?.find(c => c.name === field);
    if (matchingColumn) {
      select.push(`${tableName}.${field}`);
      continue;
    }

    const matchingRelation = baseMeta.relations?.find(r => r.propertyName === field);
    if (matchingRelation) {
      if (!fieldsByRelation.has(field)) {
        fieldsByRelation.set(field, ['id']);
      }
      continue;
    }
  }

  for (const [relationName, nestedFields] of fieldsByRelation.entries()) {
    if (relationName === '') continue;

    const relation = baseMeta.relations?.find(r => r.propertyName === relationName);
    if (!relation) {
      continue;
    }

    const isIdOnly = nestedFields.length === 1 && (nestedFields[0] === 'id' || nestedFields[0] === '_id');
    const isOwnerRelation = relation.type === 'many-to-one' || (relation.type === 'one-to-one' && !(relation as any).isInverse);

    if (isIdOnly && isOwnerRelation) {
      const fkColumn = relation.foreignKeyColumn || getForeignKeyColumnName(relation.targetTableName);
      const jsonObjectFunc = dbType === 'postgres' ? 'jsonb_build_object' : 'JSON_OBJECT';
      const quotedTable = quoteIdentifier(tableName, dbType);
      const quotedFkCol = quoteIdentifier(fkColumn, dbType);
      const quotedRelation = quoteIdentifier(relationName, dbType);
      const fkRef = `${quotedTable}.${quotedFkCol}`;

      const mapping = `(CASE WHEN ${fkRef} IS NULL THEN NULL ELSE ${jsonObjectFunc}('id', ${fkRef}) END) as ${quotedRelation}`;
      select.push(mapping);
    } else {
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
  }

  return { select };
}
