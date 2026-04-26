import { DatabaseType } from '../../../../../shared/types/query-builder.types';
import { getForeignKeyColumnName } from '../../../query-dsl/utils/sql-schema-naming.util';
import { quoteIdentifier } from '../../../../../engine/knex';
import { BatchFetchDescriptor } from './batch-relation-fetcher';

interface FieldExpansionResult {
  select: string[];
  batchFetchDescriptors: BatchFetchDescriptor[];
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
  maxDepth?: number,
  deepOptions?: Record<string, any>,
): Promise<FieldExpansionResult> {
  const select: string[] = [];
  const batchFetchDescriptors: BatchFetchDescriptor[] = [];

  const baseMeta = await metadataGetter(tableName);
  if (!baseMeta) {
    return { select: [], batchFetchDescriptors: [] };
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
        if (
          rel.type === 'many-to-one' ||
          (rel.type === 'one-to-one' && !(rel as any).isInverse)
        ) {
          const fkCol =
            rel.foreignKeyColumn ||
            getForeignKeyColumnName(rel.targetTableName);
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
        if (
          !fieldsByRelation.has(rel.propertyName) &&
          !addedColumnNames.has(rel.propertyName)
        ) {
          fieldsByRelation.set(rel.propertyName, ['id']);
        }
      }

      continue;
    }

    if (field.includes('.')) {
      select.push(`${tableName}.${field}`);
      continue;
    }

    const matchingColumn = baseMeta.columns?.find((c) => c.name === field);
    if (matchingColumn) {
      select.push(`${tableName}.${field}`);
      continue;
    }

    const matchingRelation = baseMeta.relations?.find(
      (r) => r.propertyName === field,
    );
    if (matchingRelation) {
      if (!fieldsByRelation.has(field)) {
        fieldsByRelation.set(field, ['id']);
      }
      continue;
    }
  }

  fieldsByRelation.forEach((nestedFields, relationName) => {
    if (relationName === '') return;

    const relation = baseMeta.relations?.find(
      (r) => r.propertyName === relationName,
    );
    if (!relation) return;

    const isOwnerRelation =
      relation.type === 'many-to-one' ||
      (relation.type === 'one-to-one' && !(relation as any).isInverse);

    if (isOwnerRelation) {
      const fkColumn =
        relation.foreignKeyColumn ||
        getForeignKeyColumnName(
          relation.targetTableName || (relation as any).targetTable,
        );
      const quotedTable = quoteIdentifier(tableName, dbType);
      const quotedFk = quoteIdentifier(fkColumn, dbType);
      const quotedAlias = quoteIdentifier(relationName, dbType);
      select.push(`${quotedTable}.${quotedFk} as ${quotedAlias}`);
    }

    const targetTable =
      relation.targetTableName || (relation as any).targetTable;

    const deepEntry = deepOptions?.[relationName];
    const resolvedFields =
      deepEntry?.fields != null
        ? Array.isArray(deepEntry.fields)
          ? deepEntry.fields
          : String(deepEntry.fields)
              .split(',')
              .map((s: string) => s.trim())
              .filter(Boolean)
        : nestedFields;

    batchFetchDescriptors.push({
      relationName,
      type: relation.type,
      targetTable,
      fields: resolvedFields,
      isInverse: (relation as any).isInverse,
      fkColumn: relation.foreignKeyColumn,
      mappedBy: (relation as any).mappedBy,
      junctionTableName: relation.junctionTableName,
      junctionSourceColumn: relation.junctionSourceColumn,
      junctionTargetColumn: relation.junctionTargetColumn,
      userFilter: deepEntry?.filter,
      userSort: deepEntry?.sort,
      userLimit:
        deepEntry?.limit !== undefined ? Number(deepEntry.limit) : undefined,
      userPage:
        deepEntry?.page !== undefined ? Number(deepEntry.page) : undefined,
      nestedDeep: deepEntry?.deep,
    });
  });

  return {
    select,
    batchFetchDescriptors,
  };
}
