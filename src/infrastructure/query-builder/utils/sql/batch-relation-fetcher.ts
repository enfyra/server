import { Knex } from 'knex';
import { getForeignKeyColumnName } from '../../../knex/utils/sql-schema-naming.util';
import { getPrimaryKeyColumn } from '../../../knex/utils/metadata-loader';

export interface BatchFetchDescriptor {
  relationName: string;
  type: 'many-to-one' | 'one-to-one' | 'one-to-many' | 'many-to-many';
  targetTable: string;
  fields: string[];
  isInverse?: boolean;
  fkColumn?: string;
  mappedBy?: string;
  junctionTableName?: string;
  junctionSourceColumn?: string;
  junctionTargetColumn?: string;
}

interface RelationMeta {
  propertyName: string;
  type: string;
  targetTableName?: string;
  targetTable?: string;
  foreignKeyColumn?: string;
  mappedBy?: string;
  isInverse?: boolean;
  junctionTableName?: string;
  junctionSourceColumn?: string;
  junctionTargetColumn?: string;
}

interface TableMeta {
  name: string;
  columns: Array<{ name: string; type: string }>;
  relations: RelationMeta[];
}

type MetadataGetter = (tableName: string) => Promise<TableMeta | null>;

export async function executeBatchFetches(
  knex: Knex,
  parentRows: any[],
  descriptors: BatchFetchDescriptor[],
  metadataGetter: MetadataGetter,
  maxDepth: number = 3,
  currentDepth: number = 0,
  parentTableName?: string,
): Promise<void> {
  if (parentRows.length === 0 || descriptors.length === 0) return;
  if (currentDepth >= maxDepth) return;

  let parentMeta: TableMeta | undefined;
  if (parentTableName) {
    parentMeta = await metadataGetter(parentTableName) ?? undefined;
  }

  for (const desc of descriptors) {
    if (desc.type === 'many-to-one' || (desc.type === 'one-to-one' && !desc.isInverse)) {
      await fetchOwnerRelation(knex, parentRows, desc, metadataGetter, maxDepth, currentDepth);
    } else if (desc.type === 'one-to-many' || (desc.type === 'one-to-one' && desc.isInverse)) {
      await fetchInverseRelation(knex, parentRows, desc, metadataGetter, maxDepth, currentDepth, parentMeta);
    } else if (desc.type === 'many-to-many') {
      await fetchManyToMany(knex, parentRows, desc, metadataGetter, maxDepth, currentDepth, parentMeta);
    }
  }
}

async function fetchOwnerRelation(
  knex: Knex,
  parentRows: any[],
  desc: BatchFetchDescriptor,
  metadataGetter: MetadataGetter,
  maxDepth: number,
  currentDepth: number,
): Promise<void> {
  const fkKey = desc.relationName;
  const fkValues = Array.from(new Set(parentRows.map((r) => r[fkKey]).filter((v) => v != null)));
  if (fkValues.length === 0) {
    for (const row of parentRows) {
      row[fkKey] = null;
    }
    return;
  }

  const targetMeta = await metadataGetter(desc.targetTable);
  if (!targetMeta) {
    throw new Error(`Metadata not found for target table: ${desc.targetTable}`);
  }

  const pkCol = getPrimaryKeyColumn(targetMeta as any)?.name || 'id';
  const { selectCols, nestedDescs } = resolveFieldsAndNested(desc.fields, targetMeta);

  const rows = await knex(desc.targetTable)
    .select(selectCols)
    .whereIn(pkCol, fkValues);

  if (nestedDescs.length > 0) {
    await executeBatchFetches(knex, rows, nestedDescs, metadataGetter, maxDepth, currentDepth + 1, desc.targetTable);
  }

  const map = new Map<any, any>();
  for (const row of rows) {
    map.set(row[pkCol], row);
  }

  for (const parentRow of parentRows) {
    const fkVal = parentRow[fkKey];
    parentRow[fkKey] = fkVal != null ? (map.get(fkVal) || null) : null;
  }
}

async function fetchInverseRelation(
  knex: Knex,
  parentRows: any[],
  desc: BatchFetchDescriptor,
  metadataGetter: MetadataGetter,
  maxDepth: number,
  currentDepth: number,
  parentMeta?: TableMeta,
): Promise<void> {
  const parentPk = parentMeta ? getPrimaryKeyColumn(parentMeta as any)?.name || 'id' : 'id';
  const parentIds = parentRows.map((r) => r[parentPk]).filter((v) => v != null);
  if (parentIds.length === 0) return;

  let fkColumn = desc.fkColumn;
  if (!fkColumn && desc.mappedBy) {
    fkColumn = getForeignKeyColumnName(desc.mappedBy);
  }
  if (!fkColumn) {
    fkColumn = getForeignKeyColumnName(desc.targetTable);
  }

  const targetMeta = await metadataGetter(desc.targetTable);
  if (!targetMeta) {
    throw new Error(`Metadata not found for target table: ${desc.targetTable}`);
  }

  const pkCol = getPrimaryKeyColumn(targetMeta as any)?.name || 'id';
  const { selectCols, nestedDescs } = resolveFieldsAndNested(desc.fields, targetMeta);

  let groupKey = fkColumn;
  const aliasEntry = selectCols.find((c) => c.startsWith(`${fkColumn} as `));
  if (aliasEntry) {
    groupKey = aliasEntry.split(' as ')[1].trim();
  } else if (!selectCols.includes(fkColumn)) {
    selectCols.push(fkColumn);
  }

  const rows = await knex(desc.targetTable)
    .select(selectCols)
    .whereIn(fkColumn, parentIds)
    .orderBy(pkCol, 'asc');

  const grouped = new Map<any, any[]>();
  for (const row of rows) {
    const key = row[groupKey];
    if (!grouped.has(key)) grouped.set(key, []);
    grouped.get(key)!.push(row);
  }

  if (nestedDescs.length > 0) {
    await executeBatchFetches(knex, rows, nestedDescs, metadataGetter, maxDepth, currentDepth + 1, desc.targetTable);
  }

  const isO2O = desc.type === 'one-to-one';
  for (const parentRow of parentRows) {
    const children = grouped.get(parentRow[parentPk]) || [];
    parentRow[desc.relationName] = isO2O ? (children[0] || null) : children;
  }
}

async function fetchManyToMany(
  knex: Knex,
  parentRows: any[],
  desc: BatchFetchDescriptor,
  metadataGetter: MetadataGetter,
  maxDepth: number,
  currentDepth: number,
  parentMeta?: TableMeta,
): Promise<void> {
  const parentPk = parentMeta ? getPrimaryKeyColumn(parentMeta as any)?.name || 'id' : 'id';
  const parentIds = parentRows.map((r) => r[parentPk]).filter((v) => v != null);
  if (parentIds.length === 0) return;

  const junctionTable = desc.junctionTableName;
  const sourceCol = desc.junctionSourceColumn;
  const targetCol = desc.junctionTargetColumn;
  if (!junctionTable || !sourceCol || !targetCol) {
    throw new Error(`Missing junction table config for relation: ${desc.relationName}`);
  }

  const targetMeta = await metadataGetter(desc.targetTable);
  if (!targetMeta) {
    throw new Error(`Metadata not found for target table: ${desc.targetTable}`);
  }

  const pkCol = getPrimaryKeyColumn(targetMeta as any)?.name || 'id';
  const { selectCols, nestedDescs } = resolveFieldsAndNested(desc.fields, targetMeta);

  const targetSelectCols = selectCols.map((c) => {
    const asIdx = c.indexOf(' as ');
    if (asIdx !== -1) {
      const col = c.substring(0, asIdx);
      const alias = c.substring(asIdx + 4);
      return `t.${col} as ${alias}`;
    }
    return `t.${c} as ${c}`;
  });

  const rows: any[] = await knex(junctionTable)
    .join(`${desc.targetTable} as t`, `${junctionTable}.${targetCol}`, `t.${pkCol}`)
    .select([`${junctionTable}.${sourceCol} as __sourceId__`, ...targetSelectCols])
    .whereIn(`${junctionTable}.${sourceCol}`, parentIds)
    .orderBy(`t.${pkCol}`, 'asc');

  const cleanRows = rows.map((r) => {
    const { __sourceId__, ...rest } = r;
    return rest;
  });

  if (nestedDescs.length > 0) {
    await executeBatchFetches(knex, cleanRows, nestedDescs, metadataGetter, maxDepth, currentDepth + 1, desc.targetTable);
  }

  const grouped = new Map<any, any[]>();
  for (let i = 0; i < rows.length; i++) {
    const key = rows[i].__sourceId__;
    if (!grouped.has(key)) grouped.set(key, []);
    grouped.get(key)!.push(cleanRows[i]);
  }

  for (const parentRow of parentRows) {
    parentRow[desc.relationName] = grouped.get(parentRow[parentPk]) || [];
  }
}

function resolveFieldsAndNested(
  fields: string[],
  targetMeta: TableMeta,
): { selectCols: string[]; nestedDescs: BatchFetchDescriptor[] } {
  const rootFields: string[] = [];
  const subRelations = new Map<string, string[]>();

  for (const field of fields) {
    if (field === '*' || !field.includes('.')) {
      rootFields.push(field);
    } else {
      const parts = field.split('.');
      const relName = parts[0];
      const remaining = parts.slice(1).join('.');
      if (!subRelations.has(relName)) subRelations.set(relName, []);
      subRelations.get(relName)!.push(remaining);
    }
  }

  const selectCols: string[] = [];

  if (rootFields.includes('*')) {
    const fkColumnsToOmit = new Set<string>();
    for (const rel of targetMeta.relations || []) {
      if (
        rel.type === 'many-to-one' ||
        (rel.type === 'one-to-one' && !(rel as any).isInverse)
      ) {
        const fkCol = rel.foreignKeyColumn || getForeignKeyColumnName(rel.propertyName);
        if (fkCol) fkColumnsToOmit.add(fkCol);
      }
    }
    for (const col of targetMeta.columns) {
      if (fkColumnsToOmit.has(col.name)) continue;
      selectCols.push(col.name);
    }
    for (const rel of targetMeta.relations || []) {
      if (!subRelations.has(rel.propertyName)) {
        subRelations.set(rel.propertyName, ['id']);
      }
    }
  } else {
    for (const field of rootFields) {
      const col = targetMeta.columns.find((c) => c.name === field);
      if (col) {
        selectCols.push(col.name);
      } else {
        const rel = targetMeta.relations?.find((r) => r.propertyName === field);
        if (rel && !subRelations.has(field)) {
          subRelations.set(field, ['id']);
        }
      }
    }
  }

  const pkName = getPrimaryKeyColumn(targetMeta as any)?.name || 'id';
  if (!selectCols.includes(pkName)) selectCols.push(pkName);

  subRelations.forEach((_relFields, relName) => {
    const rel = targetMeta.relations?.find((r) => r.propertyName === relName);
    if (
      rel &&
      (rel.type === 'many-to-one' || (rel.type === 'one-to-one' && !(rel as any).isInverse))
    ) {
      const fkCol = rel.foreignKeyColumn || getForeignKeyColumnName(rel.propertyName);
      if (fkCol) selectCols.push(`${fkCol} as ${relName}`);
    }
  });

  const nestedDescs: BatchFetchDescriptor[] = [];
  subRelations.forEach((relFields, relName) => {
    const rel = targetMeta.relations?.find((r) => r.propertyName === relName);
    if (!rel) return;

    const targetTable = (rel as any).targetTableName || rel.targetTable;
    if (!targetTable) return;

    nestedDescs.push({
      relationName: relName,
      type: rel.type as BatchFetchDescriptor['type'],
      targetTable,
      fields: relFields,
      isInverse: (rel as any).isInverse,
      fkColumn: rel.foreignKeyColumn,
      mappedBy: (rel as any).mappedBy,
      junctionTableName: rel.junctionTableName,
      junctionSourceColumn: rel.junctionSourceColumn,
      junctionTargetColumn: rel.junctionTargetColumn,
    });
  });

  return { selectCols, nestedDescs };
}
