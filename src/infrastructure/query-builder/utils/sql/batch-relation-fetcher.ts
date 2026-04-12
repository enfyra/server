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

const WHERE_IN_CHUNK_SIZE = 5000;

async function chunkedFetch<T>(
  values: any[],
  fetchFn: (chunk: any[]) => Promise<T[]>,
): Promise<T[]> {
  if (values.length <= WHERE_IN_CHUNK_SIZE) {
    return fetchFn(values);
  }
  const chunks: any[][] = [];
  for (let i = 0; i < values.length; i += WHERE_IN_CHUNK_SIZE) {
    chunks.push(values.slice(i, i + WHERE_IN_CHUNK_SIZE));
  }
  const results = await Promise.all(chunks.map(fetchFn));
  return results.flat();
}

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

  await Promise.all(descriptors.map((desc) => {
    if (desc.type === 'many-to-one' || (desc.type === 'one-to-one' && !desc.isInverse)) {
      return fetchOwnerRelation(knex, parentRows, desc, metadataGetter, maxDepth, currentDepth);
    } else if (desc.type === 'one-to-many' || (desc.type === 'one-to-one' && desc.isInverse)) {
      return fetchInverseRelation(knex, parentRows, desc, metadataGetter, maxDepth, currentDepth, parentMeta);
    } else if (desc.type === 'many-to-many') {
      return fetchManyToMany(knex, parentRows, desc, metadataGetter, maxDepth, currentDepth, parentMeta);
    }
    return Promise.resolve();
  }));
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

  const isPkOnly = selectCols.length === 1 && selectCols[0] === pkCol && nestedDescs.length === 0;
  if (isPkOnly) {
    for (const parentRow of parentRows) {
      const fkVal = parentRow[fkKey];
      parentRow[fkKey] = fkVal != null ? { [pkCol]: fkVal } : null;
    }
    return;
  }

  const rows = await chunkedFetch(fkValues, (chunk) =>
    knex(desc.targetTable).select(selectCols).whereIn(pkCol, chunk),
  );

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
  let fkPushedAsRaw = false;
  const aliasEntry = selectCols.find((c) => c.startsWith(`${fkColumn} as `));
  if (aliasEntry) {
    groupKey = aliasEntry.split(' as ')[1].trim();
  } else if (!selectCols.includes(fkColumn)) {
    selectCols.push(fkColumn);
    fkPushedAsRaw = true;
  }

  const rows = await chunkedFetch(parentIds, (chunk) =>
    knex(desc.targetTable)
      .select(selectCols)
      .whereIn(fkColumn, chunk)
      .orderBy(pkCol, 'asc'),
  );

  const grouped = new Map<any, any[]>();
  for (const row of rows) {
    const key = row[groupKey];
    if (fkPushedAsRaw) {
      delete row[fkColumn];
    }
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

  const isPkOnly = selectCols.length === 1 && selectCols[0] === pkCol && nestedDescs.length === 0;
  if (isPkOnly) {
    const junctionRows = await chunkedFetch(parentIds, (chunk) =>
      knex(junctionTable)
        .select([`${sourceCol} as __sourceId__`, `${targetCol} as __targetId__`])
        .whereIn(sourceCol, chunk)
        .orderBy(targetCol, 'asc'),
    );

    const grouped = new Map<any, any[]>();
    for (const row of junctionRows) {
      const key = row.__sourceId__;
      if (!grouped.has(key)) grouped.set(key, []);
      grouped.get(key)!.push({ [pkCol]: row.__targetId__ });
    }

    for (const parentRow of parentRows) {
      parentRow[desc.relationName] = grouped.get(parentRow[parentPk]) || [];
    }
    return;
  }

  const targetSelectCols = selectCols.map((c) => {
    const asIdx = c.indexOf(' as ');
    if (asIdx !== -1) {
      const col = c.substring(0, asIdx);
      const alias = c.substring(asIdx + 4);
      return `t.${col} as ${alias}`;
    }
    return `t.${c} as ${c}`;
  });

  const rows = await chunkedFetch(parentIds, (chunk) =>
    knex(junctionTable)
      .join(`${desc.targetTable} as t`, `${junctionTable}.${targetCol}`, `t.${pkCol}`)
      .select([`${junctionTable}.${sourceCol} as __sourceId__`, ...targetSelectCols])
      .whereIn(`${junctionTable}.${sourceCol}`, chunk)
      .orderBy(`t.${pkCol}`, 'asc'),
  );

  const grouped = new Map<any, any[]>();
  for (const row of rows) {
    const sourceId = row.__sourceId__;
    delete row.__sourceId__;
    if (!grouped.has(sourceId)) grouped.set(sourceId, []);
    grouped.get(sourceId)!.push(row);
  }

  if (nestedDescs.length > 0) {
    await executeBatchFetches(knex, rows, nestedDescs, metadataGetter, maxDepth, currentDepth + 1, desc.targetTable);
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
