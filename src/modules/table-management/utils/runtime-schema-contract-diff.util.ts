import type {
  RuntimeSchemaColumnContract,
  RuntimeSchemaRelationContract,
  RuntimeTableSchemaContract,
} from '../types/runtime-schema-mutation.types';

const VALUE_CAP = 100;
const ITEM_CAP = 8;

export interface RuntimeSchemaContractDiff {
  sections: string[];
}

export function formatRuntimeSchemaContractDiff(
  expected: RuntimeTableSchemaContract,
  current: RuntimeTableSchemaContract,
): string {
  const parts: string[] = [];

  const scalarKeys = [
    'name',
    'description',
    'alias',
    'isSingleRecord',
    'graphqlEnabled',
    'validateBody',
  ] as const;

  const scalarParts: string[] = [];
  for (const key of scalarKeys) {
    const left = capString(expected[key]);
    const right = capString(current[key]);
    if (left !== right) {
      scalarParts.push(`${key}:${left}->${right}`);
    }
  }
  if (scalarParts.length > 0) {
    parts.push(`scalars[${scalarParts.join(',')}]`);
  }

  const columnParts = diffColumns(expected.columns, current.columns);
  if (columnParts.length > 0) {
    parts.push(`columns[${columnParts.join(',')}]`);
  }

  const relationParts = diffRelations(expected.relations, current.relations);
  if (relationParts.length > 0) {
    parts.push(`relations[${relationParts.join(',')}]`);
  }

  const constraintParts = diffConstraints(
    'uniques',
    expected.uniques,
    current.uniques,
  );
  if (constraintParts.length > 0) {
    parts.push(`uniques[${constraintParts.join(',')}]`);
  }

  const indexParts = diffConstraints('indexes', expected.indexes, current.indexes);
  if (indexParts.length > 0) {
    parts.push(`indexes[${indexParts.join(',')}]`);
  }

  return parts.length > 0 ? parts.join(' ') : 'no-diff-detected';
}

function diffColumns(
  expected: readonly RuntimeSchemaColumnContract[],
  current: readonly RuntimeSchemaColumnContract[],
): string[] {
  const byName = new Map<string, RuntimeSchemaColumnContract>();
  for (const column of current) byName.set(column.name, column);

  const parts: string[] = [];
  const seen = new Set<string>();
  let count = 0;

  for (const col of expected) {
    seen.add(col.name);
    const cur = byName.get(col.name);
    if (!cur) {
      parts.push(`missing:${col.name}`);
      if (++count >= ITEM_CAP) return parts;
      continue;
    }
    const scalarKeys = [
      'type',
      'isNullable',
      'isPrimary',
      'isGenerated',
      'defaultValue',
      'description',
      'values',
      'isUnique',
      'isPublished',
      'isUpdatable',
      'isEncrypted',
      'isIndex',
      'options',
      'metadata',
      'placeholder',
    ] as const;
    const diffs: string[] = [];
    for (const key of scalarKeys) {
      const left = capString(col[key]);
      const right = capString(cur[key]);
      if (left !== right) diffs.push(`${key}:${left}->${right}`);
    }
    if (diffs.length > 0) {
      parts.push(`${col.name}(${diffs.join(',')})`);
      if (++count >= ITEM_CAP) return parts;
    }
  }

  if (count >= ITEM_CAP) return parts;
  for (const col of current) {
    if (seen.has(col.name)) continue;
    parts.push(`unexpected:${col.name}`);
    if (++count >= ITEM_CAP) return parts;
  }
  return parts;
}

function diffRelations(
  expected: readonly RuntimeSchemaRelationContract[],
  current: readonly RuntimeSchemaRelationContract[],
): string[] {
  const byKey = new Map<string, RuntimeSchemaRelationContract>();
  for (const relation of current) {
    byKey.set(relationIdentityKey(relation), relation);
  }

  const parts: string[] = [];
  const seen = new Set<string>();
  let count = 0;

  for (const rel of expected) {
    const key = relationIdentityKey(rel);
    seen.add(key);
    const cur = byKey.get(key);
    if (!cur) {
      parts.push(`missing:${rel.propertyName}->${rel.targetTableName}`);
      if (++count >= ITEM_CAP) return parts;
      continue;
    }
    const scalarKeys = [
      'propertyName',
      'type',
      'targetTableName',
      'mappedBy',
      'foreignKeyColumn',
      'junctionTableName',
      'isNullable',
      'onDelete',
      'inversePropertyName',
      'description',
      'isEager',
      'isInverseEager',
      'isPublished',
      'isUpdatable',
    ] as const;
    const diffs: string[] = [];
    for (const keyName of scalarKeys) {
      const left = capString(rel[keyName]);
      const right = capString(cur[keyName]);
      if (left !== right) diffs.push(`${keyName}:${left}->${right}`);
    }
    if (diffs.length > 0) {
      parts.push(`${rel.propertyName}(${diffs.join(',')})`);
      if (++count >= ITEM_CAP) return parts;
    }
  }

  if (count >= ITEM_CAP) return parts;
  for (const rel of current) {
    const key = relationIdentityKey(rel);
    if (seen.has(key)) continue;
    parts.push(`unexpected:${rel.propertyName}->${rel.targetTableName}`);
    if (++count >= ITEM_CAP) return parts;
  }
  return parts;
}

function relationIdentityKey(relation: RuntimeSchemaRelationContract): string {
  return `${relation.propertyName}|${relation.targetTableName}`;
}

function diffConstraints(
  _label: string,
  expected: unknown,
  current: unknown,
): string[] {
  const left = JSON.stringify(expected ?? null);
  const right = JSON.stringify(current ?? null);
  if (left === right) return [];
  return [capString(`${left} -> ${right}`)];
}

function capString(value: unknown): string {
  let text: string;
  if (value === null || value === undefined) {
    text = '';
  } else if (typeof value === 'string') {
    text = JSON.stringify(value);
  } else if (typeof value === 'object') {
    text = JSON.stringify(value);
  } else {
    text = String(value);
  }
  if (text.length <= VALUE_CAP) return text;
  return `${text.slice(0, VALUE_CAP)}...`;
}
