import { isDeepStrictEqual } from 'node:util';
import type {
  RuntimeSchemaDiff,
  RuntimeSchemaLogicalChange,
  RuntimeSchemaOperation,
  RuntimeSchemaCascadeWarning,
} from '../types/runtime-schema-mutation.types';
import {
  type NormalizedRuntimeTableSchema,
  runtimeRelationDiffKey,
} from './runtime-schema-normalization.util';

export function buildRuntimeSchemaChangePlan(input: {
  operation: RuntimeSchemaOperation;
  tableName: string;
  before: NormalizedRuntimeTableSchema | null;
  after: NormalizedRuntimeTableSchema | null;
  owningSideInverseCascadeWarnings: readonly RuntimeSchemaCascadeWarning[];
}): {
  changes: readonly RuntimeSchemaLogicalChange[];
  diff: RuntimeSchemaDiff;
} {
  const before = input.before;
  const after = input.after;
  if (input.operation === 'create') {
    const tableName = after?.contract.name || input.tableName;
    const change: RuntimeSchemaLogicalChange = {
      id: `runtime:create-table:${tableName}`,
      kind: 'create-table',
      label: `create table ${tableName}`,
      after: after?.contract ?? null,
    };
    return {
      changes: [change],
      diff: emptyDiff(input.operation, tableName, true),
    };
  }
  if (input.operation === 'delete') {
    const tableName = before?.contract.name || input.tableName;
    const change: RuntimeSchemaLogicalChange = {
      id: `runtime:delete-table:${tableName}`,
      kind: 'delete-table',
      label: `delete table ${tableName}`,
      before: before?.contract ?? null,
    };
    return {
      changes: [change],
      diff: {
        ...emptyDiff(input.operation, tableName, true),
        isDestructive: true,
      },
    };
  }
  if (!before || !after) {
    return {
      changes: [],
      diff: emptyDiff(input.operation, input.tableName, true),
    };
  }

  const changes: RuntimeSchemaLogicalChange[] = [];
  const addChange = (
    kind: RuntimeSchemaLogicalChange['kind'],
    key: string,
    label: string,
    beforeValue?: unknown,
    afterValue?: unknown,
  ) => {
    changes.push({
      id: `runtime:${kind}:${input.tableName}:${key}`,
      kind,
      label,
      ...(beforeValue === undefined ? {} : { before: beforeValue }),
      ...(afterValue === undefined ? {} : { after: afterValue }),
    });
  };

  const tableMetadataFields = [
    'description',
    'alias',
    'isSingleRecord',
    'graphqlEnabled',
    'validateBody',
  ] as const;
  const changedTableMetadataFields = tableMetadataFields.filter((field) =>
    !isDeepStrictEqual(before.contract[field], after.contract[field]),
  );
  if (changedTableMetadataFields.length > 0) {
    addChange(
      'alter-table-metadata',
      changedTableMetadataFields.join(','),
      `update table metadata ${changedTableMetadataFields.join(', ')}`,
      Object.fromEntries(
        changedTableMetadataFields.map((field) => [field, before.contract[field]]),
      ),
      Object.fromEntries(
        changedTableMetadataFields.map((field) => [field, after.contract[field]]),
      ),
    );
  }

  if (before.contract.name !== after.contract.name) {
    addChange(
      'rename-table',
      `${before.contract.name}->${after.contract.name}`,
      `rename table ${before.contract.name}->${after.contract.name}`,
      before.contract.name,
      after.contract.name,
    );
  }

  const beforeColumns = before.keyedColumns;
  const afterColumns = after.keyedColumns;
  const removedColumns = beforeColumns
    .filter((column) => !afterColumns.some((other) => other.key === column.key))
    .map((column) => column.name);
  const addedColumns = afterColumns
    .filter(
      (column) => !beforeColumns.some((other) => other.key === column.key),
    )
    .map((column) => column.name);
  const renamedColumns = afterColumns.flatMap((column) => {
    const previous = beforeColumns.find((other) => other.key === column.key);
    return previous && previous.name !== column.name
      ? [{ from: previous.name, to: column.name }]
      : [];
  });
  const changedColumns = afterColumns
    .filter((column) => {
      const previous = beforeColumns.find((other) => other.key === column.key);
      if (!previous) return false;
      return !isDeepStrictEqual(
        { ...previous, key: '', name: '' },
        { ...column, key: '', name: '' },
      );
    })
    .map((column) => column.name);

  for (const name of removedColumns) {
    addChange('remove-column', name, `remove column ${name}`, name);
  }
  for (const name of addedColumns) {
    const column = afterColumns.find((entry) => entry.name === name);
    addChange(
      'add-column',
      name,
      `add column ${name}`,
      undefined,
      stripColumnKey(column),
    );
  }
  for (const rename of renamedColumns) {
    addChange(
      'rename-column',
      `${rename.from}->${rename.to}`,
      `rename column ${rename.from}->${rename.to}`,
      rename.from,
      rename.to,
    );
  }
  for (const name of changedColumns) {
    addChange(
      'alter-column',
      name,
      `alter column ${name}`,
      stripColumnKey(
        beforeColumns.find(
          (entry) =>
            entry.key ===
            afterColumns.find((candidate) => candidate.name === name)?.key,
        ),
      ),
      stripColumnKey(afterColumns.find((entry) => entry.name === name)),
    );
  }

  const beforeRelationByKey = new Map(
    before.contract.relations.map((relation) => [
      runtimeRelationDiffKey(relation),
      relation,
    ]),
  );
  const afterRelationByKey = new Map(
    after.contract.relations.map((relation) => [
      runtimeRelationDiffKey(relation),
      relation,
    ]),
  );
  const removedRelations = [...beforeRelationByKey.keys()].filter(
    (key) => !afterRelationByKey.has(key),
  );
  const addedRelations = [...afterRelationByKey.keys()].filter(
    (key) => !beforeRelationByKey.has(key),
  );
  for (const key of removedRelations) {
    const relation = beforeRelationByKey.get(key)!;
    addChange(
      'remove-relation',
      key,
      `remove relation ${relation.propertyName}`,
      relation,
    );
  }
  for (const key of addedRelations) {
    const relation = afterRelationByKey.get(key)!;
    addChange(
      'add-relation',
      key,
      `add relation ${relation.propertyName}`,
      undefined,
      relation,
    );
  }
  const commonRelationKeys = [...beforeRelationByKey.keys()].filter((key) =>
    afterRelationByKey.has(key),
  );
  for (const key of commonRelationKeys) {
    const beforeRel = beforeRelationByKey.get(key)!;
    const afterRel = afterRelationByKey.get(key)!;
    if (!isDeepStrictEqual(beforeRel, afterRel)) {
      addChange(
        'alter-relation',
        key,
        `alter relation ${afterRel.propertyName}`,
        beforeRel,
        afterRel,
      );
    }
  }

  const constraintDiffs = [
    ['unique', before.contract.uniques, after.contract.uniques],
    ['index', before.contract.indexes, after.contract.indexes],
  ] as const;
  const constraintChanges: Record<string, string[]> = {
    removedUniques: [],
    addedUniques: [],
    removedIndexes: [],
    addedIndexes: [],
  };
  for (const [kind, beforeValue, afterValue] of constraintDiffs) {
    const beforeKeys = jsonArrayKeys(beforeValue);
    const afterKeys = jsonArrayKeys(afterValue);
    const removed = beforeKeys.filter((key) => !afterKeys.includes(key));
    const added = afterKeys.filter((key) => !beforeKeys.includes(key));
    const suffix = kind === 'unique' ? 'Uniques' : 'Indexes';
    constraintChanges[`removed${suffix}`] = removed;
    constraintChanges[`added${suffix}`] = added;
    for (const key of removed) {
      addChange(
        kind === 'unique' ? 'remove-unique' : 'remove-index',
        key,
        `remove ${kind} ${key}`,
        JSON.parse(key),
      );
    }
    for (const key of added) {
      addChange(
        kind === 'unique' ? 'add-unique' : 'add-index',
        key,
        `add ${kind} ${key}`,
        undefined,
        JSON.parse(key),
      );
    }
  }

  return {
    changes,
    diff: {
      tableName: input.tableName,
      operation: input.operation,
      schemaChanged: changes.some(
        (change) => change.kind !== 'alter-table-metadata',
      ),
      isDestructive: removedColumns.length > 0 || removedRelations.length > 0,
      removedColumns,
      addedColumns,
      renamedColumns,
      changedColumns,
      removedRelations,
      addedRelations,
      removedUniques: constraintChanges.removedUniques,
      addedUniques: constraintChanges.addedUniques,
      removedIndexes: constraintChanges.removedIndexes,
      addedIndexes: constraintChanges.addedIndexes,
      owningSideInverseCascadeWarnings: input.owningSideInverseCascadeWarnings,
    },
  };
}

function emptyDiff(
  operation: RuntimeSchemaOperation,
  tableName: string,
  schemaChanged: boolean,
): RuntimeSchemaDiff {
  return {
    tableName,
    operation,
    schemaChanged,
    isDestructive: false,
    removedColumns: [],
    addedColumns: [],
    renamedColumns: [],
    changedColumns: [],
    removedRelations: [],
    addedRelations: [],
    removedUniques: [],
    addedUniques: [],
    removedIndexes: [],
    addedIndexes: [],
    owningSideInverseCascadeWarnings: [],
  };
}

function jsonArrayKeys(value: unknown): string[] {
  return Array.isArray(value)
    ? value.map((entry) => JSON.stringify(entry))
    : [];
}

function stripColumnKey(
  column: NormalizedRuntimeTableSchema['keyedColumns'][number] | undefined,
): unknown {
  if (!column) return null;
  const { key: _key, ...contract } = column;
  return contract;
}
