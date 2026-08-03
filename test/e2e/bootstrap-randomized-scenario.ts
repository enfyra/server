import { bootstrapSourceArtifacts } from '../../src/data';
import type { BootstrapSourceArtifacts } from '../../src/engines/bootstrap/types/bootstrap-definition.types';
import type { SnapshotColumnDefinition } from '../../src/engines/bootstrap/types/snapshot-definition.types';
import type { SchemaMigrationDef } from '../../src/shared/types/schema-migration.types';
import type {
  BootstrapRandomizedOperation,
  RandomizedBootstrapScenario,
} from './types/bootstrap-randomized-matrix.types';

function createRng(seed: number): () => number {
  let value = seed >>> 0;
  return () => {
    value += 0x6d2b79f5;
    let next = value;
    next = Math.imul(next ^ (next >>> 15), next | 1);
    next ^= next + Math.imul(next ^ (next >>> 7), next | 61);
    return ((next ^ (next >>> 14)) >>> 0) / 0x100000000;
  };
}

function pick<T>(rng: () => number, values: readonly T[]): T {
  return values[Math.floor(rng() * values.length)] as T;
}

function cloneArtifacts(): BootstrapSourceArtifacts {
  return structuredClone(bootstrapSourceArtifacts);
}

function idColumn(): SnapshotColumnDefinition {
  return {
    name: 'id',
    type: 'int',
    isPrimary: true,
    isGenerated: true,
    isNullable: false,
    isSystem: true,
  };
}

function systemColumn(
  name: string,
  type: SnapshotColumnDefinition['type'],
  options: Partial<SnapshotColumnDefinition> = {},
): SnapshotColumnDefinition {
  return {
    name,
    type,
    isNullable: true,
    isSystem: true,
    ...options,
  };
}

export function createRandomizedBootstrapScenario(
  seed: number,
): RandomizedBootstrapScenario {
  if (!Number.isSafeInteger(seed)) {
    throw new Error('Bootstrap matrix seed must be a safe integer');
  }

  const rng = createRng(seed);
  const suffix = `${(seed >>> 0).toString(36)}_${Math.floor(rng() * 0xffffff)
    .toString(36)
    .padStart(4, '0')}`;
  const prefix = `enfyra_matrix_${suffix}`;
  const sourceParentTable = `${prefix}_legacy`;
  const targetParentTable = `${prefix}_account`;
  const childTable = `${prefix}_item`;
  const droppedTable = `${prefix}_retired`;
  const addedTable = `${prefix}_audit`;
  const renamedColumn = {
    from: `legacy_${pick(rng, ['label', 'title', 'name'])}`,
    to: pick(rng, ['label', 'title', 'displayName']),
  };
  const modifiedColumn = pick(rng, ['revision', 'sequence', 'counter']);
  const removedParentColumn = pick(rng, ['obsoleteText', 'legacyNote']);
  const removedChildColumn = pick(rng, ['obsoleteFlag', 'legacyFlag']);
  const addedColumn = pick(rng, ['statusText', 'summaryText', 'releaseNote']);
  const renamedRelation = {
    from: pick(rng, ['legacyOwner', 'legacyAccount']),
    to: pick(rng, ['owner', 'account']),
  };
  const removedRelation = pick(rng, ['temporaryOwner', 'legacyReviewer']);
  const addedRelation = pick(rng, ['audit', 'changeAudit']);
  const counterDefault = 10 + Math.floor(rng() * 90);
  const extraColumnCount = 1 + Math.floor(rng() * 3);
  const extraColumns = Array.from({ length: extraColumnCount }, (_, index) =>
    systemColumn(`extra_${index}_${Math.floor(rng() * 1000)}`, 'varchar', {
      defaultValue: `extra-${seed}-${index}`,
    }),
  );
  const expectedDescription = `matrix-label-${seed}-${Math.floor(rng() * 10000)}`;
  const sentinel = {
    parentLabel: `parent-${seed}-${Math.floor(rng() * 100000)}`,
    parentCounter: 100 + Math.floor(rng() * 10000),
    childPayload: `child-${seed}-${Math.floor(rng() * 100000)}`,
    retiredValue: `retired-${seed}-${Math.floor(rng() * 100000)}`,
  };

  const source = cloneArtifacts();
  source.migration = null;
  source.snapshot[sourceParentTable] = {
    name: sourceParentTable,
    description: `Randomized source parent ${seed}`,
    isSystem: true,
    uniques: [],
    indexes: [[renamedColumn.from]],
    columns: [
      idColumn(),
      systemColumn(renamedColumn.from, 'varchar', {
        isNullable: false,
        defaultValue: `source-${seed}`,
        description: `source-label-${seed}`,
      }),
      systemColumn(modifiedColumn, 'int', { defaultValue: null }),
      systemColumn(removedParentColumn, 'text'),
    ],
    relations: [],
  };
  source.snapshot[childTable] = {
    name: childTable,
    description: `Randomized source child ${seed}`,
    isSystem: true,
    columns: [
      idColumn(),
      systemColumn('payload', 'varchar'),
      systemColumn(removedChildColumn, 'boolean', { defaultValue: false }),
    ],
    relations: [
      {
        propertyName: renamedRelation.from,
        type: 'many-to-one',
        targetTable: sourceParentTable,
        isNullable: true,
        isSystem: true,
      },
      {
        propertyName: removedRelation,
        type: 'many-to-one',
        targetTable: sourceParentTable,
        isNullable: true,
        isSystem: true,
      },
    ],
  };
  source.snapshot[droppedTable] = {
    name: droppedTable,
    description: `Randomized retired table ${seed}`,
    isSystem: true,
    columns: [idColumn(), systemColumn('value', 'varchar')],
    relations: [],
  };

  const target = cloneArtifacts();
  target.snapshot[targetParentTable] = {
    name: targetParentTable,
    description: `Randomized target parent ${seed}`,
    isSystem: true,
    uniques: [[renamedColumn.to]],
    indexes: [[renamedColumn.to], [addedColumn]],
    columns: [
      idColumn(),
      systemColumn(renamedColumn.to, 'varchar', {
        isNullable: false,
        defaultValue: `source-${seed}`,
        description: expectedDescription,
      }),
      systemColumn(modifiedColumn, 'bigint', {
        isNullable: false,
        defaultValue: counterDefault,
      }),
      systemColumn(addedColumn, 'varchar', {
        isNullable: false,
        defaultValue: `added-${seed}`,
      }),
      ...extraColumns,
    ],
    relations: [],
  };
  target.snapshot[addedTable] = {
    name: addedTable,
    description: `Randomized added audit table ${seed}`,
    isSystem: true,
    columns: [
      idColumn(),
      systemColumn('message', 'text'),
      systemColumn('sequence', 'int', { defaultValue: seed % 1000 }),
    ],
    relations: [],
  };
  target.snapshot[childTable] = {
    name: childTable,
    description: `Randomized target child ${seed}`,
    isSystem: true,
    columns: [
      idColumn(),
      systemColumn('payload', 'varchar'),
      systemColumn('addedNote', 'varchar', { defaultValue: `note-${seed}` }),
    ],
    relations: [
      {
        propertyName: renamedRelation.to,
        type: 'many-to-one',
        targetTable: targetParentTable,
        isNullable: true,
        isSystem: true,
      },
      {
        propertyName: addedRelation,
        type: 'many-to-one',
        targetTable: addedTable,
        isNullable: true,
        isSystem: true,
      },
    ],
  };

  const migration: SchemaMigrationDef = {
    tablesToRename: [{ from: sourceParentTable, to: targetParentTable }],
    tablesToDrop: [droppedTable],
    tables: [
      {
        _unique: { name: { _eq: targetParentTable } },
        tableToModify: {
          from: {
            description: `Randomized source parent ${seed}`,
            uniques: [],
            indexes: [[renamedColumn.from]],
          },
          to: {
            description: `Randomized target parent ${seed}`,
            uniques: [[renamedColumn.to]],
            indexes: [[renamedColumn.to], [addedColumn]],
          },
        },
        columnsToModify: [
          {
            from: {
              name: renamedColumn.from,
              description: `source-label-${seed}`,
            },
            to: {
              name: renamedColumn.to,
              description: expectedDescription,
            },
          },
          {
            from: {
              name: modifiedColumn,
              type: 'int',
              isNullable: true,
              defaultValue: null,
            },
            to: {
              name: modifiedColumn,
              type: 'bigint',
              isNullable: false,
              defaultValue: counterDefault,
            },
          },
        ],
        columnsToRemove: [removedParentColumn],
      },
      {
        _unique: { name: { _eq: childTable } },
        columnsToRemove: [removedChildColumn],
        relationsToModify: [
          {
            from: {
              propertyName: renamedRelation.from,
              targetTable: sourceParentTable,
            },
            to: {
              propertyName: renamedRelation.to,
              targetTable: targetParentTable,
            },
          },
        ],
        relationsToRemove: [removedRelation],
      },
    ],
  };
  target.migration = migration;

  const operations: BootstrapRandomizedOperation[] = [
    {
      kind: 'table-rename',
      table: targetParentTable,
      from: sourceParentTable,
      to: targetParentTable,
    },
    { kind: 'table-drop', table: droppedTable },
    { kind: 'table-add', table: addedTable },
    {
      kind: 'column-rename',
      table: targetParentTable,
      from: renamedColumn.from,
      to: renamedColumn.to,
    },
    {
      kind: 'column-modify',
      table: targetParentTable,
      from: modifiedColumn,
      to: modifiedColumn,
    },
    {
      kind: 'column-remove',
      table: targetParentTable,
      from: removedParentColumn,
    },
    { kind: 'column-remove', table: childTable, from: removedChildColumn },
    { kind: 'column-add', table: targetParentTable, to: addedColumn },
    {
      kind: 'relation-rename',
      table: childTable,
      from: renamedRelation.from,
      to: renamedRelation.to,
    },
    { kind: 'relation-remove', table: childTable, from: removedRelation },
    { kind: 'relation-add', table: childTable, to: addedRelation },
    { kind: 'index-modify', table: targetParentTable },
    { kind: 'unique-modify', table: targetParentTable },
  ];

  return {
    seed,
    prefix,
    source,
    target,
    operations,
    assertions: {
      sourceParentTable,
      targetParentTable,
      childTable,
      droppedTable,
      addedTable,
      renamedColumn,
      modifiedColumn,
      removedParentColumn,
      removedChildColumn,
      addedColumn,
      renamedRelation,
      removedRelation,
      addedRelation,
      healing: {
        table: targetParentTable,
        metadataColumn: renamedColumn.to,
        expectedDescription,
        physicalColumn: addedColumn,
        indexColumns: [addedColumn],
      },
      sentinel,
    },
  };
}
