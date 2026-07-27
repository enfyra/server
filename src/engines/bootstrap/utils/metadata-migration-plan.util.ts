import type { SchemaMigrationDef } from '../../../shared/types/schema-migration.types';
import type {
  BootstrapSchemaExecutionPlan,
  BootstrapSchemaOperation,
} from '../types';
import {
  getValidTableRenames,
  hasColumnMetadataChanges,
  hasRelationMetadataChanges,
  hasTableMetadataChanges,
} from './metadata-diff.util';
import {
  buildBootstrapExecutionPhases,
  type UnphasedBootstrapSchemaExecutionNode,
} from './bootstrap-execution-phases.util';

export interface MetadataMigrationPlanContext {
  mode: BootstrapSchemaExecutionPlan['mode'];
  database: BootstrapSchemaExecutionPlan['database'];
  targetTableCount: number;
  observedMetadata: BootstrapSchemaExecutionPlan['observedMetadata'];
}

export function compileMetadataMigrationExecutionPlan(
  migration: SchemaMigrationDef | null,
  context: MetadataMigrationPlanContext,
): BootstrapSchemaExecutionPlan {
  const operations =
    context.mode === 'upgrade'
      ? compileOperations(migration)
      : Object.freeze([] as BootstrapSchemaOperation[]);
  const phases = compileExecutionPhases(operations, context.database);
  return Object.freeze({
    ...context,
    observedMetadata: Object.freeze({ ...context.observedMetadata }),
    operations,
    phases,
  });
}

function compileOperations(
  migration: SchemaMigrationDef | null,
): readonly BootstrapSchemaOperation[] {
  const operations: BootstrapSchemaOperation[] = [];
  const add = (operation: BootstrapSchemaOperation) => {
    operations.push(Object.freeze(operation));
  };

  getValidTableRenames(migration?.coreTablesToRename ?? []).forEach(
    (rename, index) =>
      add({
        id: `schema:rename-core-table:${index}:${rename.from}->${rename.to}`,
        label: `rename core table ${rename.from}->${rename.to}`,
        kind: 'rename-core-table',
        rename,
      }),
  );
  getValidTableRenames(migration?.tablesToRename ?? []).forEach(
    (rename, index) =>
      add({
        id: `schema:rename-table:${index}:${rename.from}->${rename.to}`,
        label: `rename table ${rename.from}->${rename.to}`,
        kind: 'rename-table',
        rename,
      }),
  );
  getValidTableRenames(migration?.physicalTablesToRename ?? []).forEach(
    (rename, index) =>
      add({
        id: `schema:rename-physical-table:${index}:${rename.from}->${rename.to}`,
        label: `rename physical table ${rename.from}->${rename.to}`,
        kind: 'rename-physical-table',
        rename,
      }),
  );
  (migration?.physicalTablesToDrop ?? []).forEach((tableName, index) =>
    add({
      id: `schema:drop-physical-table:${index}:${tableName}`,
      label: `drop physical table ${tableName}`,
      kind: 'drop-physical-table',
      tableName,
    }),
  );
  (migration?.tablesToDrop ?? []).forEach((tableName, index) =>
    add({
      id: `schema:drop-table:${index}:${tableName}`,
      label: `drop table ${tableName}`,
      kind: 'drop-table',
      tableName,
    }),
  );
  for (const [tableIndex, tableMigration] of (
    migration?.tables ?? []
  ).entries()) {
    const tableName = tableMigration._unique.name._eq;
    if (
      tableMigration.tableToModify &&
      hasTableMetadataChanges(tableMigration.tableToModify)
    ) {
      add({
        id: `schema:modify-table:${tableIndex}:${tableName}`,
        label: `modify table ${tableName}`,
        kind: 'modify-table',
        tableName,
        modification: tableMigration.tableToModify,
      });
    }
    (tableMigration.columnsToModify ?? []).forEach((modification, index) => {
      if (!hasColumnMetadataChanges(modification)) return;
      add({
        id: `schema:modify-column:${tableIndex}:${index}:${tableName}.${modification.from.name}`,
        label: `modify column ${tableName}.${modification.from.name}`,
        kind: 'modify-column',
        tableName,
        modification,
      });
    });
    (tableMigration.columnsToRemove ?? []).forEach((columnName, index) =>
      add({
        id: `schema:remove-column:${tableIndex}:${index}:${tableName}.${columnName}`,
        label: `remove column ${tableName}.${columnName}`,
        kind: 'remove-column',
        tableName,
        columnName,
      }),
    );
    (tableMigration.relationsToModify ?? []).forEach((modification, index) => {
      if (!hasRelationMetadataChanges(modification)) return;
      add({
        id: `schema:modify-relation:${tableIndex}:${index}:${tableName}.${modification.from.propertyName}`,
        label: `modify relation ${tableName}.${modification.from.propertyName}`,
        kind: 'modify-relation',
        tableName,
        modification,
      });
    });
    (tableMigration.relationsToRemove ?? []).forEach((propertyName, index) =>
      add({
        id: `schema:remove-relation:${tableIndex}:${index}:${tableName}.${propertyName}`,
        label: `remove relation ${tableName}.${propertyName}`,
        kind: 'remove-relation',
        tableName,
        propertyName,
      }),
    );
  }

  return Object.freeze(operations);
}

function compileExecutionPhases(
  operations: readonly BootstrapSchemaOperation[],
  backend: BootstrapSchemaExecutionPlan['database'],
): BootstrapSchemaExecutionPlan['phases'] {
  const nodes: UnphasedBootstrapSchemaExecutionNode[] = [];
  const addNode = (
    operation: BootstrapSchemaOperation,
    commandKind: UnphasedBootstrapSchemaExecutionNode['command']['kind'],
    dependsOn: readonly string[],
    checkpoint: UnphasedBootstrapSchemaExecutionNode['checkpoint'],
    completesChange: boolean,
  ): string => {
    const id = `${operation.id}:command:${commandKind}`;
    nodes.push({
      id,
      changeId: operation.id,
      dependsOn: [...dependsOn],
      checkpoint,
      completesChange,
      command: { backend, kind: commandKind, operation },
    });
    return id;
  };

  let renameBarrier: string[] = [];
  for (const operation of operations) {
    if (operation.kind !== 'rename-core-table') continue;
    renameBarrier = [
      addNode(operation, 'rename-core-table', renameBarrier, 'core', false),
    ];
  }
  for (const operation of operations) {
    if (operation.kind !== 'rename-table') continue;
    renameBarrier = [
      addNode(operation, 'rename-table', renameBarrier, 'remaining', false),
    ];
  }

  const physicalDirectNodeIds: string[] = [];
  for (const operation of operations) {
    if (operation.kind === 'rename-physical-table') {
      physicalDirectNodeIds.push(
        addNode(
          operation,
          'rename-physical-table',
          renameBarrier,
          'remaining',
          true,
        ),
      );
    } else if (operation.kind === 'drop-physical-table') {
      physicalDirectNodeIds.push(
        addNode(
          operation,
          'drop-physical-table',
          renameBarrier,
          'remaining',
          true,
        ),
      );
    }
  }

  const physicalTableDropNodeIds: string[] = [];
  for (const operation of operations) {
    if (operation.kind !== 'drop-table') continue;
    physicalTableDropNodeIds.push(
      addNode(
        operation,
        'apply-physical-change',
        [...renameBarrier, ...physicalDirectNodeIds],
        'remaining',
        false,
      ),
    );
  }

  const physicalChangeNodeIds = [...physicalTableDropNodeIds];
  const previousPhysicalByTable = new Map<string, string>();
  for (const operation of operations) {
    if (
      operation.kind !== 'modify-column' &&
      operation.kind !== 'remove-column' &&
      operation.kind !== 'modify-relation' &&
      operation.kind !== 'remove-relation'
    ) {
      continue;
    }
    const previous = previousPhysicalByTable.get(operation.tableName);
    const nodeId = addNode(
      operation,
      'apply-physical-change',
      [
        ...renameBarrier,
        ...physicalDirectNodeIds,
        ...physicalTableDropNodeIds,
        ...(previous ? [previous] : []),
      ],
      'remaining',
      false,
    );
    previousPhysicalByTable.set(operation.tableName, nodeId);
    physicalChangeNodeIds.push(nodeId);
  }

  const physicalBarrier = [
    ...renameBarrier,
    ...physicalDirectNodeIds,
    ...physicalChangeNodeIds,
  ];
  const metadataTableDropNodeIds: string[] = [];
  for (const operation of operations) {
    if (operation.kind !== 'drop-table') continue;
    metadataTableDropNodeIds.push(
      addNode(
        operation,
        'apply-metadata-change',
        physicalBarrier,
        'remaining',
        true,
      ),
    );
  }

  const metadataChangeNodeIds = [...metadataTableDropNodeIds];
  const previousMetadataByTable = new Map<string, string>();
  for (const operation of operations) {
    if (
      operation.kind !== 'modify-table' &&
      operation.kind !== 'modify-column' &&
      operation.kind !== 'remove-column' &&
      operation.kind !== 'modify-relation' &&
      operation.kind !== 'remove-relation'
    ) {
      continue;
    }
    const previous = previousMetadataByTable.get(operation.tableName);
    const nodeId = addNode(
      operation,
      'apply-metadata-change',
      [
        ...physicalBarrier,
        ...metadataTableDropNodeIds,
        ...(previous ? [previous] : []),
      ],
      'remaining',
      true,
    );
    previousMetadataByTable.set(operation.tableName, nodeId);
    metadataChangeNodeIds.push(nodeId);
  }

  let cleanupBarrier = [...physicalBarrier, ...metadataChangeNodeIds];
  for (const operation of [...operations].reverse()) {
    if (
      operation.kind === 'rename-core-table' ||
      operation.kind === 'rename-table'
    ) {
      cleanupBarrier = [
        addNode(
          operation,
          'cleanup-renamed-table',
          cleanupBarrier,
          'remaining',
          true,
        ),
      ];
    }
  }

  const phases = buildBootstrapExecutionPhases(nodes);
  const completionCount = new Map<string, number>();
  for (const phase of phases) {
    for (const node of phase.nodes) {
      if (!node.completesChange) continue;
      completionCount.set(
        node.changeId,
        (completionCount.get(node.changeId) ?? 0) + 1,
      );
    }
  }
  const invalid = operations.filter(
    (operation) => completionCount.get(operation.id) !== 1,
  );
  if (invalid.length > 0) {
    throw new Error(
      `Bootstrap execution plan must complete each change exactly once: ${invalid
        .map((operation) => operation.id)
        .join(', ')}.`,
    );
  }
  return phases;
}
