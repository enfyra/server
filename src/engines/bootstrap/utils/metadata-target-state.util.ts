import type {
  SchemaMigrationDef,
  SnapshotMigrationMetadataState,
} from '../../../shared/types/schema-migration.types';
import {
  COLUMN_DEFAULTS,
  COLUMN_FIELDS,
  RELATION_DEFAULTS,
  TABLE_DEFAULTS,
  TABLE_FIELDS,
  buildExpectedRelations,
  changedFields,
  duplicateValues,
  relationFieldsForTarget,
} from './metadata-comparison.util';

function validateSnapshotDefinition(
  snapshot: Record<string, any>,
  errors: string[],
): void {
  for (const [tableName, table] of Object.entries(snapshot)) {
    if (table?.name !== tableName) {
      errors.push(
        `snapshot table key ${tableName} disagrees with name ${String(table?.name)}`,
      );
    }

    for (const columnName of duplicateValues(
      (table?.columns ?? []).map((column: any) => column?.name),
    )) {
      errors.push(`duplicate snapshot column ${tableName}.${columnName}`);
    }

    for (const propertyName of duplicateValues(
      (table?.relations ?? []).map((relation: any) => relation?.propertyName),
    )) {
      errors.push(`duplicate snapshot relation ${tableName}.${propertyName}`);
    }

    for (const relation of table?.relations ?? []) {
      if (relation?.targetTable && !snapshot[relation.targetTable]) {
        errors.push(
          `relation ${tableName}.${String(relation.propertyName)} targets missing table ${relation.targetTable}`,
        );
      }
    }
  }
}

export function validateSnapshotTargetState(
  snapshot: Record<string, any>,
  state: SnapshotMigrationMetadataState,
  migration: SchemaMigrationDef | null = null,
  dataTargetSnapshot: Record<string, any> = snapshot,
): void {
  const errors: string[] = [];
  validateSnapshotDefinition(snapshot, errors);
  const tableMigrations = new Map(
    (migration?.tables ?? []).map((entry) => [entry._unique.name._eq, entry]),
  );
  const droppedTables = new Set(migration?.tablesToDrop ?? []);
  for (const tableName of duplicateValues(
    state.tables.map((table) => table.name),
  )) {
    errors.push(`duplicate table ${tableName}`);
  }
  const currentTables = new Map(
    state.tables.map((table) => [table.name, table]),
  );

  for (const [tableName, targetTable] of Object.entries(snapshot)) {
    const currentTable = currentTables.get(tableName);
    if (!currentTable) {
      errors.push(`table ${tableName} is missing`);
      continue;
    }
    const changed = changedFields(
      currentTable,
      targetTable,
      TABLE_FIELDS,
      TABLE_DEFAULTS,
    );
    const dataTargetChanged = changedFields(
      currentTable,
      dataTargetSnapshot[tableName] ?? targetTable,
      TABLE_FIELDS,
      TABLE_DEFAULTS,
    );
    if (changed.length > 0 && dataTargetChanged.length > 0) {
      errors.push(`table ${tableName} differs on ${changed.join(', ')}`);
    }

    const currentColumns = new Map(
      state.columns
        .filter((column) => column.tableName === tableName)
        .map((column) => [column.name, column]),
    );
    for (const columnName of duplicateValues(
      state.columns
        .filter((column) => column.tableName === tableName)
        .map((column) => column.name),
    )) {
      errors.push(`duplicate column ${tableName}.${columnName}`);
    }
    const targetColumns = new Map(
      ((targetTable as any).columns ?? []).map((column: any) => [
        column.name,
        column,
      ]),
    );
    const dataTargetColumns = new Map(
      (dataTargetSnapshot[tableName]?.columns ?? []).map((column: any) => [
        column.name,
        column,
      ]),
    );
    for (const [columnName, targetColumn] of targetColumns) {
      const currentColumn = currentColumns.get(columnName);
      if (!currentColumn) {
        errors.push(`column ${tableName}.${columnName} is missing`);
        continue;
      }
      const columnChanges = changedFields(
        currentColumn,
        targetColumn as Record<string, any>,
        COLUMN_FIELDS,
        COLUMN_DEFAULTS,
      );
      const dataTargetColumn =
        dataTargetColumns.get(columnName) ?? targetColumn;
      const dataTargetChanges = changedFields(
        currentColumn,
        dataTargetColumn as Record<string, any>,
        COLUMN_FIELDS,
        COLUMN_DEFAULTS,
      );
      if (columnChanges.length > 0 && dataTargetChanges.length > 0) {
        errors.push(
          `column ${tableName}.${columnName} differs on ${columnChanges.join(', ')}`,
        );
      }
    }
    for (const columnName of currentColumns.keys()) {
      const currentColumn = currentColumns.get(columnName);
      const tableMigration = tableMigrations.get(tableName);
      const explicitlyRemoved =
        tableMigration?.columnsToRemove?.includes(columnName) ||
        tableMigration?.columnsToModify?.some(
          (entry) =>
            entry.from.name === columnName && entry.to.name !== columnName,
        );
      if (
        !targetColumns.has(columnName) &&
        (explicitlyRemoved ||
          currentColumn?.isSystem === true ||
          currentColumn?.isSystem === 1)
      ) {
        errors.push(`column ${tableName}.${columnName} still exists`);
      }
    }
  }

  for (const currentTable of state.tables) {
    if (
      (currentTable.isSystem === true ||
        currentTable.isSystem === 1 ||
        droppedTables.has(currentTable.name)) &&
      !snapshot[currentTable.name]
    ) {
      errors.push(`table ${currentTable.name} still exists`);
    }
  }

  const expectedRelations = buildExpectedRelations(snapshot);
  const dataTargetRelations = buildExpectedRelations(dataTargetSnapshot);
  for (const key of duplicateValues(
    state.relations
      .filter((relation) => snapshot[relation.sourceTableName])
      .map(
        (relation) => `${relation.sourceTableName}.${relation.propertyName}`,
      ),
  )) {
    errors.push(`duplicate relation ${key}`);
  }
  const currentRelations = new Map(
    state.relations
      .filter((relation) => snapshot[relation.sourceTableName])
      .map((relation) => [
        `${relation.sourceTableName}.${relation.propertyName}`,
        relation,
      ]),
  );
  for (const [key, targetRelation] of expectedRelations) {
    const currentRelation = currentRelations.get(key);
    if (!currentRelation) {
      errors.push(`relation ${key} is missing`);
      continue;
    }
    const relationChanges = changedFields(
      currentRelation,
      targetRelation,
      relationFieldsForTarget(targetRelation),
      RELATION_DEFAULTS,
    );
    const dataTargetRelation = dataTargetRelations.get(key) ?? targetRelation;
    const dataTargetChanges = changedFields(
      currentRelation,
      dataTargetRelation,
      relationFieldsForTarget(dataTargetRelation),
      RELATION_DEFAULTS,
    );
    if (relationChanges.length > 0 && dataTargetChanges.length > 0) {
      errors.push(`relation ${key} differs on ${relationChanges.join(', ')}`);
    }
  }
  for (const key of currentRelations.keys()) {
    const separator = key.indexOf('.');
    const tableName = key.slice(0, separator);
    const propertyName = key.slice(separator + 1);
    const currentRelation = currentRelations.get(key);
    const tableMigration = tableMigrations.get(tableName);
    const explicitlyRemoved =
      tableMigration?.relationsToRemove?.includes(propertyName) ||
      tableMigration?.relationsToModify?.some(
        (entry) =>
          entry.from.propertyName === propertyName &&
          entry.to.propertyName !== propertyName,
      );
    if (
      !expectedRelations.has(key) &&
      (explicitlyRemoved ||
        currentRelation?.isSystem === true ||
        currentRelation?.isSystem === 1)
    ) {
      errors.push(`relation ${key} still exists`);
    }
  }

  if (errors.length > 0) {
    throw new Error(
      `Snapshot healing did not converge to the target state:\n- ${errors.join('\n- ')}`,
    );
  }
}
