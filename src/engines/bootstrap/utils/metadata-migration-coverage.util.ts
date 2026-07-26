import type {
  SchemaMigrationDef,
  SnapshotMigrationMetadataState,
} from '../../../shared/types/schema-migration.types';
import {
  COLUMN_DEFAULTS,
  COLUMN_FIELDS,
  COLUMN_HEALABLE_FIELDS,
  RELATION_DERIVED_FIELDS,
  RELATION_DEFAULTS,
  RELATION_HEALABLE_FIELDS,
  TABLE_DEFAULTS,
  TABLE_FIELDS,
  TABLE_HEALABLE_FIELDS,
  buildExpectedRelations,
  changedFields,
  relationFieldsForTarget,
  validateModificationSource,
  validateModificationTarget,
} from './metadata-comparison.util';
import { validateMigrationDefinition } from './metadata-migration-validation.util';

function excludeHealable(changed: string[], healable: Set<string>): string[] {
  return changed.filter((field) => !healable.has(field));
}

function excludeRelationNonDeclarativeFields(changed: string[]): string[] {
  return changed.filter(
    (field) =>
      !RELATION_HEALABLE_FIELDS.has(field) &&
      !RELATION_DERIVED_FIELDS.has(field),
  );
}

export function validateSnapshotMigrationCoverage(
  snapshot: Record<string, any>,
  migration: SchemaMigrationDef | null,
  state: SnapshotMigrationMetadataState,
  dataTargetSnapshot: Record<string, any> = snapshot,
): void {
  const errors: string[] = [];
  validateMigrationDefinition(snapshot, migration, errors);
  const tableMigrations = new Map(
    (migration?.tables ?? []).map((entry) => [entry._unique.name._eq, entry]),
  );
  const droppedTables = new Set(migration?.tablesToDrop ?? []);
  const renamedSources = new Set([
    ...(migration?.coreTablesToRename ?? []).map((r) => r.from),
    ...(migration?.tablesToRename ?? []).map((r) => r.from),
  ]);
  const systemTableNames = new Set(Object.keys(snapshot));

  for (const currentTable of state.tables) {
    const targetTable = snapshot[currentTable.name];
    if (!targetTable) {
      if (
        (currentTable.isSystem === true || currentTable.isSystem === 1) &&
        !droppedTables.has(currentTable.name) &&
        !renamedSources.has(currentTable.name)
      ) {
        errors.push(
          `table ${currentTable.name} is removed without tablesToDrop`,
        );
      }
      continue;
    }

    const changed = excludeHealable(
      changedFields(currentTable, targetTable, TABLE_FIELDS, TABLE_DEFAULTS),
      TABLE_HEALABLE_FIELDS,
    );
    const dataTargetChanged = excludeHealable(
      changedFields(
        currentTable,
        dataTargetSnapshot[currentTable.name] ?? targetTable,
        TABLE_FIELDS,
        TABLE_DEFAULTS,
      ),
      TABLE_HEALABLE_FIELDS,
    );
    if (changed.length > 0 && dataTargetChanged.length > 0) {
      validateModificationSource(
        `table ${currentTable.name}`,
        currentTable,
        targetTable,
        tableMigrations.get(currentTable.name)?.tableToModify,
        TABLE_FIELDS,
        TABLE_DEFAULTS,
        errors,
      );
      validateModificationTarget(
        `table ${currentTable.name}`,
        changed,
        targetTable,
        tableMigrations.get(currentTable.name)?.tableToModify,
        TABLE_DEFAULTS,
        errors,
      );
    }
  }

  for (const tableName of systemTableNames) {
    const targetTable = snapshot[tableName];
    const tableMigration = tableMigrations.get(tableName);
    const targetColumns = new Map(
      (targetTable.columns ?? []).map((column: any) => [column.name, column]),
    );
    const dataTargetColumns = new Map(
      (dataTargetSnapshot[tableName]?.columns ?? []).map((column: any) => [
        column.name,
        column,
      ]),
    );
    const currentColumns = state.columns.filter(
      (column) => column.tableName === tableName,
    );

    for (const currentColumn of currentColumns) {
      const directTarget = targetColumns.get(currentColumn.name) as
        | Record<string, any>
        | undefined;
      const modification = tableMigration?.columnsToModify?.find(
        (entry) =>
          entry.from.name === currentColumn.name ||
          entry.to.name === currentColumn.name,
      );
      const targetColumn =
        directTarget ||
        (modification
          ? (targetColumns.get(modification.to.name) as
              | Record<string, any>
              | undefined)
          : undefined);

      if (!targetColumn) {
        if (
          (currentColumn.isSystem === true || currentColumn.isSystem === 1) &&
          !tableMigration?.columnsToRemove?.includes(currentColumn.name) &&
          !modification
        ) {
          errors.push(
            `column ${tableName}.${currentColumn.name} is removed without migration`,
          );
        }
        continue;
      }

      if (tableMigration?.columnsToRemove?.includes(currentColumn.name)) {
        continue;
      }

      const changed = excludeHealable(
        changedFields(
          currentColumn,
          targetColumn,
          COLUMN_FIELDS,
          COLUMN_DEFAULTS,
        ),
        COLUMN_HEALABLE_FIELDS,
      );
      const dataTargetColumn =
        dataTargetColumns.get(targetColumn.name) ?? targetColumn;
      const dataTargetChanged = excludeHealable(
        changedFields(
          currentColumn,
          dataTargetColumn,
          COLUMN_FIELDS,
          COLUMN_DEFAULTS,
        ),
        COLUMN_HEALABLE_FIELDS,
      );
      if (changed.length > 0 && dataTargetChanged.length > 0) {
        validateModificationSource(
          `column ${tableName}.${currentColumn.name}`,
          currentColumn,
          targetColumn,
          modification,
          COLUMN_FIELDS,
          COLUMN_DEFAULTS,
          errors,
        );
        validateModificationTarget(
          `column ${tableName}.${currentColumn.name}`,
          changed,
          targetColumn,
          modification,
          COLUMN_DEFAULTS,
          errors,
        );
      }
    }
  }

  const expectedRelations = buildExpectedRelations(snapshot);
  const dataTargetRelations = buildExpectedRelations(dataTargetSnapshot);
  const explicitRelations = new Map<string, Record<string, any>>();
  for (const [tableName, table] of Object.entries(snapshot)) {
    for (const relation of (table as any).relations ?? []) {
      const key = `${tableName}.${relation.propertyName}`;
      explicitRelations.set(key, expectedRelations.get(key)!);
    }
  }

  for (const currentRelation of state.relations) {
    if (!systemTableNames.has(currentRelation.sourceTableName)) continue;

    const key = `${currentRelation.sourceTableName}.${currentRelation.propertyName}`;
    if (expectedRelations.has(key)) continue;

    const tableMigration = tableMigrations.get(currentRelation.sourceTableName);
    const directModification = tableMigration?.relationsToModify?.find(
      (entry) => entry.from.propertyName === currentRelation.propertyName,
    );
    const inverseModification = (migration?.tables ?? []).some((entry) =>
      entry.relationsToModify?.some(
        (relation) =>
          relation.from.inversePropertyName === currentRelation.propertyName &&
          expectedRelations.has(
            `${currentRelation.sourceTableName}.${relation.to.inversePropertyName}`,
          ),
      ),
    );
    if (
      (currentRelation.isSystem === true || currentRelation.isSystem === 1) &&
      !tableMigration?.relationsToRemove?.includes(
        currentRelation.propertyName,
      ) &&
      !directModification &&
      !inverseModification
    ) {
      errors.push(`relation ${key} is removed without migration`);
    }
  }

  for (const [key, targetRelation] of explicitRelations) {
    const separator = key.indexOf('.');
    const tableName = key.slice(0, separator);
    const targetPropertyName = key.slice(separator + 1);
    const tableMigration = tableMigrations.get(tableName);
    const modification = tableMigration?.relationsToModify?.find(
      (entry) =>
        entry.from.propertyName === targetPropertyName ||
        entry.to.propertyName === targetPropertyName,
    );
    const currentRelation = state.relations.find(
      (relation) =>
        relation.sourceTableName === tableName &&
        (relation.propertyName === targetPropertyName ||
          relation.propertyName === modification?.from.propertyName),
    );
    if (!currentRelation) continue;
    if (
      tableMigration?.relationsToRemove?.includes(currentRelation.propertyName)
    ) {
      continue;
    }

    const changed = excludeRelationNonDeclarativeFields(
      changedFields(
        currentRelation,
        targetRelation,
        relationFieldsForTarget(targetRelation),
        RELATION_DEFAULTS,
      ),
    );
    const dataTargetRelation = dataTargetRelations.get(key) ?? targetRelation;
    const dataTargetChanged = excludeRelationNonDeclarativeFields(
      changedFields(
        currentRelation,
        dataTargetRelation,
        relationFieldsForTarget(dataTargetRelation),
        RELATION_DEFAULTS,
      ),
    );
    if (changed.length > 0 && dataTargetChanged.length > 0) {
      validateModificationSource(
        `relation ${tableName}.${currentRelation.propertyName}`,
        currentRelation,
        targetRelation,
        modification,
        relationFieldsForTarget(targetRelation),
        RELATION_DEFAULTS,
        errors,
      );
      validateModificationTarget(
        `relation ${tableName}.${currentRelation.propertyName}`,
        changed,
        targetRelation,
        modification,
        RELATION_DEFAULTS,
        errors,
      );
    }
  }

  if (errors.length > 0) {
    throw new Error(
      `snapshot-migration.ts is missing non-additive declarations:\n- ${errors.join('\n- ')}`,
    );
  }
}
