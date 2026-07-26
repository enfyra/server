import type {
  SchemaMigrationDef,
  TableRenameDef,
} from '../../../shared/types/schema-migration.types';
import {
  COLUMN_DEFAULTS,
  COLUMN_FIELDS,
  RELATION_DEFAULTS,
  RELATION_FIELDS,
  TABLE_DEFAULTS,
  TABLE_FIELDS,
  buildExpectedRelations,
  comparableValue,
  duplicateValues,
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

function validateRenameDefinitions(
  label: string,
  renames: TableRenameDef[],
  snapshot: Record<string, any>,
  requireSnapshotTarget: boolean,
  errors: string[],
): void {
  for (const source of duplicateValues(renames.map((rename) => rename.from))) {
    errors.push(`duplicate ${label} source ${source}`);
  }
  for (const target of duplicateValues(renames.map((rename) => rename.to))) {
    errors.push(`duplicate ${label} target ${target}`);
  }
  for (const rename of renames) {
    if (!rename.from || !rename.to) {
      errors.push(`${label} entries require non-empty from and to names`);
      continue;
    }
    if (rename.from === rename.to) {
      errors.push(`${label} ${rename.from} cannot rename a table to itself`);
    }
    if (requireSnapshotTarget && snapshot[rename.from]) {
      errors.push(`${label} source ${rename.from} still exists in snapshot.ts`);
    }
    if (requireSnapshotTarget && !snapshot[rename.to]) {
      errors.push(`${label} target ${rename.to} does not exist in snapshot.ts`);
    }
    for (const mergeKey of duplicateValues(rename.mergeKeys ?? [])) {
      errors.push(`duplicate ${label} merge key ${rename.from}.${mergeKey}`);
    }
  }
}

export function validateMigrationDefinition(
  snapshot: Record<string, any>,
  migration: SchemaMigrationDef | null,
  errors: string[],
): void {
  validateSnapshotDefinition(snapshot, errors);
  if (!migration) return;
  validateRenameDefinitions(
    'coreTablesToRename',
    migration.coreTablesToRename ?? [],
    snapshot,
    true,
    errors,
  );
  validateRenameDefinitions(
    'tablesToRename',
    migration.tablesToRename ?? [],
    snapshot,
    true,
    errors,
  );
  validateRenameDefinitions(
    'physicalTablesToRename',
    migration.physicalTablesToRename ?? [],
    snapshot,
    false,
    errors,
  );
  const expectedRelations = buildExpectedRelations(snapshot);
  const tableNames = (migration.tables ?? []).map(
    (entry) => entry._unique.name._eq,
  );

  for (const tableName of duplicateValues(tableNames)) {
    errors.push(`duplicate table migration ${tableName}`);
  }
  for (const tableName of duplicateValues(migration.tablesToDrop ?? [])) {
    errors.push(`duplicate tablesToDrop ${tableName}`);
  }
  for (const tableName of duplicateValues(
    migration.physicalTablesToDrop ?? [],
  )) {
    errors.push(`duplicate physicalTablesToDrop ${tableName}`);
  }
  for (const tableName of migration.tablesToDrop ?? []) {
    if (snapshot[tableName]) {
      errors.push(`tablesToDrop ${tableName} still exists in snapshot.ts`);
    }
  }
  for (const tableName of migration.physicalTablesToDrop ?? []) {
    if (snapshot[tableName]) {
      errors.push(
        `physicalTablesToDrop ${tableName} still exists in snapshot.ts`,
      );
    }
  }
  const physicalDrops = new Set(migration.physicalTablesToDrop ?? []);
  for (const rename of migration.physicalTablesToRename ?? []) {
    if (physicalDrops.has(rename.from)) {
      errors.push(
        `physical table ${rename.from} is declared for both rename and drop`,
      );
    }
    if (physicalDrops.has(rename.to)) {
      errors.push(
        `physical table ${rename.to} is declared for both rename and drop`,
      );
    }
  }

  for (const tableMigration of migration.tables ?? []) {
    const tableName = tableMigration._unique.name._eq;
    const targetTable = snapshot[tableName];
    if (!targetTable) {
      errors.push(`table migration ${tableName} does not exist in snapshot.ts`);
      continue;
    }

    const targetColumns = new Map(
      (targetTable.columns ?? []).map((column: any) => [column.name, column]),
    );
    if (tableMigration.tableToModify) {
      const mismatched = Object.keys(tableMigration.tableToModify.to).filter(
        (field) =>
          TABLE_FIELDS.includes(field) &&
          JSON.stringify(
            comparableValue(
              tableMigration.tableToModify!.to,
              field,
              TABLE_DEFAULTS,
            ),
          ) !==
            JSON.stringify(comparableValue(targetTable, field, TABLE_DEFAULTS)),
      );
      if (mismatched.length > 0) {
        errors.push(
          `table ${tableName} migration target disagrees with snapshot.ts on ${mismatched.join(', ')}`,
        );
      }
    }
    const duplicateTargetColumns = duplicateValues(
      (targetTable.columns ?? []).map((column: any) => column.name),
    );
    for (const columnName of duplicateTargetColumns) {
      errors.push(`duplicate snapshot column ${tableName}.${columnName}`);
    }

    const columnModifications = tableMigration.columnsToModify ?? [];
    const columnsToRemove = tableMigration.columnsToRemove ?? [];
    for (const columnName of duplicateValues(columnsToRemove)) {
      errors.push(`duplicate column removal ${tableName}.${columnName}`);
    }
    for (const columnName of columnsToRemove) {
      if (targetColumns.has(columnName)) {
        errors.push(
          `column removal ${tableName}.${columnName} still exists in snapshot.ts`,
        );
      }
      if (
        columnModifications.some(
          (entry) =>
            entry.from.name === columnName || entry.to.name === columnName,
        )
      ) {
        errors.push(
          `column ${tableName}.${columnName} is declared for both modification and removal`,
        );
      }
    }
    for (const columnName of duplicateValues(
      columnModifications.map((entry) => entry.from.name),
    )) {
      errors.push(
        `duplicate column migration source ${tableName}.${columnName}`,
      );
    }
    for (const columnName of duplicateValues(
      columnModifications.map((entry) => entry.to.name),
    )) {
      errors.push(
        `duplicate column migration target ${tableName}.${columnName}`,
      );
    }
    for (const modification of columnModifications) {
      const targetColumn = targetColumns.get(modification.to.name) as
        | Record<string, any>
        | undefined;
      if (!targetColumn) {
        errors.push(
          `column ${tableName}.${modification.to.name} does not exist in snapshot.ts`,
        );
        continue;
      }
      const mismatched = Object.keys(modification.to).filter(
        (field) =>
          COLUMN_FIELDS.includes(field) &&
          JSON.stringify(
            comparableValue(modification.to, field, COLUMN_DEFAULTS),
          ) !==
            JSON.stringify(
              comparableValue(targetColumn, field, COLUMN_DEFAULTS),
            ),
      );
      if (mismatched.length > 0) {
        errors.push(
          `column ${tableName}.${modification.to.name} migration target disagrees with snapshot.ts on ${mismatched.join(', ')}`,
        );
      }
    }

    const relationModifications = tableMigration.relationsToModify ?? [];
    const relationsToRemove = tableMigration.relationsToRemove ?? [];
    for (const propertyName of duplicateValues(relationsToRemove)) {
      errors.push(`duplicate relation removal ${tableName}.${propertyName}`);
    }
    for (const propertyName of relationsToRemove) {
      if (
        relationModifications.some(
          (entry) =>
            entry.from.propertyName === propertyName ||
            entry.to.propertyName === propertyName,
        )
      ) {
        errors.push(
          `relation ${tableName}.${propertyName} is declared for both modification and removal`,
        );
      }
    }
    for (const propertyName of duplicateValues(
      relationModifications.map((entry) => entry.from.propertyName),
    )) {
      errors.push(
        `duplicate relation migration source ${tableName}.${propertyName}`,
      );
    }
    for (const propertyName of duplicateValues(
      relationModifications.map((entry) => entry.to.propertyName),
    )) {
      errors.push(
        `duplicate relation migration target ${tableName}.${propertyName}`,
      );
    }
    for (const modification of relationModifications) {
      const targetRelation = expectedRelations.get(
        `${tableName}.${modification.to.propertyName}`,
      );
      if (!targetRelation) {
        errors.push(
          `relation ${tableName}.${modification.to.propertyName} does not exist in snapshot.ts`,
        );
        continue;
      }
      const mismatched = Object.keys(modification.to).filter(
        (field) =>
          RELATION_FIELDS.includes(field) &&
          JSON.stringify(
            comparableValue(modification.to, field, RELATION_DEFAULTS),
          ) !==
            JSON.stringify(
              comparableValue(targetRelation, field, RELATION_DEFAULTS),
            ),
      );
      if (mismatched.length > 0) {
        errors.push(
          `relation ${tableName}.${modification.to.propertyName} migration target disagrees with snapshot.ts on ${mismatched.join(', ')}`,
        );
      }
    }
  }
}

export function validateSnapshotMigrationDefinition(
  snapshot: Record<string, any>,
  migration: SchemaMigrationDef | null,
): void {
  const errors: string[] = [];
  try {
    validateMigrationDefinition(snapshot, migration, errors);
  } catch (error) {
    errors.push((error as Error).message);
  }
  if (errors.length > 0) {
    throw new Error(
      `Invalid snapshot migration definition:\n- ${errors.join('\n- ')}`,
    );
  }
}
