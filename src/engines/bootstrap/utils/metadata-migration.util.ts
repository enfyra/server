import type {
  ColumnModifyDef,
  RelationModifyDef,
  SchemaMigrationDef,
  SnapshotMigrationMetadataState,
  TableModifyDef,
  TableRenameDef,
} from '../../../shared/types/schema-migration.types';
import { getScriptLegacyField } from '../../../shared/utils/script-code.util';

export function hasSchemaMigrations(
  migrations: SchemaMigrationDef | null | undefined,
): migrations is SchemaMigrationDef {
  if (!migrations) return false;
  return (
    (migrations.coreTablesToRename?.length ?? 0) > 0 ||
    (migrations.tablesToRename?.length ?? 0) > 0 ||
    (migrations.physicalTablesToRename?.length ?? 0) > 0 ||
    (migrations.physicalTablesToDrop?.length ?? 0) > 0 ||
    (migrations.tables?.length ?? 0) > 0 ||
    (migrations.tablesToDrop?.length ?? 0) > 0
  );
}

export function getValidTableRenames(
  renames: TableRenameDef[],
): TableRenameDef[] {
  return renames.filter(
    (rename) => rename.from && rename.to && rename.from !== rename.to,
  );
}

export function hasTableMetadataChanges(mod: TableModifyDef): boolean {
  return Object.entries(mod.to).some(
    ([key, value]) => JSON.stringify(value) !== JSON.stringify(mod.from[key]),
  );
}

export function buildTableMetadataUpdate(mod: TableModifyDef): any {
  const fields = [
    'isSystem',
    'isSingleRecord',
    'uniques',
    'indexes',
    'alias',
    'description',
    'metadata',
    'validateBody',
  ];
  return Object.fromEntries(
    fields
      .filter((field) => mod.to[field] !== undefined)
      .map((field) => [field, mod.to[field]]),
  );
}

export function hasColumnMetadataChanges(mod: ColumnModifyDef): boolean {
  return Object.entries(mod.to).some(
    ([key, value]) => JSON.stringify(value) !== JSON.stringify(mod.from[key]),
  );
}

export function buildColumnMetadataUpdate(mod: ColumnModifyDef): any {
  const fields = [
    'name',
    'type',
    'isPrimary',
    'isGenerated',
    'isNullable',
    'isSystem',
    'isUpdatable',
    'isPublished',
    'isEncrypted',
    'defaultValue',
    'options',
    'description',
    'placeholder',
  ];
  return Object.fromEntries(
    fields
      .filter((field) => mod.to[field] !== undefined)
      .map((field) => [field, mod.to[field]]),
  );
}

export function hasRelationMetadataChanges(mod: RelationModifyDef): boolean {
  return Object.entries(mod.to).some(
    ([key, value]) => JSON.stringify(value) !== JSON.stringify(mod.from[key]),
  );
}

export function buildRelationMetadataUpdate(mod: RelationModifyDef): any {
  const fields = [
    'propertyName',
    'type',
    'isNullable',
    'isSystem',
    'isUpdatable',
    'isPublished',
    'onDelete',
    'description',
    'foreignKeyColumn',
    'referencedColumn',
    'constraintName',
    'junctionTableName',
    'junctionSourceColumn',
    'junctionTargetColumn',
    'metadata',
  ];
  return Object.fromEntries(
    fields
      .filter((field) => mod.to[field] !== undefined)
      .map((field) => [field, mod.to[field]]),
  );
}

export function getLegacyScriptTargetColumn(
  tableName: string,
  colName: string,
): string | null {
  return getScriptLegacyField(tableName) === colName ? 'sourceCode' : null;
}

const TABLE_FIELDS = [
  'isSystem',
  'isSingleRecord',
  'uniques',
  'indexes',
  'alias',
  'description',
  'metadata',
  'validateBody',
];

const COLUMN_FIELDS = [
  'name',
  'type',
  'isPrimary',
  'isGenerated',
  'isNullable',
  'isSystem',
  'isUpdatable',
  'isPublished',
  'isEncrypted',
  'defaultValue',
  'options',
  'description',
  'placeholder',
];

const RELATION_FIELDS = [
  'propertyName',
  'type',
  'targetTable',
  'mappedBy',
  'inversePropertyName',
  'isNullable',
  'isSystem',
  'isUpdatable',
  'isPublished',
  'onDelete',
  'description',
  'foreignKeyColumn',
  'referencedColumn',
  'constraintName',
  'junctionTableName',
  'junctionSourceColumn',
  'junctionTargetColumn',
  'metadata',
];

const RELATION_EXPLICIT_TARGET_FIELDS = new Set([
  'foreignKeyColumn',
  'referencedColumn',
  'constraintName',
  'junctionTableName',
  'junctionSourceColumn',
  'junctionTargetColumn',
  'metadata',
]);

const TABLE_DEFAULTS: Record<string, any> = {
  isSystem: false,
  isSingleRecord: false,
  uniques: [],
  indexes: [],
  alias: null,
  description: null,
  metadata: null,
  validateBody: true,
};

const COLUMN_DEFAULTS: Record<string, any> = {
  isPrimary: false,
  isGenerated: false,
  isNullable: true,
  isSystem: false,
  isUpdatable: true,
  isPublished: true,
  isEncrypted: false,
  defaultValue: null,
  options: null,
  description: null,
  placeholder: null,
};

const RELATION_DEFAULTS: Record<string, any> = {
  mappedBy: null,
  inversePropertyName: null,
  isNullable: true,
  isSystem: false,
  isUpdatable: true,
  isPublished: true,
  onDelete: 'SET NULL',
  description: null,
  metadata: null,
};

function relationFieldsForTarget(target: Record<string, any>): string[] {
  return RELATION_FIELDS.filter(
    (field) => !RELATION_EXPLICIT_TARGET_FIELDS.has(field) || field in target,
  );
}

function comparableValue(
  record: Record<string, any>,
  field: string,
  defaults: Record<string, any>,
): any {
  const value =
    (record[field] === undefined || record[field] === null) && field in defaults
      ? defaults[field]
      : record[field];
  if (typeof defaults[field] === 'boolean') {
    return value === true || value === 1 || value === '1';
  }
  if (
    typeof value === 'string' &&
    ['defaultValue', 'options', 'uniques', 'indexes', 'metadata'].includes(
      field,
    )
  ) {
    try {
      return JSON.parse(value);
    } catch {
      if (value.startsWith('{') && value.endsWith('}')) {
        const content = value.slice(1, -1);
        if (!content) return [];
        try {
          return JSON.parse(`[${content}]`);
        } catch {
          return content.split(',').map((entry) => entry.trim());
        }
      }
      return value;
    }
  }
  return value ?? null;
}

function changedFields(
  current: Record<string, any>,
  target: Record<string, any>,
  fields: string[],
  defaults: Record<string, any>,
): string[] {
  return fields.filter(
    (field) =>
      JSON.stringify(comparableValue(current, field, defaults)) !==
      JSON.stringify(comparableValue(target, field, defaults)),
  );
}

function inverseRelationType(type: string): string {
  if (type === 'many-to-one') return 'one-to-many';
  if (type === 'one-to-many') return 'many-to-one';
  return type;
}

function buildExpectedRelations(
  snapshot: Record<string, any>,
): Map<string, Record<string, any>> {
  const expected = new Map<string, Record<string, any>>();
  const generatedInverseKeys = new Set<string>();
  const declaredRelations = new Map<string, Record<string, any>>();
  for (const [tableName, table] of Object.entries(snapshot)) {
    for (const relation of (table as any).relations ?? []) {
      declaredRelations.set(`${tableName}.${relation.propertyName}`, relation);
    }
  }

  for (const [tableName, table] of Object.entries(snapshot)) {
    for (const relation of (table as any).relations ?? []) {
      const currentKey = `${tableName}.${relation.propertyName}`;
      if (generatedInverseKeys.has(currentKey)) continue;

      const isOneToManyInverse =
        relation.type === 'one-to-many' && relation.inversePropertyName;
      expected.set(currentKey, {
        ...relation,
        mappedBy: isOneToManyInverse
          ? relation.inversePropertyName
          : relation.mappedBy,
      });
      if (!relation.inversePropertyName) continue;

      const inverseKey = `${relation.targetTable}.${relation.inversePropertyName}`;
      generatedInverseKeys.add(inverseKey);
      const declaredInverse = declaredRelations.get(inverseKey);
      expected.set(inverseKey, {
        isNullable: relation.isNullable !== false,
        isSystem: relation.isSystem ?? false,
        isUpdatable: relation.isUpdatable ?? true,
        isPublished: true,
        onDelete: 'SET NULL',
        description: null,
        ...declaredInverse,
        propertyName: relation.inversePropertyName,
        type: inverseRelationType(relation.type),
        targetTable: tableName,
        mappedBy: isOneToManyInverse ? null : relation.propertyName,
        inversePropertyName: relation.propertyName,
      });
    }
  }
  return expected;
}

function validateModificationTarget(
  label: string,
  changed: string[],
  target: Record<string, any>,
  modification: { to: Record<string, any> } | undefined,
  defaults: Record<string, any>,
  errors: string[],
): void {
  if (!modification) {
    errors.push(`${label} updates ${changed.join(', ')} without migration`);
    return;
  }

  const incomplete = changed.filter(
    (field) =>
      !(field in modification.to) ||
      JSON.stringify(comparableValue(modification.to, field, defaults)) !==
        JSON.stringify(comparableValue(target, field, defaults)),
  );
  if (incomplete.length > 0) {
    errors.push(
      `${label} migration does not fully declare target fields: ${incomplete.join(', ')}`,
    );
  }
}

function validateModificationSource(
  label: string,
  current: Record<string, any>,
  target: Record<string, any>,
  modification: { from: Record<string, any> } | undefined,
  fields: string[],
  defaults: Record<string, any>,
  errors: string[],
): void {
  if (!modification) return;
  const mismatched = Object.keys(modification.from).filter((field) => {
    if (!fields.includes(field)) return false;
    const currentValue = JSON.stringify(
      comparableValue(current, field, defaults),
    );
    return (
      currentValue !==
        JSON.stringify(comparableValue(modification.from, field, defaults)) &&
      currentValue !== JSON.stringify(comparableValue(target, field, defaults))
    );
  });
  if (mismatched.length > 0) {
    errors.push(
      `${label} migration source does not match current fields: ${mismatched.join(', ')}`,
    );
  }
}

function duplicateValues(values: string[]): string[] {
  const seen = new Set<string>();
  const duplicates = new Set<string>();
  for (const value of values) {
    if (seen.has(value)) duplicates.add(value);
    seen.add(value);
  }
  return [...duplicates];
}

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

function validateMigrationDefinition(
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
  const systemTableNames = new Set(Object.keys(snapshot));

  for (const currentTable of state.tables) {
    const targetTable = snapshot[currentTable.name];
    if (!targetTable) {
      if (
        (currentTable.isSystem === true || currentTable.isSystem === 1) &&
        !droppedTables.has(currentTable.name)
      ) {
        errors.push(
          `table ${currentTable.name} is removed without tablesToDrop`,
        );
      }
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
      dataTargetSnapshot[currentTable.name] ?? targetTable,
      TABLE_FIELDS,
      TABLE_DEFAULTS,
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

      const changed = changedFields(
        currentColumn,
        targetColumn,
        COLUMN_FIELDS,
        COLUMN_DEFAULTS,
      );
      const dataTargetColumn =
        dataTargetColumns.get(targetColumn.name) ?? targetColumn;
      const dataTargetChanged = changedFields(
        currentColumn,
        dataTargetColumn,
        COLUMN_FIELDS,
        COLUMN_DEFAULTS,
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

    const changed = changedFields(
      currentRelation,
      targetRelation,
      relationFieldsForTarget(targetRelation),
      RELATION_DEFAULTS,
    );
    const dataTargetRelation = dataTargetRelations.get(key) ?? targetRelation;
    const dataTargetChanged = changedFields(
      currentRelation,
      dataTargetRelation,
      relationFieldsForTarget(dataTargetRelation),
      RELATION_DEFAULTS,
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
