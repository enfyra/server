import { Knex } from 'knex';
import { Db } from 'mongodb';
import {
  SchemaMigrationDef,
  TableMigrationDef,
  ColumnModifyDef,
  RelationModifyDef,
  type MongoPhysicalMigrationOptions,
} from '../types/schema-migration.types';
import {
  getForeignKeyColumnName,
  getJunctionTableName,
  getJunctionColumnNames,
} from '@enfyra/kernel';
import { dropForeignKeyIfExists } from '../../engines/knex/utils/migration/foreign-key-operations';
import {
  generateColumnDefinition,
  supportsSqlColumnDefault,
} from '../../engines/knex/utils/migration/sql-generator';
import { getCurrentDatabaseSchema } from '../../engines/knex/utils/provision/schema-comparison';

/**
 * Apply SQL schema migrations (physical database)
 */
export async function applySqlSchemaMigrations(
  knex: Knex,
  migration: SchemaMigrationDef,
): Promise<void> {
  const dbType = knex.client.config.client;

  if (migration.tablesToDrop && migration.tablesToDrop.length > 0) {
    console.log(`🗑️ Dropping ${migration.tablesToDrop.length} table(s)...`);
    for (const tableName of migration.tablesToDrop) {
      await cleanupSqlTableDependencies(knex, tableName);
      const exists = await knex.schema.hasTable(tableName);
      if (exists) {
        if (
          dbType === 'pg' ||
          dbType === 'postgres' ||
          dbType === 'postgresql'
        ) {
          await knex.raw(`DROP TABLE IF EXISTS "${tableName}" CASCADE`);
        } else if (
          dbType === 'mysql' ||
          dbType === 'mysql2' ||
          dbType === 'mariadb'
        ) {
          await knex.raw('SET FOREIGN_KEY_CHECKS = 0');
          await knex.schema.dropTableIfExists(tableName);
          await knex.raw('SET FOREIGN_KEY_CHECKS = 1');
        } else {
          await knex.schema.dropTableIfExists(tableName);
        }
        console.log(`  ✅ Dropped table: ${tableName}`);
      } else {
        console.log(`  ⏩ Table ${tableName} does not exist, skipping`);
      }
    }
  }

  const tables = migration.tables || [];
  for (const table of tables) {
    await applySqlTableMigration(knex, table, dbType);
  }
}

function normalizeSqlMigrationDbType(knex: Knex): 'mysql' | 'postgres' {
  const dbType = String(knex.client.config.client || '').toLowerCase();
  return dbType.includes('pg') || dbType.includes('postgres')
    ? 'postgres'
    : 'mysql';
}

async function getSqlMigrationMetadata(knex: Knex): Promise<{
  tables: any[];
  relations: any[];
  tableById: Map<string, any>;
}> {
  const [hasTableStore, hasRelationStore] = await Promise.all([
    knex.schema.hasTable('enfyra_table'),
    knex.schema.hasTable('enfyra_relation'),
  ]);
  const tables = hasTableStore ? await knex('enfyra_table').select('*') : [];
  const relations = hasRelationStore
    ? await knex('enfyra_relation').select('*')
    : [];
  return {
    tables,
    relations,
    tableById: new Map(tables.map((table) => [String(table.id), table])),
  };
}

async function dropSqlPhysicalColumn(
  knex: Knex,
  tableName: string,
  columnName: string,
): Promise<void> {
  if (!(await knex.schema.hasTable(tableName))) return;
  if (!(await knex.schema.hasColumn(tableName, columnName))) return;
  await dropForeignKeyIfExists(
    knex,
    tableName,
    columnName,
    normalizeSqlMigrationDbType(knex),
  );
  await knex.schema.alterTable(tableName, (table) => {
    table.dropColumn(columnName);
  });
}

async function cleanupSqlRelationPhysical(
  knex: Knex,
  relation: any,
  tableById: Map<string, any>,
  droppedJunctions: Set<string>,
): Promise<void> {
  const sourceTable = tableById.get(String(relation.sourceTableId));
  if (!sourceTable?.name) return;

  if (relation.type === 'many-to-many') {
    const junctionName = relation.junctionTableName;
    if (
      junctionName &&
      !droppedJunctions.has(junctionName) &&
      (await knex.schema.hasTable(junctionName))
    ) {
      await knex.schema.dropTableIfExists(junctionName);
      droppedJunctions.add(junctionName);
    }
    return;
  }

  const isOwning =
    !relation.mappedById &&
    (relation.type === 'many-to-one' || relation.type === 'one-to-one');
  if (!isOwning) return;

  await dropSqlPhysicalColumn(
    knex,
    sourceTable.name,
    relation.foreignKeyColumn || getForeignKeyColumnName(relation.propertyName),
  );
}

async function cleanupSqlTableDependencies(
  knex: Knex,
  tableName: string,
): Promise<void> {
  const metadata = await getSqlMigrationMetadata(knex);
  const table = metadata.tables.find((item) => item.name === tableName);
  if (!table) return;

  const droppedJunctions = new Set<string>();
  for (const relation of metadata.relations) {
    if (
      String(relation.sourceTableId) !== String(table.id) &&
      String(relation.targetTableId) !== String(table.id)
    ) {
      continue;
    }
    await cleanupSqlRelationPhysical(
      knex,
      relation,
      metadata.tableById,
      droppedJunctions,
    );
  }
}

async function cleanupSqlRemovedRelation(
  knex: Knex,
  tableName: string,
  propertyName: string,
): Promise<boolean> {
  const metadata = await getSqlMigrationMetadata(knex);
  const table = metadata.tables.find((item) => item.name === tableName);
  const relation = table
    ? metadata.relations.find(
        (item) =>
          String(item.sourceTableId) === String(table.id) &&
          item.propertyName === propertyName,
      )
    : null;
  if (!relation) return false;
  await cleanupSqlRelationPhysical(
    knex,
    relation,
    metadata.tableById,
    new Set<string>(),
  );
  return true;
}

/**
 * Apply SQL table migration
 */
async function applySqlTableMigration(
  knex: Knex,
  migration: TableMigrationDef,
  dbType: string,
): Promise<void> {
  const tableName = migration._unique.name._eq;
  const exists = await knex.schema.hasTable(tableName);

  if (!exists) {
    console.log(`  ⏩ Table ${tableName} does not exist, skipping migration`);
    return;
  }

  console.log(`🔄 Migrating table: ${tableName}`);

  // Handle column modifications (including rename)
  if (migration.columnsToModify && migration.columnsToModify.length > 0) {
    await applySqlColumnModifications(
      knex,
      tableName,
      migration.columnsToModify,
      dbType,
    );
  }

  // Handle column removals
  if (migration.columnsToRemove && migration.columnsToRemove.length > 0) {
    await applySqlColumnRemovals(knex, tableName, migration.columnsToRemove);
  }

  // Handle relation modifications (including rename)
  if (migration.relationsToModify && migration.relationsToModify.length > 0) {
    await applySqlRelationModifications(
      knex,
      tableName,
      migration.relationsToModify,
      dbType,
    );
  }

  if (migration.relationsToRemove && migration.relationsToRemove.length > 0) {
    if (
      tableName === 'enfyra_file_permission' &&
      migration.relationsToRemove.includes('allowedUsers')
    ) {
      await migrateFilePermissionAllowedUsersToJunction(knex, dbType);
    }
    await applySqlRelationRemovals(
      knex,
      tableName,
      migration.relationsToRemove,
    );
  }

  await applySqlTableConstraintMigrations(
    knex,
    tableName,
    migration.tableToModify,
  );
}

function normalizeConstraintGroups(value: unknown): string[][] {
  if (typeof value === 'string') {
    try {
      return normalizeConstraintGroups(JSON.parse(value));
    } catch {
      return [];
    }
  }
  if (!Array.isArray(value)) return [];
  return value.filter(
    (group): group is string[] =>
      Array.isArray(group) && group.every((column) => typeof column === 'string'),
  );
}

function constraintGroupKey(columns: string[]): string {
  return columns.map((column) => column.toLowerCase()).join('|');
}

async function applySqlTableConstraintMigrations(
  knex: Knex,
  tableName: string,
  modification: TableMigrationDef['tableToModify'],
): Promise<void> {
  if (!modification) return;

  const fromUniques = normalizeConstraintGroups(modification.from?.uniques);
  const toUniqueKeys = new Set(
    normalizeConstraintGroups(modification.to?.uniques).map(constraintGroupKey),
  );
  const removedUniqueKeys = new Set(
    fromUniques
      .map(constraintGroupKey)
      .filter((key) => !toUniqueKeys.has(key)),
  );
  if (removedUniqueKeys.size === 0) return;

  const current = await getCurrentDatabaseSchema(knex, tableName);
  for (const unique of current.uniques ?? []) {
    if (!removedUniqueKeys.has(constraintGroupKey(unique.columns))) continue;
    const dbType = normalizeSqlMigrationDbType(knex);
    if (dbType === 'postgres') {
      await knex.raw('ALTER TABLE ?? DROP CONSTRAINT ??', [
        tableName,
        unique.name,
      ]);
    } else {
      await knex.raw('ALTER TABLE ?? DROP INDEX ??', [tableName, unique.name]);
    }
  }
}

async function migrateFilePermissionAllowedUsersToJunction(
  knex: Knex,
  _dbType: string,
): Promise<void> {
  const tableName = 'enfyra_file_permission';
  const fkColumn = getForeignKeyColumnName('allowedUsers');
  const hasOldColumn = await knex.schema.hasColumn(tableName, fkColumn);
  if (!hasOldColumn) return;

  const junctionTableName = getJunctionTableName(
    tableName,
    'allowedUsers',
    'enfyra_user',
  );
  const { sourceColumn, targetColumn } = getJunctionColumnNames(
    tableName,
    'allowedUsers',
    'enfyra_user',
  );

  const exists = await knex.schema.hasTable(junctionTableName);
  if (!exists) {
    console.log(
      `  📦 Migrating enfyra_file_permission.allowedUsers to junction ${junctionTableName}`,
    );
    const pkType = await getPrimaryKeyType(knex, tableName);
    const targetPkType = await getPrimaryKeyType(knex, 'enfyra_user');

    await knex.schema.createTable(junctionTableName, (table) => {
      if (pkType === 'uuid') {
        table.uuid(sourceColumn).notNullable();
      } else {
        table.integer(sourceColumn).unsigned().notNullable();
      }
      if (targetPkType === 'uuid') {
        table.uuid(targetColumn).notNullable();
      } else {
        table.integer(targetColumn).unsigned().notNullable();
      }
      table.primary([sourceColumn, targetColumn]);
      table.foreign(sourceColumn).references('id').inTable(tableName);
      table.foreign(targetColumn).references('id').inTable('enfyra_user');
    });
  }

  const rows = await knex(tableName)
    .select('id', fkColumn)
    .whereNotNull(fkColumn);
  if (rows.length > 0) {
    await knex(junctionTableName)
      .insert(
        rows.map((row) => ({
          [sourceColumn]: row.id,
          [targetColumn]: row[fkColumn],
        })),
      )
      .onConflict([sourceColumn, targetColumn])
      .ignore();
  }
  console.log(
    `  ✅ Created junction and migrated ${rows.length} row(s): ${junctionTableName}`,
  );
}

async function getPrimaryKeyType(
  knex: Knex,
  tableName: string,
): Promise<'int' | 'uuid'> {
  await knex(tableName).limit(1).select(knex.raw('1 as _'));
  const tableInfo = await knex.raw(
    knex.client.config.client === 'pg'
      ? `SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = 'public' AND table_name = ? AND column_name = 'id'`
      : `SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? AND COLUMN_NAME = 'id'`,
    [tableName],
  );
  const rows =
    knex.client.config.client === 'pg'
      ? (tableInfo as any).rows
      : (tableInfo as any)[0];
  if (rows?.length > 0) {
    const dt = (rows[0].data_type || rows[0].DATA_TYPE || '').toLowerCase();
    if (dt.includes('uuid')) return 'uuid';
  }
  return 'int';
}

/**
 * Check if column modification has actual changes
 */
function hasColumnChanges(mod: ColumnModifyDef): boolean {
  return [
    'name',
    'type',
    'options',
    'isNullable',
    'defaultValue',
    'isPrimary',
    'isGenerated',
  ].some(
    (field) =>
      field in mod.to &&
      JSON.stringify(mod.from[field]) !== JSON.stringify(mod.to[field]),
  );
}

/**
 * Check if relation modification has actual changes
 */
function hasRelationChanges(mod: RelationModifyDef): boolean {
  return [
    'propertyName',
    'type',
    'targetTable',
    'mappedBy',
    'isNullable',
    'onDelete',
    'foreignKeyColumn',
    'referencedColumn',
    'constraintName',
    'junctionTableName',
    'junctionSourceColumn',
    'junctionTargetColumn',
  ].some(
    (field) =>
      field in mod.to &&
      JSON.stringify(mod.from[field]) !== JSON.stringify(mod.to[field]),
  );
}

function readSqlMigrationCount(result: any, dbType: string): number {
  const row =
    dbType === 'pg' || dbType === 'postgres' || dbType === 'postgresql'
      ? result?.rows?.[0]
      : result?.[0]?.[0];
  return Number(row?.count ?? row?.COUNT ?? 0);
}

async function migrateSqlRenamedColumn(
  knex: Knex,
  tableName: string,
  oldName: string,
  newName: string,
  dbType: string,
): Promise<'none' | 'renamed' | 'merged'> {
  const hasOldColumn = await knex.schema.hasColumn(tableName, oldName);
  if (!hasOldColumn) return 'none';

  const hasNewColumn = await knex.schema.hasColumn(tableName, newName);
  if (!hasNewColumn) {
    await knex.schema.alterTable(tableName, (table) => {
      table.renameColumn(oldName, newName);
    });
    return 'renamed';
  }

  const conflictResult = await knex.raw(
    'SELECT COUNT(*) AS count FROM ?? WHERE ?? IS NOT NULL AND ?? IS NOT NULL AND ?? <> ??',
    [tableName, oldName, newName, oldName, newName],
  );
  const conflictCount = readSqlMigrationCount(conflictResult, dbType);
  if (conflictCount > 0) {
    throw new Error(
      `Cannot rename ${tableName}.${oldName} to ${newName}: ${conflictCount} conflicting row(s)`,
    );
  }
  await knex.raw('UPDATE ?? SET ?? = ?? WHERE ?? IS NULL', [
    tableName,
    newName,
    oldName,
    newName,
  ]);

  await dropForeignKeyIfExists(
    knex,
    tableName,
    oldName,
    normalizeSqlMigrationDbType(knex),
  );
  await knex.schema.alterTable(tableName, (table) => {
    table.dropColumn(oldName);
  });
  return 'merged';
}

function hasOwn(record: Record<string, any>, field: string): boolean {
  return Object.prototype.hasOwnProperty.call(record, field);
}

function getSqlColumnTypeDefinition(
  column: Record<string, any>,
  dbType: 'mysql' | 'postgres',
): string {
  return generateColumnDefinition(
    {
      ...column,
      isPrimary: false,
      isNullable: true,
      defaultValue: null,
    },
    dbType,
  );
}

function getSqlDefaultLiteral(
  value: any,
  type: string | undefined,
  dbType: 'mysql' | 'postgres',
): string {
  if (typeof value === 'boolean' || type === 'boolean') {
    const enabled =
      value === true ||
      value === 1 ||
      String(value).toLowerCase() === 'true' ||
      String(value) === '1';
    return dbType === 'postgres'
      ? enabled
        ? 'true'
        : 'false'
      : enabled
        ? '1'
        : '0';
  }
  if (typeof value === 'number') return String(value);
  if (
    typeof value === 'string' &&
    /^(?:CURRENT_TIMESTAMP(?:\(\))?|CURRENT_DATE|CURRENT_TIME|now\(\))$/i.test(
      value,
    )
  ) {
    return value;
  }
  const normalized =
    typeof value === 'object' ? JSON.stringify(value) : String(value);
  return `'${normalized.replace(/'/g, "''")}'`;
}

async function requireDeclaredDefaultForSqlNulls(
  knex: Knex,
  tableName: string,
  columnName: string,
  mod: ColumnModifyDef,
): Promise<boolean> {
  const result = await knex(tableName)
    .whereNull(columnName)
    .count<{ count: string | number }[]>({ count: '*' });
  if (Number(result[0]?.count ?? 0) === 0) {
    return false;
  }
  if (
    !hasOwn(mod.to, 'defaultValue') ||
    mod.to.defaultValue === null ||
    mod.to.defaultValue === undefined
  ) {
    throw new Error(
      `Cannot make ${tableName}.${columnName} non-nullable while null values exist and no target default is declared`,
    );
  }
  return true;
}

async function backfillSqlNullsWithDeclaredDefault(
  knex: Knex,
  tableName: string,
  columnName: string,
  mod: ColumnModifyDef,
  dbType: 'mysql' | 'postgres',
): Promise<void> {
  const literal = getSqlDefaultLiteral(
    mod.to.defaultValue,
    mod.to.type,
    dbType,
  );
  await knex.raw(`UPDATE ?? SET ?? = ${literal} WHERE ?? IS NULL`, [
    tableName,
    columnName,
    columnName,
  ]);
}

async function getPostgresColumnContract(
  knex: Knex,
  tableName: string,
  columnName: string,
): Promise<any> {
  const result = await knex.raw(
    `
      SELECT data_type, udt_name, is_nullable, column_default
      FROM information_schema.columns
      WHERE table_schema = current_schema()
        AND table_name = ?
        AND column_name = ?
    `,
    [tableName, columnName],
  );
  return result.rows?.[0];
}

async function applyPostgresEnumContract(
  knex: Knex,
  tableName: string,
  columnName: string,
  options: string[],
  currentUdtName: string | undefined,
): Promise<void> {
  const unsupportedValues = (
    await knex(tableName).distinct(columnName).whereNotNull(columnName)
  )
    .map((row: any) => String(row[columnName]))
    .filter((value: string) => !options.includes(value));
  if (unsupportedValues.length > 0) {
    throw new Error(
      `Cannot update ${tableName}.${columnName} enum: unsupported persisted values ${unsupportedValues.join(', ')}`,
    );
  }

  const enumType = `${tableName}_${columnName}_enum`;
  let currentOptions: string[] = [];
  if (currentUdtName) {
    const result = await knex.raw(
      `
        SELECT e.enumlabel
        FROM pg_enum e
        JOIN pg_type t ON e.enumtypid = t.oid
        WHERE t.typname = ?
        ORDER BY e.enumsortorder
      `,
      [currentUdtName],
    );
    currentOptions = result.rows.map((row: any) => row.enumlabel);
  }
  if (
    currentUdtName === enumType &&
    JSON.stringify(currentOptions) === JSON.stringify(options)
  ) {
    return;
  }

  const checkConstraints = await knex.raw(
    `
      SELECT constraint_def.conname AS constraint_name
      FROM pg_constraint constraint_def
      JOIN pg_class relation
        ON relation.oid = constraint_def.conrelid
      JOIN pg_namespace namespace
        ON namespace.oid = relation.relnamespace
      JOIN pg_attribute attribute
        ON attribute.attrelid = relation.oid
       AND attribute.attnum = ANY(constraint_def.conkey)
      WHERE constraint_def.contype = 'c'
        AND namespace.nspname = current_schema()
        AND relation.relname = ?
        AND attribute.attname = ?
    `,
    [tableName, columnName],
  );
  for (const constraint of checkConstraints.rows ?? []) {
    await knex.raw('ALTER TABLE ?? DROP CONSTRAINT ??', [
      tableName,
      constraint.constraint_name,
    ]);
  }
  await knex.raw('ALTER TABLE ?? ALTER COLUMN ?? TYPE text USING ??::text', [
    tableName,
    columnName,
    columnName,
  ]);
  await knex.raw('DROP TYPE IF EXISTS ??', [enumType]);
  const values = options
    .map((value) => `'${value.replace(/'/g, "''")}'`)
    .join(', ');
  await knex.raw(`CREATE TYPE ?? AS ENUM (${values})`, [enumType]);
  await knex.raw('ALTER TABLE ?? ALTER COLUMN ?? TYPE ?? USING ??::text::??', [
    tableName,
    columnName,
    enumType,
    columnName,
    enumType,
  ]);
}

async function applyPostgresColumnContract(
  knex: Knex,
  tableName: string,
  columnName: string,
  mod: ColumnModifyDef,
): Promise<void> {
  const current = await getPostgresColumnContract(knex, tableName, columnName);
  if (!current) {
    throw new Error(`Column ${tableName}.${columnName} does not exist`);
  }

  const typeChanged =
    (hasOwn(mod.to, 'type') &&
      JSON.stringify(mod.from.type) !== JSON.stringify(mod.to.type)) ||
    (hasOwn(mod.to, 'options') &&
      JSON.stringify(mod.from.options) !== JSON.stringify(mod.to.options));
  const nullableChanged =
    hasOwn(mod.to, 'isNullable') &&
    JSON.stringify(mod.from.isNullable) !== JSON.stringify(mod.to.isNullable);
  const defaultChanged =
    hasOwn(mod.to, 'defaultValue') &&
    JSON.stringify(mod.from.defaultValue) !==
      JSON.stringify(mod.to.defaultValue);
  const shouldBackfillNulls =
    nullableChanged && mod.to.isNullable === false
      ? await requireDeclaredDefaultForSqlNulls(
          knex,
          tableName,
          columnName,
          mod,
        )
      : false;

  if (typeChanged && current.column_default !== null) {
    await knex.raw('ALTER TABLE ?? ALTER COLUMN ?? DROP DEFAULT', [
      tableName,
      columnName,
    ]);
  }

  if (typeChanged) {
    if (mod.to.type === 'enum' && Array.isArray(mod.to.options)) {
      await applyPostgresEnumContract(
        knex,
        tableName,
        columnName,
        mod.to.options,
        current.udt_name,
      );
    } else {
      const targetType = getSqlColumnTypeDefinition(mod.to, 'postgres');
      if (mod.to.type === 'boolean') {
        await knex.raw(
          `ALTER TABLE ?? ALTER COLUMN ?? TYPE ${targetType} USING (LOWER(??::text) IN ('true', '1', 't', 'yes'))`,
          [tableName, columnName, columnName],
        );
      } else {
        await knex.raw(
          `ALTER TABLE ?? ALTER COLUMN ?? TYPE ${targetType} USING ??::text::${targetType}`,
          [tableName, columnName, columnName],
        );
      }
    }
  }

  if (shouldBackfillNulls) {
    await backfillSqlNullsWithDeclaredDefault(
      knex,
      tableName,
      columnName,
      mod,
      'postgres',
    );
  }

  if (defaultChanged || (typeChanged && current.column_default !== null)) {
    if (hasOwn(mod.to, 'defaultValue')) {
      if (mod.to.defaultValue === null || mod.to.defaultValue === undefined) {
        await knex.raw('ALTER TABLE ?? ALTER COLUMN ?? DROP DEFAULT', [
          tableName,
          columnName,
        ]);
      } else if (supportsSqlColumnDefault(mod.to, 'postgres')) {
        const literal = getSqlDefaultLiteral(
          mod.to.defaultValue,
          mod.to.type,
          'postgres',
        );
        await knex.raw(
          `ALTER TABLE ?? ALTER COLUMN ?? SET DEFAULT ${literal}`,
          [tableName, columnName],
        );
      }
    } else {
      await knex.raw(
        `ALTER TABLE ?? ALTER COLUMN ?? SET DEFAULT ${current.column_default}`,
        [tableName, columnName],
      );
    }
  }

  if (nullableChanged) {
    await knex.raw(
      `ALTER TABLE ?? ALTER COLUMN ?? ${
        mod.to.isNullable === false ? 'SET NOT NULL' : 'DROP NOT NULL'
      }`,
      [tableName, columnName],
    );
  }
}

async function applyMySqlColumnContract(
  knex: Knex,
  tableName: string,
  columnName: string,
  mod: ColumnModifyDef,
): Promise<void> {
  const result = await knex.raw(
    `
      SELECT COLUMN_TYPE, IS_NULLABLE, COLUMN_DEFAULT, EXTRA
      FROM INFORMATION_SCHEMA.COLUMNS
      WHERE TABLE_SCHEMA = DATABASE()
        AND TABLE_NAME = ?
        AND COLUMN_NAME = ?
    `,
    [tableName, columnName],
  );
  const current = result[0]?.[0];
  if (!current) {
    throw new Error(`Column ${tableName}.${columnName} does not exist`);
  }

  const targetType =
    hasOwn(mod.to, 'type') || hasOwn(mod.to, 'options')
      ? getSqlColumnTypeDefinition(mod.to, 'mysql')
      : current.COLUMN_TYPE;
  const nullable = hasOwn(mod.to, 'isNullable')
    ? mod.to.isNullable !== false
    : current.IS_NULLABLE === 'YES';
  const targetDefault = hasOwn(mod.to, 'defaultValue')
    ? mod.to.defaultValue
    : current.COLUMN_DEFAULT;
  const defaultClause =
    targetDefault === null ||
    targetDefault === undefined ||
    !supportsSqlColumnDefault(mod.to, 'mysql')
      ? ''
      : ` DEFAULT ${getSqlDefaultLiteral(targetDefault, mod.to.type, 'mysql')}`;
  const extra = String(current.EXTRA || '').includes('auto_increment')
    ? ' AUTO_INCREMENT'
    : '';
  const shouldBackfillNulls =
    hasOwn(mod.to, 'isNullable') && mod.to.isNullable === false
      ? await requireDeclaredDefaultForSqlNulls(
          knex,
          tableName,
          columnName,
          mod,
        )
      : false;

  if (shouldBackfillNulls) {
    await knex.raw(
      `ALTER TABLE ?? MODIFY COLUMN ?? ${targetType} NULL${defaultClause}${extra}`,
      [tableName, columnName],
    );
    await backfillSqlNullsWithDeclaredDefault(
      knex,
      tableName,
      columnName,
      mod,
      'mysql',
    );
  }

  await knex.raw(
    `ALTER TABLE ?? MODIFY COLUMN ?? ${targetType} ${
      nullable ? 'NULL' : 'NOT NULL'
    }${defaultClause}${extra}`,
    [tableName, columnName],
  );
}

function assertSupportedSqlColumnContract(
  tableName: string,
  mod: ColumnModifyDef,
): void {
  for (const field of ['isPrimary', 'isGenerated']) {
    if (
      hasOwn(mod.from, field) &&
      hasOwn(mod.to, field) &&
      mod.from[field] !== mod.to[field]
    ) {
      throw new Error(
        `Physical migration for ${tableName}.${mod.to.name} cannot modify ${field}`,
      );
    }
  }
}

/**
 * Apply SQL column modifications
 */
async function applySqlColumnModifications(
  knex: Knex,
  tableName: string,
  modifications: ColumnModifyDef[],
  dbType: string,
): Promise<void> {
  for (const mod of modifications) {
    if (!hasColumnChanges(mod)) {
      continue;
    }

    assertSupportedSqlColumnContract(tableName, mod);
    const oldName = mod.from.name;
    const newName = mod.to.name;

    if (oldName !== newName) {
      const result = await migrateSqlRenamedColumn(
        knex,
        tableName,
        oldName,
        newName,
        dbType,
      );
      if (result === 'renamed') {
        console.log(`  ✏️  Renamed column: ${oldName} → ${newName}`);
      } else if (result === 'merged') {
        console.log(`  ✏️  Merged duplicate column: ${oldName} → ${newName}`);
      }
    }

    if (!(await knex.schema.hasColumn(tableName, newName))) {
      throw new Error(`Column ${tableName}.${newName} does not exist`);
    }
    if (normalizeSqlMigrationDbType(knex) === 'postgres') {
      await applyPostgresColumnContract(knex, tableName, newName, mod);
    } else {
      await applyMySqlColumnContract(knex, tableName, newName, mod);
    }
  }
}

/**
 * Apply SQL column removals
 */
async function applySqlColumnRemovals(
  knex: Knex,
  tableName: string,
  columns: string[],
): Promise<void> {
  for (const colName of columns) {
    const hasColumn = await knex.schema.hasColumn(tableName, colName);
    if (hasColumn) {
      await knex.schema.alterTable(tableName, (table) => {
        table.dropColumn(colName);
      });
      console.log(`  ❌ Removed column: ${colName}`);
    }
    // Silently skip if column doesn't exist
  }
}

interface SqlForeignKeyMigrationContract {
  constraintName: string;
  targetTable: string;
  targetColumn: string;
  onDelete: string;
}

async function getSqlForeignKeyMigrationContract(
  knex: Knex,
  tableName: string,
  columnName: string,
): Promise<SqlForeignKeyMigrationContract | null> {
  if (normalizeSqlMigrationDbType(knex) === 'postgres') {
    const result = await knex.raw(
      `
        SELECT tc.constraint_name,
               ccu.table_name AS target_table,
               ccu.column_name AS target_column,
               rc.delete_rule
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
          ON tc.constraint_name = kcu.constraint_name
         AND tc.constraint_schema = kcu.constraint_schema
        JOIN information_schema.referential_constraints rc
          ON tc.constraint_name = rc.constraint_name
         AND tc.constraint_schema = rc.constraint_schema
        JOIN information_schema.constraint_column_usage ccu
          ON rc.unique_constraint_name = ccu.constraint_name
         AND rc.unique_constraint_schema = ccu.constraint_schema
        WHERE tc.constraint_type = 'FOREIGN KEY'
          AND tc.table_schema = current_schema()
          AND tc.table_name = ?
          AND kcu.column_name = ?
        LIMIT 1
      `,
      [tableName, columnName],
    );
    const row = result.rows?.[0];
    return row
      ? {
          constraintName: row.constraint_name,
          targetTable: row.target_table,
          targetColumn: row.target_column,
          onDelete: row.delete_rule,
        }
      : null;
  }

  const result = await knex.raw(
    `
      SELECT kcu.CONSTRAINT_NAME,
             kcu.REFERENCED_TABLE_NAME,
             kcu.REFERENCED_COLUMN_NAME,
             rc.DELETE_RULE
      FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
      JOIN INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS rc
        ON kcu.CONSTRAINT_SCHEMA = rc.CONSTRAINT_SCHEMA
       AND kcu.CONSTRAINT_NAME = rc.CONSTRAINT_NAME
      WHERE kcu.TABLE_SCHEMA = DATABASE()
        AND kcu.TABLE_NAME = ?
        AND kcu.COLUMN_NAME = ?
        AND kcu.REFERENCED_TABLE_NAME IS NOT NULL
      LIMIT 1
    `,
    [tableName, columnName],
  );
  const row = result[0]?.[0];
  return row
    ? {
        constraintName: row.CONSTRAINT_NAME,
        targetTable: row.REFERENCED_TABLE_NAME,
        targetColumn: row.REFERENCED_COLUMN_NAME,
        onDelete: row.DELETE_RULE,
      }
    : null;
}

async function assertSqlRelationTargetValuesExist(
  knex: Knex,
  tableName: string,
  foreignKeyColumn: string,
  targetTable: string,
  targetColumn: string,
): Promise<void> {
  if (!(await knex.schema.hasTable(targetTable))) {
    throw new Error(
      `Relation target table ${targetTable} does not exist for ${tableName}.${foreignKeyColumn}`,
    );
  }
  const result = await knex.raw(
    `
      SELECT COUNT(*) AS count
      FROM ?? source
      LEFT JOIN ?? target
        ON source.?? = target.??
      WHERE source.?? IS NOT NULL
        AND target.?? IS NULL
    `,
    [
      tableName,
      targetTable,
      foreignKeyColumn,
      targetColumn,
      foreignKeyColumn,
      targetColumn,
    ],
  );
  if (readSqlMigrationCount(result, knex.client.config.client) > 0) {
    throw new Error(
      `Cannot retarget ${tableName}.${foreignKeyColumn} to ${targetTable}.${targetColumn}: orphan values exist`,
    );
  }
}

export async function setSqlColumnNullable(
  knex: Knex,
  tableName: string,
  columnName: string,
  isNullable: boolean,
): Promise<void> {
  if (!isNullable) {
    const result = await knex(tableName)
      .whereNull(columnName)
      .count<{ count: string }[]>({ count: '*' });
    if (Number(result[0]?.count ?? 0) > 0) {
      throw new Error(
        `Cannot make physical column ${tableName}.${columnName} non-nullable while null values exist`,
      );
    }
  }
  if (normalizeSqlMigrationDbType(knex) === 'postgres') {
    await knex.raw(
      `ALTER TABLE ?? ALTER COLUMN ?? ${
        isNullable ? 'DROP NOT NULL' : 'SET NOT NULL'
      }`,
      [tableName, columnName],
    );
    return;
  }
  await applyMySqlColumnContract(knex, tableName, columnName, {
    from: { name: columnName, isNullable: !isNullable },
    to: { name: columnName, isNullable },
  });
}

async function dropSqlForeignKeyContract(
  knex: Knex,
  tableName: string,
  constraintName: string,
): Promise<void> {
  if (normalizeSqlMigrationDbType(knex) === 'postgres') {
    await knex.raw('ALTER TABLE ?? DROP CONSTRAINT ??', [
      tableName,
      constraintName,
    ]);
    return;
  }
  await knex.raw('ALTER TABLE ?? DROP FOREIGN KEY ??', [
    tableName,
    constraintName,
  ]);
}

async function applySqlRelationForeignKeyContract(
  knex: Knex,
  tableName: string,
  foreignKeyColumn: string,
  mod: RelationModifyDef,
): Promise<void> {
  const sourceType = mod.from.type;
  const targetType = mod.to.type ?? sourceType;
  const owningTypes = new Set(['many-to-one', 'one-to-one']);
  if (targetType && !owningTypes.has(targetType)) {
    if (
      sourceType &&
      owningTypes.has(sourceType) &&
      sourceType !== targetType
    ) {
      throw new Error(
        `Physical relation migration ${tableName}.${mod.to.propertyName} from ${sourceType} to ${targetType} requires explicit remove and recreate`,
      );
    }
    return;
  }
  if (
    sourceType &&
    targetType &&
    sourceType !== targetType &&
    (!owningTypes.has(sourceType) || !owningTypes.has(targetType))
  ) {
    throw new Error(
      `Unsupported physical relation type migration ${tableName}.${mod.to.propertyName}: ${sourceType} to ${targetType}`,
    );
  }
  if (!(await knex.schema.hasColumn(tableName, foreignKeyColumn))) return;

  const current = await getSqlForeignKeyMigrationContract(
    knex,
    tableName,
    foreignKeyColumn,
  );
  if (!targetType && !current) return;

  const targetTable = mod.to.targetTable ?? current?.targetTable;
  const targetColumn = mod.to.referencedColumn ?? current?.targetColumn ?? 'id';
  const onDelete = String(mod.to.onDelete ?? current?.onDelete ?? 'SET NULL');
  if (!targetTable) {
    throw new Error(
      `Relation migration ${tableName}.${mod.to.propertyName} must declare targetTable`,
    );
  }
  await assertSqlRelationTargetValuesExist(
    knex,
    tableName,
    foreignKeyColumn,
    targetTable,
    targetColumn,
  );

  const constraintName =
    mod.to.constraintName ??
    current?.constraintName ??
    `${tableName}_${foreignKeyColumn}_foreign`;
  const foreignKeyChanged =
    !current ||
    current.targetTable !== targetTable ||
    current.targetColumn !== targetColumn ||
    current.onDelete.toUpperCase() !== onDelete.toUpperCase() ||
    (hasOwn(mod.to, 'constraintName') &&
      current.constraintName !== constraintName);
  if (current && foreignKeyChanged) {
    await dropSqlForeignKeyContract(knex, tableName, current.constraintName);
  }

  if (
    hasOwn(mod.to, 'isNullable') &&
    mod.from.isNullable !== mod.to.isNullable
  ) {
    await setSqlColumnNullable(
      knex,
      tableName,
      foreignKeyColumn,
      mod.to.isNullable !== false,
    );
  }

  if (!foreignKeyChanged) return;

  await knex.schema.alterTable(tableName, (table) => {
    table
      .foreign(foreignKeyColumn, constraintName)
      .references(targetColumn)
      .inTable(targetTable)
      .onDelete(onDelete);
  });
}

/**
 * Apply SQL relation modifications (FK columns)
 */
async function applySqlRelationModifications(
  knex: Knex,
  tableName: string,
  modifications: RelationModifyDef[],
  dbType: string,
): Promise<void> {
  for (const mod of modifications) {
    if (!hasRelationChanges(mod)) {
      continue;
    }

    const oldName = mod.from.propertyName;
    const newName = mod.to.propertyName;
    const oldFkColumn =
      mod.from.foreignKeyColumn || getForeignKeyColumnName(oldName);
    const newFkColumn =
      mod.to.foreignKeyColumn || getForeignKeyColumnName(newName);
    const oldColumnExists = await knex.schema.hasColumn(tableName, oldFkColumn);

    if (oldColumnExists && mod.to.targetTable) {
      await assertSqlRelationTargetValuesExist(
        knex,
        tableName,
        oldFkColumn,
        mod.to.targetTable,
        mod.to.referencedColumn || mod.from.referencedColumn || 'id',
      );
    }
    if (
      oldColumnExists &&
      mod.to.isNullable === false &&
      mod.from.isNullable !== false
    ) {
      const result = await knex(tableName)
        .whereNull(oldFkColumn)
        .count<{ count: string }[]>({ count: '*' });
      if (Number(result[0]?.count ?? 0) > 0) {
        throw new Error(
          `Cannot make relation ${tableName}.${newFkColumn} non-nullable while null values exist`,
        );
      }
    }

    if (oldFkColumn !== newFkColumn) {
      const result = await migrateSqlRenamedColumn(
        knex,
        tableName,
        oldFkColumn,
        newFkColumn,
        dbType,
      );
      if (result !== 'none') {
        console.log(
          `  ✏️  Renamed relation FK: ${oldFkColumn} → ${newFkColumn}`,
        );
      }
    }
    await applySqlRelationForeignKeyContract(knex, tableName, newFkColumn, mod);
  }
}

/**
 * Apply SQL relation removals (FK columns)
 */
async function applySqlRelationRemovals(
  knex: Knex,
  tableName: string,
  relations: string[],
): Promise<void> {
  const dbType = knex.client.config.client;

  for (const relName of relations) {
    if (await cleanupSqlRemovedRelation(knex, tableName, relName)) {
      continue;
    }
    const fkColumn = getForeignKeyColumnName(relName);
    const hasColumn = await knex.schema.hasColumn(tableName, fkColumn);

    if (hasColumn) {
      if (dbType === 'pg') {
        const fkConstraints = await knex.raw(
          `
            SELECT tc.constraint_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
              ON tc.constraint_name = kcu.constraint_name
            WHERE tc.table_schema = 'public'
              AND tc.table_name = ?
              AND kcu.column_name = ?
              AND tc.constraint_type = 'FOREIGN KEY'
          `,
          [tableName, fkColumn],
        );
        if (fkConstraints.rows?.length > 0) {
          await knex.raw(
            `ALTER TABLE "${tableName}" DROP CONSTRAINT "${fkConstraints.rows[0].constraint_name}"`,
          );
        }
      } else {
        const fkConstraints = await knex.raw(
          `
            SELECT CONSTRAINT_NAME
            FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
            WHERE TABLE_SCHEMA = DATABASE()
              AND TABLE_NAME = ?
              AND COLUMN_NAME = ?
              AND REFERENCED_TABLE_NAME IS NOT NULL
          `,
          [tableName, fkColumn],
        );
        if (fkConstraints[0]?.length > 0) {
          await knex.raw(
            `ALTER TABLE \`${tableName}\` DROP FOREIGN KEY \`${fkConstraints[0][0].CONSTRAINT_NAME}\``,
          );
        }
      }

      await knex.schema.alterTable(tableName, (table) => {
        table.dropColumn(fkColumn);
      });
      console.log(`  ❌ Removed relation: ${relName} (FK: ${fkColumn})`);
    }
    // Silently skip if relation FK column doesn't exist
  }
}

/**
 * Apply MongoDB schema migrations (physical database)
 */
export async function applyMongoSchemaMigrations(
  db: Db,
  migration: SchemaMigrationDef,
  options: MongoPhysicalMigrationOptions = {},
): Promise<void> {
  if (migration.tablesToDrop && migration.tablesToDrop.length > 0) {
    console.log(
      `🗑️ Dropping ${migration.tablesToDrop.length} collection(s)...`,
    );
    for (const collectionName of migration.tablesToDrop) {
      await cleanupMongoTableDependencies(db, collectionName);
      const collections = await db
        .listCollections({ name: collectionName })
        .toArray();
      if (collections.length > 0) {
        await db.dropCollection(collectionName);
        console.log(`  ✅ Dropped collection: ${collectionName}`);
      }
    }
  }

  for (const tableMigration of migration.tables || []) {
    await applyMongoCollectionMigration(db, tableMigration, options);
  }
}

function sameMongoId(left: any, right: any): boolean {
  if (
    left === undefined ||
    left === null ||
    right === undefined ||
    right === null
  )
    return false;
  return String(left) === String(right);
}

async function getMongoMigrationMetadata(db: Db): Promise<{
  tables: any[];
  relations: any[];
  tableById: Map<string, any>;
}> {
  const [tableStore, relationStore] = await Promise.all([
    db.listCollections({ name: 'enfyra_table' }).toArray(),
    db.listCollections({ name: 'enfyra_relation' }).toArray(),
  ]);
  const tables =
    tableStore.length > 0
      ? await db.collection('enfyra_table').find({}).toArray()
      : [];
  const relations =
    relationStore.length > 0
      ? await db.collection('enfyra_relation').find({}).toArray()
      : [];
  return {
    tables,
    relations,
    tableById: new Map(tables.map((table) => [String(table._id), table])),
  };
}

async function dropMongoIndexesContainingField(
  db: Db,
  collectionName: string,
  fieldName: string,
): Promise<void> {
  const collections = await db
    .listCollections({ name: collectionName })
    .toArray();
  if (collections.length === 0) return;

  let indexes: any[] = [];
  try {
    indexes = await db.collection(collectionName).listIndexes().toArray();
  } catch (error: any) {
    if (error?.code === 26 || error?.codeName === 'NamespaceNotFound') return;
    throw error;
  }
  for (const index of indexes) {
    if (index.name === '_id_' || !index.key || !(fieldName in index.key))
      continue;
    await db.collection(collectionName).dropIndex(index.name);
  }
}

function renameMongoIndexObjectKeys(
  value: any,
  oldFieldName: string,
  newFieldName: string,
): any {
  if (Array.isArray(value)) {
    return value.map((item) =>
      renameMongoIndexObjectKeys(item, oldFieldName, newFieldName),
    );
  }
  if (!value || typeof value !== 'object') return value;
  return Object.fromEntries(
    Object.entries(value).map(([key, item]) => [
      key === oldFieldName ? newFieldName : key,
      renameMongoIndexObjectKeys(item, oldFieldName, newFieldName),
    ]),
  );
}

async function renameMongoIndexesContainingField(
  db: Db,
  collectionName: string,
  oldFieldName: string,
  newFieldName: string,
): Promise<void> {
  const collections = await db
    .listCollections({ name: collectionName })
    .toArray();
  if (collections.length === 0) return;

  const indexes = await db.collection(collectionName).listIndexes().toArray();
  for (const index of indexes) {
    if (index.name === '_id_' || !index.key || !(oldFieldName in index.key)) {
      continue;
    }
    const options = Object.fromEntries(
      [
        'unique',
        'sparse',
        'expireAfterSeconds',
        'partialFilterExpression',
        'collation',
        'hidden',
        'weights',
        'default_language',
        'language_override',
      ]
        .filter((key) => index[key] !== undefined)
        .map((key) => [
          key,
          renameMongoIndexObjectKeys(index[key], oldFieldName, newFieldName),
        ]),
    );
    const newIndexName = String(index.name).replaceAll(
      oldFieldName,
      newFieldName,
    );
    await db.collection(collectionName).dropIndex(index.name);
    await db
      .collection(collectionName)
      .createIndex(
        renameMongoIndexObjectKeys(index.key, oldFieldName, newFieldName),
        {
          ...options,
          name:
            newIndexName === index.name
              ? `${index.name}_${newFieldName}`
              : newIndexName,
        },
      );
  }
}

async function migrateMongoRenamedField(
  db: Db,
  collectionName: string,
  oldFieldName: string,
  newFieldName: string,
): Promise<number> {
  const collection = db.collection(collectionName);
  const conflictCount = await collection.countDocuments({
    [oldFieldName]: { $exists: true },
    [newFieldName]: { $exists: true },
    $expr: { $ne: [`$${oldFieldName}`, `$${newFieldName}`] },
  });
  if (conflictCount > 0) {
    throw new Error(
      `Cannot rename ${collectionName}.${oldFieldName} to ${newFieldName}: ${conflictCount} conflicting document(s)`,
    );
  }
  const copied = await collection.updateMany(
    {
      [oldFieldName]: { $exists: true },
      [newFieldName]: { $exists: false },
    },
    [{ $set: { [newFieldName]: `$${oldFieldName}` } }],
  );

  await collection.updateMany(
    { [oldFieldName]: { $exists: true } },
    { $unset: { [oldFieldName]: '' } },
  );
  await renameMongoIndexesContainingField(
    db,
    collectionName,
    oldFieldName,
    newFieldName,
  );
  return copied.modifiedCount;
}

async function unsetMongoPhysicalField(
  db: Db,
  collectionName: string,
  fieldName: string,
): Promise<void> {
  const collections = await db
    .listCollections({ name: collectionName })
    .toArray();
  if (collections.length === 0) return;
  await dropMongoIndexesContainingField(db, collectionName, fieldName);
  await db
    .collection(collectionName)
    .updateMany(
      { [fieldName]: { $exists: true } },
      { $unset: { [fieldName]: '' } },
    );
}

async function dropMongoCollectionIfExists(
  db: Db,
  collectionName: string,
): Promise<void> {
  const collections = await db
    .listCollections({ name: collectionName })
    .toArray();
  if (collections.length === 0) return;
  await db.dropCollection(collectionName);
}

async function cleanupMongoRelationPhysical(
  db: Db,
  relation: any,
  tableById: Map<string, any>,
  droppedJunctions: Set<string>,
): Promise<void> {
  const sourceTable = tableById.get(String(relation.sourceTable));
  if (!sourceTable?.name) return;

  if (relation.type === 'many-to-many') {
    const junctionName = relation.junctionTableName;
    if (junctionName && !droppedJunctions.has(junctionName)) {
      await dropMongoCollectionIfExists(db, junctionName);
      droppedJunctions.add(junctionName);
    }
    await unsetMongoPhysicalField(db, sourceTable.name, relation.propertyName);
    return;
  }

  const isOwning =
    !relation.mappedBy &&
    (relation.type === 'many-to-one' || relation.type === 'one-to-one');
  if (!isOwning) return;

  await unsetMongoPhysicalField(
    db,
    sourceTable.name,
    relation.foreignKeyColumn || relation.propertyName,
  );
}

async function cleanupMongoTableDependencies(
  db: Db,
  tableName: string,
): Promise<void> {
  const metadata = await getMongoMigrationMetadata(db);
  const table = metadata.tables.find((item) => item.name === tableName);
  if (!table) return;

  const droppedJunctions = new Set<string>();
  for (const relation of metadata.relations) {
    if (
      !sameMongoId(relation.sourceTable, table._id) &&
      !sameMongoId(relation.targetTable, table._id)
    ) {
      continue;
    }
    await cleanupMongoRelationPhysical(
      db,
      relation,
      metadata.tableById,
      droppedJunctions,
    );
  }
}

async function cleanupMongoRemovedRelation(
  db: Db,
  tableName: string,
  propertyName: string,
): Promise<void> {
  const metadata = await getMongoMigrationMetadata(db);
  const table = metadata.tables.find((item) => item.name === tableName);
  const relation = table
    ? metadata.relations.find(
        (item) =>
          sameMongoId(item.sourceTable, table._id) &&
          item.propertyName === propertyName,
      )
    : null;
  if (!relation) {
    await unsetMongoPhysicalField(db, tableName, propertyName);
    return;
  }
  await cleanupMongoRelationPhysical(
    db,
    relation,
    metadata.tableById,
    new Set<string>(),
  );
}

/**
 * Apply MongoDB collection migration
 */
async function applyMongoCollectionMigration(
  db: Db,
  migration: TableMigrationDef,
  options: MongoPhysicalMigrationOptions,
): Promise<void> {
  const collectionName = migration._unique.name._eq;
  const preservedFields = new Set(
    options.preserveFieldsByCollection?.[collectionName] ?? [],
  );
  const collections = await db
    .listCollections({ name: collectionName })
    .toArray();

  if (collections.length === 0) {
    // Silently skip if collection doesn't exist
    return;
  }

  console.log(`🔄 Migrating collection: ${collectionName}`);
  const collection = db.collection(collectionName);

  // Handle column modifications (field rename)
  if (migration.columnsToModify && migration.columnsToModify.length > 0) {
    for (const mod of migration.columnsToModify) {
      // Skip if no actual changes detected
      if (!hasColumnChanges(mod)) {
        continue;
      }

      const oldName = mod.from.name;
      const newName = mod.to.name;

      if (oldName !== newName && !preservedFields.has(oldName)) {
        const modifiedCount = await migrateMongoRenamedField(
          db,
          collectionName,
          oldName,
          newName,
        );
        if (modifiedCount > 0) {
          console.log(
            `  ✏️  Renamed field: ${oldName} → ${newName} (${modifiedCount} documents)`,
          );
        }
      }
    }
  }

  // Handle column removals
  if (migration.columnsToRemove && migration.columnsToRemove.length > 0) {
    for (const fieldName of migration.columnsToRemove) {
      if (preservedFields.has(fieldName)) continue;
      await unsetMongoPhysicalField(db, collectionName, fieldName);
      console.log(`  ❌ Removed field: ${fieldName}`);
    }
  }

  // Handle relation modifications (field rename)
  if (migration.relationsToModify && migration.relationsToModify.length > 0) {
    for (const mod of migration.relationsToModify) {
      // Skip if no actual changes detected
      if (!hasRelationChanges(mod)) {
        continue;
      }

      const oldName = mod.from.propertyName;
      const newName = mod.to.propertyName;

      if (oldName !== newName && !preservedFields.has(oldName)) {
        const modifiedCount = await migrateMongoRenamedField(
          db,
          collectionName,
          oldName,
          newName,
        );
        if (modifiedCount > 0) {
          console.log(
            `  ✏️  Renamed relation field: ${oldName} → ${newName} (${modifiedCount} documents)`,
          );
        }
      }
    }
  }

  if (migration.relationsToRemove && migration.relationsToRemove.length > 0) {
    const toRemove = migration.relationsToRemove.filter(
      (field) => !preservedFields.has(field),
    );
    if (
      collectionName === 'enfyra_file_permission' &&
      toRemove.includes('allowedUsers')
    ) {
      const cursor = collection.find({
        allowedUsers: { $exists: true, $not: { $type: 'array' } },
      });
      let count = 0;
      for await (const doc of cursor) {
        await collection.updateOne(
          { _id: doc._id },
          {
            $set: {
              allowedUsers: Array.isArray(doc.allowedUsers)
                ? doc.allowedUsers
                : [doc.allowedUsers],
            },
          },
        );
        count++;
      }
      if (count > 0) {
        console.log(
          `  📦 Converted allowedUsers to array (${count} documents)`,
        );
      }
      toRemove.splice(toRemove.indexOf('allowedUsers'), 1);
    }
    for (const relName of toRemove) {
      await cleanupMongoRemovedRelation(db, collectionName, relName);
    }
  }

  await applyMongoTableConstraintMigrations(
    db,
    collectionName,
    migration.tableToModify,
  );
}

async function applyMongoTableConstraintMigrations(
  db: Db,
  collectionName: string,
  modification: TableMigrationDef['tableToModify'],
): Promise<void> {
  if (!modification) return;

  const fromUniques = normalizeConstraintGroups(modification.from?.uniques);
  const toUniqueKeys = new Set(
    normalizeConstraintGroups(modification.to?.uniques).map(constraintGroupKey),
  );
  const removedUniqueKeys = new Set(
    fromUniques
      .map(constraintGroupKey)
      .filter((key) => !toUniqueKeys.has(key)),
  );
  if (removedUniqueKeys.size === 0) return;

  const indexes = await db.collection(collectionName).listIndexes().toArray();
  for (const index of indexes) {
    if (
      index.name === '_id_' ||
      index.unique !== true ||
      !index.key ||
      !removedUniqueKeys.has(constraintGroupKey(Object.keys(index.key)))
    ) {
      continue;
    }
    await db.collection(collectionName).dropIndex(index.name);
  }
}
