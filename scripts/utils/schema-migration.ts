import { Knex } from 'knex';
import { Db } from 'mongodb';
import { SchemaMigrationDef, TableMigrationDef, ColumnModifyDef, RelationModifyDef } from '../../src/shared/types/schema-migration.types';
import {
  getForeignKeyColumnName,
  getJunctionTableName,
  getJunctionColumnNames,
} from '../../src/infrastructure/knex/utils/sql-schema-naming.util';
import * as fs from 'fs';
import * as path from 'path';

export function loadSchemaMigration(): SchemaMigrationDef | null {
  try {
    const filePath = path.join(process.cwd(), 'data/snapshot-migration.json');
    if (fs.existsSync(filePath)) {
      const content = fs.readFileSync(filePath, 'utf8');
      const parsed = JSON.parse(content);
      if (parsed && (parsed.tables?.length > 0 || parsed.tablesToDrop?.length > 0)) {
        return parsed;
      }
    }
    return null;
  } catch (error) {
    console.warn(`⚠️ Failed to load snapshot-migration.json: ${error.message}`);
    return null;
  }
}

export function hasSchemaMigrations(migration: SchemaMigrationDef | null): boolean {
  if (!migration) return false;
  return (
    (migration.tables?.length > 0) ||
    (migration.tablesToDrop?.length > 0)
  );
}

/**
 * Apply SQL schema migrations (physical database)
 */
export async function applySqlSchemaMigrations(
  knex: Knex,
  migration: SchemaMigrationDef
): Promise<void> {
  const dbType = knex.client.config.client;

  // Drop tables
  if (migration.tablesToDrop?.length > 0) {
    console.log(`🗑️ Dropping ${migration.tablesToDrop.length} table(s)...`);
    for (const tableName of migration.tablesToDrop) {
      const exists = await knex.schema.hasTable(tableName);
      if (exists) {
        if (dbType === 'pg' || dbType === 'postgres' || dbType === 'postgresql') {
          await knex.raw(`DROP TABLE IF EXISTS "${tableName}" CASCADE`);
        } else if (dbType === 'mysql' || dbType === 'mysql2' || dbType === 'mariadb') {
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

  // Apply table migrations
  for (const tableMigration of migration.tables || []) {
    await applySqlTableMigration(knex, tableMigration, dbType);
  }
}

/**
 * Apply SQL table migration
 */
async function applySqlTableMigration(
  knex: Knex,
  migration: TableMigrationDef,
  dbType: string
): Promise<void> {
  const tableName = migration._unique.name._eq;
  const exists = await knex.schema.hasTable(tableName);

  if (!exists) {
    console.log(`  ⏩ Table ${tableName} does not exist, skipping migration`);
    return;
  }

  console.log(`🔄 Migrating table: ${tableName}`);

  // Handle column modifications (including rename)
  if (migration.columnsToModify?.length > 0) {
    await applySqlColumnModifications(knex, tableName, migration.columnsToModify, dbType);
  }

  // Handle column removals
  if (migration.columnsToRemove?.length > 0) {
    await applySqlColumnRemovals(knex, tableName, migration.columnsToRemove);
  }

  // Handle relation modifications (including rename)
  if (migration.relationsToModify?.length > 0) {
    await applySqlRelationModifications(knex, tableName, migration.relationsToModify, dbType);
  }

  if (migration.relationsToRemove?.length > 0) {
    if (
      tableName === 'file_permission_definition' &&
      migration.relationsToRemove.includes('allowedUsers')
    ) {
      await migrateFilePermissionAllowedUsersToJunction(knex, dbType);
    }
    await applySqlRelationRemovals(knex, tableName, migration.relationsToRemove);
  }
}

async function migrateFilePermissionAllowedUsersToJunction(
  knex: Knex,
  dbType: string
): Promise<void> {
  const tableName = 'file_permission_definition';
  const fkColumn = getForeignKeyColumnName('allowedUsers');
  const hasOldColumn = await knex.schema.hasColumn(tableName, fkColumn);
  if (!hasOldColumn) return;

  const junctionTableName = getJunctionTableName(
    tableName,
    'allowedUsers',
    'user_definition'
  );
  const { sourceColumn, targetColumn } = getJunctionColumnNames(
    tableName,
    'allowedUsers',
    'user_definition'
  );

  const exists = await knex.schema.hasTable(junctionTableName);
  if (exists) {
    console.log(`  ⏩ Junction ${junctionTableName} already exists, skipping data migration`);
    return;
  }

  console.log(`  📦 Migrating file_permission_definition.allowedUsers to junction ${junctionTableName}`);
  const pkType = await getPrimaryKeyType(knex, tableName);
  const targetPkType = await getPrimaryKeyType(knex, 'user_definition');

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
    table.foreign(targetColumn).references('id').inTable('user_definition');
  });

  const rows = await knex(tableName).select('id', fkColumn).whereNotNull(fkColumn);
  for (const row of rows) {
    await knex(junctionTableName).insert({
      [sourceColumn]: row.id,
      [targetColumn]: row[fkColumn],
    });
  }
  console.log(`  ✅ Created junction and migrated ${rows.length} row(s): ${junctionTableName}`);
}

async function getPrimaryKeyType(knex: Knex, tableName: string): Promise<'int' | 'uuid'> {
  const result = await knex(tableName).limit(1).select(knex.raw('1 as _'));
  const tableInfo = await knex.raw(
    knex.client.config.client === 'pg'
      ? `SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = 'public' AND table_name = ? AND column_name = 'id'`
      : `SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? AND COLUMN_NAME = 'id'`,
    [tableName]
  );
  const rows = knex.client.config.client === 'pg' ? (tableInfo as any).rows : (tableInfo as any)[0];
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
  // Check name change
  if (mod.from.name !== mod.to.name) return true;

  // Check type change
  if (mod.from.type !== undefined && mod.to.type !== undefined) {
    if (mod.from.type !== mod.to.type) return true;
  }

  // Check options change (for enum types)
  if (mod.from.options !== undefined && mod.to.options !== undefined) {
    const fromOptions = Array.isArray(mod.from.options) ? mod.from.options : [];
    const toOptions = Array.isArray(mod.to.options) ? mod.to.options : [];
    if (fromOptions.length !== toOptions.length || !fromOptions.every((v, i) => v === toOptions[i])) {
      return true;
    }
  }

  // Check nullable change
  if (mod.from.isNullable !== undefined && mod.to.isNullable !== undefined) {
    if (mod.from.isNullable !== mod.to.isNullable) return true;
  }

  // Check other property changes
  if (mod.from.isUpdatable !== undefined && mod.to.isUpdatable !== undefined) {
    if (mod.from.isUpdatable !== mod.to.isUpdatable) return true;
  }

  return false;
}

/**
 * Check if relation modification has actual changes
 */
function hasRelationChanges(mod: RelationModifyDef): boolean {
  // Check propertyName change
  if (mod.from.propertyName !== mod.to.propertyName) return true;

  // Check other property changes
  if (mod.from.isNullable !== undefined && mod.to.isNullable !== undefined) {
    if (mod.from.isNullable !== mod.to.isNullable) return true;
  }

  return false;
}

/**
 * Apply SQL column modifications
 */
async function applySqlColumnModifications(
  knex: Knex,
  tableName: string,
  modifications: ColumnModifyDef[],
  dbType: string
): Promise<void> {
  for (const mod of modifications) {
    // Skip if no actual changes detected
    if (!hasColumnChanges(mod)) {
      continue;
    }

    const oldName = mod.from.name;
    const newName = mod.to.name;

    // Check if rename is needed
    if (oldName !== newName) {
      const hasOldColumn = await knex.schema.hasColumn(tableName, oldName);
      const hasNewColumn = await knex.schema.hasColumn(tableName, newName);

      if (hasOldColumn && !hasNewColumn) {
        await knex.schema.alterTable(tableName, (table) => {
          table.renameColumn(oldName, newName);
        });
        console.log(`  ✏️  Renamed column: ${oldName} → ${newName}`);
      }
      // Silently skip if already renamed or old column doesn't exist
    }

    // Handle other property changes
    const targetColumn = newName;
    const hasColumn = await knex.schema.hasColumn(tableName, targetColumn);

    if (hasColumn) {
      // Handle type change (including varchar/text to enum conversion)
      if (mod.from.type !== undefined && mod.to.type !== undefined && mod.from.type !== mod.to.type) {
        if (mod.to.type === 'enum' && Array.isArray(mod.to.options)) {
          // Check actual current type in database
          const currentTypeResult = await knex.raw(`
            SELECT data_type, udt_name
            FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name = ?
              AND column_name = ?
          `, [tableName, targetColumn]);
          const currentDbType = currentTypeResult.rows[0]?.udt_name || currentTypeResult.rows[0]?.data_type;

          // Skip if already an ENUM type (migration already applied)
          if (currentDbType && currentDbType.endsWith('_enum')) {
            console.log(`  ⏩ Column ${targetColumn} is already ENUM (${currentDbType}), skipping migration`);
          } else {
            console.log(`  ✏️  Converting column type: ${targetColumn} from ${currentDbType || mod.from.type} to ENUM...`);
            const enumValues = mod.to.options.map((val: string) => `'${val.replace(/'/g, "''")}'`).join(', ');
            const newEnumType = `${tableName}_${targetColumn}_enum`;

            if (dbType === 'pg') {
              try {
                // Get current default if exists
                const defaultResult = await knex.raw(`
                  SELECT column_default, is_nullable
                  FROM information_schema.columns
                  WHERE table_schema = 'public'
                    AND table_name = ?
                    AND column_name = ?
                `, [tableName, targetColumn]);
                const currentDefault = defaultResult.rows[0]?.column_default;
                const isNullable = defaultResult.rows[0]?.is_nullable === 'YES';

                // Drop default if exists
                if (currentDefault) {
                  await knex.raw(`ALTER TABLE "${tableName}" ALTER COLUMN "${targetColumn}" DROP DEFAULT`);
                }

                // Convert to text first if not already
                if (currentDbType !== 'text') {
                  await knex.raw(`ALTER TABLE "${tableName}" ALTER COLUMN "${targetColumn}" TYPE text USING "${targetColumn}"::text`);
                }

                // Create ENUM type and apply
                await knex.raw(`DROP TYPE IF EXISTS "${newEnumType}" CASCADE`);
                await knex.raw(`CREATE TYPE "${newEnumType}" AS ENUM (${enumValues})`);
                await knex.raw(`ALTER TABLE "${tableName}" ALTER COLUMN "${targetColumn}" TYPE "${newEnumType}" USING "${targetColumn}"::"${newEnumType}"`);

                // Restore default if needed
                if (currentDefault) {
                  let defaultVal = currentDefault;
                  if (defaultVal && defaultVal.includes('::')) {
                    defaultVal = defaultVal.split('::')[0];
                  }
                  defaultVal = defaultVal?.replace(/^'|'$/g, '');
                  if (defaultVal && mod.to.options.includes(defaultVal)) {
                    await knex.raw(`ALTER TABLE "${tableName}" ALTER COLUMN "${targetColumn}" SET DEFAULT '${defaultVal.replace(/'/g, "''")}'`);
                  }
                }

                // Set nullable if needed
                if (mod.to.isNullable === false) {
                  await knex.raw(`ALTER TABLE "${tableName}" ALTER COLUMN "${targetColumn}" SET NOT NULL`);
                } else if (isNullable && mod.from.isNullable === false) {
                  await knex.raw(`ALTER TABLE "${tableName}" ALTER COLUMN "${targetColumn}" DROP NOT NULL`);
                }

                console.log(`  ✅ Converted ${targetColumn} to ENUM(${mod.to.options.join(', ')})`);
              } catch (error) {
                console.log(`  ⚠️  Failed to convert ${targetColumn} to ENUM: ${error.message}`);
              }
            } else {
              // MySQL
              try {
                const nullable = mod.to.isNullable === false ? 'NOT NULL' : 'NULL';
                const defaultValue = mod.to.defaultValue ? `DEFAULT '${mod.to.defaultValue}'` : '';
                await knex.raw(`ALTER TABLE \`${tableName}\` MODIFY COLUMN \`${targetColumn}\` ENUM(${enumValues}) ${nullable} ${defaultValue}`.trim());
                console.log(`  ✅ Converted ${targetColumn} to ENUM(${mod.to.options.join(', ')})`);
              } catch (error) {
                console.log(`  ⚠️  Failed to convert ${targetColumn} to ENUM: ${error.message}`);
              }
            }
          }
        } else {
          // Other type conversions
          console.log(`  ✏️  Type change detected: ${mod.from.type} → ${mod.to.type} for ${targetColumn}`);
        }
      }

      // Handle nullable change
      if (mod.from.isNullable !== undefined && mod.to.isNullable !== undefined) {
        if (mod.from.isNullable !== mod.to.isNullable) {
          try {
            if (dbType === 'pg') {
              if (mod.to.isNullable === false) {
                await knex.raw(`ALTER TABLE "${tableName}" ALTER COLUMN "${targetColumn}" SET NOT NULL`);
              } else {
                await knex.raw(`ALTER TABLE "${tableName}" ALTER COLUMN "${targetColumn}" DROP NOT NULL`);
              }
              console.log(`  ✏️  Modified column nullable: ${targetColumn}`);
            } else {
              console.log(`  ✏️  Modified column nullable: ${targetColumn} (MySQL requires full column definition)`);
            }
          } catch {
            // Silently skip if modification fails (column may have been modified already)
          }
        }
      }
    }
  }
}

/**
 * Apply SQL column removals
 */
async function applySqlColumnRemovals(
  knex: Knex,
  tableName: string,
  columns: string[]
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

/**
 * Apply SQL relation modifications (FK columns)
 */
async function applySqlRelationModifications(
  knex: Knex,
  tableName: string,
  modifications: RelationModifyDef[],
  dbType: string
): Promise<void> {
  for (const mod of modifications) {
    // Skip if no actual changes detected
    if (!hasRelationChanges(mod)) {
      continue;
    }

    const oldName = mod.from.propertyName;
    const newName = mod.to.propertyName;

    // Check if rename is needed
    if (oldName !== newName) {
      const oldFkColumn = getForeignKeyColumnName(oldName);
      const newFkColumn = getForeignKeyColumnName(newName);

      const hasOldColumn = await knex.schema.hasColumn(tableName, oldFkColumn);
      const hasNewColumn = await knex.schema.hasColumn(tableName, newFkColumn);

      if (hasOldColumn && !hasNewColumn) {
        // Drop FK constraint first
        try {
          if (dbType === 'pg') {
            const fkConstraints = await knex.raw(`
              SELECT tc.constraint_name
              FROM information_schema.table_constraints tc
              JOIN information_schema.key_column_usage kcu
                ON tc.constraint_name = kcu.constraint_name
              WHERE tc.table_schema = 'public'
                AND tc.table_name = ?
                AND kcu.column_name = ?
                AND tc.constraint_type = 'FOREIGN KEY'
            `, [tableName, oldFkColumn]);
            if (fkConstraints.rows?.length > 0) {
              await knex.raw(`ALTER TABLE "${tableName}" DROP CONSTRAINT "${fkConstraints.rows[0].constraint_name}"`);
            }
          } else {
            const fkConstraints = await knex.raw(`
              SELECT CONSTRAINT_NAME
              FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
              WHERE TABLE_SCHEMA = DATABASE()
                AND TABLE_NAME = ?
                AND COLUMN_NAME = ?
                AND REFERENCED_TABLE_NAME IS NOT NULL
            `, [tableName, oldFkColumn]);
            if (fkConstraints[0]?.length > 0) {
              await knex.raw(`ALTER TABLE \`${tableName}\` DROP FOREIGN KEY \`${fkConstraints[0][0].CONSTRAINT_NAME}\``);
            }
          }
        } catch {
          // FK might not exist, continue silently
        }

        // Rename column
        await knex.schema.alterTable(tableName, (table) => {
          table.renameColumn(oldFkColumn, newFkColumn);
        });
        console.log(`  ✏️  Renamed relation FK: ${oldFkColumn} → ${newFkColumn}`);
      }
      // Silently skip if already renamed or old column doesn't exist
    }
  }
}

/**
 * Apply SQL relation removals (FK columns)
 */
async function applySqlRelationRemovals(
  knex: Knex,
  tableName: string,
  relations: string[]
): Promise<void> {
  const dbType = knex.client.config.client;

  for (const relName of relations) {
    const fkColumn = getForeignKeyColumnName(relName);
    const hasColumn = await knex.schema.hasColumn(tableName, fkColumn);

    if (hasColumn) {
      // Drop FK constraint first
      try {
        if (dbType === 'pg') {
          const fkConstraints = await knex.raw(`
            SELECT tc.constraint_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
              ON tc.constraint_name = kcu.constraint_name
            WHERE tc.table_schema = 'public'
              AND tc.table_name = ?
              AND kcu.column_name = ?
              AND tc.constraint_type = 'FOREIGN KEY'
          `, [tableName, fkColumn]);
          if (fkConstraints.rows?.length > 0) {
            await knex.raw(`ALTER TABLE "${tableName}" DROP CONSTRAINT "${fkConstraints.rows[0].constraint_name}"`);
          }
        } else {
          const fkConstraints = await knex.raw(`
            SELECT CONSTRAINT_NAME
            FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
            WHERE TABLE_SCHEMA = DATABASE()
              AND TABLE_NAME = ?
              AND COLUMN_NAME = ?
              AND REFERENCED_TABLE_NAME IS NOT NULL
          `, [tableName, fkColumn]);
          if (fkConstraints[0]?.length > 0) {
            await knex.raw(`ALTER TABLE \`${tableName}\` DROP FOREIGN KEY \`${fkConstraints[0][0].CONSTRAINT_NAME}\``);
          }
        }
      } catch {
        // FK might not exist, continue silently
      }

      // Drop column
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
  migration: SchemaMigrationDef
): Promise<void> {
  // Drop collections
  if (migration.tablesToDrop?.length > 0) {
    console.log(`🗑️ Dropping ${migration.tablesToDrop.length} collection(s)...`);
    for (const collectionName of migration.tablesToDrop) {
      const collections = await db.listCollections({ name: collectionName }).toArray();
      if (collections.length > 0) {
        await db.dropCollection(collectionName);
        console.log(`  ✅ Dropped collection: ${collectionName}`);
      }
      // Silently skip if collection doesn't exist
    }
  }

  // Apply collection migrations
  for (const tableMigration of migration.tables || []) {
    await applyMongoCollectionMigration(db, tableMigration);
  }
}

/**
 * Apply MongoDB collection migration
 */
async function applyMongoCollectionMigration(
  db: Db,
  migration: TableMigrationDef
): Promise<void> {
  const collectionName = migration._unique.name._eq;
  const collections = await db.listCollections({ name: collectionName }).toArray();

  if (collections.length === 0) {
    // Silently skip if collection doesn't exist
    return;
  }

  console.log(`🔄 Migrating collection: ${collectionName}`);
  const collection = db.collection(collectionName);

  // Handle column modifications (field rename)
  if (migration.columnsToModify?.length > 0) {
    for (const mod of migration.columnsToModify) {
      // Skip if no actual changes detected
      if (!hasColumnChanges(mod)) {
        continue;
      }

      const oldName = mod.from.name;
      const newName = mod.to.name;

      if (oldName !== newName) {
        try {
          const result = await collection.updateMany(
            { [oldName]: { $exists: true } },
            { $rename: { [oldName]: newName } }
          );
          if (result.modifiedCount > 0) {
            console.log(`  ✏️  Renamed field: ${oldName} → ${newName} (${result.modifiedCount} documents)`);
          }
        } catch {
          // Silently skip if rename fails (field may not exist)
        }
      }
    }
  }

  // Handle column removals
  if (migration.columnsToRemove?.length > 0) {
    for (const fieldName of migration.columnsToRemove) {
      try {
        const result = await collection.updateMany(
          { [fieldName]: { $exists: true } },
          { $unset: { [fieldName]: '' } }
        );
        if (result.modifiedCount > 0) {
          console.log(`  ❌ Removed field: ${fieldName} (${result.modifiedCount} documents)`);
        }
      } catch {
        // Silently skip if removal fails
      }
    }
  }

  // Handle relation modifications (field rename)
  if (migration.relationsToModify?.length > 0) {
    for (const mod of migration.relationsToModify) {
      // Skip if no actual changes detected
      if (!hasRelationChanges(mod)) {
        continue;
      }

      const oldName = mod.from.propertyName;
      const newName = mod.to.propertyName;

      if (oldName !== newName) {
        try {
          const result = await collection.updateMany(
            { [oldName]: { $exists: true } },
            { $rename: { [oldName]: newName } }
          );
          if (result.modifiedCount > 0) {
            console.log(`  ✏️  Renamed relation field: ${oldName} → ${newName} (${result.modifiedCount} documents)`);
          }
        } catch {
          // Silently skip if rename fails
        }
      }
    }
  }

  if (migration.relationsToRemove?.length > 0) {
    const toRemove = [...migration.relationsToRemove];
    if (
      collectionName === 'file_permission_definition' &&
      toRemove.includes('allowedUsers')
    ) {
      try {
        const cursor = collection.find({
          allowedUsers: { $exists: true, $not: { $type: 'array' } },
        });
        let count = 0;
        for await (const doc of cursor) {
          await collection.updateOne(
            { _id: doc._id },
            { $set: { allowedUsers: Array.isArray(doc.allowedUsers) ? doc.allowedUsers : [doc.allowedUsers] } }
          );
          count++;
        }
        if (count > 0) {
          console.log(`  📦 Converted allowedUsers to array (${count} documents)`);
        }
        toRemove.splice(toRemove.indexOf('allowedUsers'), 1);
      } catch {
        toRemove.splice(toRemove.indexOf('allowedUsers'), 1);
      }
    }
    for (const relName of toRemove) {
      try {
        const result = await collection.updateMany(
          { [relName]: { $exists: true } },
          { $unset: { [relName]: '' } }
        );
        if (result.modifiedCount > 0) {
          console.log(`  ❌ Removed relation field: ${relName} (${result.modifiedCount} documents)`);
        }
      } catch {
        // Silently skip if removal fails
      }
    }
  }
}