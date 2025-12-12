import { Knex } from 'knex';
import { getForeignKeyColumnName } from '../../../src/infrastructure/knex/utils/naming-helpers';
import { KnexTableSchema } from '../../../src/shared/types/database-init.types';
import { getKnexColumnType, getPrimaryKeyType } from './schema-parser';
import { compareSchemas, getCurrentDatabaseSchema, isTypeCompatible } from './schema-comparison';

export async function applyColumnMigrations(
  knex: Knex,
  tableName: string,
  diff: ReturnType<typeof compareSchemas>,
  schemas: KnexTableSchema[],
): Promise<void> {
  const dbType = knex.client.config.client;

  if (diff.columnsToAdd.length > 0) {
    console.log(`  📝 Adding ${diff.columnsToAdd.length} column(s) to ${tableName}:`);
    for (const col of diff.columnsToAdd) {
      console.log(`    + ${col.name} (${col.type})`);
    }

    await knex.schema.alterTable(tableName, (table) => {
      for (const col of diff.columnsToAdd) {
        let column: Knex.ColumnBuilder;
        const knexType = getKnexColumnType(col);

        switch (knexType) {
          case 'integer':
            column = table.integer(col.name);
            break;
          case 'bigInteger':
            column = table.bigInteger(col.name);
            break;
          case 'string':
            column = table.string(col.name, 255);
            break;
          case 'text':
            column = table.text(col.name);
            break;
          case 'boolean':
            column = table.boolean(col.name);
            break;
          case 'uuid':
            column = table.uuid(col.name);
            break;
          case 'timestamp':
            column = table.timestamp(col.name);
            break;
          case 'datetime':
            column = table.datetime(col.name);
            break;
          case 'json':
            column = table.json(col.name);
            break;
          case 'enum':
            if (Array.isArray(col.options)) {
              column = table.enum(col.name, col.options);
            } else {
              column = table.text(col.name);
            }
            break;
          default:
            column = table.text(col.name);
        }

        if (col.isNullable === false) {
          column.notNullable();
        } else {
          column.nullable();
        }

        if (col.defaultValue !== undefined && col.defaultValue !== null) {
          if (col.type === 'boolean') {
            let defVal: any = col.defaultValue;
            if (typeof defVal === 'number') defVal = defVal === 1;
            else if (typeof defVal === 'string') {
              const t = defVal.trim().toLowerCase();
              if (t === '1' || t === 'true') defVal = true;
              else if (t === '0' || t === 'false') defVal = false;
            }
            column.defaultTo(!!defVal);
          } else {
          column.defaultTo(col.defaultValue);
          }
        }

        if (col.isUnique) {
          column.unique();
        }

        if (col.type === 'datetime' || col.type === 'timestamp' || col.type === 'date') {
          table.index([col.name]);
        }
      }
    });
  }

  if (diff.columnsToRemove.length > 0) {
    console.log(`  🗑️  Removing ${diff.columnsToRemove.length} column(s) from ${tableName}:`);
    for (const colName of diff.columnsToRemove) {
      console.log(`    - ${colName}`);
    }

    for (const colName of diff.columnsToRemove) {
      try {
        if (dbType === 'mysql2') {
          const fkConstraints = await knex.raw(`
            SELECT CONSTRAINT_NAME
            FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
            WHERE TABLE_SCHEMA = DATABASE()
              AND TABLE_NAME = ?
              AND COLUMN_NAME = ?
              AND REFERENCED_TABLE_NAME IS NOT NULL
          `, [tableName, colName]);

          for (const row of fkConstraints[0]) {
            const constraintName = row.CONSTRAINT_NAME;
            console.log(`    ⚠️  Dropping FK constraint: ${constraintName}`);
            await knex.raw(`ALTER TABLE \`${tableName}\` DROP FOREIGN KEY \`${constraintName}\``);
          }

          const uniqueConstraints = await knex.raw(`
            SELECT DISTINCT CONSTRAINT_NAME
            FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
            WHERE TABLE_SCHEMA = DATABASE()
              AND TABLE_NAME = ?
              AND COLUMN_NAME = ?
              AND CONSTRAINT_NAME != 'PRIMARY'
              AND REFERENCED_TABLE_NAME IS NULL
          `, [tableName, colName]);

          for (const row of uniqueConstraints[0]) {
            const constraintName = row.CONSTRAINT_NAME;
            console.log(`    ⚠️  Dropping UNIQUE constraint/index: ${constraintName}`);
            try {
              await knex.raw(`ALTER TABLE \`${tableName}\` DROP INDEX \`${constraintName}\``);
            } catch (err) {
            }
          }
        } else if (dbType === 'pg') {
          const fkConstraints = await knex.raw(`
            SELECT tc.constraint_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
              ON tc.constraint_name = kcu.constraint_name
            WHERE tc.table_schema = 'public'
              AND tc.table_name = ?
              AND kcu.column_name = ?
              AND tc.constraint_type = 'FOREIGN KEY'
          `, [tableName, colName]);

          for (const row of fkConstraints.rows) {
            const constraintName = row.constraint_name;
            console.log(`    ⚠️  Dropping FK constraint: ${constraintName}`);
            await knex.raw(`ALTER TABLE "${tableName}" DROP CONSTRAINT "${constraintName}"`);
          }

          const uniqueConstraints = await knex.raw(`
            SELECT tc.constraint_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
              ON tc.constraint_name = kcu.constraint_name
            WHERE tc.table_schema = 'public'
              AND tc.table_name = ?
              AND kcu.column_name = ?
              AND tc.constraint_type = 'UNIQUE'
          `, [tableName, colName]);

          for (const row of uniqueConstraints.rows) {
            const constraintName = row.constraint_name;
            console.log(`    ⚠️  Dropping UNIQUE constraint: ${constraintName}`);
            try {
              await knex.raw(`ALTER TABLE "${tableName}" DROP CONSTRAINT "${constraintName}"`);
            } catch (err) {
            }
          }
        }
      } catch (error) {
        console.log(`    ⚠️  Failed to drop constraints for ${colName}: ${error.message}`);
      }
    }

    await knex.schema.alterTable(tableName, (table) => {
      for (const colName of diff.columnsToRemove) {
        table.dropColumn(colName);
      }
    });
  }

  if (diff.columnsToModify.length > 0) {
    console.log(`  ✏️  Modifying ${diff.columnsToModify.length} column(s) in ${tableName}:`);
    for (const { column: col, changes } of diff.columnsToModify) {
      console.log(`    ~ ${col.name} (${changes.join(', ')})`);
    }

    if (dbType === 'mysql2') {
      for (const { column: col, changes } of diff.columnsToModify) {
        if (changes.includes('enum-options') && col.type === 'enum' && Array.isArray(col.options)) {
          const currentEnumResult = await knex.raw(`
            SELECT COLUMN_TYPE
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = DATABASE()
              AND TABLE_NAME = ?
              AND COLUMN_NAME = ?
          `, [tableName, col.name]);
          
          if (currentEnumResult[0]?.length > 0) {
            const currentColumnType = currentEnumResult[0][0].COLUMN_TYPE;
            const enumMatch = currentColumnType.match(/^enum\((.+)\)$/i);
            const currentEnumValues = enumMatch 
              ? enumMatch[1].split(',').map((val: string) => val.trim().replace(/^'|'$/g, ''))
              : [];
            const newEnumValues = col.options || [];
            
            const valueMap: Record<string, string> = {};
            for (const oldVal of currentEnumValues) {
              const match = newEnumValues.find((newVal: string) => 
                newVal.toLowerCase() === oldVal.toLowerCase()
              );
              if (match && oldVal !== match) {
                valueMap[oldVal] = match;
              }
            }
            
            for (const [oldVal, newVal] of Object.entries(valueMap)) {
              await knex(tableName).where(col.name, oldVal).update({ [col.name]: newVal });
            }
            
            const enumValues = newEnumValues.map((val: string) => `'${val.replace(/'/g, "''")}'`).join(',');
            const nullable = col.isNullable === false ? 'NOT NULL' : 'NULL';
            const defaultValue = col.defaultValue ? `DEFAULT '${col.defaultValue.replace(/'/g, "''")}'` : '';
            
            await knex.raw(`ALTER TABLE \`${tableName}\` MODIFY COLUMN \`${col.name}\` ENUM(${enumValues}) ${nullable} ${defaultValue}`.trim());
            
            continue;
          }
        }
      }
      
      for (const { column: col, changes } of diff.columnsToModify) {
        if (changes.includes('enum-options')) {
          continue;
        }
        
        const knexType = getKnexColumnType(col);
        let sqlType = knexType;

        const typeMap: Record<string, string> = {
          'integer': 'INT',
          'bigInteger': 'BIGINT',
          'string': 'VARCHAR(255)',
          'text': 'TEXT',
          'boolean': 'TINYINT(1)',
          'uuid': 'CHAR(36)',
          'timestamp': 'TIMESTAMP',
          'datetime': 'DATETIME',
          'json': 'JSON',
        };

        sqlType = typeMap[knexType] || 'TEXT';
        const nullable = col.isNullable === false ? 'NOT NULL' : 'NULL';

        await knex.raw(`ALTER TABLE \`${tableName}\` MODIFY COLUMN \`${col.name}\` ${sqlType} ${nullable}`);
      }
    } else {
      for (const { column: col, changes } of diff.columnsToModify) {
        if (changes.includes('enum-options') && col.type === 'enum' && Array.isArray(col.options)) {
          const enumTypeResult = await knex.raw(`
            SELECT udt_name
            FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name = ?
              AND column_name = ?
          `, [tableName, col.name]);
          
          if (enumTypeResult.rows.length > 0) {
            const oldEnumType = enumTypeResult.rows[0].udt_name;
            const newEnumType = `${tableName}_${col.name}_enum`;
            
            const currentEnumResult = await knex.raw(`
              SELECT e.enumlabel
              FROM pg_enum e
              JOIN pg_type t ON e.enumtypid = t.oid
              WHERE t.typname = ?
              ORDER BY e.enumsortorder
            `, [oldEnumType]);
            
            const currentEnumValues = currentEnumResult.rows.map((r: any) => r.enumlabel);
            const newEnumValues = col.options || [];
            
            const valueMap: Record<string, string> = {};
            for (const oldVal of currentEnumValues) {
              const match = newEnumValues.find((newVal: string) => 
                newVal.toLowerCase() === oldVal.toLowerCase()
              );
              if (match && oldVal !== match) {
                valueMap[oldVal] = match;
              }
            }
            
            const defaultResult = await knex.raw(`
              SELECT column_default
              FROM information_schema.columns
              WHERE table_schema = 'public'
                AND table_name = ?
                AND column_name = ?
            `, [tableName, col.name]);
            
            const currentDefault = defaultResult.rows[0]?.column_default;
            const hasDefault = !!currentDefault;
            
            if (hasDefault) {
              await knex.raw(`ALTER TABLE "${tableName}" ALTER COLUMN "${col.name}" DROP DEFAULT`);
            }
            
            await knex.raw(`ALTER TABLE "${tableName}" ALTER COLUMN "${col.name}" TYPE text USING "${col.name}"::text`);
            
            for (const [oldVal, newVal] of Object.entries(valueMap)) {
              await knex(tableName).where(col.name, oldVal).update({ [col.name]: newVal });
            }
            
            try {
              await knex.raw(`ALTER TABLE "${tableName}" DROP CONSTRAINT IF EXISTS "${tableName}_${col.name}_check"`);
            } catch (e) {}
            
            const enumValues = newEnumValues.map((val: string) => `'${val.replace(/'/g, "''")}'`).join(', ');
            await knex.raw(`DROP TYPE IF EXISTS "${newEnumType}" CASCADE`);
            await knex.raw(`CREATE TYPE "${newEnumType}" AS ENUM (${enumValues})`);
            
            await knex.raw(`ALTER TABLE "${tableName}" ALTER COLUMN "${col.name}" TYPE "${newEnumType}" USING "${col.name}"::"${newEnumType}"`);
            
            if (hasDefault) {
              let defaultVal = currentDefault;
              if (defaultVal && defaultVal.includes('::')) {
                defaultVal = defaultVal.split('::')[0];
              }
              defaultVal = defaultVal?.replace(/^'|'$/g, '');
              
              if (defaultVal && valueMap[defaultVal]) {
                defaultVal = valueMap[defaultVal];
              } else if (col.defaultValue) {
                defaultVal = col.defaultValue;
              }
              
              if (defaultVal) {
                await knex.raw(`ALTER TABLE "${tableName}" ALTER COLUMN "${col.name}" SET DEFAULT '${defaultVal.replace(/'/g, "''")}'`);
              }
            } else if (col.defaultValue) {
              await knex.raw(`ALTER TABLE "${tableName}" ALTER COLUMN "${col.name}" SET DEFAULT '${col.defaultValue.replace(/'/g, "''")}'`);
            }
            
            if (oldEnumType !== newEnumType) {
              try {
                await knex.raw(`DROP TYPE IF EXISTS "${oldEnumType}" CASCADE`);
              } catch (e) {}
            }
            
            continue;
          }
        }
      }
      
        for (const { column: col, changes } of diff.columnsToModify) {
          if (changes.includes('enum-options')) {
            continue;
          }
          
          const knexType = getKnexColumnType(col);
        
        const currentTypeResult = await knex.raw(`
          SELECT data_type, udt_name
          FROM information_schema.columns
          WHERE table_schema = 'public'
            AND table_name = ?
            AND column_name = ?
        `, [tableName, col.name]);
        
        const currentDataType = currentTypeResult.rows[0]?.data_type;
        const currentUdtName = currentTypeResult.rows[0]?.udt_name;
        const isCurrentJson = currentDataType === 'jsonb' || currentUdtName === 'jsonb';
        const isCurrentText = currentDataType === 'text' || currentUdtName === 'text';
        
        if (knexType === 'json') {
          if (isCurrentText) {
            await knex.raw(`ALTER TABLE "${tableName}" ALTER COLUMN "${col.name}" TYPE jsonb USING "${col.name}"::jsonb`);
          } else if (!isCurrentJson) {
            await knex.raw(`ALTER TABLE "${tableName}" ALTER COLUMN "${col.name}" TYPE jsonb USING "${col.name}"::jsonb`);
          }
        } else if (knexType === 'text') {
          if (isCurrentJson) {
            await knex.raw(`ALTER TABLE "${tableName}" ALTER COLUMN "${col.name}" TYPE text USING "${col.name}"::text`);
          } else if (!isCurrentText) {
            await knex.raw(`ALTER TABLE "${tableName}" ALTER COLUMN "${col.name}" TYPE text USING "${col.name}"::text`);
          }
        } else {
          await knex.schema.alterTable(tableName, (table) => {
          let column: Knex.ColumnBuilder;

          switch (knexType) {
            case 'integer':
              column = table.integer(col.name).alter();
              break;
            case 'string':
              column = table.string(col.name, 255).alter();
              break;
            default:
                return;
          }

          if (col.isNullable === false) {
            column.notNullable();
          } else {
            column.nullable();
          }
      });
        }
        
        if (changes.includes('nullable')) {
          if (col.isNullable === false) {
            await knex.raw(`ALTER TABLE "${tableName}" ALTER COLUMN "${col.name}" SET NOT NULL`);
          } else {
            await knex.raw(`ALTER TABLE "${tableName}" ALTER COLUMN "${col.name}" DROP NOT NULL`);
          }
        }
      }
    }
  }
}

export async function applyRelationMigrations(
  knex: Knex,
  tableName: string,
  diff: ReturnType<typeof compareSchemas>,
  schemas: KnexTableSchema[],
): Promise<void> {
  if (diff.relationsToRemove.length > 0) {
    console.log(`  🗑️  Removing ${diff.relationsToRemove.length} relation(s) from ${tableName}:`);
    const dbType = knex.client.config.client;

    for (const fkColumn of diff.relationsToRemove) {
      console.log(`    - ${fkColumn}`);

      if (dbType === 'mysql2') {
        const fkConstraints = await knex.raw(`
          SELECT CONSTRAINT_NAME
          FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
          WHERE TABLE_SCHEMA = DATABASE()
            AND TABLE_NAME = ?
            AND COLUMN_NAME = ?
            AND REFERENCED_TABLE_NAME IS NOT NULL
        `, [tableName, fkColumn]);

        if (fkConstraints[0]?.length > 0) {
          const constraintName = fkConstraints[0][0].CONSTRAINT_NAME;
          await knex.raw(`ALTER TABLE \`${tableName}\` DROP FOREIGN KEY \`${constraintName}\``);
        }
      } else if (dbType === 'pg') {
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
          const constraintName = fkConstraints.rows[0].constraint_name;
          await knex.raw(`ALTER TABLE "${tableName}" DROP CONSTRAINT "${constraintName}"`);
        }
      }

      await knex.schema.alterTable(tableName, (table) => {
        table.dropColumn(fkColumn);
      });
    }
  }

  if (diff.relationsToAdd.length > 0) {
    const m2oRelations = diff.relationsToAdd.filter(r => {
      if (r.type === 'many-to-one') return true;
      if (r.type === 'one-to-one' && !(r as any)._isInverseGenerated) return true;
      return false;
    });

    if (m2oRelations.length > 0) {
      console.log(`  📝 Adding ${m2oRelations.length} relation(s) to ${tableName}:`);

      for (const rel of m2oRelations) {
        const fkColumn = getForeignKeyColumnName(rel.propertyName);
        console.log(`    + ${fkColumn} → ${rel.targetTable}.id`);

        const targetPkType = getPrimaryKeyType(schemas, rel.targetTable);
        const dbType = knex.client.config.client;

        await knex.schema.alterTable(tableName, (table) => {
          let col;
          if (targetPkType === 'uuid') {
            if (dbType === 'pg') {
              col = table.uuid(fkColumn);
            } else {
              col = table.string(fkColumn, 36);
            }
          } else {
            col = table.integer(fkColumn).unsigned();
          }

          if (rel.isNullable === false) {
            col.notNullable();
          } else {
            col.nullable();
          }
        });

        await knex.schema.alterTable(tableName, (table) => {
          const fk = table
            .foreign(fkColumn)
            .references('id')
            .inTable(rel.targetTable);

          const onDeleteAction = (rel as any).onDelete || 'SET NULL';
          fk.onDelete(onDeleteAction).onUpdate('CASCADE');

          table.index([fkColumn]);
        });
      }
    }
  }
}

export async function applyIndexAndUniqueMigrations(
  knex: Knex,
  tableName: string,
  diff: ReturnType<typeof compareSchemas>,
): Promise<void> {
  const normalizeCols = (cols: string[] | string): string[] => {
    if (Array.isArray(cols)) return cols;
    return String(cols || '')
      .split(',')
      .map((c) => c.trim())
      .filter(Boolean);
  };

  if (
    diff.uniquesToAdd.length === 0 &&
    diff.uniquesToRemove.length === 0 &&
    diff.indexesToAdd.length === 0 &&
    diff.indexesToRemove.length === 0
  ) {
    return;
  }

  console.log(`  ⚙️  Syncing uniques/indexes for ${tableName}`);

  if (diff.uniquesToRemove.length > 0) {
    for (const u of diff.uniquesToRemove) {
      const colsArr = normalizeCols(u.columns);
      try {
        await knex.schema.alterTable(tableName, (table) => {
          if (u.name) {
            table.dropUnique(colsArr, u.name);
          } else {
            table.dropUnique(colsArr);
          }
        });
        console.log(`    - Dropped UNIQUE (${colsArr.join(', ')})`);
      } catch (err: any) {
        console.log(`    ⚠️  Failed to drop UNIQUE (${colsArr.join(', ')}): ${err?.message}`);
      }
    }
  }

  if (diff.indexesToRemove.length > 0) {
    for (const idx of diff.indexesToRemove) {
      const colsArr = normalizeCols(idx.columns);
      try {
        await knex.schema.alterTable(tableName, (table) => {
          if (idx.name) {
            table.dropIndex(colsArr, idx.name);
          } else {
            table.dropIndex(colsArr);
          }
        });
        console.log(`    - Dropped INDEX (${colsArr.join(', ')})`);
      } catch (err: any) {
        console.log(`    ⚠️  Failed to drop INDEX (${colsArr.join(', ')}): ${err?.message}`);
      }
    }
  }

  if (diff.uniquesToAdd.length > 0) {
    for (const cols of diff.uniquesToAdd) {
      const colsArr = normalizeCols(cols);
      try {
        await knex.schema.alterTable(tableName, (table) => {
          table.unique(colsArr);
        });
        console.log(`    + Added UNIQUE (${colsArr.join(', ')})`);
      } catch (err: any) {
        console.log(`    ⚠️  Failed to add UNIQUE (${colsArr.join(', ')}): ${err?.message}`);
      }
    }
  }

  if (diff.indexesToAdd.length > 0) {
    for (const cols of diff.indexesToAdd) {
      const colsArr = normalizeCols(cols);
      try {
        await knex.schema.alterTable(tableName, (table) => {
          table.index(colsArr);
        });
        console.log(`    + Added INDEX (${colsArr.join(', ')})`);
      } catch (err: any) {
        console.log(`    ⚠️  Failed to add INDEX (${colsArr.join(', ')}): ${err?.message}`);
      }
    }
  }
}

export async function syncTable(
  knex: Knex,
  schema: KnexTableSchema,
  schemas: KnexTableSchema[],
): Promise<void> {
  const { tableName } = schema;

  const currentSchema = await getCurrentDatabaseSchema(knex, tableName);

  const diff = compareSchemas(schema, currentSchema);

  const hasChanges =
    diff.columnsToAdd.length > 0 ||
    diff.columnsToRemove.length > 0 ||
    diff.columnsToModify.length > 0 ||
    diff.relationsToAdd.length > 0 ||
    diff.relationsToRemove.length > 0 ||
    diff.uniquesToAdd.length > 0 ||
    diff.uniquesToRemove.length > 0 ||
    diff.indexesToAdd.length > 0 ||
    diff.indexesToRemove.length > 0;

  if (!hasChanges) {
    console.log(`⏩ No changes for table: ${tableName}`);
    return;
  }

  console.log(`🔄 Syncing table: ${tableName}`);

  await applyColumnMigrations(knex, tableName, diff, schemas);
  await applyRelationMigrations(knex, tableName, diff, schemas);
  await applyIndexAndUniqueMigrations(knex, tableName, diff);
  await syncRelationOnDeleteChanges(knex, tableName, schema);

  console.log(`✅ Synced table: ${tableName}`);
}

async function syncRelationOnDeleteChanges(
  knex: Knex,
  tableName: string,
  schema: KnexTableSchema,
): Promise<void> {
  const dbType = knex.client.config.client;

  // Ensure onDelete column exists in relation_definition table
  const hasOnDeleteColumn = await knex.schema.hasColumn('relation_definition', 'onDelete');
  if (!hasOnDeleteColumn) {
    // Column doesn't exist yet, create it first
    await knex.schema.alterTable('relation_definition', (table) => {
      table.enum('onDelete', ['CASCADE', 'RESTRICT', 'SET NULL']).notNullable().defaultTo('SET NULL');
    });
  }

  // Load current relations from database
  const tableDefRow = await knex('table_definition')
    .where('name', tableName)
    .first();

  if (!tableDefRow) {
    return;
  }

  const dbRelations = await knex('relation_definition')
    .where('sourceTableId', tableDefRow.id)
    .select('id', 'propertyName', 'type', 'onDelete');

  const snapshotRelations = schema.definition.relations || [];

  for (const snapshotRel of snapshotRelations) {
    const dbRel = dbRelations.find(r => r.propertyName === snapshotRel.propertyName);

    if (!dbRel) {
      continue; // New relation, will be handled by applyRelationMigrations
    }

    const snapshotOnDelete = (snapshotRel as any).onDelete || 'SET NULL';
    const dbOnDelete = dbRel.onDelete || 'SET NULL';

    if (snapshotOnDelete !== dbOnDelete) {
      console.log(`  🔄 Updating onDelete for ${snapshotRel.propertyName}: ${dbOnDelete} → ${snapshotOnDelete}`);

      // Only handle M2O and O2O relations (FK in current table)
      if (snapshotRel.type === 'many-to-one' || snapshotRel.type === 'one-to-one') {
        const fkColumn = getForeignKeyColumnName(snapshotRel.propertyName);
        const targetTable = snapshotRel.targetTable;

        // Drop existing FK constraint
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
            const constraintName = fkConstraints.rows[0].constraint_name;
            await knex.raw(`ALTER TABLE "${tableName}" DROP CONSTRAINT "${constraintName}"`);
            console.log(`    Dropped FK constraint: ${constraintName}`);
          }
        } else if (dbType === 'mysql2') {
          const fkConstraints = await knex.raw(`
            SELECT CONSTRAINT_NAME
            FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
            WHERE TABLE_SCHEMA = DATABASE()
              AND TABLE_NAME = ?
              AND COLUMN_NAME = ?
              AND REFERENCED_TABLE_NAME IS NOT NULL
          `, [tableName, fkColumn]);

          if (fkConstraints[0]?.length > 0) {
            const constraintName = fkConstraints[0][0].CONSTRAINT_NAME;
            await knex.raw(`ALTER TABLE \`${tableName}\` DROP FOREIGN KEY \`${constraintName}\``);
            console.log(`    Dropped FK constraint: ${constraintName}`);
          }
        }

        // Recreate FK constraint with new onDelete action
        await knex.schema.alterTable(tableName, (table) => {
          const fk = table
            .foreign(fkColumn)
            .references('id')
            .inTable(targetTable);

          fk.onDelete(snapshotOnDelete).onUpdate('CASCADE');
        });

        console.log(`    Recreated FK constraint with onDelete: ${snapshotOnDelete}`);

        // Update relation_definition table
        await knex('relation_definition')
          .where('id', dbRel.id)
          .update({ onDelete: snapshotOnDelete });

        console.log(`    Updated relation_definition.onDelete to: ${snapshotOnDelete}`);
      }
    }
  }
}

