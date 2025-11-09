import 'reflect-metadata';
import * as fs from 'fs';
import * as path from 'path';
import * as dotenv from 'dotenv';
import { Knex, knex } from 'knex';
import {
  getJunctionTableName,
  getForeignKeyColumnName,
  getShortFkName,
  getShortIndexName,
  getShortPkName,
  getShortFkConstraintName,
} from '../src/infrastructure/knex/utils/naming-helpers';
import {
  ColumnDef,
  RelationDef,
  TableDef,
  JunctionTableDef,
  KnexTableSchema,
} from '../src/shared/types/database-init.types';

dotenv.config();



function parseSnapshotToSchema(snapshot: Record<string, any>): KnexTableSchema[] {
  const schemas: KnexTableSchema[] = [];
  const inverseRelationsToAdd: Array<{ tableName: string; relation: any }> = [];

  for (const [tableName, def] of Object.entries(snapshot)) {
    const tableDef = def as TableDef;

    if (tableDef.relations) {
      for (const relation of tableDef.relations) {
        if (relation.inversePropertyName) {
          let inverseType = relation.type;
          if (relation.type === 'many-to-one') {
            inverseType = 'one-to-many';
          } else if (relation.type === 'one-to-many') {
            inverseType = 'many-to-one';
          }

          inverseRelationsToAdd.push({
            tableName: relation.targetTable,
            relation: {
              propertyName: relation.inversePropertyName,
              type: inverseType,
              targetTable: tableName,
              inversePropertyName: relation.propertyName,
              isSystem: relation.isSystem,
              isNullable: relation.isNullable,
              _isInverseGenerated: true,
            },
          });
        }
      }
    }

    schemas.push({
      tableName,
      definition: { ...tableDef },
      junctionTables: [],
    });
  }

  for (const { tableName, relation } of inverseRelationsToAdd) {
    const schema = schemas.find((s) => s.tableName === tableName);
    if (schema) {
      if (!schema.definition.relations) {
        schema.definition.relations = [];
      }
      const exists = schema.definition.relations.some(
        (r) => r.propertyName === relation.propertyName,
      );
      if (!exists) {
        schema.definition.relations.push(relation);
      }
    }
  }

  const createdJunctionNames = new Set<string>();

  for (const schema of schemas) {
    const { tableName, definition } = schema;
    const junctionTables: JunctionTableDef[] = [];

    if (definition.relations) {
      for (const relation of definition.relations) {
        if ((relation as any)._isInverseGenerated) {
          continue;
        }

        if (relation.type === 'many-to-many') {
          const junctionTableName = getJunctionTableName(
            tableName,
            relation.propertyName,
            relation.targetTable,
          );

          const reverseJunctionName = getJunctionTableName(
            relation.targetTable,
            relation.inversePropertyName || 'inverse',
            tableName,
          );

          if (createdJunctionNames.has(junctionTableName) || createdJunctionNames.has(reverseJunctionName)) {
            continue;
          }

          junctionTables.push({
            tableName: junctionTableName,
            sourceTable: tableName,
            targetTable: relation.targetTable,
            sourceColumn: getForeignKeyColumnName(tableName),
            targetColumn: getForeignKeyColumnName(relation.targetTable),
            sourcePropertyName: relation.propertyName,
          });

          createdJunctionNames.add(junctionTableName);
          createdJunctionNames.add(reverseJunctionName);
        }
      }
    }

    schema.junctionTables = junctionTables;
  }

  return schemas;
}


function getKnexColumnType(columnDef: ColumnDef): string {
  const typeMap: Record<string, string> = {
    int: 'integer',
    integer: 'integer',
    bigint: 'bigInteger',
    smallint: 'smallint',
    uuid: 'uuid',
    varchar: 'string',
    text: 'text',
    boolean: 'boolean',
    bool: 'boolean',
    date: 'timestamp',
    datetime: 'datetime',
    timestamp: 'timestamp',
    'simple-json': 'json',
    richtext: 'text',
    code: 'text',
    'array-select': 'json',
    enum: 'enum',
  };

  return typeMap[columnDef.type] || 'text';
}


function getPrimaryKeyType(schemas: KnexTableSchema[], tableName: string): 'uuid' | 'integer' {
  const schema = schemas.find(s => s.tableName === tableName);
  if (!schema) return 'integer';

  const pkColumn = schema.definition.columns.find(c => c.isPrimary);
  if (!pkColumn) return 'integer';

  return pkColumn.type === 'uuid' ? 'uuid' : 'integer';
}

async function createTable(
  knex: Knex,
  schema: KnexTableSchema,
  dbType: string,
  schemas: KnexTableSchema[],
): Promise<void> {
  const { tableName, definition } = schema;

  console.log(`📝 Creating table: ${tableName}`);

  await knex.schema.createTable(tableName, (table) => {
    for (const col of definition.columns) {
      let column: Knex.ColumnBuilder;

      const knexType = getKnexColumnType(col);

      if (col.isPrimary && col.isGenerated) {
        if (col.type === 'uuid') {
          if (dbType === 'postgres') {
            column = table.uuid(col.name).primary().defaultTo(knex.raw('gen_random_uuid()'));
          } else {
            column = table.uuid(col.name).primary();
          }
        } else {
          column = table.increments(col.name).primary();
        }
      }
      else if (col.type === 'enum' && Array.isArray(col.options)) {
        column = table.enum(col.name, col.options);
      }
      else {
        switch (knexType) {
          case 'integer':
            column = table.integer(col.name);
            break;
          case 'bigInteger':
            column = table.bigInteger(col.name);
            break;
          case 'smallint':
            column = table.integer(col.name);
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
          default:
            column = table.text(col.name);
        }
      }

      if (col.isNullable === false) {
        column.notNullable();
      } else {
        column.nullable();
      }

      if (col.defaultValue !== undefined && col.defaultValue !== null) {
        if (typeof col.defaultValue === 'string' && col.defaultValue.toLowerCase() === 'now') {
          column.defaultTo(knex.fn.now());
        } else {
          column.defaultTo(col.defaultValue);
        }
      }

      if (col.isUnique) {
        column.unique();
      }

      if (col.description) {
        column.comment(col.description);
      }
    }

    if (definition.relations) {
      for (const relation of definition.relations) {
        if (relation.type === 'many-to-many' || relation.type === 'one-to-many') {
          continue;
        }

        if (relation.type === 'one-to-one' && (relation as any)._isInverseGenerated) {
          continue;
        }

        const foreignKeyColumn = getForeignKeyColumnName(relation.propertyName);

        const targetPkType = getPrimaryKeyType(schemas, relation.targetTable);

        let col;
        if (targetPkType === 'uuid') {
          if (dbType === 'postgres') {
            col = table.uuid(foreignKeyColumn);
          } else {
            col = table.string(foreignKeyColumn, 36);
          }
        } else {
          col = table.integer(foreignKeyColumn).unsigned();
        }

        if (relation.isNullable === false) {
          col.notNullable();
        } else {
          col.nullable();
        }

      }
    }

    if (dbType === 'postgres') {
      table.timestamp('createdAt', { useTz: true }).defaultTo(knex.fn.now());
      table.timestamp('updatedAt', { useTz: true }).defaultTo(knex.fn.now());
    } else {
      table.timestamp('createdAt').defaultTo(knex.fn.now());
      table.timestamp('updatedAt').defaultTo(knex.fn.now());
    }

    if (definition.uniques && definition.uniques.length > 0) {
      for (const uniqueGroup of definition.uniques) {
        if (Array.isArray(uniqueGroup) && uniqueGroup.length > 0) {
          const columnNames = uniqueGroup.map((fieldName) => {
            const relation = definition.relations?.find(r => r.propertyName === fieldName);
            if (relation) {
              return getForeignKeyColumnName(relation.propertyName);
            }
            return fieldName;
          });
          table.unique(columnNames);
        }
      }
    }

    if (definition.indexes && definition.indexes.length > 0) {
      for (const indexGroup of definition.indexes) {
        if (Array.isArray(indexGroup) && indexGroup.length > 0) {
          const columnNames = indexGroup.map((fieldName) => {
            const relation = definition.relations?.find(r => r.propertyName === fieldName);
            if (relation) {
              return getForeignKeyColumnName(relation.propertyName);
            }
            return fieldName;
          });
          table.index(columnNames);
        }
      }
    }

    table.index(['createdAt']);
    table.index(['updatedAt']);
    table.index(['createdAt', 'updatedAt']);

    const timestampFields = definition.columns.filter(col =>
      col.type === 'datetime' || col.type === 'timestamp' || col.type === 'date'
    );

    for (const field of timestampFields) {
      table.index([field.name]);
    }
  });

  console.log(`✅ Created table: ${tableName}`);
}

async function addForeignKeys(
  knex: Knex,
  schemas: KnexTableSchema[],
): Promise<void> {
  console.log('🔗 Adding foreign key constraints...');

  for (const schema of schemas) {
    const { tableName, definition } = schema;

    if (!definition.relations || definition.relations.length === 0) {
      continue;
    }

    for (const relation of definition.relations) {
      if (relation.type === 'many-to-many' || relation.type === 'one-to-many') {
        continue;
      }

      if (relation.type === 'one-to-one' && (relation as any)._isInverseGenerated) {
        continue;
      }

      const foreignKeyColumn = getForeignKeyColumnName(relation.propertyName);
      const targetTable = relation.targetTable;

      console.log(
        `  Adding FK: ${tableName}.${foreignKeyColumn} → ${targetTable}.id`,
      );

      try {
        await knex.schema.alterTable(tableName, (table) => {
          const fk = table
            .foreign(foreignKeyColumn)
            .references('id')
            .inTable(targetTable);

          fk.onDelete('SET NULL').onUpdate('CASCADE');

          table.index([foreignKeyColumn]);
        });
      } catch (error) {
        console.log(`  ⚠️ FK constraint already exists: ${tableName}.${foreignKeyColumn}`);
      }
    }
  }

  console.log('✅ Foreign keys added');
}

async function createJunctionTables(
  knex: Knex,
  schemas: KnexTableSchema[],
  dbType: string,
): Promise<void> {
  console.log('🔗 Creating junction tables...');

  const createdJunctions = new Set<string>();

  for (const schema of schemas) {
    for (const junction of schema.junctionTables) {
      if (createdJunctions.has(junction.tableName)) {
        continue;
      }

      const exists = await knex.schema.hasTable(junction.tableName);
      if (exists) {
        console.log(`⏩ Junction table already exists: ${junction.tableName}`);
        createdJunctions.add(junction.tableName);
        continue;
      }

      console.log(`📝 Creating junction table: ${junction.tableName}`);

      const sourcePkType = getPrimaryKeyType(schemas, junction.sourceTable);
      const targetPkType = getPrimaryKeyType(schemas, junction.targetTable);

      await knex.schema.createTable(junction.tableName, (table) => {
        let sourceCol;
        if (sourcePkType === 'uuid') {
          sourceCol = table.uuid(junction.sourceColumn).notNullable();
        } else {
          sourceCol = table.integer(junction.sourceColumn).unsigned().notNullable();
        }

        const sourceFk = sourceCol
          .references('id')
          .inTable(junction.sourceTable)
          .onDelete('CASCADE')
          .onUpdate('CASCADE');

        if (dbType === 'postgres') {
          const sourceFkName = getShortFkConstraintName(junction.tableName, junction.sourceColumn, 'src');
          sourceFk.withKeyName(sourceFkName);
        } else {
          const sourceFkName = getShortFkName(junction.sourceTable, junction.sourcePropertyName, 'src');
          sourceFk.withKeyName(sourceFkName);
        }

        let targetCol;
        if (targetPkType === 'uuid') {
          targetCol = table.uuid(junction.targetColumn).notNullable();
        } else {
          targetCol = table.integer(junction.targetColumn).unsigned().notNullable();
        }

        const targetFk = targetCol
          .references('id')
          .inTable(junction.targetTable)
          .onDelete('CASCADE')
          .onUpdate('CASCADE');

        if (dbType === 'postgres') {
          const targetFkName = getShortFkConstraintName(junction.tableName, junction.targetColumn, 'tgt');
          targetFk.withKeyName(targetFkName);
        } else {
          const targetFkName = getShortFkName(junction.sourceTable, junction.sourcePropertyName, 'tgt');
          targetFk.withKeyName(targetFkName);
        }

        const pkName = getShortPkName(junction.tableName);
        table.primary([junction.sourceColumn, junction.targetColumn], pkName);

        if (dbType !== 'postgres') {
          const sourceIndexName = getShortIndexName(junction.sourceTable, junction.sourcePropertyName, 'src');
          const targetIndexName = getShortIndexName(junction.sourceTable, junction.sourcePropertyName, 'tgt');
          table.index([junction.sourceColumn], sourceIndexName);
          table.index([junction.targetColumn], targetIndexName);
        }
      });

      console.log(`✅ Created junction table: ${junction.tableName}`);
      createdJunctions.add(junction.tableName);
    }
  }

  console.log('✅ Junction tables created');
}

async function createAllTables(
  knex: Knex,
  schemas: KnexTableSchema[],
  dbType: string,
): Promise<void> {
  console.log('🚀 Creating all tables...');

  for (const schema of schemas) {
    const exists = await knex.schema.hasTable(schema.tableName);
    if (!exists) {
      await createTable(knex, schema, dbType, schemas);
    } else {
      console.log(`⏩ Table already exists: ${schema.tableName}`);
    }
  }

  await addForeignKeys(knex, schemas);

  await createJunctionTables(knex, schemas, dbType);

  console.log('✅ All tables created successfully!');
}


async function getCurrentDatabaseSchema(knex: Knex, tableName: string): Promise<{
  columns: Array<{ name: string; type: string; isNullable: boolean; defaultValue: any }>;
  foreignKeys: Array<{ column: string; references: string; referencesTable: string }>;
}> {
  const dbType = knex.client.config.client;

  if (dbType === 'mysql2') {
    const columnsResult = await knex.raw(`
      SELECT
        COLUMN_NAME as name,
        DATA_TYPE as type,
        IS_NULLABLE as isNullable,
        COLUMN_DEFAULT as defaultValue,
        COLUMN_TYPE as fullType
      FROM INFORMATION_SCHEMA.COLUMNS
      WHERE TABLE_SCHEMA = DATABASE()
        AND TABLE_NAME = ?
        AND COLUMN_NAME NOT IN ('id', 'createdAt', 'updatedAt')
      ORDER BY ORDINAL_POSITION
    `, [tableName]);

    const fkResult = await knex.raw(`
      SELECT
        COLUMN_NAME as \`column\`,
        REFERENCED_COLUMN_NAME as \`references\`,
        REFERENCED_TABLE_NAME as referencesTable
      FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
      WHERE TABLE_SCHEMA = DATABASE()
        AND TABLE_NAME = ?
        AND REFERENCED_TABLE_NAME IS NOT NULL
    `, [tableName]);

    return {
      columns: columnsResult[0].map((col: any) => ({
        name: col.name,
        type: col.type,
        isNullable: col.isNullable === 'YES',
        defaultValue: col.defaultValue,
      })),
      foreignKeys: fkResult[0],
    };
  } else if (dbType === 'pg') {
    const columnsResult = await knex.raw(`
      SELECT
        column_name as name,
        data_type as type,
        is_nullable as "isNullable",
        column_default as "defaultValue"
      FROM information_schema.columns
      WHERE table_schema = 'public'
        AND table_name = ?
        AND column_name NOT IN ('id', 'createdAt', 'updatedAt')
      ORDER BY ordinal_position
    `, [tableName]);

    const fkResult = await knex.raw(`
      SELECT
        kcu.column_name as "column",
        ccu.column_name as "references",
        ccu.table_name as "referencesTable"
      FROM information_schema.table_constraints tc
      JOIN information_schema.key_column_usage kcu
        ON tc.constraint_name = kcu.constraint_name
      JOIN information_schema.constraint_column_usage ccu
        ON tc.constraint_name = ccu.constraint_name
      WHERE tc.table_name = ?
        AND tc.constraint_type = 'FOREIGN KEY'
    `, [tableName]);

    return {
      columns: columnsResult.rows.map((col: any) => ({
        name: col.name,
        type: col.type,
        isNullable: col.isNullable === 'YES',
        defaultValue: col.defaultValue,
      })),
      foreignKeys: fkResult.rows,
    };
  }

  return { columns: [], foreignKeys: [] };
}

function compareSchemas(
  snapshotSchema: KnexTableSchema,
  currentSchema: { columns: any[]; foreignKeys: any[] }
): {
  columnsToAdd: ColumnDef[];
  columnsToRemove: string[];
  columnsToModify: Array<{ column: ColumnDef; changes: string[] }>;
  relationsToAdd: RelationDef[];
  relationsToRemove: string[];
} {
  const diff = {
    columnsToAdd: [] as ColumnDef[],
    columnsToRemove: [] as string[],
    columnsToModify: [] as Array<{ column: ColumnDef; changes: string[] }>,
    relationsToAdd: [] as RelationDef[],
    relationsToRemove: [] as string[],
  };

  const snapshotColumns = snapshotSchema.definition.columns.filter(
    col => !col.isPrimary && col.name !== 'createdAt' && col.name !== 'updatedAt'
  );

  const snapshotColumnNames = new Set(snapshotColumns.map(c => c.name));
  const currentColumnNamesSet = new Set(currentSchema.columns.map(c => c.name));

  for (const col of snapshotColumns) {
    if (!currentColumnNamesSet.has(col.name)) {
      diff.columnsToAdd.push(col);
    }
  }

  const snapshotFkColumnNames = new Set<string>();
  if (snapshotSchema.definition.relations) {
    for (const rel of snapshotSchema.definition.relations) {
      if (rel.type === 'many-to-one' || rel.type === 'one-to-one') {
        const fkColumn = getForeignKeyColumnName(rel.propertyName);
        snapshotFkColumnNames.add(fkColumn);
      }
    }
  }

  for (const col of currentSchema.columns) {
    if (snapshotColumnNames.has(col.name)) {
      continue;
    }
    const isCurrentFkColumn = currentSchema.foreignKeys.some(fk => fk.column === col.name);
    if (isCurrentFkColumn && !snapshotFkColumnNames.has(col.name)) {
      continue;
    }
    if (snapshotFkColumnNames.has(col.name)) {
      continue;
    }
    diff.columnsToRemove.push(col.name);
  }

  for (const snapshotCol of snapshotColumns) {
    const currentCol = currentSchema.columns.find(c => c.name === snapshotCol.name);
    if (currentCol) {
      const changes: string[] = [];

      const snapshotType = getKnexColumnType(snapshotCol);
      if (snapshotType !== currentCol.type && !isTypeCompatible(snapshotType, currentCol.type)) {
        changes.push('type');
      }

      const snapshotNullable = snapshotCol.isNullable !== false;
      if (snapshotNullable !== currentCol.isNullable) {
        changes.push('nullable');
      }

      if (changes.length > 0) {
        diff.columnsToModify.push({ column: snapshotCol, changes });
      }
    }
  }

  const snapshotFkColumns = new Set<string>();
  if (snapshotSchema.definition.relations) {
    for (const rel of snapshotSchema.definition.relations) {
      if (rel.type === 'many-to-one' || rel.type === 'one-to-one') {
        snapshotFkColumns.add(getForeignKeyColumnName(rel.propertyName));
      }
    }
  }

  const currentFkColumns = new Set(currentSchema.foreignKeys.map(fk => fk.column));

  const currentColumnNames = new Set(currentSchema.columns.map(c => c.name));

  if (snapshotSchema.definition.relations) {
    for (const rel of snapshotSchema.definition.relations) {
      if (rel.type === 'many-to-one' || rel.type === 'one-to-one') {
        const fkColumn = getForeignKeyColumnName(rel.propertyName);
        if (!currentFkColumns.has(fkColumn) && !currentColumnNames.has(fkColumn)) {
          diff.relationsToAdd.push(rel);
        }
      } else if (rel.type === 'many-to-many') {
        diff.relationsToAdd.push(rel);
      }
    }
  }

  for (const fk of currentSchema.foreignKeys) {
    if (!snapshotFkColumns.has(fk.column)) {
      diff.relationsToRemove.push(fk.column);
    }
  }

  return diff;
}

function isTypeCompatible(type1: string, type2: string): boolean {
  const compatibleTypes: Record<string, string[]> = {
    'integer': ['int', 'integer', 'bigint', 'smallint', 'tinyint'],
    'string': ['varchar', 'text', 'char'],
    'text': ['varchar', 'text', 'longtext', 'mediumtext'],
    'timestamp': ['timestamp', 'datetime'],
    'boolean': ['tinyint', 'boolean', 'bool'],
  };

  for (const [baseType, variants] of Object.entries(compatibleTypes)) {
    if ((type1 === baseType || variants.includes(type1)) &&
        (type2 === baseType || variants.includes(type2))) {
      return true;
    }
  }

  return type1 === type2;
}

async function applyColumnMigrations(
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
          column.defaultTo(col.defaultValue);
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
      for (const { column: col } of diff.columnsToModify) {
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
      await knex.schema.alterTable(tableName, (table) => {
        for (const { column: col } of diff.columnsToModify) {
          const knexType = getKnexColumnType(col);
          let column: Knex.ColumnBuilder;

          switch (knexType) {
            case 'integer':
              column = table.integer(col.name).alter();
              break;
            case 'string':
              column = table.string(col.name, 255).alter();
              break;
            case 'text':
              column = table.text(col.name).alter();
              break;
            default:
              continue;
          }

          if (col.isNullable === false) {
            column.notNullable();
          } else {
            column.nullable();
          }
        }
      });
    }
  }
}

async function applyRelationMigrations(
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

          fk.onDelete('SET NULL').onUpdate('CASCADE');

          table.index([fkColumn]);
        });
      }
    }
  }
}

async function syncJunctionTables(
  knex: Knex,
  schemas: KnexTableSchema[],
  dbType: string,
): Promise<void> {
  console.log('🔗 Syncing junction tables...');

  const createdJunctions = new Set<string>();

  for (const schema of schemas) {
    for (const junction of schema.junctionTables) {
      if (createdJunctions.has(junction.tableName)) {
        continue;
      }

      const exists = await knex.schema.hasTable(junction.tableName);

      if (!exists) {
        console.log(`  📝 Creating junction table: ${junction.tableName}`);

        const sourcePkType = getPrimaryKeyType(schemas, junction.sourceTable);
        const targetPkType = getPrimaryKeyType(schemas, junction.targetTable);

        await knex.schema.createTable(junction.tableName, (table) => {
          const sourceFkName = getShortFkName(junction.sourceTable, junction.sourcePropertyName, 'src');
          let sourceCol;
          if (sourcePkType === 'uuid') {
            sourceCol = table.uuid(junction.sourceColumn).notNullable();
          } else {
            sourceCol = table.integer(junction.sourceColumn).unsigned().notNullable();
          }

          sourceCol
            .references('id')
            .inTable(junction.sourceTable)
            .onDelete('CASCADE')
            .onUpdate('CASCADE')
            .withKeyName(sourceFkName);

          const targetFkName = getShortFkName(junction.sourceTable, junction.sourcePropertyName, 'tgt');
          let targetCol;
          if (targetPkType === 'uuid') {
            targetCol = table.uuid(junction.targetColumn).notNullable();
          } else {
            targetCol = table.integer(junction.targetColumn).unsigned().notNullable();
          }

          targetCol
            .references('id')
            .inTable(junction.targetTable)
            .onDelete('CASCADE')
            .onUpdate('CASCADE')
            .withKeyName(targetFkName);

          const pkName = getShortPkName(junction.tableName);
          table.primary([junction.sourceColumn, junction.targetColumn], pkName);

          if (dbType !== 'postgres') {
            const sourceIndexName = getShortIndexName(junction.sourceTable, junction.sourcePropertyName, 'src');
            const targetIndexName = getShortIndexName(junction.sourceTable, junction.sourcePropertyName, 'tgt');
            table.index([junction.sourceColumn], sourceIndexName);
            table.index([junction.targetColumn], targetIndexName);
          }
        });

        console.log(`  ✅ Created junction table: ${junction.tableName}`);
      } else {
        console.log(`  ⏩ Junction table already exists: ${junction.tableName}`);
      }

      createdJunctions.add(junction.tableName);
    }
  }
}

async function syncTable(
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
    diff.relationsToRemove.length > 0;

  if (!hasChanges) {
    console.log(`⏩ No changes for table: ${tableName}`);
    return;
  }

  console.log(`🔄 Syncing table: ${tableName}`);

  await applyColumnMigrations(knex, tableName, diff, schemas);
  await applyRelationMigrations(knex, tableName, diff, schemas);

  console.log(`✅ Synced table: ${tableName}`);
}


async function ensureDatabaseExists(): Promise<void> {
  const DB_TYPE = process.env.DB_TYPE || 'mysql';
  const DB_HOST = process.env.DB_HOST || 'localhost';
  const DB_PORT =
    Number(process.env.DB_PORT) || (DB_TYPE === 'postgres' ? 5432 : 3306);
  const DB_USERNAME = process.env.DB_USERNAME || 'root';
  const DB_PASSWORD = process.env.DB_PASSWORD || '';
  const DB_NAME = process.env.DB_NAME || 'enfyra';

  const tempKnex = knex({
    client: DB_TYPE === 'postgres' ? 'pg' : 'mysql2',
    connection: {
      host: DB_HOST,
      port: DB_PORT,
      user: DB_USERNAME,
      password: DB_PASSWORD,
      ...(DB_TYPE === 'postgres' && { database: 'postgres' }),
    },
  });

  try {
    if (DB_TYPE === 'mysql') {
      const result = await tempKnex.raw(
        `SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = ?`,
        [DB_NAME],
      );
      if (result[0].length === 0) {
        await tempKnex.raw(`CREATE DATABASE IF NOT EXISTS \`${DB_NAME}\``);
        console.log(`✅ MySQL: Created database ${DB_NAME}`);
      } else {
        console.log(`✅ MySQL: Database ${DB_NAME} already exists`);
      }
    } else if (DB_TYPE === 'postgres') {
      const result = await tempKnex.raw(
        `SELECT 1 FROM pg_database WHERE datname = ?`,
        [DB_NAME],
      );
      if (result.rows.length === 0) {
        await tempKnex.raw(`CREATE DATABASE "${DB_NAME}" WITH ENCODING 'UTF8'`);
        console.log(`✅ Postgres: Created database ${DB_NAME}`);
      } else {
        console.log(`✅ Postgres: Database ${DB_NAME} already exists`);
      }
    }
  } finally {
    await tempKnex.destroy();
  }
}

export async function initializeDatabaseSql(): Promise<void> {
  const DB_TYPE = process.env.DB_TYPE || 'mysql';
  const DB_HOST = process.env.DB_HOST || 'localhost';
  const DB_PORT =
    Number(process.env.DB_PORT) || (DB_TYPE === 'postgres' ? 5432 : 3306);
  const DB_USERNAME = process.env.DB_USERNAME || 'root';
  const DB_PASSWORD = process.env.DB_PASSWORD || '';
  const DB_NAME = process.env.DB_NAME || 'enfyra';

  await ensureDatabaseExists();

  const knexInstance = knex({
    client: DB_TYPE === 'postgres' ? 'pg' : 'mysql2',
    connection: {
      host: DB_HOST,
      port: DB_PORT,
      user: DB_USERNAME,
      password: DB_PASSWORD,
      database: DB_NAME,
    },
  });

  try {
    const hasSettingTable = await knexInstance.schema.hasTable(
      'setting_definition',
    );

    if (hasSettingTable) {
      const result = await knexInstance('setting_definition')
        .select('isInit')
        .first();

      if (result?.isInit === true || result?.isInit === 1) {
        console.log('⚠️ Database already initialized, skipping init.');
        return;
      }
    }

    const snapshotPath = path.resolve(process.cwd(), 'data/snapshot.json');
    const snapshot = JSON.parse(fs.readFileSync(snapshotPath, 'utf8'));

    console.log('📖 Loaded snapshot.json');

    const schemas = parseSnapshotToSchema(snapshot);

    console.log(`📊 Found ${schemas.length} tables to create`);

    console.log('🚀 Creating/syncing all tables...');

    for (const schema of schemas) {
      const exists = await knexInstance.schema.hasTable(schema.tableName);
      if (!exists) {
        await createTable(knexInstance, schema, DB_TYPE, schemas);
      } else {
        console.log(`⏩ Table already exists: ${schema.tableName}`);
      }
    }

    await addForeignKeys(knexInstance, schemas);

    console.log('\n🔄 Syncing tables with snapshot...');
    for (const schema of schemas) {
      const exists = await knexInstance.schema.hasTable(schema.tableName);
      if (exists) {
        await syncTable(knexInstance, schema, schemas);
      }
    }

    await syncJunctionTables(knexInstance, schemas, DB_TYPE);

    console.log('\n🎉 Database initialization/sync completed!');
  } catch (error) {
    console.error('❌ Error during database initialization:', error);
    throw error;
  } finally {
    await knexInstance.destroy();
  }
}

if (require.main === module) {
  initializeDatabaseSql()
    .then(() => {
      console.log('✅ Done!');
      process.exit(0);
    })
    .catch((error) => {
      console.error('❌ Failed:', error);
      process.exit(1);
    });
}


