import { DatabaseType } from '../../../../shared/types/query-builder.types';

export function quoteIdentifier(identifier: string, dbType: DatabaseType | string): string {
  switch (dbType) {
    case 'mysql':
      return `\`${identifier}\``;
    case 'postgres':
    case 'pg':
      return `"${identifier}"`;
    case 'sqlite':
      return `"${identifier}"`;
    case 'mongodb':
      return identifier;
    default:
      return `\`${identifier}\``;
  }
}

export function getJsonObjectFunc(dbType: DatabaseType | string): string {
  return dbType === 'postgres' || dbType === 'pg' ? 'json_build_object' : 'JSON_OBJECT';
}

export function getJsonArrayAggFunc(dbType: DatabaseType | string): string {
  return dbType === 'postgres' || dbType === 'pg' ? 'COALESCE(json_agg' : 'ifnull(JSON_ARRAYAGG';
}

export function getEmptyJsonArray(dbType: DatabaseType | string): string {
  return dbType === 'postgres' || dbType === 'pg' ? "'[]'::json" : 'JSON_ARRAY()';
}

export function castToText(columnRef: string, dbType: DatabaseType | string): string {
  return dbType === 'postgres' || dbType === 'pg' ? `${columnRef}::text` : columnRef;
}

export function generateRenameTableSQL(
  oldName: string,
  newName: string,
  dbType: 'mysql' | 'postgres' | 'sqlite',
): string {
  const oldQuoted = quoteIdentifier(oldName, dbType);
  const newQuoted = quoteIdentifier(newName, dbType);

  switch (dbType) {
    case 'mysql':
      return `RENAME TABLE ${oldQuoted} TO ${newQuoted}`;
    case 'postgres':
    case 'sqlite':
      return `ALTER TABLE ${oldQuoted} RENAME TO ${newQuoted}`;
    default:
      return `RENAME TABLE ${oldQuoted} TO ${newQuoted}`;
  }
}

export function generateRenameColumnSQL(
  tableName: string,
  oldColumnName: string,
  newColumnName: string,
  dbType: 'mysql' | 'postgres' | 'sqlite',
): string {
  const table = quoteIdentifier(tableName, dbType);
  const oldCol = quoteIdentifier(oldColumnName, dbType);
  const newCol = quoteIdentifier(newColumnName, dbType);

  switch (dbType) {
    case 'mysql':
    case 'sqlite':
      return `ALTER TABLE ${table} RENAME COLUMN ${oldCol} TO ${newCol}`;
    case 'postgres':
      return `ALTER TABLE ${table} RENAME COLUMN ${oldCol} TO ${newCol}`;
    default:
      return `ALTER TABLE ${table} RENAME COLUMN ${oldCol} TO ${newCol}`;
  }
}

export function generateModifyColumnSQL(
  tableName: string,
  columnName: string,
  columnDef: string,
  dbType: 'mysql' | 'postgres' | 'sqlite',
): string {
  const table = quoteIdentifier(tableName, dbType);
  const column = quoteIdentifier(columnName, dbType);

  switch (dbType) {
    case 'mysql':
      return `ALTER TABLE ${table} MODIFY COLUMN ${column} ${columnDef}`;
    case 'postgres':
      return `ALTER TABLE ${table} ALTER COLUMN ${column} TYPE ${columnDef}`;
    case 'sqlite':
      throw new Error('SQLite does not support ALTER COLUMN. Use table recreation instead.');
    default:
      return `ALTER TABLE ${table} MODIFY COLUMN ${column} ${columnDef}`;
  }
}

export function generateDropForeignKeySQL(
  tableName: string,
  constraintName: string,
  dbType: 'mysql' | 'postgres' | 'sqlite',
): string {
  const table = quoteIdentifier(tableName, dbType);
  const constraint = quoteIdentifier(constraintName, dbType);

  switch (dbType) {
    case 'mysql':
      return `ALTER TABLE ${table} DROP FOREIGN KEY ${constraint}`;
    case 'postgres':
    case 'sqlite':
      return `ALTER TABLE ${table} DROP CONSTRAINT ${constraint}`;
    default:
      return `ALTER TABLE ${table} DROP FOREIGN KEY ${constraint}`;
  }
}

export function generateAddIndexSQL(
  tableName: string,
  indexName: string,
  columns: string[],
  dbType: 'mysql' | 'postgres' | 'sqlite',
): string {
  const table = quoteIdentifier(tableName, dbType);
  const index = quoteIdentifier(indexName, dbType);
  const cols = columns.map(c => quoteIdentifier(c, dbType)).join(', ');

  switch (dbType) {
    case 'mysql':
      return `ALTER TABLE ${table} ADD INDEX ${index} (${cols})`;
    case 'postgres':
    case 'sqlite':
      return `CREATE INDEX ${index} ON ${table} (${cols})`;
    default:
      return `ALTER TABLE ${table} ADD INDEX ${index} (${cols})`;
  }
}

export function getForeignKeyConstraintsQuery(
  tableName: string,
  columnName: string,
  dbType: 'mysql' | 'postgres' | 'sqlite',
): { query: string; bindings: any[] } {
  switch (dbType) {
    case 'mysql':
      return {
        query: `
          SELECT CONSTRAINT_NAME
          FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
          WHERE TABLE_SCHEMA = DATABASE()
          AND TABLE_NAME = ?
          AND COLUMN_NAME = ?
          AND REFERENCED_TABLE_NAME IS NOT NULL
        `,
        bindings: [tableName, columnName],
      };
    case 'postgres':
      return {
        query: `
          SELECT con.conname AS constraint_name
          FROM pg_constraint con
          INNER JOIN pg_class rel ON rel.oid = con.conrelid
          INNER JOIN pg_attribute att ON att.attrelid = con.conrelid AND att.attnum = ANY(con.conkey)
          WHERE rel.relname = $1
          AND att.attname = $2
          AND con.contype = 'f'
        `,
        bindings: [tableName, columnName],
      };
    case 'sqlite':
      return {
        query: `PRAGMA foreign_key_list(${tableName})`,
        bindings: [],
      };
    default:
      return {
        query: `
          SELECT CONSTRAINT_NAME
          FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
          WHERE TABLE_SCHEMA = DATABASE()
          AND TABLE_NAME = ?
          AND COLUMN_NAME = ?
          AND REFERENCED_TABLE_NAME IS NOT NULL
        `,
        bindings: [tableName, columnName],
      };
  }
}

export function getAllForeignKeyConstraintsReferencingTableQuery(
  tableName: string,
  dbType: 'mysql' | 'postgres' | 'sqlite',
): { query: string; bindings: any[] } {
  switch (dbType) {
    case 'mysql':
      return {
        query: `
          SELECT
            TABLE_NAME,
            COLUMN_NAME,
            CONSTRAINT_NAME,
            REFERENCED_TABLE_NAME,
            REFERENCED_COLUMN_NAME
          FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
          WHERE TABLE_SCHEMA = DATABASE()
          AND REFERENCED_TABLE_NAME = ?
          AND REFERENCED_COLUMN_NAME IS NOT NULL
        `,
        bindings: [tableName],
      };
    case 'postgres':
      return {
        query: `
          SELECT
            rel.relname AS table_name,
            att.attname AS column_name,
            con.conname AS constraint_name,
            rrel.relname AS referenced_table_name,
            ratt.attname AS referenced_column_name
          FROM pg_constraint con
          INNER JOIN pg_class rel ON rel.oid = con.conrelid
          INNER JOIN pg_class rrel ON rrel.oid = con.confrelid
          INNER JOIN pg_attribute att ON att.attrelid = con.conrelid AND att.attnum = ANY(con.conkey)
          INNER JOIN pg_attribute ratt ON ratt.attrelid = con.confrelid AND ratt.attnum = ANY(con.confkey)
          WHERE rrel.relname = $1
          AND con.contype = 'f'
        `,
        bindings: [tableName],
      };
    case 'sqlite':
      throw new Error('SQLite does not support querying all foreign keys referencing a table');
    default:
      return {
        query: `
          SELECT
            TABLE_NAME,
            COLUMN_NAME,
            CONSTRAINT_NAME,
            REFERENCED_TABLE_NAME,
            REFERENCED_COLUMN_NAME
          FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
          WHERE TABLE_SCHEMA = DATABASE()
          AND REFERENCED_TABLE_NAME = ?
          AND REFERENCED_COLUMN_NAME IS NOT NULL
        `,
        bindings: [tableName],
      };
  }
}
