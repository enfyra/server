import { Knex } from 'knex';
import { Logger } from '@nestjs/common';
import {
  quoteIdentifier,
  getForeignKeyConstraintsQuery,
  generateDropForeignKeySQL,
} from './sql-dialect';

const logger = new Logger('ForeignKeyOperations');

export async function dropForeignKeyIfExists(
  knex: Knex,
  tableName: string,
  columnName: string,
  dbType: 'mysql' | 'postgres' | 'sqlite',
): Promise<void> {
  try {
    logger.log(`Querying FK constraints for table: ${tableName}, column: ${columnName}`);

    const { query, bindings } = getForeignKeyConstraintsQuery(tableName, columnName, dbType);
    const fkConstraints = await knex.raw(query, bindings);

    let constraintName: string | null = null;

    // Parse results based on DB type
    if (dbType === 'mysql') {
      if (fkConstraints[0] && fkConstraints[0].length > 0) {
        constraintName = fkConstraints[0][0].CONSTRAINT_NAME;
      }
    } else if (dbType === 'postgres') {
      if (fkConstraints.rows && fkConstraints.rows.length > 0) {
        constraintName = fkConstraints.rows[0].constraint_name;
      }
    } else if (dbType === 'sqlite') {
      // SQLite PRAGMA returns different structure
      if (fkConstraints && fkConstraints.length > 0) {
        // SQLite doesn't support named FK constraints easily
        logger.log(`SQLite FK drop requires table recreation`);
        return;
      }
    }

    if (constraintName) {
      logger.log(`Found FK constraint: ${constraintName}`);
      const dropSQL = generateDropForeignKeySQL(tableName, constraintName, dbType);
      await knex.raw(dropSQL);
      logger.log(`Successfully dropped FK constraint: ${constraintName}`);
    } else {
      logger.log(`No FK constraint found for column ${columnName}`);
    }
  } catch (error) {
    logger.log(`Error checking/dropping FK constraint for ${columnName}: ${error.message}`);
  }
}

export async function dropAllForeignKeysReferencingTable(
  knex: Knex,
  targetTableName: string,
  dbType: 'mysql' | 'postgres' | 'sqlite',
): Promise<void> {
  logger.log(`Checking for FK constraints referencing table: ${targetTableName}`);

  const { query, bindings } = getAllForeignKeyConstraintsReferencingTableQuery(targetTableName, dbType);
  const fkConstraints = await knex.raw(query, bindings);

  let constraints: any[] = [];

  // Parse results based on DB type
  if (dbType === 'mysql') {
    constraints = fkConstraints[0] || [];
  } else if (dbType === 'postgres') {
    constraints = fkConstraints.rows || [];
  } else if (dbType === 'sqlite') {
    logger.log(`SQLite does not support querying all FKs referencing a table`);
    return;
  }

  if (constraints.length > 0) {
    logger.log(`Found ${constraints.length} FK constraint(s) referencing ${targetTableName}`);

    for (const fk of constraints) {
      const tableName = dbType === 'mysql' ? fk.TABLE_NAME : fk.table_name;
      const constraintName = dbType === 'mysql' ? fk.CONSTRAINT_NAME : fk.constraint_name;
      const columnName = dbType === 'mysql' ? fk.COLUMN_NAME : fk.column_name;

      logger.log(`   Dropping FK: ${constraintName} from ${tableName}.${columnName}`);

      const dropSQL = generateDropForeignKeySQL(tableName, constraintName, dbType);
      await knex.raw(dropSQL);
      logger.log(`  Dropped FK constraint: ${constraintName}`);
    }
  } else {
    logger.log(`No FK constraints reference ${targetTableName}`);
  }
}

export function generateForeignKeySQL(
  tableName: string,
  columnName: string,
  targetTable: string,
  targetColumn: string = 'id',
  isNullable: boolean = true,
  dbType: 'mysql' | 'postgres' | 'sqlite' = 'mysql',
  onDelete?: string,
): string {
  const qt = (id: string) => quoteIdentifier(id, dbType);
  const onDeleteAction = onDelete || (isNullable ? 'SET NULL' : 'RESTRICT');
  const fkName = `fk_${tableName}_${columnName}`;
  return `ALTER TABLE ${qt(tableName)} ADD CONSTRAINT ${qt(fkName)} FOREIGN KEY (${qt(columnName)}) REFERENCES ${qt(targetTable)} (${qt(targetColumn)}) ON DELETE ${onDeleteAction} ON UPDATE CASCADE`;
}

function getAllForeignKeyConstraintsReferencingTableQuery(
  tableName: string,
  dbType: 'mysql' | 'postgres' | 'sqlite',
): { query: string; bindings: any[] } {
  if (dbType === 'mysql') {
    return {
      query: `
        SELECT
          TABLE_NAME,
          CONSTRAINT_NAME,
          COLUMN_NAME
        FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
        WHERE TABLE_SCHEMA = DATABASE()
        AND REFERENCED_TABLE_NAME = ?
      `,
      bindings: [tableName],
    };
  } else if (dbType === 'postgres') {
    return {
      query: `
        SELECT
          rel.relname AS table_name,
          con.conname AS constraint_name,
          att.attname AS column_name
        FROM pg_constraint con
        INNER JOIN pg_class rel ON rel.oid = con.conrelid
        INNER JOIN pg_class rrel ON rrel.oid = con.confrelid
        INNER JOIN pg_attribute att ON att.attrelid = con.conrelid AND att.attnum = ANY(con.conkey)
        WHERE rrel.relname = $1
        AND con.contype = 'f'
      `,
      bindings: [tableName],
    };
  } else {
    return { query: '', bindings: [] };
  }
}
