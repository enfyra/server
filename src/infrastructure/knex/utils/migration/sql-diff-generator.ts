import { Knex } from 'knex';
import { Logger } from '@nestjs/common';
import { generateColumnDefinition } from './sql-generator';
import { dropForeignKeyIfExists } from './foreign-key-operations';
import {
  quoteIdentifier,
  generateRenameTableSQL,
  generateRenameColumnSQL,
  generateModifyColumnSQL,
  generateAddIndexSQL,
} from './sql-dialect';

const logger = new Logger('SqlDiffGenerator');

export async function generateSQLFromDiff(
  knex: Knex,
  tableName: string,
  diff: any,
  dbType: 'mysql' | 'postgres' | 'sqlite',
): Promise<string[]> {
  const sqlStatements: string[] = [];
  const qt = (id: string) => quoteIdentifier(id, dbType); // Quote helper

  if (diff.table.update) {
    sqlStatements.push(generateRenameTableSQL(diff.table.update.oldName, diff.table.update.newName, dbType));
  }

  const renamedColumns = new Set<string>();
  for (const rename of diff.columns.rename) {
    sqlStatements.push(generateRenameColumnSQL(tableName, rename.oldName, rename.newName, dbType));
    renamedColumns.add(rename.oldName);
    renamedColumns.add(rename.newName);
  }

  // DELETE columns first (before CREATE) to avoid duplicate column name errors
  for (const col of diff.columns.delete) {
    if (col.isForeignKey) {
      await dropForeignKeyIfExists(knex, tableName, col.name, dbType);
    }
    sqlStatements.push(`ALTER TABLE ${qt(tableName)} DROP COLUMN ${qt(col.name)}`);
  }

  // CREATE columns after DELETE
  for (const col of diff.columns.create) {
    const columnDef = generateColumnDefinition(col, dbType);
    sqlStatements.push(`ALTER TABLE ${qt(tableName)} ADD COLUMN ${qt(col.name)} ${columnDef}`);

    // Skip FK constraint if column is nullable (will have NULL values that violate constraint)
    // User must populate data first, then manually add constraint or change to NOT NULL
    if (col.isForeignKey && col.foreignKeyTarget) {
      if (col.isNullable === false) {
        // NOT NULL columns: Use RESTRICT (don't allow deleting parent if children exist)
        const onDelete = 'RESTRICT';
        sqlStatements.push(
          `ALTER TABLE ${qt(tableName)} ADD CONSTRAINT ${qt(`fk_${tableName}_${col.name}`)} FOREIGN KEY (${qt(col.name)}) REFERENCES ${qt(col.foreignKeyTarget)} (${qt(col.foreignKeyColumn || 'id')}) ON DELETE ${onDelete} ON UPDATE CASCADE`
        );
      } else {
        // NULLABLE columns: Skip FK constraint to avoid errors with existing NULL data
        // User should populate data first, then add FK constraint manually if needed
        logger.log(`  Skipping FK constraint for nullable column ${col.name} - user must populate data first`);
      }
    }

    // Add UNIQUE constraint for O2O relations
    if (col.isUnique) {
      const uniqueConstraintName = `uq_${tableName}_${col.name}`;
      sqlStatements.push(
        `ALTER TABLE ${qt(tableName)} ADD CONSTRAINT ${qt(uniqueConstraintName)} UNIQUE (${qt(col.name)})`
      );
      logger.log(`  Added UNIQUE constraint on ${col.name} for one-to-one relation`);
    }
  }

  const processedUpdates = new Set<string>();
  for (const update of diff.columns.update) {
    const colName = update.newColumn.name;

    if (renamedColumns.has(colName)) {
      logger.log(`  Skipping MODIFY for ${colName} - column was renamed`);
      continue;
    }

    if (processedUpdates.has(colName)) {
      logger.log(`  Skipping duplicate MODIFY for ${colName}`);
      continue;
    }

    processedUpdates.add(colName);
    const columnDef = generateColumnDefinition(update.newColumn, dbType);
    sqlStatements.push(generateModifyColumnSQL(tableName, update.newColumn.name, columnDef, dbType));
  }

  // Handle UNIQUE constraint CREATE
  for (const uniqueGroup of diff.constraints.uniques.create || []) {
    const columns = uniqueGroup.map((col: string) => qt(col)).join(', ');
    const constraintName = `uq_${tableName}_${uniqueGroup.join('_')}`;
    sqlStatements.push(`ALTER TABLE ${qt(tableName)} ADD CONSTRAINT ${qt(constraintName)} UNIQUE (${columns})`);
    logger.log(`  Added UNIQUE constraint ${constraintName} on (${uniqueGroup.join(', ')})`);
  }

  // Handle UNIQUE constraint UPDATE (should be same as CREATE for now)
  for (const uniqueGroup of diff.constraints.uniques.update || []) {
    const columns = uniqueGroup.map((col: string) => qt(col)).join(', ');
    sqlStatements.push(`ALTER TABLE ${qt(tableName)} ADD UNIQUE (${columns})`);
  }

  for (const indexGroup of diff.constraints.indexes.update || []) {
    const indexName = `idx_${tableName}_${indexGroup.join('_')}`;
    sqlStatements.push(generateAddIndexSQL(tableName, indexName, indexGroup, dbType));
  }

  for (const crossOp of diff.crossTableOperations || []) {
    if (crossOp.operation === 'createColumn') {
      const columnDef = generateColumnDefinition(crossOp.column, dbType);
      sqlStatements.push(`ALTER TABLE ${qt(crossOp.targetTable)} ADD COLUMN ${qt(crossOp.column.name)} ${columnDef}`);

      if (crossOp.column.isForeignKey) {
        const onDelete = crossOp.column.isNullable !== false ? 'SET NULL' : 'RESTRICT';
        sqlStatements.push(
          `ALTER TABLE ${qt(crossOp.targetTable)} ADD CONSTRAINT ${qt(`fk_${crossOp.targetTable}_${crossOp.column.name}`)} FOREIGN KEY (${qt(crossOp.column.name)}) REFERENCES ${qt(crossOp.column.foreignKeyTarget)} (${qt(crossOp.column.foreignKeyColumn)}) ON DELETE ${onDelete} ON UPDATE CASCADE`
        );
      }
    } else if (crossOp.operation === 'dropColumn') {
      await dropForeignKeyIfExists(knex, crossOp.targetTable, crossOp.columnName, dbType);
      sqlStatements.push(`ALTER TABLE ${qt(crossOp.targetTable)} DROP COLUMN ${qt(crossOp.columnName)}`);
    } else if (crossOp.operation === 'renameColumn') {
      sqlStatements.push(generateRenameColumnSQL(crossOp.targetTable, crossOp.oldColumnName, crossOp.newColumnName, dbType));
    }
  }

  // Junction table RENAME should execute before CREATE/DROP to avoid conflicts
  for (const junctionRename of diff.junctionTables?.rename || []) {
    const { oldTableName, newTableName } = junctionRename;
    logger.log(`Renaming junction table: ${oldTableName} → ${newTableName}`);
    sqlStatements.push(generateRenameTableSQL(oldTableName, newTableName, dbType));
  }

  for (const junctionCreate of diff.junctionTables?.create || []) {
    const { tableName: junctionName, sourceTable, targetTable, sourceColumn, targetColumn } = junctionCreate;

    const tableExists = await knex.schema.hasTable(junctionName);
    if (tableExists) {
      logger.log(`  Junction table ${junctionName} already exists, skipping`);
      continue;
    }

    // Generate database-specific CREATE TABLE syntax
    let createJunctionSQL: string;
    if (dbType === 'postgres') {
      createJunctionSQL = `
        CREATE TABLE ${qt(junctionName)} (
          ${qt('id')} SERIAL PRIMARY KEY,
          ${qt(sourceColumn)} INTEGER NOT NULL,
          ${qt(targetColumn)} INTEGER NOT NULL,
          FOREIGN KEY (${qt(sourceColumn)}) REFERENCES ${qt(sourceTable)} (${qt('id')}) ON DELETE CASCADE ON UPDATE CASCADE,
          FOREIGN KEY (${qt(targetColumn)}) REFERENCES ${qt(targetTable)} (${qt('id')}) ON DELETE CASCADE ON UPDATE CASCADE,
          UNIQUE (${qt(sourceColumn)}, ${qt(targetColumn)})
        )
      `.trim().replace(/\s+/g, ' ');
    } else if (dbType === 'sqlite') {
      createJunctionSQL = `
        CREATE TABLE ${qt(junctionName)} (
          ${qt('id')} INTEGER PRIMARY KEY AUTOINCREMENT,
          ${qt(sourceColumn)} INTEGER NOT NULL,
          ${qt(targetColumn)} INTEGER NOT NULL,
          FOREIGN KEY (${qt(sourceColumn)}) REFERENCES ${qt(sourceTable)} (${qt('id')}) ON DELETE CASCADE ON UPDATE CASCADE,
          FOREIGN KEY (${qt(targetColumn)}) REFERENCES ${qt(targetTable)} (${qt('id')}) ON DELETE CASCADE ON UPDATE CASCADE,
          UNIQUE (${qt(sourceColumn)}, ${qt(targetColumn)})
        )
      `.trim().replace(/\s+/g, ' ');
    } else {
      // MySQL
      createJunctionSQL = `
        CREATE TABLE ${qt(junctionName)} (
          ${qt('id')} INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
          ${qt(sourceColumn)} INT UNSIGNED NOT NULL,
          ${qt(targetColumn)} INT UNSIGNED NOT NULL,
          FOREIGN KEY (${qt(sourceColumn)}) REFERENCES ${qt(sourceTable)} (${qt('id')}) ON DELETE CASCADE ON UPDATE CASCADE,
          FOREIGN KEY (${qt(targetColumn)}) REFERENCES ${qt(targetTable)} (${qt('id')}) ON DELETE CASCADE ON UPDATE CASCADE,
          UNIQUE KEY ${qt(`unique_${sourceColumn}_${targetColumn}`)} (${qt(sourceColumn)}, ${qt(targetColumn)})
        )
      `.trim().replace(/\s+/g, ' ');
    }

    sqlStatements.push(createJunctionSQL);
  }

  for (const junctionDrop of diff.junctionTables?.drop || []) {
    const { tableName: junctionName } = junctionDrop;
    sqlStatements.push(`DROP TABLE IF EXISTS ${qt(junctionName)}`);
  }

  return sqlStatements;
}

/**
 * Generate batch SQL from multiple statements
 * @returns Single SQL string with all statements separated by semicolons
 */
export function generateBatchSQL(sqlStatements: string[]): string {
  if (sqlStatements.length === 0) {
    return '';
  }

  // Join all SQL statements with semicolon
  const batchSQL = sqlStatements.join(';\n') + ';';

  logger.log(`📦 Generated batch SQL with ${sqlStatements.length} statement(s):`);
  sqlStatements.forEach((sql, i) => {
    logger.log(`  [${i+1}/${sqlStatements.length}] ${sql.substring(0, 80)}${sql.length > 80 ? '...' : ''}`);
  });

  return batchSQL;
}

/**
 * Execute batch SQL with transaction support
 * @param knex - Knex instance
 * @param batchSQL - SQL string with multiple statements separated by semicolons
 * @param dbType - Database type (mysql, postgres, sqlite)
 */
export async function executeBatchSQL(
  knex: Knex,
  batchSQL: string,
  dbType?: 'mysql' | 'postgres' | 'sqlite',
): Promise<void> {
  if (!batchSQL || batchSQL.trim() === '' || batchSQL.trim() === ';') {
    logger.log('No SQL to execute (empty batch)');
    return;
  }

  // Detect DB type if not provided
  const detectedDbType = dbType || (knex.client.config.client as string);
  const isPostgres = detectedDbType.includes('pg') || detectedDbType.includes('postgres');

  if (isPostgres) {
    logger.log(`Executing batch SQL with TRANSACTION (PostgreSQL)...`);

    // PostgreSQL: Use transaction for atomic DDL
    try {
      await knex.transaction(async (trx) => {
        await trx.raw(batchSQL);
      });
      logger.log(`Batch SQL executed successfully (transaction committed)`);
    } catch (error) {
      logger.error(`Batch SQL execution failed (transaction rolled back)`);
      logger.error(`Error: ${error.message}`);
      logger.error(`Failed SQL:\n${batchSQL.substring(0, 500)}${batchSQL.length > 500 ? '...' : ''}`);
      throw error;
    }
  } else {
    logger.log(`Executing batch SQL (${detectedDbType})...`);
    logger.warn(`${detectedDbType.toUpperCase()} does not support transactional DDL - changes cannot be rolled back`);

    // MySQL/SQLite: Execute batch without transaction
    try {
      await knex.raw(batchSQL);
      logger.log(`Batch SQL executed successfully`);
    } catch (error) {
      logger.error(`Batch SQL execution failed`);
      logger.error(`Some statements may have been executed before failure - manual recovery may be required`);
      logger.error(`Error: ${error.message}`);
      logger.error(`Failed SQL:\n${batchSQL.substring(0, 500)}${batchSQL.length > 500 ? '...' : ''}`);
      throw error;
    }
  }
}

/**
 * Legacy function - Generate statements array and execute as batch
 * @deprecated Use generateBatchSQL + executeBatchSQL for better control
 */
export async function executeSQLStatements(
  knex: Knex,
  sqlStatements: string[],
): Promise<void> {
  const batchSQL = generateBatchSQL(sqlStatements);
  await executeBatchSQL(knex, batchSQL);
}
