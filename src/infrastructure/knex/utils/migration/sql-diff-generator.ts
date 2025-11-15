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

async function getPrimaryKeyTypeForTable(
  knex: Knex,
  tableName: string,
  metadataCacheService?: any,
): Promise<'uuid' | 'int'> {
  try {
    if (metadataCacheService) {
      const targetMetadata = await metadataCacheService.lookupTableByName(tableName);
      if (targetMetadata) {
        const pkColumn = targetMetadata.columns.find((c: any) => c.isPrimary);
        if (pkColumn) {
          const type = pkColumn.type?.toLowerCase() || '';
          return type === 'uuid' || type === 'uuidv4' || type.includes('uuid') ? 'uuid' : 'int';
        }
      }
    }
    
    const pkInfo = await knex('column_definition')
      .join('table_definition', 'column_definition.table', '=', 'table_definition.id')
      .where('table_definition.name', tableName)
      .where('column_definition.isPrimary', true)
      .select('column_definition.type')
      .first();
    
    if (pkInfo) {
      const type = pkInfo.type?.toLowerCase() || '';
      return type === 'uuid' || type === 'uuidv4' || type.includes('uuid') ? 'uuid' : 'int';
    }
    
    logger.warn(`Could not find primary key for table ${tableName}, defaulting to int`);
    return 'int';
  } catch (error) {
    logger.warn(`Error getting primary key type for ${tableName}: ${error.message}, defaulting to int`);
    return 'int';
  }
}

/**
 * Generate rollback SQL for a DDL statement
 * @param statement - The original SQL statement
 * @param dbType - Database type
 * @returns Rollback SQL statement or null if rollback is not possible
 */
function generateRollbackSQL(statement: string, dbType: string): string | null {
  const stmt = statement.trim().toUpperCase();
  const qt = (id: string) => {
    if (dbType === 'mysql') return `\`${id}\``;
    return `"${id}"`;
  };

  // ALTER TABLE ... ADD COLUMN
  const addColumnMatch = stmt.match(/ALTER\s+TABLE\s+([^\s]+)\s+ADD\s+COLUMN\s+([^\s]+)/i);
  if (addColumnMatch) {
    const tableName = addColumnMatch[1].replace(/[`"]/g, '');
    const columnName = addColumnMatch[2].replace(/[`"]/g, '');
    return `ALTER TABLE ${qt(tableName)} DROP COLUMN ${qt(columnName)}`;
  }

  // ALTER TABLE ... DROP COLUMN
  const dropColumnMatch = stmt.match(/ALTER\s+TABLE\s+([^\s]+)\s+DROP\s+COLUMN\s+([^\s]+)/i);
  if (dropColumnMatch) {
    // Cannot rollback DROP COLUMN without original column definition
    return null;
  }

  // ALTER TABLE ... ADD CONSTRAINT ... FOREIGN KEY
  const addFkMatch = stmt.match(/ALTER\s+TABLE\s+([^\s]+)\s+ADD\s+CONSTRAINT\s+([^\s]+)\s+FOREIGN\s+KEY/i);
  if (addFkMatch) {
    const tableName = addFkMatch[1].replace(/[`"]/g, '');
    const constraintName = addFkMatch[2].replace(/[`"]/g, '');
    return `ALTER TABLE ${qt(tableName)} DROP CONSTRAINT ${qt(constraintName)}`;
  }

  // ALTER TABLE ... DROP CONSTRAINT
  const dropFkMatch = stmt.match(/ALTER\s+TABLE\s+([^\s]+)\s+DROP\s+CONSTRAINT\s+([^\s]+)/i);
  if (dropFkMatch) {
    // Cannot rollback DROP CONSTRAINT without original constraint definition
    return null;
  }

  // ALTER TABLE ... ADD CONSTRAINT ... UNIQUE
  const addUniqueMatch = stmt.match(/ALTER\s+TABLE\s+([^\s]+)\s+ADD\s+CONSTRAINT\s+([^\s]+)\s+UNIQUE/i);
  if (addUniqueMatch) {
    const tableName = addUniqueMatch[1].replace(/[`"]/g, '');
    const constraintName = addUniqueMatch[2].replace(/[`"]/g, '');
    return `ALTER TABLE ${qt(tableName)} DROP CONSTRAINT ${qt(constraintName)}`;
  }

  // CREATE INDEX
  const createIndexMatch = stmt.match(/CREATE\s+(?:UNIQUE\s+)?INDEX\s+([^\s]+)\s+ON\s+([^\s]+)/i);
  if (createIndexMatch) {
    const indexName = createIndexMatch[1].replace(/[`"]/g, '');
    const tableName = createIndexMatch[2].replace(/[`"]/g, '');
    return `DROP INDEX ${qt(indexName)} ON ${qt(tableName)}`;
  }

  // DROP INDEX
  const dropIndexMatch = stmt.match(/DROP\s+INDEX\s+([^\s]+)\s+ON\s+([^\s]+)/i);
  if (dropIndexMatch) {
    // Cannot rollback DROP INDEX without original index definition
    return null;
  }

  // RENAME TABLE
  const renameTableMatch = stmt.match(/ALTER\s+TABLE\s+([^\s]+)\s+RENAME\s+TO\s+([^\s]+)/i);
  if (renameTableMatch) {
    const oldName = renameTableMatch[1].replace(/[`"]/g, '');
    const newName = renameTableMatch[2].replace(/[`"]/g, '');
    return `ALTER TABLE ${qt(newName)} RENAME TO ${qt(oldName)}`;
  }

  // RENAME COLUMN
  const renameColumnMatch = stmt.match(/ALTER\s+TABLE\s+([^\s]+)\s+RENAME\s+COLUMN\s+([^\s]+)\s+TO\s+([^\s]+)/i);
  if (renameColumnMatch) {
    const tableName = renameColumnMatch[1].replace(/[`"]/g, '');
    const oldName = renameColumnMatch[2].replace(/[`"]/g, '');
    const newName = renameColumnMatch[3].replace(/[`"]/g, '');
    if (dbType === 'postgres') {
      return `ALTER TABLE ${qt(tableName)} RENAME COLUMN ${qt(newName)} TO ${qt(oldName)}`;
    } else {
      return `ALTER TABLE ${qt(tableName)} CHANGE ${qt(newName)} ${qt(oldName)}`;
    }
  }

  // MODIFY COLUMN - Cannot rollback without original definition
  if (stmt.includes('MODIFY COLUMN') || stmt.includes('ALTER COLUMN')) {
    return null;
  }

  return null;
}

export async function generateSQLFromDiff(
  knex: Knex,
  tableName: string,
  diff: any,
  dbType: 'mysql' | 'postgres' | 'sqlite',
  metadataCacheService?: any,
): Promise<string[]> {
  const sqlStatements: string[] = [];
  const qt = (id: string) => quoteIdentifier(id, dbType); // Quote helper

  const ensureArray = <T>(value: T | T[] | undefined | null): T[] => {
    if (!value) {
      return [];
    }
    return Array.isArray(value) ? value : [value];
  };

  const tableDiff = diff.table || {};
  const columnDiff = diff.columns || {};
  const constraintDiff = diff.constraints || {};
  const junctionDiff = diff.junctionTables || {};
  const fkDiff = diff.foreignKeys || {};
  const crossTableOps = ensureArray(diff.crossTableOperations);

  if (tableDiff.update) {
    sqlStatements.push(generateRenameTableSQL(tableDiff.update.oldName, tableDiff.update.newName, dbType));
  }

  const renamedColumns = new Set<string>();
  for (const rename of ensureArray(columnDiff.rename)) {
    sqlStatements.push(generateRenameColumnSQL(tableName, rename.oldName, rename.newName, dbType));
    renamedColumns.add(rename.oldName);
    renamedColumns.add(rename.newName);
  }

  // DELETE columns first (before CREATE) to avoid duplicate column name errors
  for (const col of ensureArray(columnDiff.delete)) {
    if (col.isForeignKey) {
      await dropForeignKeyIfExists(knex, tableName, col.name, dbType);
    }
    sqlStatements.push(`ALTER TABLE ${qt(tableName)} DROP COLUMN ${qt(col.name)}`);
  }

  // CREATE columns after DELETE
  for (const col of ensureArray(columnDiff.create)) {
    const columnDef = generateColumnDefinition(col, dbType);
    sqlStatements.push(`ALTER TABLE ${qt(tableName)} ADD COLUMN ${qt(col.name)} ${columnDef}`);

    if (col.isForeignKey && col.foreignKeyTarget) {
      const onDelete = col.onDelete || (col.isNullable === false ? 'RESTRICT' : 'SET NULL');
      sqlStatements.push(
        `ALTER TABLE ${qt(tableName)} ADD CONSTRAINT ${qt(`fk_${tableName}_${col.name}`)} FOREIGN KEY (${qt(col.name)}) REFERENCES ${qt(col.foreignKeyTarget)} (${qt(col.foreignKeyColumn || 'id')}) ON DELETE ${onDelete} ON UPDATE CASCADE`
      );
    }

    // Add UNIQUE constraint for O2O relations
    if (col.isUnique) {
      const uniqueConstraintName = `uq_${tableName}_${col.name}`;
      sqlStatements.push(
        `ALTER TABLE ${qt(tableName)} ADD CONSTRAINT ${qt(uniqueConstraintName)} UNIQUE (${qt(col.name)})`
      );
      logger.log(`  Added UNIQUE constraint on ${col.name} for one-to-one relation`);
    }

    // Add index for datetime/timestamp/date columns
    if (col.type === 'datetime' || col.type === 'timestamp' || col.type === 'date') {
      const indexName = `idx_${tableName}_${col.name}`;
      sqlStatements.push(generateAddIndexSQL(tableName, indexName, [col.name], dbType));
      logger.log(`  Added index on datetime column: ${col.name}`);
    }
  }

  const processedUpdates = new Set<string>();
  for (const update of ensureArray(columnDiff.update)) {
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
  for (const uniqueGroup of ensureArray(constraintDiff.uniques?.create) || []) {
    const columns = uniqueGroup.map((col: string) => qt(col)).join(', ');
    const constraintName = `uq_${tableName}_${uniqueGroup.join('_')}`;
    sqlStatements.push(`ALTER TABLE ${qt(tableName)} ADD CONSTRAINT ${qt(constraintName)} UNIQUE (${columns})`);
    logger.log(`  Added UNIQUE constraint ${constraintName} on (${uniqueGroup.join(', ')})`);
  }

  // Handle UNIQUE constraint UPDATE (should be same as CREATE for now)
  for (const uniqueGroup of ensureArray(constraintDiff.uniques?.update) || []) {
    const columns = uniqueGroup.map((col: string) => qt(col)).join(', ');
    sqlStatements.push(`ALTER TABLE ${qt(tableName)} ADD UNIQUE (${columns})`);
  }

  for (const indexGroup of ensureArray(constraintDiff.indexes?.update) || []) {
    const indexName = `idx_${tableName}_${indexGroup.join('_')}`;
    sqlStatements.push(generateAddIndexSQL(tableName, indexName, indexGroup, dbType));
  }

  for (const crossOp of crossTableOps) {
    if (crossOp.operation === 'createColumn') {
      const columnDef = generateColumnDefinition(crossOp.column, dbType);
      sqlStatements.push(`ALTER TABLE ${qt(crossOp.targetTable)} ADD COLUMN ${qt(crossOp.column.name)} ${columnDef}`);

      if (crossOp.column.isForeignKey) {
        const onDelete = crossOp.column.onDelete || (crossOp.column.isNullable !== false ? 'SET NULL' : 'RESTRICT');
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
  for (const junctionRename of ensureArray(junctionDiff.rename)) {
    const { oldTableName, newTableName } = junctionRename;
    logger.log(`Renaming junction table: ${oldTableName} → ${newTableName}`);
    sqlStatements.push(generateRenameTableSQL(oldTableName, newTableName, dbType));
  }

  for (const junctionCreate of ensureArray(junctionDiff.create)) {
    const { tableName: junctionName, sourceTable, targetTable, sourceColumn, targetColumn } = junctionCreate;

    const tableExists = await knex.schema.hasTable(junctionName);
    if (tableExists) {
      logger.log(`  Junction table ${junctionName} already exists, skipping`);
      continue;
    }

    const sourcePkType = await getPrimaryKeyTypeForTable(knex, sourceTable, metadataCacheService);
    const targetPkType = await getPrimaryKeyTypeForTable(knex, targetTable, metadataCacheService);

    logger.log(`  Junction table ${junctionName}: Source PK type: ${sourcePkType}, Target PK type: ${targetPkType}`);

    const getSourceColumnType = () => {
      if (sourcePkType === 'uuid') {
        if (dbType === 'postgres') {
          return 'UUID';
        } else {
          return 'VARCHAR(36)';
        }
      }
      if (dbType === 'postgres') {
        return 'INTEGER';
      } else if (dbType === 'sqlite') {
        return 'INTEGER';
      } else {
        return 'INT UNSIGNED';
      }
    };

    const getTargetColumnType = () => {
      if (targetPkType === 'uuid') {
        if (dbType === 'postgres') {
          return 'UUID';
        } else {
          return 'VARCHAR(36)';
        }
      }
      if (dbType === 'postgres') {
        return 'INTEGER';
      } else if (dbType === 'sqlite') {
        return 'INTEGER';
      } else {
        return 'INT UNSIGNED';
      }
    };

    const sourceColType = getSourceColumnType();
    const targetColType = getTargetColumnType();

    // Generate database-specific CREATE TABLE syntax
    let createJunctionSQL: string;
    if (dbType === 'postgres') {
      createJunctionSQL = `
        CREATE TABLE ${qt(junctionName)} (
          ${qt('id')} SERIAL PRIMARY KEY,
          ${qt(sourceColumn)} ${sourceColType} NOT NULL,
          ${qt(targetColumn)} ${targetColType} NOT NULL,
          FOREIGN KEY (${qt(sourceColumn)}) REFERENCES ${qt(sourceTable)} (${qt('id')}) ON DELETE CASCADE ON UPDATE CASCADE,
          FOREIGN KEY (${qt(targetColumn)}) REFERENCES ${qt(targetTable)} (${qt('id')}) ON DELETE CASCADE ON UPDATE CASCADE,
          UNIQUE (${qt(sourceColumn)}, ${qt(targetColumn)})
        )
      `.trim().replace(/\s+/g, ' ');
    } else if (dbType === 'sqlite') {
      createJunctionSQL = `
        CREATE TABLE ${qt(junctionName)} (
          ${qt('id')} INTEGER PRIMARY KEY AUTOINCREMENT,
          ${qt(sourceColumn)} ${sourceColType} NOT NULL,
          ${qt(targetColumn)} ${targetColType} NOT NULL,
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
          ${qt(sourceColumn)} ${sourceColType} NOT NULL,
          ${qt(targetColumn)} ${targetColType} NOT NULL,
          FOREIGN KEY (${qt(sourceColumn)}) REFERENCES ${qt(sourceTable)} (${qt('id')}) ON DELETE CASCADE ON UPDATE CASCADE,
          FOREIGN KEY (${qt(targetColumn)}) REFERENCES ${qt(targetTable)} (${qt('id')}) ON DELETE CASCADE ON UPDATE CASCADE,
          UNIQUE KEY ${qt(`unique_${sourceColumn}_${targetColumn}`)} (${qt(sourceColumn)}, ${qt(targetColumn)})
        )
      `.trim().replace(/\s+/g, ' ');
    }

    sqlStatements.push(createJunctionSQL);
  }

  for (const junctionDrop of ensureArray(junctionDiff.drop)) {
    const { tableName: junctionName } = junctionDrop;
    sqlStatements.push(`DROP TABLE IF EXISTS ${qt(junctionName)}`);
  }

  // Handle FK constraint recreation (for onDelete changes)
  for (const fkRecreate of ensureArray(fkDiff.recreate)) {
    const { tableName: fkTableName, columnName, targetTable, targetColumn, onDelete } = fkRecreate;
    const fkName = `fk_${fkTableName}_${columnName}`;

    logger.log(`Recreating FK constraint ${fkName} with onDelete: ${onDelete}`);

    // Drop existing FK constraint
    await dropForeignKeyIfExists(knex, fkTableName, columnName, dbType);

    // Recreate FK constraint with new onDelete action
    sqlStatements.push(
      `ALTER TABLE ${qt(fkTableName)} ADD CONSTRAINT ${qt(fkName)} FOREIGN KEY (${qt(columnName)}) REFERENCES ${qt(targetTable)} (${qt(targetColumn || 'id')}) ON DELETE ${onDelete} ON UPDATE CASCADE`
    );
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
    logger.log(`Executing SQL statements individually (${detectedDbType})...`);
    logger.warn(`${detectedDbType.toUpperCase()} does not support transactional DDL - changes cannot be automatically rolled back`);

    // MySQL/SQLite: Execute each statement individually
    // MySQL doesn't support multiple statements in a single query by default
    const statements = batchSQL
      .split(';')
      .map(s => s.trim())
      .filter(s => s.length > 0);

    logger.log(`Executing ${statements.length} statement(s) individually...`);

    const executedStatements: string[] = [];
    const rollbackStatements: string[] = [];

    try {
      for (let i = 0; i < statements.length; i++) {
        const statement = statements[i];
        logger.log(`  [${i + 1}/${statements.length}] Executing: ${statement.substring(0, 80)}${statement.length > 80 ? '...' : ''}`);
        
        try {
          await knex.raw(statement);
          executedStatements.push(statement);
          
          // Generate rollback statement for DDL operations
          const rollbackSQL = generateRollbackSQL(statement, detectedDbType);
          if (rollbackSQL) {
            rollbackStatements.push(rollbackSQL);
          }
        } catch (statementError: any) {
          logger.error(`Failed at statement [${i + 1}/${statements.length}]: ${statement.substring(0, 100)}`);
          logger.error(`Error: ${statementError.message}`);
          
          if (executedStatements.length > 0) {
            logger.error(`\n${'='.repeat(80)}`);
            logger.error(`⚠️  PARTIAL MIGRATION DETECTED`);
            logger.error(`${'='.repeat(80)}`);
            logger.error(`Successfully executed ${executedStatements.length} statement(s) before failure:`);
            executedStatements.forEach((stmt, idx) => {
              logger.error(`  [${idx + 1}] ${stmt.substring(0, 100)}${stmt.length > 100 ? '...' : ''}`);
            });
            logger.error(`\nTo rollback, execute these statements in reverse order:`);
            rollbackStatements.reverse().forEach((stmt, idx) => {
              logger.error(`  [${idx + 1}] ${stmt.substring(0, 100)}${stmt.length > 100 ? '...' : ''}`);
            });
            logger.error(`${'='.repeat(80)}\n`);
          }
          
          throw statementError;
        }
      }
      logger.log(`All ${statements.length} statement(s) executed successfully`);
    } catch (error: any) {
      logger.error(`\n${'='.repeat(80)}`);
      logger.error(`❌ MIGRATION FAILED`);
      logger.error(`${'='.repeat(80)}`);
      logger.error(`Error: ${error.message}`);
      if (executedStatements.length > 0) {
        logger.error(`\n⚠️  ${executedStatements.length} statement(s) were executed before failure.`);
        logger.error(`Manual rollback may be required. See rollback statements above.`);
      }
      logger.error(`${'='.repeat(80)}\n`);
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
