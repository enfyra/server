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
  generateDropIndexSQL,
  generateDropColumnSQL,
} from './sql-dialect';
const logger = new Logger('SqlDiffGenerator');
async function getPrimaryKeyTypeForTable(
  knex: Knex,
  tableName: string,
  metadataCacheService?: any,
): Promise<'uuid' | 'int'> {
  try {
    if (metadataCacheService) {
      const targetMetadata =
        await metadataCacheService.lookupTableByName(tableName);
      if (targetMetadata) {
        const pkColumn = targetMetadata.columns.find((c: any) => c.isPrimary);
        if (pkColumn) {
          const type = pkColumn.type?.toLowerCase() || '';
          return type === 'uuid' || type === 'uuidv4' || type.includes('uuid')
            ? 'uuid'
            : 'int';
        }
      }
    }
    const pkInfo = await knex('column_definition')
      .join(
        'table_definition',
        'column_definition.table',
        '=',
        'table_definition.id',
      )
      .where('table_definition.name', tableName)
      .where('column_definition.isPrimary', true)
      .select('column_definition.type')
      .first();
    if (pkInfo) {
      const type = pkInfo.type?.toLowerCase() || '';
      return type === 'uuid' || type === 'uuidv4' || type.includes('uuid')
        ? 'uuid'
        : 'int';
    }
    logger.warn(
      `Could not find primary key for table ${tableName}, defaulting to int`,
    );
    return 'int';
  } catch (error) {
    logger.warn(
      `Error getting primary key type for ${tableName}: ${error.message}, defaulting to int`,
    );
    return 'int';
  }
}
function isIdempotentDDLError(err: any, dbType: string): boolean {
  const code = err?.code || err?.errno;
  const msg = String(err?.message || '').toLowerCase();
  if (dbType.includes('mysql') || dbType.includes('mariadb')) {
    const idempotentErrnos = new Set([
      1060, // Duplicate column name
      1061, // Duplicate key name
      1050, // Table already exists
      1091, // Can't DROP; doesn't exist
      1826, // Duplicate foreign key constraint
      3822, // Duplicate check constraint
      1068, // Multiple primary keys defined (already has one)
    ]);
    if (idempotentErrnos.has(Number(code))) return true;
    if (
      msg.includes('duplicate column') ||
      msg.includes('duplicate key name') ||
      msg.includes('already exists') ||
      msg.includes("check that column/key exists") ||
      msg.includes('duplicate foreign key')
    ) {
      return true;
    }
  }
  if (dbType.includes('pg') || dbType.includes('postgres')) {
    const pgCodes = new Set([
      '42701', // duplicate_column
      '42P07', // duplicate_table
      '42710', // duplicate_object (constraint/index)
      '42P16', // invalid_table_definition (PK already)
    ]);
    if (pgCodes.has(String(code))) return true;
  }
  return false;
}
function generateRollbackSQL(statement: string, dbType: string): string | null {
  const stmt = statement.trim().toUpperCase();
  const qt = (id: string) => {
    if (dbType === 'mysql') return `\`${id}\``;
    return `"${id}"`;
  };
  const addColumnMatch = stmt.match(
    /ALTER\s+TABLE\s+([^\s]+)\s+ADD\s+COLUMN\s+([^\s]+)/i,
  );
  if (addColumnMatch) {
    const tableName = addColumnMatch[1].replace(/[`"]/g, '');
    const columnName = addColumnMatch[2].replace(/[`"]/g, '');
    return `ALTER TABLE ${qt(tableName)} DROP COLUMN ${qt(columnName)}`;
  }
  const dropColumnMatch = stmt.match(
    /ALTER\s+TABLE\s+([^\s]+)\s+DROP\s+COLUMN\s+([^\s]+)/i,
  );
  if (dropColumnMatch) {
    return null;
  }
  const addFkMatch = stmt.match(
    /ALTER\s+TABLE\s+([^\s]+)\s+ADD\s+CONSTRAINT\s+([^\s]+)\s+FOREIGN\s+KEY/i,
  );
  if (addFkMatch) {
    const tableName = addFkMatch[1].replace(/[`"]/g, '');
    const constraintName = addFkMatch[2].replace(/[`"]/g, '');
    return `ALTER TABLE ${qt(tableName)} DROP CONSTRAINT ${qt(constraintName)}`;
  }
  const dropFkMatch = stmt.match(
    /ALTER\s+TABLE\s+([^\s]+)\s+DROP\s+CONSTRAINT\s+([^\s]+)/i,
  );
  if (dropFkMatch) {
    return null;
  }
  const addUniqueMatch = stmt.match(
    /ALTER\s+TABLE\s+([^\s]+)\s+ADD\s+CONSTRAINT\s+([^\s]+)\s+UNIQUE/i,
  );
  if (addUniqueMatch) {
    const tableName = addUniqueMatch[1].replace(/[`"]/g, '');
    const constraintName = addUniqueMatch[2].replace(/[`"]/g, '');
    return `ALTER TABLE ${qt(tableName)} DROP CONSTRAINT ${qt(constraintName)}`;
  }
  const createIndexMatch = stmt.match(
    /CREATE\s+(?:UNIQUE\s+)?INDEX\s+([^\s]+)\s+ON\s+([^\s]+)/i,
  );
  if (createIndexMatch) {
    const indexName = createIndexMatch[1].replace(/[`"]/g, '');
    const tableName = createIndexMatch[2].replace(/[`"]/g, '');
    return `DROP INDEX ${qt(indexName)} ON ${qt(tableName)}`;
  }
  const dropIndexMatch = stmt.match(/DROP\s+INDEX\s+([^\s]+)\s+ON\s+([^\s]+)/i);
  if (dropIndexMatch) {
    return null;
  }
  const renameTableMatch = stmt.match(
    /ALTER\s+TABLE\s+([^\s]+)\s+RENAME\s+TO\s+([^\s]+)/i,
  );
  if (renameTableMatch) {
    const oldName = renameTableMatch[1].replace(/[`"]/g, '');
    const newName = renameTableMatch[2].replace(/[`"]/g, '');
    return `ALTER TABLE ${qt(newName)} RENAME TO ${qt(oldName)}`;
  }
  const renameColumnMatch = stmt.match(
    /ALTER\s+TABLE\s+([^\s]+)\s+RENAME\s+COLUMN\s+([^\s]+)\s+TO\s+([^\s]+)/i,
  );
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
  const qt = (id: string) => quoteIdentifier(id, dbType);
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
    sqlStatements.push(
      generateRenameTableSQL(
        tableDiff.update.oldName,
        tableDiff.update.newName,
        dbType,
      ),
    );
  }
  const renamedColumns = new Set<string>();
  for (const rename of ensureArray(columnDiff.rename)) {
    sqlStatements.push(
      generateRenameColumnSQL(
        tableName,
        rename.oldName,
        rename.newName,
        dbType,
      ),
    );
    renamedColumns.add(rename.oldName);
    renamedColumns.add(rename.newName);
  }
  for (const col of ensureArray(columnDiff.delete)) {
    if (col.isForeignKey) {
      await dropForeignKeyIfExists(knex, tableName, col.name, dbType);
    }
    sqlStatements.push(generateDropColumnSQL(tableName, col.name, dbType));
  }
  for (const col of ensureArray(columnDiff.create)) {
    const columnDef = generateColumnDefinition(col, dbType);
    sqlStatements.push(
      `ALTER TABLE ${qt(tableName)} ADD COLUMN ${qt(col.name)} ${columnDef}`,
    );
    if (col.isForeignKey && col.foreignKeyTarget) {
      const onDelete =
        col.onDelete || (col.isNullable === false ? 'RESTRICT' : 'SET NULL');
      sqlStatements.push(
        `ALTER TABLE ${qt(tableName)} ADD CONSTRAINT ${qt(`fk_${tableName}_${col.name}`)} FOREIGN KEY (${qt(col.name)}) REFERENCES ${qt(col.foreignKeyTarget)} (${qt(col.foreignKeyColumn || 'id')}) ON DELETE ${onDelete} ON UPDATE CASCADE`,
      );
    }
    if (col.isUnique) {
      const uniqueConstraintName = `uq_${tableName}_${col.name}`;
      sqlStatements.push(
        `ALTER TABLE ${qt(tableName)} ADD CONSTRAINT ${qt(uniqueConstraintName)} UNIQUE (${qt(col.name)})`,
      );
      logger.log(
        `  Added UNIQUE constraint on ${col.name} for one-to-one relation`,
      );
    }
    if (
      col.type === 'datetime' ||
      col.type === 'timestamp' ||
      col.type === 'date'
    ) {
      const indexName = `idx_${tableName}_${col.name}`;
      sqlStatements.push(
        generateAddIndexSQL(tableName, indexName, [col.name, 'id'], dbType),
      );
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
    const modifySQL = generateModifyColumnSQL(
      tableName,
      update.newColumn.name,
      columnDef,
      dbType,
      update.oldColumn,
    );
    if (Array.isArray(modifySQL)) {
      sqlStatements.push(...modifySQL);
    } else {
      sqlStatements.push(modifySQL);
    }
  }
  for (const uniqueGroup of ensureArray(constraintDiff.uniques?.create) || []) {
    const columns = uniqueGroup.map((col: string) => qt(col)).join(', ');
    const constraintName = `uq_${tableName}_${uniqueGroup.join('_')}`;
    sqlStatements.push(
      `ALTER TABLE ${qt(tableName)} ADD CONSTRAINT ${qt(constraintName)} UNIQUE (${columns})`,
    );
    logger.log(
      `  Added UNIQUE constraint ${constraintName} on (${uniqueGroup.join(', ')})`,
    );
  }
  for (const uniqueGroup of ensureArray(constraintDiff.uniques?.update) || []) {
    const columns = uniqueGroup.map((col: string) => qt(col)).join(', ');
    sqlStatements.push(`ALTER TABLE ${qt(tableName)} ADD UNIQUE (${columns})`);
  }
  for (const indexGroup of ensureArray(constraintDiff.indexes?.delete) || []) {
    const cols = Array.isArray(indexGroup)
      ? indexGroup
      : indexGroup?.value || [];
    if (cols.length === 0) continue;
    const indexName = `idx_${tableName}_${cols.join('_')}`;
    sqlStatements.push(generateDropIndexSQL(tableName, indexName, dbType));
    logger.log(`  Drop index ${indexName} (columns: ${cols.join(', ')})`);
  }
  for (const indexGroup of ensureArray(constraintDiff.indexes?.create) || []) {
    const cols = Array.isArray(indexGroup)
      ? indexGroup
      : indexGroup?.value || [];
    if (cols.length === 0) continue;
    const indexName = `idx_${tableName}_${cols.join('_')}`;
    const physicalCols = cols.includes('id') ? cols : [...cols, 'id'];
    sqlStatements.push(
      generateAddIndexSQL(tableName, indexName, physicalCols, dbType),
    );
    logger.log(
      `  Add index ${indexName} (columns: ${physicalCols.join(', ')})`,
    );
  }
  for (const crossOp of crossTableOps) {
    if (crossOp.operation === 'createColumn') {
      const columnDef = generateColumnDefinition(crossOp.column, dbType);
      sqlStatements.push(
        `ALTER TABLE ${qt(crossOp.targetTable)} ADD COLUMN ${qt(crossOp.column.name)} ${columnDef}`,
      );
      if (crossOp.column.isForeignKey) {
        const onDelete =
          crossOp.column.onDelete ||
          (crossOp.column.isNullable !== false ? 'SET NULL' : 'RESTRICT');
        sqlStatements.push(
          `ALTER TABLE ${qt(crossOp.targetTable)} ADD CONSTRAINT ${qt(`fk_${crossOp.targetTable}_${crossOp.column.name}`)} FOREIGN KEY (${qt(crossOp.column.name)}) REFERENCES ${qt(crossOp.column.foreignKeyTarget)} (${qt(crossOp.column.foreignKeyColumn)}) ON DELETE ${onDelete} ON UPDATE CASCADE`,
        );
      }
    } else if (crossOp.operation === 'dropColumn') {
      await dropForeignKeyIfExists(
        knex,
        crossOp.targetTable,
        crossOp.columnName,
        dbType,
      );
      sqlStatements.push(
        generateDropColumnSQL(crossOp.targetTable, crossOp.columnName, dbType),
      );
    } else if (crossOp.operation === 'renameColumn') {
      sqlStatements.push(
        generateRenameColumnSQL(
          crossOp.targetTable,
          crossOp.oldColumnName,
          crossOp.newColumnName,
          dbType,
        ),
      );
    }
  }
  for (const junctionRename of ensureArray(junctionDiff.rename)) {
    const { oldTableName, newTableName } = junctionRename;
    logger.log(`Renaming junction table: ${oldTableName} → ${newTableName}`);
    sqlStatements.push(
      generateRenameTableSQL(oldTableName, newTableName, dbType),
    );
  }
  for (const junctionCreate of ensureArray(junctionDiff.create)) {
    const {
      tableName: junctionName,
      sourceTable,
      targetTable,
      sourceColumn,
      targetColumn,
    } = junctionCreate;
    const tableExists = await knex.schema.hasTable(junctionName);
    if (tableExists) {
      logger.log(`  Junction table ${junctionName} already exists, skipping`);
      continue;
    }
    const sourcePkType = await getPrimaryKeyTypeForTable(
      knex,
      sourceTable,
      metadataCacheService,
    );
    const targetPkType = await getPrimaryKeyTypeForTable(
      knex,
      targetTable,
      metadataCacheService,
    );
    logger.log(
      `  Junction table ${junctionName}: Source PK type: ${sourcePkType}, Target PK type: ${targetPkType}`,
    );
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
    let createJunctionSQL: string;
    if (dbType === 'postgres') {
      createJunctionSQL = `
        CREATE TABLE ${qt(junctionName)} (
          ${qt(sourceColumn)} ${sourceColType} NOT NULL,
          ${qt(targetColumn)} ${targetColType} NOT NULL,
          PRIMARY KEY (${qt(sourceColumn)}, ${qt(targetColumn)}),
          FOREIGN KEY (${qt(sourceColumn)}) REFERENCES ${qt(sourceTable)} (${qt('id')}) ON DELETE CASCADE ON UPDATE CASCADE,
          FOREIGN KEY (${qt(targetColumn)}) REFERENCES ${qt(targetTable)} (${qt('id')}) ON DELETE CASCADE ON UPDATE CASCADE
        )
      `
        .trim()
        .replace(/\s+/g, ' ');
    } else if (dbType === 'sqlite') {
      createJunctionSQL = `
        CREATE TABLE ${qt(junctionName)} (
          ${qt(sourceColumn)} ${sourceColType} NOT NULL,
          ${qt(targetColumn)} ${targetColType} NOT NULL,
          PRIMARY KEY (${qt(sourceColumn)}, ${qt(targetColumn)}),
          FOREIGN KEY (${qt(sourceColumn)}) REFERENCES ${qt(sourceTable)} (${qt('id')}) ON DELETE CASCADE ON UPDATE CASCADE,
          FOREIGN KEY (${qt(targetColumn)}) REFERENCES ${qt(targetTable)} (${qt('id')}) ON DELETE CASCADE ON UPDATE CASCADE
        )
      `
        .trim()
        .replace(/\s+/g, ' ');
    } else {
      createJunctionSQL = `
        CREATE TABLE ${qt(junctionName)} (
          ${qt(sourceColumn)} ${sourceColType} NOT NULL,
          ${qt(targetColumn)} ${targetColType} NOT NULL,
          PRIMARY KEY (${qt(sourceColumn)}, ${qt(targetColumn)}),
          KEY ${qt(`idx_${sourceColumn}`)} (${qt(sourceColumn)}),
          KEY ${qt(`idx_${targetColumn}`)} (${qt(targetColumn)}),
          KEY ${qt(`idx_${targetColumn}_${sourceColumn}`)} (${qt(targetColumn)}, ${qt(sourceColumn)}),
          FOREIGN KEY (${qt(sourceColumn)}) REFERENCES ${qt(sourceTable)} (${qt('id')}) ON DELETE CASCADE ON UPDATE CASCADE,
          FOREIGN KEY (${qt(targetColumn)}) REFERENCES ${qt(targetTable)} (${qt('id')}) ON DELETE CASCADE ON UPDATE CASCADE
        )
      `
        .trim()
        .replace(/\s+/g, ' ');
    }
    sqlStatements.push(createJunctionSQL);
    if (dbType === 'postgres') {
      const indexSourceSQL = `CREATE INDEX ${qt(`idx_${sourceColumn}`)} ON ${qt(junctionName)} (${qt(sourceColumn)})`;
      const indexTargetSQL = `CREATE INDEX ${qt(`idx_${targetColumn}`)} ON ${qt(junctionName)} (${qt(targetColumn)})`;
      const indexReverseSQL = `CREATE INDEX ${qt(`idx_${targetColumn}_${sourceColumn}`)} ON ${qt(junctionName)} (${qt(targetColumn)}, ${qt(sourceColumn)})`;
      sqlStatements.push(indexSourceSQL, indexTargetSQL, indexReverseSQL);
    }
  }
  for (const junctionDrop of ensureArray(junctionDiff.drop)) {
    const { tableName: junctionName } = junctionDrop;
    sqlStatements.push(`DROP TABLE IF EXISTS ${qt(junctionName)}`);
  }
  for (const fkRecreate of ensureArray(fkDiff.recreate)) {
    const {
      tableName: fkTableName,
      columnName,
      targetTable,
      targetColumn,
      onDelete,
    } = fkRecreate;
    const fkName = `fk_${fkTableName}_${columnName}`;
    logger.log(`Recreating FK constraint ${fkName} with onDelete: ${onDelete}`);
    await dropForeignKeyIfExists(knex, fkTableName, columnName, dbType);
    sqlStatements.push(
      `ALTER TABLE ${qt(fkTableName)} ADD CONSTRAINT ${qt(fkName)} FOREIGN KEY (${qt(columnName)}) REFERENCES ${qt(targetTable)} (${qt(targetColumn || 'id')}) ON DELETE ${onDelete} ON UPDATE CASCADE`,
    );
  }
  return sqlStatements;
}
export function generateBatchSQL(sqlStatements: string[]): string {
  if (sqlStatements.length === 0) {
    return '';
  }
  const batchSQL = sqlStatements.join(';\n') + ';';
  logger.log(
    `📦 Generated batch SQL with ${sqlStatements.length} statement(s):`,
  );
  sqlStatements.forEach((sql, i) => {
    logger.log(
      `  [${i + 1}/${sqlStatements.length}] ${sql.substring(0, 80)}${sql.length > 80 ? '...' : ''}`,
    );
  });
  return batchSQL;
}
export async function executeBatchSQL(
  knex: Knex,
  batchSQL: string,
  dbType?: 'mysql' | 'postgres' | 'sqlite',
  trx?: Knex.Transaction,
): Promise<void> {
  if (!batchSQL || batchSQL.trim() === '' || batchSQL.trim() === ';') {
    logger.log('No SQL to execute (empty batch)');
    return;
  }
  const detectedDbType = dbType || (knex.client.config.client as string);
  const isPostgres =
    detectedDbType.includes('pg') || detectedDbType.includes('postgres');
  if (isPostgres) {
    logger.log(`Executing batch SQL with TRANSACTION (PostgreSQL)...`);
    try {
      if (trx) {
        await trx.raw(batchSQL);
      } else {
        await knex.transaction(async (pgTrx) => {
          await pgTrx.raw(batchSQL);
        });
      }
      logger.log(`Batch SQL executed successfully (transaction committed)`);
    } catch (error) {
      logger.error(`Batch SQL execution failed (transaction rolled back)`);
      logger.error(`Error: ${error.message}`);
      logger.error(
        `Failed SQL:\n${batchSQL.substring(0, 500)}${batchSQL.length > 500 ? '...' : ''}`,
      );
      throw error;
    }
  } else {
    logger.log(`Executing SQL statements individually (${detectedDbType})...`);
    logger.warn(
      `${detectedDbType.toUpperCase()} does not support transactional DDL - changes cannot be automatically rolled back`,
    );
    const statements = batchSQL
      .split(';')
      .map((s) => s.trim())
      .filter((s) => s.length > 0);
    logger.log(`Executing ${statements.length} statement(s) individually...`);
    const executedStatements: string[] = [];
    const rollbackStatements: string[] = [];
    const ddlTimeoutSec = 30;
    try {
      for (let i = 0; i < statements.length; i++) {
        const statement = statements[i];
        logger.log(
          `  [${i + 1}/${statements.length}] Executing: ${statement.substring(0, 80)}${statement.length > 80 ? '...' : ''}`,
        );
        const isMysqlLike =
          detectedDbType.includes('mysql') ||
          detectedDbType.includes('mariadb');
        try {
          await knex.transaction(async (conn) => {
            if (isMysqlLike) {
              await conn.raw(
                `SET SESSION lock_wait_timeout = ${ddlTimeoutSec}`,
              );
              await conn.raw(
                `SET SESSION innodb_lock_wait_timeout = ${ddlTimeoutSec}`,
              );
            }
            await conn.raw(statement);
          });
          executedStatements.push(statement);
          const rollbackSQL = generateRollbackSQL(statement, detectedDbType);
          if (rollbackSQL) {
            rollbackStatements.push(rollbackSQL);
          }
        } catch (statementError: any) {
          if (isIdempotentDDLError(statementError, detectedDbType)) {
            logger.warn(
              `  [${i + 1}/${statements.length}] Skipping idempotent error: ${statementError.message}`,
            );
            continue;
          }
          logger.error(
            `Failed at statement [${i + 1}/${statements.length}]: ${statement.substring(0, 100)}`,
          );
          logger.error(`Error: ${statementError.message}`);
          if (executedStatements.length > 0) {
            logger.error(`\n${'='.repeat(80)}`);
            logger.error(`⚠️  PARTIAL MIGRATION DETECTED`);
            logger.error(`${'='.repeat(80)}`);
            logger.error(
              `Successfully executed ${executedStatements.length} statement(s) before failure:`,
            );
            executedStatements.forEach((stmt, idx) => {
              logger.error(
                `  [${idx + 1}] ${stmt.substring(0, 100)}${stmt.length > 100 ? '...' : ''}`,
              );
            });
            logger.error(
              `\nTo rollback, execute these statements in reverse order:`,
            );
            rollbackStatements.reverse().forEach((stmt, idx) => {
              logger.error(
                `  [${idx + 1}] ${stmt.substring(0, 100)}${stmt.length > 100 ? '...' : ''}`,
              );
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
        logger.error(
          `\n⚠️  ${executedStatements.length} statement(s) were executed before failure.`,
        );
        logger.error(
          `Manual rollback may be required. See rollback statements above.`,
        );
      }
      logger.error(`${'='.repeat(80)}\n`);
      throw error;
    }
  }
}
export async function executeSQLStatements(
  knex: Knex,
  sqlStatements: string[],
): Promise<void> {
  const batchSQL = generateBatchSQL(sqlStatements);
  await executeBatchSQL(knex, batchSQL);
}
