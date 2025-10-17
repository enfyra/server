import { Knex } from 'knex';
import { Logger } from '@nestjs/common';
import { generateColumnDefinition } from './sql-generator';
import { dropForeignKeyIfExists } from './foreign-key-operations';

const logger = new Logger('SqlDiffGenerator');

export async function generateSQLFromDiff(
  knex: Knex,
  tableName: string,
  diff: any,
): Promise<string[]> {
  const sqlStatements: string[] = [];

  if (diff.table.update) {
    sqlStatements.push(`ALTER TABLE \`${diff.table.update.oldName}\` RENAME TO \`${diff.table.update.newName}\``);
  }

  const renamedColumns = new Set<string>();
  for (const rename of diff.columns.rename) {
    sqlStatements.push(`ALTER TABLE \`${tableName}\` RENAME COLUMN \`${rename.oldName}\` TO \`${rename.newName}\``);
    renamedColumns.add(rename.oldName);
    renamedColumns.add(rename.newName);
  }

  for (const col of diff.columns.create) {
    const columnDef = generateColumnDefinition(col);
    sqlStatements.push(`ALTER TABLE \`${tableName}\` ADD COLUMN \`${col.name}\` ${columnDef}`);

    if (col.isForeignKey && col.foreignKeyTarget) {
      const onDelete = col.isNullable !== false ? 'SET NULL' : 'RESTRICT';
      sqlStatements.push(
        `ALTER TABLE \`${tableName}\` ADD CONSTRAINT \`fk_${tableName}_${col.name}\` FOREIGN KEY (\`${col.name}\`) REFERENCES \`${col.foreignKeyTarget}\` (\`${col.foreignKeyColumn || 'id'}\`) ON DELETE ${onDelete} ON UPDATE CASCADE`
      );
    }
  }

  for (const col of diff.columns.delete) {
    const columnExists = await knex.schema.hasColumn(tableName, col.name);
    if (!columnExists) {
      logger.log(`  ⏭️  Skipping DROP for ${col.name} - column does not exist`);
      continue;
    }

    if (col.isForeignKey) {
      await dropForeignKeyIfExists(knex, tableName, col.name);
    }
    sqlStatements.push(`ALTER TABLE \`${tableName}\` DROP COLUMN \`${col.name}\``);
  }

  const processedUpdates = new Set<string>();
  for (const update of diff.columns.update) {
    const colName = update.newColumn.name;

    if (renamedColumns.has(colName)) {
      logger.log(`  ⏭️  Skipping MODIFY for ${colName} - column was renamed`);
      continue;
    }

    if (processedUpdates.has(colName)) {
      logger.log(`  ⏭️  Skipping duplicate MODIFY for ${colName}`);
      continue;
    }

    const columnExists = await knex.schema.hasColumn(tableName, colName);
    if (!columnExists) {
      logger.log(`  ⏭️  Skipping MODIFY for ${colName} - column does not exist`);
      continue;
    }

    processedUpdates.add(colName);
    const columnDef = generateColumnDefinition(update.newColumn);
    sqlStatements.push(`ALTER TABLE \`${tableName}\` MODIFY COLUMN \`${update.newColumn.name}\` ${columnDef}`);
  }

  for (const uniqueGroup of diff.constraints.uniques.update || []) {
    const columns = uniqueGroup.map((col: string) => `\`${col}\``).join(', ');
    sqlStatements.push(`ALTER TABLE \`${tableName}\` ADD UNIQUE (${columns})`);
  }

  for (const indexGroup of diff.constraints.indexes.update || []) {
    const columns = indexGroup.map((col: string) => `\`${col}\``).join(', ');
    sqlStatements.push(`ALTER TABLE \`${tableName}\` ADD INDEX \`idx_${tableName}_${indexGroup.join('_')}\` (${columns})`);
  }

  for (const crossOp of diff.crossTableOperations || []) {
    if (crossOp.operation === 'createColumn') {
      const columnDef = generateColumnDefinition(crossOp.column);
      sqlStatements.push(`ALTER TABLE \`${crossOp.targetTable}\` ADD COLUMN \`${crossOp.column.name}\` ${columnDef}`);

      if (crossOp.column.isForeignKey) {
        const onDelete = crossOp.column.isNullable !== false ? 'SET NULL' : 'RESTRICT';
        sqlStatements.push(
          `ALTER TABLE \`${crossOp.targetTable}\` ADD CONSTRAINT \`fk_${crossOp.targetTable}_${crossOp.column.name}\` FOREIGN KEY (\`${crossOp.column.name}\`) REFERENCES \`${crossOp.column.foreignKeyTarget}\` (\`${crossOp.column.foreignKeyColumn}\`) ON DELETE ${onDelete} ON UPDATE CASCADE`
        );
      }
    } else if (crossOp.operation === 'dropColumn') {
      await dropForeignKeyIfExists(knex, crossOp.targetTable, crossOp.columnName);
      sqlStatements.push(`ALTER TABLE \`${crossOp.targetTable}\` DROP COLUMN \`${crossOp.columnName}\``);
    }
  }

  for (const junctionCreate of diff.junctionTables?.create || []) {
    const { tableName: junctionName, sourceTable, targetTable, sourceColumn, targetColumn } = junctionCreate;

    const tableExists = await knex.schema.hasTable(junctionName);
    if (tableExists) {
      logger.log(`  ⏭️  Junction table ${junctionName} already exists, skipping`);
      continue;
    }

    const createJunctionSQL = `
      CREATE TABLE \`${junctionName}\` (
        \`id\` INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
        \`${sourceColumn}\` INT UNSIGNED NOT NULL,
        \`${targetColumn}\` INT UNSIGNED NOT NULL,
        \`createdAt\` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        \`updatedAt\` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        FOREIGN KEY (\`${sourceColumn}\`) REFERENCES \`${sourceTable}\` (\`id\`) ON DELETE CASCADE ON UPDATE CASCADE,
        FOREIGN KEY (\`${targetColumn}\`) REFERENCES \`${targetTable}\` (\`id\`) ON DELETE CASCADE ON UPDATE CASCADE,
        UNIQUE KEY \`unique_${sourceColumn}_${targetColumn}\` (\`${sourceColumn}\`, \`${targetColumn}\`)
      )
    `.trim().replace(/\s+/g, ' ');

    sqlStatements.push(createJunctionSQL);
  }

  for (const junctionDrop of diff.junctionTables?.drop || []) {
    const { tableName: junctionName } = junctionDrop;
    sqlStatements.push(`DROP TABLE IF EXISTS \`${junctionName}\``);
  }

  return sqlStatements;
}

export async function executeSQLStatements(
  knex: Knex,
  sqlStatements: string[],
): Promise<void> {
  for (const sql of sqlStatements) {
    logger.log(`📝 Executing SQL: ${sql.substring(0, 100)}${sql.length > 100 ? '...' : ''}`);
    try {
      await knex.raw(sql);
    } catch (error) {
      logger.error(`❌ Failed to execute SQL: ${sql}`);
      logger.error(`Error: ${error.message}`);
      throw error;
    }
  }
}
