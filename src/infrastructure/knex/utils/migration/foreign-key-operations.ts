import { Knex } from 'knex';
import { Logger } from '@nestjs/common';

const logger = new Logger('ForeignKeyOperations');

export async function dropForeignKeyIfExists(
  knex: Knex,
  tableName: string,
  columnName: string,
): Promise<void> {
  try {
    logger.log(`🔍 Querying FK constraints for table: ${tableName}, column: ${columnName}`);
    const fkConstraints = await knex.raw(`
      SELECT CONSTRAINT_NAME
      FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
      WHERE TABLE_SCHEMA = DATABASE()
      AND TABLE_NAME = ?
      AND COLUMN_NAME = ?
      AND REFERENCED_TABLE_NAME IS NOT NULL
    `, [tableName, columnName]);

    if (fkConstraints[0] && fkConstraints[0].length > 0) {
      const actualFkName = fkConstraints[0][0].CONSTRAINT_NAME;
      logger.log(`🔍 Found FK constraint: ${actualFkName}`);
      await knex.raw(`ALTER TABLE \`${tableName}\` DROP FOREIGN KEY \`${actualFkName}\``);
      logger.log(`✅ Successfully dropped FK constraint: ${actualFkName}`);
    } else {
      logger.log(`⚠️  No FK constraint found for column ${columnName}`);
    }
  } catch (error) {
    logger.log(`⚠️  Error checking/dropping FK constraint for ${columnName}: ${error.message}`);
  }
}

export async function dropAllForeignKeysReferencingTable(
  knex: Knex,
  targetTableName: string,
): Promise<void> {
  logger.log(`🔍 Checking for FK constraints referencing table: ${targetTableName}`);

  const fkConstraints = await knex.raw(`
    SELECT
      TABLE_NAME,
      CONSTRAINT_NAME,
      COLUMN_NAME
    FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
    WHERE TABLE_SCHEMA = DATABASE()
    AND REFERENCED_TABLE_NAME = ?
  `, [targetTableName]);

  if (fkConstraints[0] && fkConstraints[0].length > 0) {
    logger.log(`⚠️  Found ${fkConstraints[0].length} FK constraint(s) referencing ${targetTableName}`);

    for (const fk of fkConstraints[0]) {
      const { TABLE_NAME, CONSTRAINT_NAME, COLUMN_NAME } = fk;
      logger.log(`  🗑️  Dropping FK: ${CONSTRAINT_NAME} from ${TABLE_NAME}.${COLUMN_NAME}`);

      await knex.raw(`ALTER TABLE \`${TABLE_NAME}\` DROP FOREIGN KEY \`${CONSTRAINT_NAME}\``);
      logger.log(`  ✅ Dropped FK constraint: ${CONSTRAINT_NAME}`);
    }
  } else {
    logger.log(`✅ No FK constraints reference ${targetTableName}`);
  }
}

export function generateForeignKeySQL(
  tableName: string,
  columnName: string,
  targetTable: string,
  targetColumn: string = 'id',
  isNullable: boolean = true,
): string {
  const onDelete = isNullable ? 'SET NULL' : 'RESTRICT';
  const fkName = `fk_${tableName}_${columnName}`;
  return `ALTER TABLE \`${tableName}\` ADD CONSTRAINT \`${fkName}\` FOREIGN KEY (\`${columnName}\`) REFERENCES \`${targetTable}\` (\`${targetColumn}\`) ON DELETE ${onDelete} ON UPDATE CASCADE`;
}
