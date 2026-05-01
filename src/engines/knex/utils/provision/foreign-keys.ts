import { Knex } from 'knex';
import { KnexTableSchema } from '../../../../shared/types/database-init.types';
import { getErrorMessage } from '../../../../shared/utils/error.util';
import { buildSqlForeignKeyContracts } from '../sql-physical-schema-contract';
import type { SqlForeignKeyContract } from '../../types/sql-physical-schema-contract.types';

export async function addForeignKeys(
  knex: Knex,
  schemas: KnexTableSchema[],
  _dbType: string,
): Promise<void> {
  console.log('🔗 Adding foreign key constraints...');

  const fkOperations: SqlForeignKeyContract[] = [];

  for (const schema of schemas) {
    const { tableName, definition } = schema;

    if (!definition.relations || definition.relations.length === 0) {
      continue;
    }

    fkOperations.push(
      ...buildSqlForeignKeyContracts(tableName, definition.relations as any[]),
    );
  }

  if (fkOperations.length === 0) {
    console.log('✅ No foreign keys to add');
    return;
  }

  for (const fkOp of fkOperations) {
    console.log(
      `  Adding FK: ${fkOp.tableName}.${fkOp.columnName} → ${fkOp.targetTable}.id (onDelete: ${fkOp.onDelete})`,
    );
    try {
      await knex.schema.alterTable(fkOp.tableName, (table) => {
        const fk = table
          .foreign(fkOp.columnName, fkOp.constraintName)
          .references(fkOp.targetColumn)
          .inTable(fkOp.targetTable);

        fk.onDelete(fkOp.onDelete).onUpdate('CASCADE');

        table.index([fkOp.columnName]);
      });
    } catch (error) {
      const msg = getErrorMessage(error).toLowerCase();
      if (msg.includes('already exists') || msg.includes('duplicate')) {
        console.log(
          `  ⏩ FK already exists: ${fkOp.tableName}.${fkOp.columnName}`,
        );
      } else {
        console.error(
          `  ❌ Failed to add FK ${fkOp.tableName}.${fkOp.columnName}: ${getErrorMessage(error)}`,
        );
      }
    }
  }

  console.log('✅ Foreign keys added');
}
