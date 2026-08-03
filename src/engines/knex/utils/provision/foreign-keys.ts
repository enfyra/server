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

  const isPostgres = String(knex.client.config.client).toLowerCase() === 'pg';
  const existingPostgresConstraints = new Set<string>();
  if (isPostgres) {
    const result = await knex.raw<{ rows: Array<{ name: string }> }>(
      `SELECT conname AS name
       FROM pg_constraint
       WHERE connamespace = current_schema()::regnamespace
         AND contype = 'f'`,
    );
    for (const row of result.rows) {
      existingPostgresConstraints.add(row.name);
      // Postgres folds unquoted constraint names to lowercase, so the same
      // FK can exist under a case-variant name (e.g. knex auto-named
      // `enfyra_guard_tableid_foreign` vs explicit `enfyra_guard_tableId_foreign`).
      // Dedupe case-insensitively to avoid re-creating a duplicate constraint.
      existingPostgresConstraints.add(row.name.toLowerCase());
    }
  }

  for (const fkOp of fkOperations) {
    if (
      existingPostgresConstraints.has(fkOp.constraintName) ||
      existingPostgresConstraints.has(fkOp.constraintName.toLowerCase())
    ) {
      console.log(
        `  ⏩ FK already exists: ${fkOp.tableName}.${fkOp.columnName}`,
      );
      continue;
    }
    if (!(await knex.schema.hasColumn(fkOp.tableName, fkOp.columnName))) {
      console.log(
        `  ⏩ FK column missing, deferring: ${fkOp.tableName}.${fkOp.columnName} (column will be created by syncTable relation migration)`,
      );
      continue;
    }
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
      existingPostgresConstraints.add(fkOp.constraintName);
      existingPostgresConstraints.add(fkOp.constraintName.toLowerCase());
    } catch (error) {
      if (isPostgres) throw error;
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
