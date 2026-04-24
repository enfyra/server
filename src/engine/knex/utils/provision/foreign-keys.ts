import { Knex } from 'knex';
import { getForeignKeyColumnName } from '../sql-schema-naming.util';
import { KnexTableSchema } from '../../../../shared/types/database-init.types';
import { getErrorMessage } from '../../../../shared/utils/error.util';

export async function addForeignKeys(
  knex: Knex,
  schemas: KnexTableSchema[],
  dbType: string,
): Promise<void> {
  console.log('🔗 Adding foreign key constraints...');

  const fkOperations: Array<{
    tableName: string;
    foreignKeyColumn: string;
    targetTable: string;
    onDelete: string;
  }> = [];

  for (const schema of schemas) {
    const { tableName, definition } = schema;

    if (!definition.relations || definition.relations.length === 0) {
      continue;
    }

    for (const relation of definition.relations) {
      if (relation.type === 'many-to-many' || relation.type === 'one-to-many') {
        continue;
      }

      if (
        relation.type === 'one-to-one' &&
        (relation as any)._isInverseGenerated
      ) {
        continue;
      }

      const foreignKeyColumn = getForeignKeyColumnName(relation.propertyName);
      const targetTable = relation.targetTable;
      const onDelete = (relation as any).onDelete || 'SET NULL';

      fkOperations.push({
        tableName,
        foreignKeyColumn,
        targetTable,
        onDelete,
      });
    }
  }

  if (fkOperations.length === 0) {
    console.log('✅ No foreign keys to add');
    return;
  }

  for (const fkOp of fkOperations) {
    console.log(
      `  Adding FK: ${fkOp.tableName}.${fkOp.foreignKeyColumn} → ${fkOp.targetTable}.id (onDelete: ${fkOp.onDelete})`,
    );
    try {
      await knex.schema.alterTable(fkOp.tableName, (table) => {
        const fk = table
          .foreign(fkOp.foreignKeyColumn)
          .references('id')
          .inTable(fkOp.targetTable);

        fk.onDelete(fkOp.onDelete).onUpdate('CASCADE');

        table.index([fkOp.foreignKeyColumn]);
      });
    } catch (error) {
      const msg = getErrorMessage(error).toLowerCase();
      if (msg.includes('already exists') || msg.includes('duplicate')) {
        console.log(
          `  ⏩ FK already exists: ${fkOp.tableName}.${fkOp.foreignKeyColumn}`,
        );
      } else {
        console.error(
          `  ❌ Failed to add FK ${fkOp.tableName}.${fkOp.foreignKeyColumn}: ${getErrorMessage(error)}`,
        );
      }
    }
  }

  console.log('✅ Foreign keys added');
}
