import { Knex } from 'knex';
import {
  JunctionTableDef,
  KnexTableSchema,
} from '../../../../shared/types/database-init.types';
import { getPrimaryKeyType } from './schema-parser';
import { buildSqlJunctionTableContract } from '../sql-physical-schema-contract';

function addJunctionTableColumns(
  table: Knex.CreateTableBuilder,
  junction: JunctionTableDef,
  schemas: KnexTableSchema[],
): void {
  const contract = buildSqlJunctionTableContract(junction);
  const sourcePkType = getPrimaryKeyType(schemas, contract.sourceTable);
  const targetPkType = getPrimaryKeyType(schemas, contract.targetTable);

  const sourceCol =
    sourcePkType === 'uuid'
      ? table.uuid(contract.sourceColumn).notNullable()
      : table.integer(contract.sourceColumn).unsigned().notNullable();
  sourceCol
    .references('id')
    .inTable(contract.sourceTable)
    .onDelete(contract.onDelete)
    .onUpdate(contract.onUpdate)
    .withKeyName(contract.sourceForeignKeyName);

  const targetCol =
    targetPkType === 'uuid'
      ? table.uuid(contract.targetColumn).notNullable()
      : table.integer(contract.targetColumn).unsigned().notNullable();
  targetCol
    .references('id')
    .inTable(contract.targetTable)
    .onDelete(contract.onDelete)
    .onUpdate(contract.onUpdate)
    .withKeyName(contract.targetForeignKeyName);

  table.primary([contract.sourceColumn, contract.targetColumn], contract.primaryKeyName);
  table.index([contract.sourceColumn], contract.sourceIndexName);
  table.index([contract.targetColumn], contract.targetIndexName);
  table.index([contract.targetColumn, contract.sourceColumn], contract.reverseIndexName);
}

export async function createJunctionTables(
  knex: Knex,
  schemas: KnexTableSchema[],
): Promise<void> {
  console.log('🔗 Creating junction tables...');

  const createdJunctions = new Set<string>();
  const junctionsToCreate: JunctionTableDef[] = [];

  for (const schema of schemas) {
    for (const junction of schema.junctionTables) {
      if (createdJunctions.has(junction.tableName)) {
        continue;
      }

      const exists = await knex.schema.hasTable(junction.tableName);
      if (exists) {
        console.log(`⏩ Junction table already exists: ${junction.tableName}`);
        createdJunctions.add(junction.tableName);
        continue;
      }

      junctionsToCreate.push(junction);
      createdJunctions.add(junction.tableName);
    }
  }

  if (junctionsToCreate.length === 0) {
    console.log('✅ No junction tables to create');
    return;
  }

  for (const junction of junctionsToCreate) {
    console.log(`📝 Creating junction table: ${junction.tableName}`);

    try {
      await knex.schema.createTable(junction.tableName, (table) => {
        addJunctionTableColumns(table, junction, schemas);
      });

      console.log(`✅ Created junction table: ${junction.tableName}`);
    } catch (error) {
      const nowExists = await knex.schema.hasTable(junction.tableName);
      if (nowExists) {
        console.log(
          `⏩ Junction table created by another instance: ${junction.tableName}`,
        );
      } else {
        throw error;
      }
    }
  }

  console.log('✅ Junction tables created');
}

export async function syncJunctionTables(
  knex: Knex,
  schemas: KnexTableSchema[],
): Promise<void> {
  console.log('🔗 Syncing junction tables...');

  const createdJunctions = new Set<string>();
  const uniqueJunctions: (typeof schemas)[0]['junctionTables'] = [];
  for (const schema of schemas) {
    for (const junction of schema.junctionTables) {
      if (!createdJunctions.has(junction.tableName)) {
        createdJunctions.add(junction.tableName);
        uniqueJunctions.push(junction);
      }
    }
  }
  for (const junction of uniqueJunctions) {
    const exists = await knex.schema.hasTable(junction.tableName);

    if (!exists) {
      console.log(`  📝 Creating junction table: ${junction.tableName}`);

      try {
        await knex.schema.createTable(junction.tableName, (table) => {
          addJunctionTableColumns(table, junction, schemas);
        });

        console.log(`  ✅ Created junction table: ${junction.tableName}`);
      } catch (error) {
        const nowExists = await knex.schema.hasTable(junction.tableName);
        if (nowExists) {
          console.log(
            `  ⏩ Junction table created by another instance: ${junction.tableName}`,
          );
        } else {
          throw error;
        }
      }
    } else {
      console.log(`  ⏩ Junction table already exists: ${junction.tableName}`);
    }
  }
}
