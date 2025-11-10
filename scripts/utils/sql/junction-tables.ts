import { Knex } from 'knex';
import {
  getShortFkName,
  getShortIndexName,
  getShortPkName,
  getShortFkConstraintName,
} from '../../../src/infrastructure/knex/utils/naming-helpers';
import { JunctionTableDef, KnexTableSchema } from '../../../src/shared/types/database-init.types';
import { getPrimaryKeyType } from './schema-parser';

export async function createJunctionTables(
  knex: Knex,
  schemas: KnexTableSchema[],
  dbType: string,
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

    const sourcePkType = getPrimaryKeyType(schemas, junction.sourceTable);
    const targetPkType = getPrimaryKeyType(schemas, junction.targetTable);

    await knex.schema.createTable(junction.tableName, (table) => {
      let sourceCol;
      if (sourcePkType === 'uuid') {
        sourceCol = table.uuid(junction.sourceColumn).notNullable();
      } else {
        sourceCol = table.integer(junction.sourceColumn).unsigned().notNullable();
      }

      const sourceFk = sourceCol
        .references('id')
        .inTable(junction.sourceTable)
        .onDelete('CASCADE')
        .onUpdate('CASCADE');

      const sourceFkName = getShortFkConstraintName(junction.tableName, junction.sourceColumn, 'src');
      sourceFk.withKeyName(sourceFkName);

      let targetCol;
      if (targetPkType === 'uuid') {
        targetCol = table.uuid(junction.targetColumn).notNullable();
      } else {
        targetCol = table.integer(junction.targetColumn).unsigned().notNullable();
      }

      const targetFk = targetCol
        .references('id')
        .inTable(junction.targetTable)
        .onDelete('CASCADE')
        .onUpdate('CASCADE');

      const targetFkName = getShortFkConstraintName(junction.tableName, junction.targetColumn, 'tgt');
      targetFk.withKeyName(targetFkName);

      const pkName = getShortPkName(junction.tableName);
      table.primary([junction.sourceColumn, junction.targetColumn], pkName);
    });

    console.log(`✅ Created junction table: ${junction.tableName}`);
  }

  console.log('✅ Junction tables created');
}

export async function syncJunctionTables(
  knex: Knex,
  schemas: KnexTableSchema[],
  dbType: string,
): Promise<void> {
  console.log('🔗 Syncing junction tables...');

  const createdJunctions = new Set<string>();

  for (const schema of schemas) {
    for (const junction of schema.junctionTables) {
      if (createdJunctions.has(junction.tableName)) {
        continue;
      }

      const exists = await knex.schema.hasTable(junction.tableName);

      if (!exists) {
        console.log(`  📝 Creating junction table: ${junction.tableName}`);

        const sourcePkType = getPrimaryKeyType(schemas, junction.sourceTable);
        const targetPkType = getPrimaryKeyType(schemas, junction.targetTable);

        await knex.schema.createTable(junction.tableName, (table) => {
          const sourceFkName = getShortFkName(junction.sourceTable, junction.sourcePropertyName, 'src');
          let sourceCol;
          if (sourcePkType === 'uuid') {
            sourceCol = table.uuid(junction.sourceColumn).notNullable();
          } else {
            sourceCol = table.integer(junction.sourceColumn).unsigned().notNullable();
          }

          sourceCol
            .references('id')
            .inTable(junction.sourceTable)
            .onDelete('CASCADE')
            .onUpdate('CASCADE')
            .withKeyName(sourceFkName);

          const targetFkName = getShortFkName(junction.sourceTable, junction.sourcePropertyName, 'tgt');
          let targetCol;
          if (targetPkType === 'uuid') {
            targetCol = table.uuid(junction.targetColumn).notNullable();
          } else {
            targetCol = table.integer(junction.targetColumn).unsigned().notNullable();
          }

          targetCol
            .references('id')
            .inTable(junction.targetTable)
            .onDelete('CASCADE')
            .onUpdate('CASCADE')
            .withKeyName(targetFkName);

          const pkName = getShortPkName(junction.tableName);
          table.primary([junction.sourceColumn, junction.targetColumn], pkName);

          if (dbType !== 'postgres') {
            const sourceIndexName = getShortIndexName(junction.sourceTable, junction.sourcePropertyName, 'src');
            const targetIndexName = getShortIndexName(junction.sourceTable, junction.sourcePropertyName, 'tgt');
            table.index([junction.sourceColumn], sourceIndexName);
            table.index([junction.targetColumn], targetIndexName);
          }
        });

        console.log(`  ✅ Created junction table: ${junction.tableName}`);
      } else {
        console.log(`  ⏩ Junction table already exists: ${junction.tableName}`);
      }

      createdJunctions.add(junction.tableName);
    }
  }
}

