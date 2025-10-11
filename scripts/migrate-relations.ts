import 'reflect-metadata';
import * as fs from 'fs';
import * as path from 'path';
import * as dotenv from 'dotenv';
import { Knex, knex } from 'knex';

dotenv.config();

interface RelationDef {
  propertyName: string;
  type: 'one-to-one' | 'many-to-one' | 'one-to-many' | 'many-to-many';
  targetTable: string;
  inversePropertyName?: string;
  isNullable?: boolean;
  isSystem?: boolean;
  isInverseEager?: boolean;
}

interface TableDef {
  name: string;
  isSystem?: boolean;
  relations?: RelationDef[];
}

async function migrateRelations(): Promise<void> {
  const DB_TYPE = process.env.DB_TYPE || 'mysql';
  const DB_HOST = process.env.DB_HOST || 'localhost';
  const DB_PORT = Number(process.env.DB_PORT) || (DB_TYPE === 'postgres' ? 5432 : 3306);
  const DB_USERNAME = process.env.DB_USERNAME || 'root';
  const DB_PASSWORD = process.env.DB_PASSWORD || '';
  const DB_NAME = process.env.DB_NAME || 'enfyra';

  const knexInstance = knex({
    client: DB_TYPE === 'postgres' ? 'pg' : 'mysql2',
    connection: {
      host: DB_HOST,
      port: DB_PORT,
      user: DB_USERNAME,
      password: DB_PASSWORD,
      database: DB_NAME,
    },
  });

  try {
    console.log('🚀 Starting relations migration...');

    // Load snapshot
    const snapshotPath = path.resolve(process.cwd(), 'data/snapshot.json');
    const snapshot = JSON.parse(fs.readFileSync(snapshotPath, 'utf8'));

    await knexInstance.transaction(async (trx) => {
      // Get table name to ID mapping
      const tables = await trx('table_definition').select('id', 'name');
      const tableNameToId: Record<string, number> = {};
      for (const table of tables) {
        tableNameToId[table.name] = table.id;
      }

      console.log(`📊 Found ${tables.length} tables in metadata`);

      // Collect all relations to process
      const allRelationsToProcess: Array<{
        tableName: string;
        tableId: number;
        relation: RelationDef;
        isInverse: boolean;
      }> = [];

      // First pass: collect direct relations from snapshot
      for (const [name, defRaw] of Object.entries(snapshot)) {
        const def = defRaw as TableDef;
        const tableId = tableNameToId[name];
        if (!tableId) {
          console.log(`⚠️ Table ${name} not found in metadata, skipping`);
          continue;
        }

        for (const rel of def.relations || []) {
          if (!rel.propertyName || !rel.targetTable || !rel.type) continue;
          const targetId = tableNameToId[rel.targetTable];
          if (!targetId) {
            console.log(`⚠️ Target table ${rel.targetTable} not found for relation ${rel.propertyName} in ${name}`);
            continue;
          }

          allRelationsToProcess.push({
            tableName: name,
            tableId,
            relation: rel,
            isInverse: false,
          });

          // Auto-generate inverse relation if inversePropertyName exists
          if (rel.inversePropertyName) {
            let inverseType = rel.type;
            if (rel.type === 'many-to-one') {
              inverseType = 'one-to-many';
            } else if (rel.type === 'one-to-many') {
              inverseType = 'many-to-one';
            }

            const inverseRelation: RelationDef = {
              propertyName: rel.inversePropertyName,
              type: inverseType as any,
              targetTable: name,
              inversePropertyName: rel.propertyName,
              isSystem: rel.isSystem,
              isNullable: rel.isNullable,
            };

            allRelationsToProcess.push({
              tableName: rel.targetTable,
              tableId: targetId,
              relation: inverseRelation,
              isInverse: true,
            });
          }
        }
      }

      console.log(`📝 Processing ${allRelationsToProcess.length} relations...`);

      // Process all relations
      for (const { tableName, tableId, relation: rel, isInverse } of allRelationsToProcess) {
        const targetId = tableNameToId[rel.targetTable];
        if (!targetId) continue;

        const existingRel = await trx('relation_definition')
          .where('sourceTableId', tableId)
          .where('propertyName', rel.propertyName)
          .first();

        if (existingRel) {
          console.log(`⏩ Relation ${rel.propertyName} already exists for ${tableName}${isInverse ? ' (inverse)' : ''}`);
        } else {
          const insertData: any = {
            propertyName: rel.propertyName,
            type: rel.type,
            inversePropertyName: rel.inversePropertyName,
            isNullable: rel.isNullable !== false,
            isSystem: rel.isSystem || false,
            description: null,
            sourceTableId: tableId,
            targetTableId: targetId,
          };

          await trx('relation_definition').insert(insertData);
          console.log(`✅ Added relation ${rel.propertyName} for ${tableName}${isInverse ? ' (inverse)' : ''}`);
        }
      }

      console.log('🎉 Relations migration completed!');
    });

  } catch (error) {
    console.error('❌ Error during relations migration:', error);
    throw error;
  } finally {
    await knexInstance.destroy();
  }
}

// For direct execution
if (require.main === module) {
  migrateRelations()
    .then(() => {
      console.log('✅ Done!');
      process.exit(0);
    })
    .catch((error) => {
      console.error('❌ Failed:', error);
      process.exit(1);
    });
}

export { migrateRelations };

