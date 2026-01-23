import 'reflect-metadata';
import * as fs from 'fs';
import * as path from 'path';
import * as dotenv from 'dotenv';
import { Knex, knex } from 'knex';
import {
  KnexTableSchema,
} from '../src/shared/types/database-init.types';
import {
  parseSnapshotToSchema,
} from './utils/sql/schema-parser';
import { ensureDatabaseExists } from './utils/sql/database-setup';
import {
  createTable,
  createAllTables,
} from './utils/sql/table-builder';
import { addForeignKeys } from './utils/sql/foreign-keys';
import {
  createJunctionTables,
  syncJunctionTables,
} from './utils/sql/junction-tables';
import { syncTable } from './utils/sql/migrations';
import { parseDatabaseUri } from '../src/infrastructure/knex/utils/uri-parser';

dotenv.config();







export async function initializeDatabaseSql(): Promise<void> {
  const DB_TYPE = process.env.DB_TYPE || 'mysql';
  
  let connectionConfig: {
    host: string;
    port: number;
    user: string;
    password: string;
    database: string;
  };

  const DB_URI = process.env.DB_URI;
  console.log(`🔍 DB_URI: ${DB_URI ? DB_URI.replace(/:[^:@]+@/, ':****@') : 'NOT SET'}`);
  
  if (DB_URI) {
    try {
      const parsed = parseDatabaseUri(DB_URI);
      connectionConfig = {
        host: parsed.host,
        port: parsed.port,
        user: parsed.user,
        password: parsed.password,
        database: parsed.database,
      };
      console.log(`✅ Parsed from URI: host=${connectionConfig.host}, port=${connectionConfig.port}, user=${connectionConfig.user}, password=${connectionConfig.password ? '****' : 'EMPTY'}, database=${connectionConfig.database}`);
    } catch (error) {
      console.error('❌ Failed to parse DB_URI:', error);
      throw error;
    }
  } else {
    connectionConfig = {
      host: process.env.DB_HOST || 'localhost',
      port: Number(process.env.DB_PORT) || (DB_TYPE === 'postgres' ? 5432 : 3306),
      user: process.env.DB_USERNAME || 'root',
      password: process.env.DB_PASSWORD || '',
      database: process.env.DB_NAME || 'enfyra',
    };
    console.log(`⚠️ Using legacy env vars: host=${connectionConfig.host}, port=${connectionConfig.port}, user=${connectionConfig.user}, password=${connectionConfig.password ? '****' : 'EMPTY'}, database=${connectionConfig.database}`);
  }

  await ensureDatabaseExists();

  const knexInstance = knex({
    client: DB_TYPE === 'postgres' ? 'pg' : 'mysql2',
    connection: {
      host: connectionConfig.host,
      port: connectionConfig.port,
      user: connectionConfig.user,
      password: connectionConfig.password,
      database: connectionConfig.database,
    },
  });

  try {
    const hasSettingTable = await knexInstance.schema.hasTable(
      'setting_definition',
    );

    if (hasSettingTable) {
      const result = await knexInstance('setting_definition')
        .select('isInit')
        .first();

      if (result?.isInit === true || result?.isInit === 1) {
        console.log('⚠️ Database already initialized, skipping init.');
        return;
      }
    }

    const snapshotPath = path.resolve(process.cwd(), 'data/snapshot.json');
    const snapshot = JSON.parse(fs.readFileSync(snapshotPath, 'utf8'));

    console.log('📖 Loaded snapshot.json');

    const schemas = parseSnapshotToSchema(snapshot);

    console.log(`📊 Found ${schemas.length} tables to create`);

    console.log('🚀 Creating/syncing all tables...');

    await createAllTables(knexInstance, schemas, DB_TYPE);

    console.log('\n🔄 Syncing tables with snapshot...');
    for (const schema of schemas) {
      const exists = await knexInstance.schema.hasTable(schema.tableName);
      if (exists) {
        await syncTable(knexInstance, schema, schemas);
      }
    }

    await syncJunctionTables(knexInstance, schemas);

    console.log('\n🎉 Database initialization/sync completed!');
  } catch (error) {
    console.error('❌ Error during database initialization:', error);
    throw error;
  } finally {
    await knexInstance.destroy();
  }
}

if (require.main === module) {
  initializeDatabaseSql()
    .then(() => {
      console.log('✅ Done!');
      process.exit(0);
    })
    .catch((error) => {
      console.error('❌ Failed:', error);
      process.exit(1);
    });
}


