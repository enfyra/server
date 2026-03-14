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

interface SnapshotOld {
  tables?: string[];
  deletedTables?: string[];
}







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

    const snapshotOldPath = path.resolve(process.cwd(), 'data/snapshot-migration.json');
    let snapshotOld: SnapshotOld = { tables: [], deletedTables: [] };
    if (fs.existsSync(snapshotOldPath)) {
      try {
        snapshotOld = JSON.parse(fs.readFileSync(snapshotOldPath, 'utf8'));
      } catch (e) {
        console.warn('⚠️ Failed to parse snapshot-migration.json, using empty structure');
      }
    }

    if (snapshotOld.deletedTables && snapshotOld.deletedTables.length > 0) {
      console.log(`🗑️ Processing ${snapshotOld.deletedTables.length} table(s) to delete...`);
      for (const tableName of snapshotOld.deletedTables) {
        const exists = await knexInstance.schema.hasTable(tableName);
        if (exists) {
          console.log(`  Dropping table: ${tableName}`);
          await knexInstance.schema.dropTableIfExists(tableName);
          console.log(`  ✅ Dropped table: ${tableName}`);
        } else {
          console.log(`  ⏩ Table ${tableName} does not exist, skipping`);
        }
      }
    }

    const snapshotPath = path.resolve(process.cwd(), 'data/snapshot.json');
    const snapshot = JSON.parse(fs.readFileSync(snapshotPath, 'utf8'));

    const schemas = parseSnapshotToSchema(snapshot);

    await createAllTables(knexInstance, schemas, DB_TYPE);

    for (const schema of schemas) {
      const exists = await knexInstance.schema.hasTable(schema.tableName);
      if (exists) {
        await syncTable(knexInstance, schema, schemas);
      }
    }

    await syncJunctionTables(knexInstance, schemas);

    console.log('✅ Database initialized');
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


