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
} from '../src/infrastructure/knex/utils/provision/schema-parser';
import { ensureDatabaseExists } from '../src/infrastructure/knex/utils/provision/database-setup';
import {
  createTable,
  createAllTables,
} from '../src/infrastructure/knex/utils/provision/table-builder';
import { addForeignKeys } from '../src/infrastructure/knex/utils/provision/foreign-keys';
import {
  createJunctionTables,
  syncJunctionTables,
} from '../src/infrastructure/knex/utils/provision/junction-tables';
import { syncTable } from '../src/infrastructure/knex/utils/provision/sync-table';
import { parseDatabaseUri } from '../src/infrastructure/knex/utils/uri-parser';
import {
  loadSchemaMigration,
  hasSchemaMigrations,
  applySqlSchemaMigrations,
} from '../src/shared/utils/provision-schema-migration';
import { resolveDbTypeFromEnv } from '../src/shared/utils/resolve-db-type';

dotenv.config();







export async function initializeDatabaseSql(): Promise<void> {
  const dbType = resolveDbTypeFromEnv();

  const DB_URI = process.env.DB_URI;
  if (!DB_URI) {
    throw new Error('DB_URI environment variable is required but not set.');
  }

  const parsed = parseDatabaseUri(DB_URI);
  const connectionConfig = {
    host: parsed.host,
    port: parsed.port,
    user: parsed.user,
    password: parsed.password,
    database: parsed.database,
  };

  await ensureDatabaseExists();

  const knexInstance = knex({
    client: dbType === 'postgres' ? 'pg' : 'mysql2',
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

    const schemaMigration = loadSchemaMigration();

    // Apply schema migrations (dangerous operations: remove, modify)
    if (schemaMigration && hasSchemaMigrations(schemaMigration)) {
      console.log('📋 Applying schema migrations from snapshot-migration.json...');
      await applySqlSchemaMigrations(knexInstance, schemaMigration);
    }

    const snapshotPath = path.resolve(process.cwd(), 'data/snapshot.json');
    const snapshot = JSON.parse(fs.readFileSync(snapshotPath, 'utf8'));

    const schemas = parseSnapshotToSchema(snapshot);

    await createAllTables(knexInstance, schemas, dbType);

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


