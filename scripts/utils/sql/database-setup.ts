import { knex } from 'knex';
import { parseDatabaseUri } from '../../../src/infrastructure/knex/utils/uri-parser';

export async function ensureDatabaseExists(): Promise<void> {
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
    const parsed = parseDatabaseUri(DB_URI);
    connectionConfig = {
      host: parsed.host,
      port: parsed.port,
      user: parsed.user,
      password: parsed.password,
      database: parsed.database,
    };
  } else {
    connectionConfig = {
      host: process.env.DB_HOST || 'localhost',
      port: Number(process.env.DB_PORT) || (DB_TYPE === 'postgres' ? 5432 : 3306),
      user: process.env.DB_USERNAME || 'root',
      password: process.env.DB_PASSWORD || '',
      database: process.env.DB_NAME || 'enfyra',
    };
  }

  const tempKnex = knex({
    client: DB_TYPE === 'postgres' ? 'pg' : 'mysql2',
    connection: {
      host: connectionConfig.host,
      port: connectionConfig.port,
      user: connectionConfig.user,
      password: connectionConfig.password,
      ...(DB_TYPE === 'postgres' && { database: 'postgres' }),
    },
  });

  try {
    if (DB_TYPE === 'mysql') {
      const result = await tempKnex.raw(
        `SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = ?`,
        [connectionConfig.database],
      );
      if (result[0].length === 0) {
        await tempKnex.raw(`CREATE DATABASE IF NOT EXISTS \`${connectionConfig.database}\``);
        console.log(`✅ MySQL: Created database ${connectionConfig.database}`);
      } else {
        console.log(`✅ MySQL: Database ${connectionConfig.database} already exists`);
      }
    } else if (DB_TYPE === 'postgres') {
      const result = await tempKnex.raw(
        `SELECT 1 FROM pg_database WHERE datname = ?`,
        [connectionConfig.database],
      );
      if (result.rows.length === 0) {
        await tempKnex.raw(`CREATE DATABASE "${connectionConfig.database}" WITH ENCODING 'UTF8'`);
        console.log(`✅ Postgres: Created database ${connectionConfig.database}`);
      } else {
        console.log(`✅ Postgres: Database ${connectionConfig.database} already exists`);
      }
    }
  } finally {
    await tempKnex.destroy();
  }
}






















