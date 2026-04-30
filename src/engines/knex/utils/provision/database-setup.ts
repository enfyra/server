import { knex } from 'knex';
import { parseDatabaseUri } from '../uri-parser';
import { resolveDbTypeFromEnv } from '../../../../shared/utils/resolve-db-type';

export async function ensureDatabaseExists(): Promise<void> {
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

  const client = dbType === 'postgres' ? 'pg' : 'mysql2';
  const serverConnection =
    dbType === 'postgres'
      ? {
          host: connectionConfig.host,
          port: connectionConfig.port,
          user: connectionConfig.user,
          password: connectionConfig.password,
          database: 'postgres',
        }
      : {
          host: connectionConfig.host,
          port: connectionConfig.port,
          user: connectionConfig.user,
          password: connectionConfig.password,
        };

  const tempKnex = knex({
    client,
    connection: serverConnection,
  });

  try {
    if (dbType === 'mysql') {
      await tempKnex.raw(
        `CREATE DATABASE IF NOT EXISTS ?? CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci`,
        [connectionConfig.database],
      );
    } else if (dbType === 'postgres') {
      const result = await tempKnex.raw(
        `SELECT 1 FROM pg_database WHERE datname = ?`,
        [connectionConfig.database],
      );
      if (result.rows.length === 0) {
        await tempKnex.raw(
          `CREATE DATABASE "${connectionConfig.database}" WITH ENCODING 'UTF8'`,
        );
      }
    }
  } finally {
    await tempKnex.destroy();
  }
}
