import { knex } from 'knex';

export async function ensureDatabaseExists(): Promise<void> {
  const DB_TYPE = process.env.DB_TYPE || 'mysql';
  const DB_HOST = process.env.DB_HOST || 'localhost';
  const DB_PORT =
    Number(process.env.DB_PORT) || (DB_TYPE === 'postgres' ? 5432 : 3306);
  const DB_USERNAME = process.env.DB_USERNAME || 'root';
  const DB_PASSWORD = process.env.DB_PASSWORD || '';
  const DB_NAME = process.env.DB_NAME || 'enfyra';

  const tempKnex = knex({
    client: DB_TYPE === 'postgres' ? 'pg' : 'mysql2',
    connection: {
      host: DB_HOST,
      port: DB_PORT,
      user: DB_USERNAME,
      password: DB_PASSWORD,
      ...(DB_TYPE === 'postgres' && { database: 'postgres' }),
    },
  });

  try {
    if (DB_TYPE === 'mysql') {
      const result = await tempKnex.raw(
        `SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = ?`,
        [DB_NAME],
      );
      if (result[0].length === 0) {
        await tempKnex.raw(`CREATE DATABASE IF NOT EXISTS \`${DB_NAME}\``);
        console.log(`✅ MySQL: Created database ${DB_NAME}`);
      } else {
        console.log(`✅ MySQL: Database ${DB_NAME} already exists`);
      }
    } else if (DB_TYPE === 'postgres') {
      const result = await tempKnex.raw(
        `SELECT 1 FROM pg_database WHERE datname = ?`,
        [DB_NAME],
      );
      if (result.rows.length === 0) {
        await tempKnex.raw(`CREATE DATABASE "${DB_NAME}" WITH ENCODING 'UTF8'`);
        console.log(`✅ Postgres: Created database ${DB_NAME}`);
      } else {
        console.log(`✅ Postgres: Database ${DB_NAME} already exists`);
      }
    }
  } finally {
    await tempKnex.destroy();
  }
}






















