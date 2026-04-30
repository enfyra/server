import { randomBytes } from 'crypto';
import knex, { Knex } from 'knex';
import { MongoClient, Db } from 'mongodb';
import {
  fixtureExtensionRows,
  fixtureMenuRows,
  fixtureUserRows,
} from './filter-dsl-metadata';
import { installMysqlUnaccent } from '../../src/engines/sql';

export function makeSafeDbName(): string {
  return `enfyra_f_${randomBytes(8).toString('hex')}`;
}

export async function seedSqlTables(k: Knex): Promise<void> {
  await k.schema.createTable('menu', (t) => {
    t.integer('id').primary();
    t.string('label', 512);
  });
  await k.schema.createTable('user', (t) => {
    t.integer('id').primary();
    t.string('name', 512);
  });
  await k.schema.createTable('extension', (t) => {
    t.integer('id').primary();
    t.string('title', 512);
    t.integer('prio').defaultTo(0);
    t.integer('menuId').nullable();
    t.integer('ownerId').nullable();
  });
  await k('menu').insert(fixtureMenuRows());
  await k('user').insert(fixtureUserRows());
  await k('extension').insert(fixtureExtensionRows());
}

export type PgContext = {
  knex: Knex;
  dbName: string;
  cleanup: () => Promise<void>;
};

export async function createIsolatedPostgres(
  adminUrl: string,
): Promise<PgContext> {
  const dbName = makeSafeDbName();
  const admin = knex({
    client: 'pg',
    connection: adminUrl,
    pool: { min: 0, max: 2 },
  });
  await admin.raw(`CREATE DATABASE ??`, [dbName]);
  await admin.destroy();
  const urlObj = new URL(adminUrl.replace(/^postgresql:/, 'http:'));
  const userPass =
    decodeURIComponent(urlObj.username) +
    (urlObj.password ? `:${decodeURIComponent(urlObj.password)}` : '');
  const hostPort = urlObj.hostname + (urlObj.port ? `:${urlObj.port}` : '');
  const childUrl = `postgresql://${userPass}@${hostPort}/${dbName}`;
  const k = knex({
    client: 'pg',
    connection: childUrl,
    pool: { min: 0, max: 10 },
  });
  try {
    await k.raw('CREATE EXTENSION IF NOT EXISTS unaccent');
  } catch {
    /* optional in minimal images */
  }
  await seedSqlTables(k);
  return {
    knex: k,
    dbName,
    cleanup: async () => {
      await k.destroy();
      const a2 = knex({
        client: 'pg',
        connection: adminUrl,
        pool: { min: 0, max: 2 },
      });
      await a2.raw(
        `SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = ? AND pid <> pg_backend_pid()`,
        [dbName],
      );
      await a2.raw(`DROP DATABASE IF EXISTS ??`, [dbName]);
      await a2.destroy();
    },
  };
}

export type MysqlContext = {
  knex: Knex;
  dbName: string;
  cleanup: () => Promise<void>;
};

function mysqlConnectionConfig(
  parsed: URL,
  database: string,
): {
  host: string;
  port: number;
  user: string;
  password: string;
  database: string;
} {
  return {
    host: parsed.hostname,
    port: parseInt(parsed.port || '3306', 10),
    user: decodeURIComponent(parsed.username || 'root'),
    password:
      parsed.password !== undefined && parsed.password !== ''
        ? decodeURIComponent(parsed.password)
        : '',
    database,
  };
}

export async function createIsolatedMysql(
  adminUrl: string,
): Promise<MysqlContext> {
  const dbName = makeSafeDbName();
  const parsed = new URL(
    adminUrl.startsWith('mysql://')
      ? adminUrl
      : adminUrl.replace(/^mysql:/, 'http:'),
  );
  const adminDb =
    parsed.pathname && parsed.pathname.length > 1
      ? parsed.pathname.slice(1).split('/')[0]
      : 'mysql';
  const admin = knex({
    client: 'mysql2',
    connection: mysqlConnectionConfig(parsed, adminDb),
    pool: { min: 0, max: 2 },
  });
  await admin.raw(`CREATE DATABASE \`${dbName.replace(/`/g, '')}\``);
  await admin.destroy();
  const k = knex({
    client: 'mysql2',
    connection: mysqlConnectionConfig(parsed, dbName),
    pool: { min: 0, max: 10 },
  });
  await installMysqlUnaccent(k);
  await seedSqlTables(k);
  return {
    knex: k,
    dbName,
    cleanup: async () => {
      await k.destroy();
      const a2 = knex({
        client: 'mysql2',
        connection: mysqlConnectionConfig(parsed, adminDb),
        pool: { min: 0, max: 2 },
      });
      await a2.raw(`DROP DATABASE IF EXISTS \`${dbName.replace(/`/g, '')}\``);
      await a2.destroy();
    },
  };
}

export type MongoContext = {
  db: Db;
  client: MongoClient;
  dbName: string;
  cleanup: () => Promise<void>;
};

export async function createIsolatedMongo(
  baseUri: string,
): Promise<MongoContext> {
  const dbName = makeSafeDbName();
  const client = new MongoClient(baseUri, { maxPoolSize: 5 });
  await client.connect();
  let db: Db;
  try {
    db = client.db(dbName);
    const menus = fixtureMenuRows().map((r) => ({ _id: r.id, ...r }));
    const users = fixtureUserRows().map((r) => ({ _id: r.id, ...r }));
    const exts = fixtureExtensionRows().map((r) => ({ _id: r.id, ...r }));
    await db.collection('menu').insertMany(menus as any[]);
    await db.collection('user').insertMany(users as any[]);
    await db.collection('extension').insertMany(exts as any[]);
  } catch (e) {
    try {
      await client.db(dbName).dropDatabase();
    } catch {
      /* empty */
    }
    await client.close().catch(() => undefined);
    throw e;
  }
  return {
    db,
    client,
    dbName,
    cleanup: async () => {
      try {
        await db.dropDatabase();
      } catch {
        /* empty */
      }
      await client.close().catch(() => undefined);
    },
  };
}
