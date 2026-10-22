import assert from 'node:assert/strict';
import { execFileSync, spawn } from 'node:child_process';
import { randomUUID } from 'node:crypto';
import { join } from 'node:path';
import knex from 'knex';
import { MongoClient } from 'mongodb';

type Backend = 'postgres' | 'mysql' | 'mongodb';

function containerEnv(name: string): Record<string, string> {
  const [container] = JSON.parse(
    execFileSync('docker', ['inspect', name], { encoding: 'utf8' }),
  );
  return Object.fromEntries(
    container.Config.Env.map((entry: string) => {
      const separator = entry.indexOf('=');
      return [entry.slice(0, separator), entry.slice(separator + 1)];
    }),
  );
}

function connectionUri(backend: Backend, database: string): string {
  const name =
    backend === 'postgres'
      ? 'postgres-dev'
      : backend === 'mysql'
        ? 'enfyra-mysql'
        : 'enfyra-mongodb';
  const env = containerEnv(name);
  const url = new URL(
    backend === 'postgres'
      ? 'postgresql://127.0.0.1:5432'
      : backend === 'mysql'
        ? 'mysql://127.0.0.1:3306'
        : 'mongodb://127.0.0.1:27017',
  );
  url.username =
    backend === 'postgres'
      ? env.POSTGRES_USER
      : backend === 'mysql'
        ? 'root'
        : env.MONGO_INITDB_ROOT_USERNAME;
  url.password =
    backend === 'postgres'
      ? env.POSTGRES_PASSWORD
      : backend === 'mysql'
        ? env.MYSQL_ROOT_PASSWORD
        : env.MONGO_INITDB_ROOT_PASSWORD;
  url.pathname = `/${database}`;
  if (backend === 'mongodb') url.searchParams.set('authSource', 'admin');
  return url.toString();
}

async function database(backend: Backend, name: string, create: boolean) {
  assert.match(name, /^enfyra_audit_delete_[a-f0-9]+$/);
  if (backend === 'mongodb') {
    const client = new MongoClient(connectionUri(backend, name), {
      serverSelectionTimeoutMS: 5000,
    });
    try {
      await client.connect();
      if (create)
        assert.equal(
          (await client.db('admin').admin().listDatabases()).databases.some(
            (db) => db.name === name,
          ),
          false,
        );
      else await client.db(name).dropDatabase();
    } finally {
      await client.close();
    }
    return;
  }
  const db = knex({
    client: backend === 'postgres' ? 'pg' : 'mysql2',
    connection: connectionUri(
      backend,
      backend === 'postgres' ? 'postgres' : 'mysql',
    ),
    pool: { min: 0, max: 1 },
  });
  try {
    await db.raw(create ? 'create database ??' : 'drop database ??', [name]);
  } finally {
    await db.destroy();
  }
}

async function boot(backend: Backend, name: string, mode: string) {
  const child = spawn(
    join(process.cwd(), 'node_modules/.bin/tsx'),
    ['test/e2e/fixtures/code-audit-delete-boot.ts'],
    {
      cwd: process.cwd(),
      detached: true,
      stdio: ['ignore', 'pipe', 'pipe'],
      env: {
        ...process.env,
        DB_URI: connectionUri(backend, name),
        REDIS_URI: 'redis://127.0.0.1:6379/15',
        NODE_NAME: name,
        NODE_ENV: 'test',
        NODE_OPTIONS: '--no-node-snapshot',
        SECRET_KEY: randomUUID(),
        ADMIN_EMAIL: 'audit-delete-fixture@localhost.test',
        ADMIN_PASSWORD: randomUUID(),
        DOTENV_CONFIG_PATH: '/dev/null',
        STARTUP_VERBOSE: '0',
        BOOTSTRAP_VERBOSE: '0',
        AUDIT_DELETE_MODE: mode,
        SQL_POOL_MIN: '0',
        SQL_POOL_MAX: '2',
      },
    },
  );
  let output = '';
  child.stdout.on('data', (chunk) => {
    output += String(chunk);
  });
  child.stderr.on('data', (chunk) => {
    output += String(chunk);
  });
  let expired = false;
  const stop = (signal: NodeJS.Signals) => {
    try {
      if (child.pid) process.kill(-child.pid, signal);
    } catch {}
  };
  const timer = setTimeout(() => {
    expired = true;
    stop('SIGKILL');
  }, 180_000);
  try {
    const code = await new Promise<number | null>((resolve, reject) => {
      child.once('error', reject);
      child.once('exit', resolve);
    });
    assert.equal(
      expired,
      false,
      `${backend} ${mode}: bootstrap exceeded deadline`,
    );
    assert.equal(code, 0, `${backend} ${mode}: ${output.slice(-8000)}`);
    const result = output
      .split('\n')
      .find((line) => line.startsWith('AUDIT_DELETE_RESULT '));
    assert.ok(result, `${backend} ${mode}: fixture did not report success`);
    console.log(result);
  } finally {
    clearTimeout(timer);
    stop('SIGKILL');
  }
}

async function main() {
  const requested = (
    process.env.MATRIX_DATABASES ?? 'postgres,mysql,mongodb'
  ).split(',');
  for (const backend of requested) {
    assert.ok(['postgres', 'mysql', 'mongodb'].includes(backend));
    const target = backend as Backend;
    const name = `enfyra_audit_delete_${randomUUID().replaceAll('-', '')}`;
    await database(target, name, true);
    try {
      await boot(target, name, 'fresh');
    } finally {
      await database(target, name, false);
    }
    const upgrade = `enfyra_audit_delete_${randomUUID().replaceAll('-', '')}`;
    await database(target, upgrade, true);
    try {
      await boot(target, upgrade, 'source');
      await boot(target, upgrade, 'upgrade');
      await boot(target, upgrade, 'stable');
    } finally {
      await database(target, upgrade, false);
    }
  }
}

void main().catch((error) => {
  console.error(error.message);
  process.exitCode = 1;
});
