import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { randomBytes, randomUUID } from 'node:crypto';
import { mkdtemp, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { knex } from 'knex';
import { MongoClient } from 'mongodb';
import { Redis } from 'ioredis';

const BACKENDS = ['postgres', 'mysql', 'mariadb', 'mongodb'] as const;
type Backend = (typeof BACKENDS)[number];

function selectedBackends(): Backend[] {
  const selected = (process.env.MATRIX_DATABASES || BACKENDS.join(','))
    .split(',')
    .map((value) => value.trim())
    .filter(Boolean);
  assert.ok(selected.length > 0, 'MATRIX_DATABASES must not be empty');
  for (const value of selected) {
    assert.ok(
      BACKENDS.includes(value as Backend),
      `Unsupported backend: ${value}`,
    );
  }
  return [...new Set(selected)] as Backend[];
}

function localUri(value: string | undefined, label: string): URL {
  assert.ok(value, `${label} is required`);
  const uri = new URL(value);
  assert.ok(
    ['127.0.0.1', 'localhost'].includes(uri.hostname),
    `${label} must select a local database`,
  );
  return uri;
}

async function boot(
  backend: Backend,
  databaseUri: string,
  redisUri: string,
  namespace: string,
  expectedRuns: number,
  logPath: string,
): Promise<void> {
  const child = spawn(
    'yarn',
    ['tsx', 'test/e2e/fixtures/storage-contract-boot.ts'],
    {
      cwd: join(import.meta.dirname, '../..'),
      detached: true,
      env: {
        ...process.env,
        DB_URI: databaseUri,
        DB_REPLICA_URIS: '',
        REDIS_URI: redisUri,
        NODE_NAME: namespace,
        INSTANCE_ID: `${namespace}-boot`,
        NODE_ENV: 'test',
        NODE_OPTIONS: '--no-node-snapshot',
        DOTENV_CONFIG_PATH: '/dev/null',
        ADMIN_EMAIL: 'fresh-matrix@localhost.test',
        ADMIN_PASSWORD:
          process.env.ADMIN_PASSWORD || randomBytes(24).toString('hex'),
        SECRET_KEY: process.env.SECRET_KEY || randomBytes(32).toString('hex'),
        SQL_POOL_MIN: '0',
        SQL_POOL_MAX: '4',
        EXPECTED_BOOTSTRAP_RUNS: String(expectedRuns),
        STARTUP_VERBOSE: '1',
        BOOTSTRAP_VERBOSE: '1',
        REDIS_RUNTIME_CACHE: 'false',
        MONGO_FORCE_APP_TRANSACTION: '0',
      },
      stdio: ['ignore', 'pipe', 'pipe'],
    },
  );
  const stop = () => {
    if (!child.pid) return;
    try {
      process.kill(-child.pid, 'SIGKILL');
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code !== 'ESRCH') throw error;
    }
  };
  let output = '';
  child.stdout.on('data', (chunk: Buffer) => {
    output += chunk.toString();
  });
  child.stderr.on('data', (chunk: Buffer) => {
    output += chunk.toString();
  });
  let timedOut = false;
  const timeout = setTimeout(() => {
    timedOut = true;
    stop();
  }, 120_000);
  try {
    const code = await new Promise<number | null>((resolve, reject) => {
      child.once('error', reject);
      child.once('close', resolve);
    });
    const redacted = output.replace(
      /((?:mysql|postgres(?:ql)?|mongodb|redis):\/\/)[^\s"']+/g,
      '$1[REDACTED]',
    );
    await writeFile(logPath, redacted, { mode: 0o600 });
    assert.ok(
      !timedOut,
      `${backend} boot exceeded 120 seconds; see ${logPath}`,
    );
    if (code !== 0) {
      const failure =
        redacted.match(/Error: ([^\n]+)/)?.[1] || redacted.slice(-1000);
      throw new Error(`${backend} boot failed: ${failure}; see ${logPath}`);
    }
    const marker = redacted.match(/STORAGE_BOOT_RESULT (\{[^\n]+\})/);
    assert.ok(marker, `${backend} missing boot attestation; see ${logPath}`);
    const result = JSON.parse(marker[1]) as {
      bootstrapRuns: number;
      dashboardLinked: boolean;
    };
    assert.equal(result.bootstrapRuns, expectedRuns);
    assert.equal(result.dashboardLinked, true);
  } finally {
    clearTimeout(timeout);
    stop();
  }
}

async function cleanNamespace(redis: Redis, namespace: string): Promise<void> {
  let cursor = '0';
  do {
    const [next, keys] = await redis.scan(
      cursor,
      'MATCH',
      `${namespace}:*`,
      'COUNT',
      100,
    );
    cursor = next;
    if (keys.length > 0) await redis.unlink(...keys);
  } while (cursor !== '0');
}

async function verifyBackend(
  backend: Backend,
  directory: string,
  redisUri: string,
): Promise<void> {
  const variable = `MATRIX_${backend.toUpperCase()}_URI`;
  const adminUri = localUri(process.env[variable], variable);
  const mongo = backend === 'mongodb';
  assert.ok(
    mongo
      ? adminUri.protocol === 'mongodb:'
      : backend === 'postgres'
        ? ['postgres:', 'postgresql:'].includes(adminUri.protocol)
        : adminUri.protocol === 'mysql:',
    `${variable} has an incompatible protocol`,
  );
  const database = `enfyra_fresh_matrix_${randomUUID().replaceAll('-', '')}`;
  const namespace = `fresh-matrix-${backend}-${randomUUID()}`;
  const uri = new URL(adminUri);
  uri.pathname = `/${database}`;
  const sql = mongo
    ? null
    : knex({
        client: backend === 'postgres' ? 'pg' : 'mysql2',
        connection: adminUri.href,
        pool: { min: 0, max: 1 },
        acquireConnectionTimeout: 5000,
      });
  const client = mongo
    ? new MongoClient(adminUri.href, { serverSelectionTimeoutMS: 5000 })
    : null;
  const redis = new Redis(redisUri, {
    maxRetriesPerRequest: 1,
    connectTimeout: 5000,
  });
  let created = false;
  try {
    if (client) {
      await client.connect();
      await client.db(database).createCollection('fresh_matrix_probe');
      created = true;
      await client.db(database).collection('fresh_matrix_probe').drop();
      console.log(
        'DATABASE_VERSION',
        backend,
        (await client.db('admin').command({ buildInfo: 1 })).version,
      );
    } else {
      const version = JSON.stringify(
        await sql!.raw('SELECT VERSION() AS version'),
      ).match(/"version":"([^"]+)"/)?.[1];
      assert.ok(version, `${backend} did not report its database version`);
      if (backend === 'mariadb') assert.match(version, /MariaDB/i);
      if (backend === 'mysql') assert.doesNotMatch(version, /MariaDB/i);
      console.log('DATABASE_VERSION', backend, version);
      await sql!.raw('CREATE DATABASE ??', [database]);
      created = true;
    }
    await boot(
      backend,
      uri.href,
      redisUri,
      namespace,
      1,
      join(directory, `${backend}-fresh.log`),
    );
    console.log(
      'PASS',
      backend,
      'fresh install, full init, Dashboard/default-page attestation',
    );
    await boot(
      backend,
      uri.href,
      redisUri,
      namespace,
      0,
      join(directory, `${backend}-stable.log`),
    );
    console.log('PASS', backend, 'stable boot skips bootstrap');
    if (client) {
      await client
        .db(database)
        .collection('enfyra_setting')
        .updateMany({}, { $set: { isInit: false } });
    } else {
      const target = knex({
        client: backend === 'postgres' ? 'pg' : 'mysql2',
        connection: uri.href,
        pool: { min: 0, max: 1 },
      });
      try {
        await target('enfyra_setting').update({ isInit: false });
      } finally {
        await target.destroy();
      }
    }
    await boot(
      backend,
      uri.href,
      redisUri,
      namespace,
      1,
      join(directory, `${backend}-repeat-bootstrap.log`),
    );
    console.log(
      'PASS',
      backend,
      'same-version bootstrap repair and attestation',
    );
    await boot(
      backend,
      uri.href,
      redisUri,
      namespace,
      0,
      join(directory, `${backend}-post-repair-stable.log`),
    );
    console.log('PASS', backend, 'stable boot after repeated bootstrap');
  } finally {
    try {
      if (created) {
        if (client) await client.db(database).dropDatabase();
        else
          await sql!.raw(
            backend === 'postgres'
              ? 'DROP DATABASE ?? WITH (FORCE)'
              : 'DROP DATABASE ??',
            [database],
          );
      }
    } finally {
      await Promise.allSettled([client?.close(), sql?.destroy()]);
      try {
        await cleanNamespace(redis, namespace);
      } finally {
        redis.disconnect();
      }
    }
  }
}

async function main(): Promise<void> {
  const backends = selectedBackends();
  process.env.ADMIN_PASSWORD ||= randomBytes(24).toString('hex');
  process.env.SECRET_KEY ||= randomBytes(32).toString('hex');
  const redisUri = localUri(process.env.MATRIX_REDIS_URI, 'MATRIX_REDIS_URI');
  const directory = await mkdtemp(join(tmpdir(), 'enfyra-fresh-matrix-'));
  const failures: string[] = [];
  for (const backend of backends) {
    try {
      await verifyBackend(backend, directory, redisUri.href);
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      failures.push(backend);
      console.error('FAIL', message);
    }
  }
  console.log(
    'FRESH_INSTALL_MATRIX_RESULT',
    JSON.stringify({ backends, failures, directory }),
  );
  if (failures.length > 0) process.exitCode = 1;
}

void main().catch((error: unknown) => {
  console.error(error);
  process.exitCode = 1;
});
