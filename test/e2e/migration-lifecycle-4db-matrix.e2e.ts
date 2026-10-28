import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { randomBytes, randomUUID } from 'node:crypto';
import { mkdtemp, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { knex } from 'knex';
import { MongoClient } from 'mongodb';
import { Redis } from 'ioredis';

const BACKENDS = ['postgres', 'mysql', 'mariadb', 'mongodb'] as const;
type Backend = (typeof BACKENDS)[number];

const LEGACY_SOURCE_VERSION = '2.2.19-patch-1';

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

function defaultBackendUri(backend: Backend): string {
  switch (backend) {
    case 'postgres':
      return 'postgres://root:1234@127.0.0.1:5432/postgres';
    case 'mysql':
      return 'mysql://root:1234@127.0.0.1:3306/mysql';
    case 'mariadb':
      return 'mysql://root:2300e284e8b097913cdc8ef2f188763addae4c4d1d97509bb2dc96113ef8e6dc@127.0.0.1:3307/mysql';
    case 'mongodb':
      return 'mongodb://enfyra_admin:enfyra_password_123@127.0.0.1:27017/admin?authSource=admin';
  }
}

function localUri(backend: Backend): URL {
  const variable = `MATRIX_${backend.toUpperCase()}_URI`;
  const value = process.env[variable] || defaultBackendUri(backend);
  const uri = new URL(value);
  assert.ok(
    ['127.0.0.1', 'localhost'].includes(uri.hostname),
    `${variable} must select a local database`,
  );
  return uri;
}

async function bootLifecycle(
  backend: Backend,
  databaseUri: string,
  redisUri: string,
  namespace: string,
  expectedRuns: number,
  runRuntimeMatrix: boolean,
  logPath: string,
): Promise<{ bootstrapRuns: number; version: string; runtimeChecks: string[] }> {
  const child = spawn(
    'yarn',
    ['tsx', 'test/e2e/fixtures/migration-lifecycle-4db-boot.ts'],
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
        RUN_RUNTIME_MIGRATION_MATRIX: runRuntimeMatrix ? '1' : '0',
        ADMIN_EMAIL: 'lifecycle-matrix@localhost.test',
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
  }, 180_000);
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
      `${backend} boot exceeded 180 seconds; see ${logPath}`,
    );
    if (code !== 0) {
      const failure = redacted.slice(-1500);
      throw new Error(`${backend} boot failed: ${failure}; see ${logPath}`);
    }
    const marker = redacted.match(/LIFECYCLE_BOOT_RESULT (\{[^\n]+\})/);
    assert.ok(
      marker,
      `${backend} missing lifecycle boot attestation; see ${logPath}`,
    );
    return JSON.parse(marker[1]);
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

async function verifyBackendLifecycle(
  backend: Backend,
  directory: string,
  redisUri: string,
): Promise<void> {
  const adminUri = localUri(backend);
  const mongo = backend === 'mongodb';
  const database = `enfyra_lifecycle_matrix_${randomUUID().replaceAll('-', '')}`;
  const namespace = `lifecycle-matrix-${backend}-${randomUUID()}`;
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
      await client.db(database).createCollection('lifecycle_probe');
      created = true;
      await client.db(database).collection('lifecycle_probe').drop();
    } else {
      const version = JSON.stringify(
        await sql!.raw('SELECT VERSION() AS version'),
      ).match(/"version":"([^"]+)"/)?.[1];
      assert.ok(version, `${backend} did not report its database version`);
      if (backend === 'mariadb') assert.match(version, /MariaDB/i);
      if (backend === 'mysql') assert.doesNotMatch(version, /MariaDB/i);
      await sql!.raw('CREATE DATABASE ??', [database]);
      created = true;
    }

    // Phase 1: Fresh install
    await bootLifecycle(
      backend,
      uri.href,
      redisUri,
      namespace,
      1,
      false,
      join(directory, `${backend}-phase1-fresh.log`),
    );
    console.log('PASS', backend, 'Phase 1: Fresh install completed');

    // Phase 2: Simulate pre-upgrade version (2.2.19-patch-1) + user extension field + physical schema drift,
    // then run Upgrade + Schema Healing and verify both preservation and healing
    if (client) {
      const db = client.db(database);
      const settingTable = await db
        .collection('enfyra_table')
        .findOne({ name: 'enfyra_setting' });
      assert.ok(settingTable?._id);
      await db.collection('enfyra_column').insertOne({
        table: settingTable._id,
        name: 'userExtensionField',
        type: 'string',
        isSystem: false,
        isNullable: true,
      });
      await db
        .collection('enfyra_setting')
        .updateMany(
          {},
          {
            $set: {
              enfyraVersion: LEGACY_SOURCE_VERSION,
              userExtensionField: 'preserved',
            },
          },
        );
      await db.command({
        collMod: 'enfyra_audit',
        validationAction: 'warn',
      });
    } else {
      const target = knex({
        client: backend === 'postgres' ? 'pg' : 'mysql2',
        connection: uri.href,
        pool: { min: 0, max: 1 },
      });
      try {
        const settingTable = await target('enfyra_table')
          .where({ name: 'enfyra_setting' })
          .first();
        assert.ok(settingTable?.id);
        await target.schema.alterTable('enfyra_setting', (table) => {
          table.text('userExtensionField').nullable();
        });
        await target('enfyra_column').insert({
          tableId: settingTable.id,
          name: 'userExtensionField',
          type: 'text',
          isSystem: false,
          isNullable: true,
        });
        await target('enfyra_file')
          .whereNull('description')
          .update({ description: '' });
        await target.schema.alterTable('enfyra_file', (table) => {
          table.text('description').notNullable().alter();
        });
        await target('enfyra_setting').update({
          enfyraVersion: LEGACY_SOURCE_VERSION,
          userExtensionField: 'preserved',
        });
      } finally {
        await target.destroy();
      }
    }

    await bootLifecycle(
      backend,
      uri.href,
      redisUri,
      namespace,
      1,
      false,
      join(directory, `${backend}-phase2-upgrade-healing.log`),
    );

    if (client) {
      const db = client.db(database);
      const settingDoc = await db.collection('enfyra_setting').findOne({});
      assert.equal(settingDoc?.userExtensionField, 'preserved');
      const collections = await db
        .listCollections({ name: 'enfyra_audit' }, { nameOnly: false })
        .toArray();
      assert.equal(collections[0]?.options?.validationAction, 'error');
    } else {
      const target = knex({
        client: backend === 'postgres' ? 'pg' : 'mysql2',
        connection: uri.href,
        pool: { min: 0, max: 1 },
      });
      try {
        const settingRow = await target('enfyra_setting').first();
        assert.equal(settingRow?.userExtensionField, 'preserved');
        assert.equal(
          await target.schema.hasColumn('enfyra_setting', 'userExtensionField'),
          true,
        );
        const fileDescInfo =
          await target('enfyra_file').columnInfo('description');
        assert.equal(
          fileDescInfo.nullable,
          true,
          `${backend} did not heal enfyra_file.description back to nullable`,
        );
      } finally {
        await target.destroy();
      }
    }
    console.log(
      'PASS',
      backend,
      'Phase 2: Version upgrade, user-column preservation & physical schema healing converged',
    );

    // Phase 3: Stable boot + Full Runtime Auto-Migration Matrix (8 checks)
    const phase3 = await bootLifecycle(
      backend,
      uri.href,
      redisUri,
      namespace,
      0,
      true,
      join(directory, `${backend}-phase3-runtime-migration.log`),
    );
    assert.equal(phase3.runtimeChecks.length, 8);
    console.log(
      'PASS',
      backend,
      `Phase 3: Runtime auto-migration matrix passed (${phase3.runtimeChecks.join(', ')})`,
    );
  } finally {
    try {
      await cleanNamespace(redis, namespace);
    } finally {
      redis.disconnect();
    }
    if (client) {
      try {
        if (created) await client.db(database).dropDatabase();
      } finally {
        await client.close();
      }
    }
    if (sql) {
      try {
        if (created) await sql.raw('DROP DATABASE IF EXISTS ??', [database]);
      } finally {
        await sql.destroy();
      }
    }
  }
}

async function main() {
  const redisUri = process.env.MATRIX_REDIS_URI || 'redis://127.0.0.1:6379/15';
  const directory = await mkdtemp(join(tmpdir(), 'enfyra-lifecycle-4db-'));
  console.log('LIFECYCLE_LOG_DIR', directory);
  let succeeded = false;
  try {
    for (const backend of selectedBackends()) {
      await verifyBackendLifecycle(backend, directory, redisUri);
    }
    succeeded = true;
    console.log('ALL_4DB_MIGRATION_LIFECYCLE_PASSED');
  } finally {
    if (succeeded && process.env.KEEP_LIFECYCLE_LOGS !== '1') {
      await rm(directory, { recursive: true, force: true });
    }
  }
}

main().then(
  () => process.exit(0),
  (error) => {
    console.error(error);
    process.exit(1);
  },
);
