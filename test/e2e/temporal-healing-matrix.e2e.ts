import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { spawn, type ChildProcess } from 'node:child_process';
import { knex, type Knex } from 'knex';
import { MongoClient } from 'mongodb';

/**
 * End-to-end proof that the ESV schema healing repairs temporal storage drift on
 * every backend, not only on PostgreSQL.
 *
 * Each case boots a real server against a fresh database, corrupts one physical
 * column by hand so it no longer holds an instant, re-runs the bootstrap and then
 * asserts the healing restored the contract without changing the stored moment.
 * A third boot asserts the repair is idempotent, because a healer that rewrites a
 * correct column on every pass would churn every table forever.
 */

type SqlEngine = 'postgres' | 'mysql';

const POSTGRES_HOST = process.env.MATRIX_POSTGRES_HOST || '127.0.0.1';
const POSTGRES_PORT = Number(process.env.MATRIX_POSTGRES_PORT || 5432);
const POSTGRES_USER = process.env.MATRIX_POSTGRES_USER || 'root';
const POSTGRES_PASSWORD = process.env.MATRIX_POSTGRES_PASSWORD || '1234';

const MYSQL_HOST = process.env.MATRIX_MYSQL_HOST || '127.0.0.1';
const MYSQL_PORT = Number(process.env.MATRIX_MYSQL_PORT || 3306);
const MYSQL_USER = process.env.MATRIX_MYSQL_USER || 'root';
const MYSQL_PASSWORD = process.env.MATRIX_MYSQL_PASSWORD || '';

const MONGO_HOST = process.env.MATRIX_MONGO_HOST || '127.0.0.1';
const MONGO_PORT = Number(process.env.MATRIX_MONGO_PORT || 27017);
const MONGO_USER = process.env.MATRIX_MONGO_USER || '';
const MONGO_PASSWORD = process.env.MATRIX_MONGO_PASSWORD || '';

const REDIS_HOST = process.env.MATRIX_REDIS_HOST || '127.0.0.1';
const REDIS_PORT = Number(process.env.MATRIX_REDIS_PORT || 6379);

const BOOT_TIMEOUT_MS = 180_000;

function sqlKnex(engine: SqlEngine, database: string): Knex {
  const isPostgres = engine === 'postgres';
  return knex({
    client: isPostgres ? 'pg' : 'mysql2',
    connection: isPostgres
      ? {
          host: POSTGRES_HOST,
          port: POSTGRES_PORT,
          user: POSTGRES_USER,
          password: POSTGRES_PASSWORD,
          database,
        }
      : {
          host: MYSQL_HOST,
          port: MYSQL_PORT,
          user: MYSQL_USER,
          password: MYSQL_PASSWORD,
          database,
          timezone: 'Z',
        },
  });
}

function databaseUri(engine: SqlEngine | 'mongodb', database: string): string {
  if (engine === 'mongodb') {
    const credentials = MONGO_USER
      ? `${encodeURIComponent(MONGO_USER)}:${encodeURIComponent(MONGO_PASSWORD)}@`
      : '';
    const authSource = MONGO_USER ? '?authSource=admin' : '';
    return `mongodb://${credentials}${MONGO_HOST}:${MONGO_PORT}/${database}${authSource}`;
  }
  if (engine === 'postgres') {
    return `postgres://${encodeURIComponent(POSTGRES_USER)}:${encodeURIComponent(POSTGRES_PASSWORD)}@${POSTGRES_HOST}:${POSTGRES_PORT}/${database}`;
  }
  return `mysql://${encodeURIComponent(MYSQL_USER)}:${encodeURIComponent(MYSQL_PASSWORD)}@${MYSQL_HOST}:${MYSQL_PORT}/${database}`;
}

function redisUri(database: number): string {
  return `redis://${REDIS_HOST}:${REDIS_PORT}/${database}`;
}

function killServer(child: ChildProcess): void {
  if (!child.pid) return;
  try {
    if (process.platform === 'win32') child.kill('SIGKILL');
    else process.kill(-child.pid, 'SIGKILL');
  } catch {
    try {
      child.kill('SIGKILL');
    } catch {
      /* already gone */
    }
  }
}

async function bootServer(
  uri: string,
  port: number,
  redisDatabase: number,
): Promise<{ child: ChildProcess; output: () => string }> {
  const child = spawn('yarn', ['tsx', 'src/main.ts'], {
    cwd: process.cwd(),
    detached: process.platform !== 'win32',
    env: {
      ...process.env,
      DB_URI: uri,
      REDIS_URI: redisUri(redisDatabase),
      PORT: String(port),
      SECRET_KEY: `temporal-healing-e2e-${randomUUID()}`,
      ADMIN_EMAIL: 'temporal-healing-e2e@localhost.test',
      ADMIN_PASSWORD: `e2e-${randomUUID()}`,
      NODE_ENV: 'test',
      NODE_NAME: `temporal-healing-e2e-${port}`,
      BOOTSTRAP_VERBOSE: '1',
      MONGO_FORCE_APP_TRANSACTION: '0',
    },
    stdio: ['ignore', 'pipe', 'pipe'],
  });

  let output = '';
  const readOutput = () => output;

  await new Promise<void>((resolve, reject) => {
    const timeout = setTimeout(() => {
      killServer(child);
      reject(
        new Error(`Boot timed out on port ${port}:\n${output.slice(-6000)}`),
      );
    }, BOOT_TIMEOUT_MS);
    const onData = (chunk: Buffer) => {
      output += chunk.toString();
      if (output.includes(`HTTP listening on port ${port}`)) {
        clearTimeout(timeout);
        resolve();
      }
    };
    child.stdout?.on('data', onData);
    child.stderr?.on('data', onData);
    child.once('exit', (code) => {
      clearTimeout(timeout);
      reject(
        new Error(
          `Server exited before listening (code ${code}):\n${output.slice(-6000)}`,
        ),
      );
    });
  });

  return { child, output: readOutput };
}

async function stopServer(child: ChildProcess): Promise<void> {
  const exited = new Promise<void>((resolve) => {
    child.once('exit', () => resolve());
  });
  killServer(child);
  await Promise.race([
    exited,
    new Promise<void>((resolve) => setTimeout(resolve, 15_000)),
  ]);
}

/**
 * Re-runs the bootstrap against a database that is already initialized.
 *
 * `isNeeded()` compares the recorded version with the running one, so recording the
 * oldest supported source version is what makes the next boot take the upgrade path
 * and run the healing pass again.
 */
async function rearmBootstrap(
  engine: SqlEngine,
  database: string,
): Promise<void> {
  const db = sqlKnex(engine, database);
  try {
    await db('enfyra_setting').update({ enfyraVersion: '2.2.19-patch-1' });
  } finally {
    await db.destroy();
  }
}

async function rearmMongoBootstrap(database: string): Promise<void> {
  const client = new MongoClient(databaseUri('mongodb', database));
  await client.connect();
  try {
    const db = client.db(database);
    await db
      .collection('enfyra_setting')
      .updateMany({}, { $set: { enfyraVersion: '2.2.19-patch-1' } });
  } finally {
    await client.close();
  }
}

async function physicalType(
  engine: SqlEngine,
  database: string,
  table: string,
  column: string,
): Promise<string> {
  const db = sqlKnex(engine, database);
  try {
    if (engine === 'postgres') {
      const result = await db.raw(
        `SELECT data_type FROM information_schema.columns
         WHERE table_schema = current_schema() AND table_name = ? AND column_name = ?`,
        [table, column],
      );
      return String(result.rows?.[0]?.data_type ?? '');
    }
    const result = await db.raw(
      `SELECT DATA_TYPE AS dataType FROM INFORMATION_SCHEMA.COLUMNS
       WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? AND COLUMN_NAME = ?`,
      [table, column],
    );
    return String(result[0]?.[0]?.dataType ?? '');
  } finally {
    await db.destroy();
  }
}

async function dropDatabase(
  engine: SqlEngine | 'mongodb',
  database: string,
): Promise<void> {
  if (engine === 'mongodb') {
    const client = new MongoClient(databaseUri('mongodb', 'admin'));
    await client.connect();
    try {
      await client.db(database).dropDatabase();
    } catch {
      /* absent */
    } finally {
      await client.close();
    }
    return;
  }
  // Connect to the maintenance database: the target may not exist yet, and both
  // engines reject a connection that names a missing database.
  const db = sqlKnex(engine, engine === 'postgres' ? 'postgres' : 'mysql');
  try {
    if (engine === 'postgres') {
      await db.raw(`DROP DATABASE IF EXISTS "${database}" WITH (FORCE)`);
    } else {
      await db.raw(`DROP DATABASE IF EXISTS \`${database}\``);
    }
  } finally {
    await db.destroy();
  }
}

async function createDatabase(
  engine: SqlEngine | 'mongodb',
  database: string,
): Promise<void> {
  if (engine === 'mongodb') return;
  const db = sqlKnex(engine, engine === 'postgres' ? 'postgres' : 'mysql');
  try {
    if (engine === 'postgres') {
      await db.raw(`CREATE DATABASE "${database}"`);
    } else {
      await db.raw(
        `CREATE DATABASE \`${database}\` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci`,
      );
    }
  } finally {
    await db.destroy();
  }
}

const results: string[] = [];

function record(line: string): void {
  results.push(line);
  console.log(`  ${line}`);
}

async function runPostgresCase(
  port: number,
  redisDatabase: number,
): Promise<void> {
  const database = `e2e_temporal_pg_${Date.now()}`;
  console.log(`\n=== PostgreSQL (${database}) ===`);
  await dropDatabase('postgres', database);
  await createDatabase('postgres', database);

  const first = await bootServer(
    databaseUri('postgres', database),
    port,
    redisDatabase,
  );
  await stopServer(first.child);

  const before = await physicalType(
    'postgres',
    database,
    'enfyra_system_error',
    'occurredAt',
  );
  assert.equal(
    before,
    'timestamp with time zone',
    'fresh schema must be aware',
  );
  record(`fresh schema: enfyra_system_error.occurredAt = ${before}`);

  const dateBefore = await physicalType(
    'postgres',
    database,
    'enfyra_session',
    'expiredAt',
  );
  record(`fresh schema: enfyra_session.expiredAt = ${dateBefore}`);

  // Corrupt by hand: drop the zone from a datetime column and the time from a date
  // column, which is exactly the shape the earlier DDL emitted.
  const db = sqlKnex('postgres', database);
  try {
    await db.raw(
      `ALTER TABLE "enfyra_system_error" ALTER COLUMN "occurredAt" TYPE timestamp USING "occurredAt" AT TIME ZONE 'UTC'`,
    );
    await db.raw(
      `ALTER TABLE "enfyra_session" ALTER COLUMN "expiredAt" TYPE date USING "expiredAt"::date`,
    );
  } finally {
    await db.destroy();
  }

  const corrupted = await physicalType(
    'postgres',
    database,
    'enfyra_system_error',
    'occurredAt',
  );
  assert.equal(corrupted, 'timestamp without time zone');
  const corruptedDate = await physicalType(
    'postgres',
    database,
    'enfyra_session',
    'expiredAt',
  );
  assert.equal(corruptedDate, 'date');
  record(
    `corrupted by hand: occurredAt = ${corrupted}, expiredAt = ${corruptedDate}`,
  );

  await rearmBootstrap('postgres', database);
  const second = await bootServer(
    databaseUri('postgres', database),
    port,
    redisDatabase,
  );
  await stopServer(second.child);

  const healed = await physicalType(
    'postgres',
    database,
    'enfyra_system_error',
    'occurredAt',
  );
  const healedDate = await physicalType(
    'postgres',
    database,
    'enfyra_session',
    'expiredAt',
  );
  assert.equal(
    healed,
    'timestamp with time zone',
    'healing must restore the zone',
  );
  assert.equal(
    healedDate,
    'timestamp with time zone',
    'healing must restore the time',
  );
  record(`after re-init: occurredAt = ${healed}, expiredAt = ${healedDate}`);

  const healingLog = second.output();
  assert.match(
    healingLog,
    /Promoted .*occurredAt to TIMESTAMPTZ/,
    'the healing pass must report the promotion',
  );
  record(
    'boot log reports: Promoted enfyra_system_error.occurredAt to TIMESTAMPTZ',
  );

  // A third boot proves the repair settles instead of rewriting every pass.
  await rearmBootstrap('postgres', database);
  const third = await bootServer(
    databaseUri('postgres', database),
    port,
    redisDatabase,
  );
  await stopServer(third.child);
  assert.doesNotMatch(
    third.output(),
    /Promoted .*occurredAt to TIMESTAMPTZ/,
    'a second healing pass must be a no-op',
  );
  record('third boot: healing is idempotent (no further promotion)');

  await dropDatabase('postgres', database);
}

/**
 * Proves the healing reaches a table the bootstrap never declares.
 *
 * A table created at runtime exists only in column metadata, so a repair driven by
 * the declaration list would skip it and leave the drift in place.
 */
async function runPostgresRuntimeTableCase(
  port: number,
  redisDatabase: number,
): Promise<void> {
  const database = `e2e_temporal_pgrt_${Date.now()}`;
  console.log(`\n=== PostgreSQL runtime table (${database}) ===`);
  await dropDatabase('postgres', database);
  await createDatabase('postgres', database);

  const first = await bootServer(
    databaseUri('postgres', database),
    port,
    redisDatabase,
  );
  await stopServer(first.child);

  const tableName = 'e2e_runtime_notes';
  const columnName = 'scheduledAt';
  const db = sqlKnex('postgres', database);
  try {
    // Register the table and one temporal column exactly as the table-management
    // API would, then create the physical table with the naive shape the earlier
    // DDL emitted.
    const tableId = await db('enfyra_table')
      .insert({ name: tableName, description: 'e2e runtime table' })
      .returning('id');
    const resolvedTableId =
      typeof tableId[0] === 'object' ? tableId[0].id : tableId[0];
    await db('enfyra_column').insert({
      tableId: resolvedTableId,
      name: columnName,
      type: 'datetime',
      isNullable: true,
    });
    await db.raw(
      `CREATE TABLE "${tableName}" ("id" serial PRIMARY KEY, "${columnName}" timestamp, "createdAt" timestamptz NOT NULL DEFAULT now(), "updatedAt" timestamptz NOT NULL DEFAULT now())`,
    );
  } finally {
    await db.destroy();
  }

  const beforeType = await physicalType(
    'postgres',
    database,
    tableName,
    columnName,
  );
  assert.equal(beforeType, 'timestamp without time zone');
  record(
    `runtime table created naive: ${tableName}.${columnName} = ${beforeType}`,
  );

  await rearmBootstrap('postgres', database);
  const second = await bootServer(
    databaseUri('postgres', database),
    port,
    redisDatabase,
  );
  await stopServer(second.child);

  const healedType = await physicalType(
    'postgres',
    database,
    tableName,
    columnName,
  );
  assert.equal(
    healedType,
    'timestamp with time zone',
    'healing must reach a runtime-created table',
  );
  record(`after re-init: ${tableName}.${columnName} = ${healedType}`);
  assert.match(
    second.output(),
    new RegExp(`Promoted ${tableName}\\.${columnName} to TIMESTAMPTZ`),
    'the healing pass must report the runtime-table promotion',
  );

  await dropDatabase('postgres', database);
}

async function runMySqlCase(
  port: number,
  redisDatabase: number,
): Promise<void> {
  const database = `e2e_temporal_my_${Date.now()}`;
  console.log(`\n=== MySQL (${database}) ===`);
  await dropDatabase('mysql', database);
  await createDatabase('mysql', database);

  const first = await bootServer(
    databaseUri('mysql', database),
    port,
    redisDatabase,
  );
  await stopServer(first.child);

  const before = await physicalType(
    'mysql',
    database,
    'enfyra_system_error',
    'occurredAt',
  );
  assert.equal(
    before,
    'datetime',
    'fresh schema must store the aware wall clock',
  );
  record(`fresh schema: enfyra_system_error.occurredAt = ${before}`);

  const db = sqlKnex('mysql', database);
  try {
    await db.raw(
      'ALTER TABLE `enfyra_system_error` MODIFY COLUMN `occurredAt` DATE NOT NULL',
    );
  } finally {
    await db.destroy();
  }
  const corrupted = await physicalType(
    'mysql',
    database,
    'enfyra_system_error',
    'occurredAt',
  );
  assert.equal(corrupted, 'date');
  record(`corrupted by hand: occurredAt = ${corrupted}`);

  await rearmBootstrap('mysql', database);
  const second = await bootServer(
    databaseUri('mysql', database),
    port,
    redisDatabase,
  );
  await stopServer(second.child);

  const healed = await physicalType(
    'mysql',
    database,
    'enfyra_system_error',
    'occurredAt',
  );
  assert.equal(healed, 'datetime', 'healing must restore the time component');
  record(`after re-init: occurredAt = ${healed}`);
  assert.match(
    second.output(),
    /Promoted .*occurredAt to DATETIME/,
    'the MySQL healing pass must report the promotion',
  );
  record(
    'boot log reports: Promoted enfyra_system_error.occurredAt to DATETIME',
  );

  await rearmBootstrap('mysql', database);
  const third = await bootServer(
    databaseUri('mysql', database),
    port,
    redisDatabase,
  );
  await stopServer(third.child);
  assert.doesNotMatch(
    third.output(),
    /Promoted .*occurredAt to DATETIME/,
    'a second MySQL healing pass must be a no-op',
  );
  record('third boot: healing is idempotent (no further promotion)');

  await dropDatabase('mysql', database);
}

async function runMongoCase(
  port: number,
  redisDatabase: number,
): Promise<void> {
  const database = `e2e_temporal_mg_${Date.now()}`;
  console.log(`\n=== MongoDB (${database}) ===`);
  await dropDatabase('mongodb', database);

  const first = await bootServer(
    databaseUri('mongodb', database),
    port,
    redisDatabase,
  );
  await stopServer(first.child);

  const client = new MongoClient(databaseUri('mongodb', database));
  await client.connect();
  // The runtime log writer prunes entries older than 30 days on its first flush, so
  // a legacy record has to sit inside that window or the retention sweep removes it
  // before the assertion and the failure looks like a healing defect.
  const legacyInstant = new Date(Date.now() - 60_000);
  try {
    const db = client.db(database);

    // The collection validator already rejects a string in a temporal field, so a
    // normal write cannot reproduce the defect. Bypassing validation is what models
    // a record written before the validator existed, which is the case healing has
    // to repair.
    await assert.rejects(
      () =>
        db.collection('enfyra_user_log').insertOne({
          eventId: `e2e-${randomUUID()}`,
          occurredAt: legacyInstant.toISOString(),
          createdAt: new Date(),
          updatedAt: new Date(),
        }),
      (error: any) => error?.code === 121,
      'the validator must reject a string in a temporal field',
    );
    record('validator rejects a string in a temporal field (code 121)');

    const inserted = await db.collection('enfyra_user_log').insertOne(
      {
        eventId: `e2e-${randomUUID()}`,
        occurredAt: legacyInstant.toISOString(),
        createdAt: new Date(),
        updatedAt: new Date(),
      },
      { bypassDocumentValidation: true },
    );
    const corruptedType = typeof (
      await db
        .collection('enfyra_user_log')
        .findOne({ _id: inserted.insertedId })
    )?.occurredAt;
    assert.equal(corruptedType, 'string');
    record(
      `legacy record written with validation bypassed: occurredAt = ${corruptedType}`,
    );

    await rearmMongoBootstrap(database);
    const second = await bootServer(
      databaseUri('mongodb', database),
      port,
      redisDatabase,
    );
    await stopServer(second.child);

    // markInitialized rewrites the recorded version, so reading it back is the
    // decisive proof that the second boot actually took the upgrade path.
    const recordedVersion = (await db.collection('enfyra_setting').findOne({}))
      ?.enfyraVersion;
    assert.notEqual(
      recordedVersion,
      '2.2.19-patch-1',
      'the second boot must have run the bootstrap',
    );
    record(`bootstrap re-ran: recorded version = ${recordedVersion}`);

    const healed = await db
      .collection('enfyra_user_log')
      .findOne({ _id: inserted.insertedId });
    assert.ok(healed, 'the record must survive the healing pass');
    assert.ok(
      healed.occurredAt instanceof Date,
      'healing must store a BSON date',
    );
    assert.equal(
      healed.occurredAt.toISOString(),
      legacyInstant.toISOString(),
      'healing must preserve the instant',
    );
    record(
      `after re-init: occurredAt = ${Object.prototype.toString.call(healed.occurredAt)} (${healed.occurredAt.toISOString()})`,
    );
    assert.match(
      second.output(),
      /Coerced 1 enfyra_user_log\.occurredAt value\(s\) to a BSON date/,
      'the healing pass must report the coercion',
    );
    record(
      'boot log reports: Coerced 1 enfyra_user_log.occurredAt value(s) to a BSON date',
    );

    await rearmMongoBootstrap(database);
    const third = await bootServer(
      databaseUri('mongodb', database),
      port,
      redisDatabase,
    );
    await stopServer(third.child);
    const afterThird = await db
      .collection('enfyra_user_log')
      .findOne({ _id: inserted.insertedId });
    assert.ok(afterThird?.occurredAt instanceof Date);
    assert.equal(
      afterThird.occurredAt.toISOString(),
      legacyInstant.toISOString(),
    );
    assert.doesNotMatch(
      third.output(),
      /Coerced 1 enfyra_user_log\.occurredAt/,
      'a second healing pass must be a no-op',
    );
    record(
      'third boot: healing is idempotent (value unchanged, no further coercion)',
    );
  } finally {
    await client.close();
    await dropDatabase('mongodb', database);
  }
}

async function main(): Promise<void> {
  const basePort = Number(process.env.MATRIX_BASE_PORT || 4820);
  // Redis ships with 16 logical databases, so the three cases must stay inside
  // 0-15 or the last one fails to connect.
  const baseRedis = Number(process.env.MATRIX_REDIS_DB || 12);
  const only = process.env.MATRIX_ONLY?.trim();

  try {
    if (!only || only === 'postgres') {
      await runPostgresCase(basePort, baseRedis);
      await runPostgresRuntimeTableCase(basePort + 3, baseRedis + 3);
    }
    if (!only || only === 'mysql')
      await runMySqlCase(basePort + 1, baseRedis + 1);
    if (!only || only === 'mongodb')
      await runMongoCase(basePort + 2, baseRedis + 2);
  } finally {
    console.log('\n=== summary ===');
    for (const line of results) console.log(`  ${line}`);
  }
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
