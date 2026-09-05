import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { spawn, type ChildProcess } from 'node:child_process';
import { mkdtemp, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { MongoClient, ObjectId } from 'mongodb';
import { Redis } from 'ioredis';
import { knex, type Knex } from 'knex';

type Database = 'postgres' | 'mysql' | 'mongodb';
type SqlDatabase = Exclude<Database, 'mongodb'>;

const PROVISION_LOCK_KEY = 'sys:provision_init_lock';
const SUPPORTED_DATABASES: Database[] = ['postgres', 'mysql', 'mongodb'];

function sqlConnection(database: SqlDatabase, databaseName?: string) {
  const prefix = database === 'postgres' ? 'POSTGRES' : 'MYSQL';
  return {
    host: process.env[`MATRIX_${prefix}_HOST`] || '127.0.0.1',
    port: Number(
      process.env[`MATRIX_${prefix}_PORT`] ||
        (database === 'postgres' ? 5432 : 3306),
    ),
    user: process.env[`MATRIX_${prefix}_USER`] || 'root',
    password: process.env[`MATRIX_${prefix}_PASSWORD`] || '1234',
    database:
      databaseName ||
      process.env[`MATRIX_${prefix}_DATABASE`] ||
      (database === 'postgres' ? 'enfyra' : 'enfyra_matrix'),
  };
}

function sqlClient(database: SqlDatabase, databaseName?: string): Knex {
  return knex({
    client: database === 'postgres' ? 'pg' : 'mysql2',
    connection: sqlConnection(database, databaseName),
  });
}

function databaseUri(database: SqlDatabase, databaseName: string): string {
  const connection = sqlConnection(database, databaseName);
  const protocol = database === 'postgres' ? 'postgresql' : 'mysql';
  const uri = new URL(`${protocol}://${connection.host}`);
  uri.port = String(connection.port);
  uri.username = connection.user;
  uri.password = connection.password;
  uri.pathname = `/${databaseName}`;
  return uri.toString();
}

function mongoSettings() {
  return {
    host: process.env.MATRIX_MONGO_HOST || '127.0.0.1',
    port: Number(process.env.MATRIX_MONGO_PORT || 27017),
    user: process.env.MATRIX_MONGO_USER || 'enfyra_admin',
    password: process.env.MATRIX_MONGO_PASSWORD || 'enfyra_password_123',
    authDatabase: process.env.MATRIX_MONGO_AUTH_DATABASE || 'admin',
  };
}

function mongoUri(databaseName: string): string {
  const settings = mongoSettings();
  const uri = new URL(`mongodb://${settings.host}`);
  uri.port = String(settings.port);
  uri.username = settings.user;
  uri.password = settings.password;
  uri.pathname = `/${databaseName}`;
  uri.searchParams.set('authSource', settings.authDatabase);
  return uri.toString();
}

function redisUri(): string {
  const configured =
    process.env.MATRIX_REDIS_URI || 'redis://127.0.0.1:6379/13';
  const uri = new URL(configured);
  uri.pathname = `/${process.env.MATRIX_BOOT_RACE_REDIS_DB || '13'}`;
  return uri.toString();
}

function selectedDatabases(): Database[] {
  const requested = (
    process.env.MATRIX_DATABASES || SUPPORTED_DATABASES.join(',')
  )
    .split(',')
    .map((value) => value.trim())
    .filter(Boolean) as Database[];
  const unsupported = requested.filter(
    (database) => !SUPPORTED_DATABASES.includes(database),
  );
  if (unsupported.length > 0) {
    throw new Error(
      `Unsupported MATRIX_DATABASES value: ${unsupported.join(', ')}`,
    );
  }
  return [...new Set(requested)];
}

type ServerHandle = {
  child: ChildProcess;
  port: number;
  nodeName: string;
  output: string;
};

function spawnServer(
  dbUri: string,
  port: number,
  nodeName: string,
  secretKey: string,
  adminPassword: string,
  options: { entrypoint?: string; env?: NodeJS.ProcessEnv } = {},
): ServerHandle {
  const child = spawn('yarn', ['tsx', options.entrypoint ?? 'src/main.ts'], {
    cwd: process.cwd(),
    detached: process.platform !== 'win32',
    env: {
      ...process.env,
      DB_URI: dbUri,
      REDIS_URI: redisUri(),
      PORT: String(port),
      SECRET_KEY: secretKey,
      ADMIN_EMAIL: `boot-race-${nodeName}@localhost.test`,
      ADMIN_PASSWORD: adminPassword,
      NODE_ENV: 'test',
      NODE_NAME: nodeName,
      BOOTSTRAP_VERBOSE: '1',
      MONGO_FORCE_APP_TRANSACTION: '0',
      ...options.env,
    },
    stdio: ['ignore', 'pipe', 'pipe'],
  });
  const handle: ServerHandle = { child, port, nodeName, output: '' };
  const onData = (chunk: Buffer) => {
    const text = chunk.toString();
    handle.output += text;
    if (process.env.BOOT_RACE_LIVE_LOG === '1') {
      process.stdout.write(`[boot-race:${port}] ${text}`);
    }
  };
  child.stdout?.on('data', onData);
  child.stderr?.on('data', onData);
  return handle;
}

async function waitForExit(
  handle: ServerHandle,
  timeoutMs = 120_000,
): Promise<void> {
  if (handle.child.exitCode !== null || handle.child.signalCode !== null)
    return;
  await new Promise<void>((resolve, reject) => {
    const timeout = setTimeout(() => {
      reject(
        new Error(
          `Server ${handle.port} did not exit: ${handle.output.slice(-4000)}`,
        ),
      );
    }, timeoutMs);
    handle.child.once('exit', () => {
      clearTimeout(timeout);
      resolve();
    });
  });
}

function killServer(handle: ServerHandle, signal: NodeJS.Signals): void {
  try {
    if (process.platform !== 'win32' && handle.child.pid) {
      process.kill(-handle.child.pid, signal);
      return;
    }
  } catch {}
  handle.child.kill(signal);
}

async function stopServer(handle: ServerHandle | null): Promise<void> {
  if (
    !handle ||
    handle.child.exitCode !== null ||
    handle.child.signalCode !== null
  ) {
    return;
  }
  await new Promise<void>((resolve) => {
    const timeout = setTimeout(() => {
      if (handle.child.exitCode === null) killServer(handle, 'SIGKILL');
    }, 10_000);
    handle.child.once('exit', () => {
      clearTimeout(timeout);
      resolve();
    });
    killServer(handle, 'SIGTERM');
  });
}

async function waitForMarker(
  handle: ServerHandle,
  marker: string,
  timeoutMs = 90_000,
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (handle.output.includes(marker)) return;
    if (handle.child.exitCode !== null || handle.child.signalCode !== null) {
      throw new Error(
        `Server ${handle.port} exited before "${marker}": ${handle.output.slice(-4000)}`,
      );
    }
    await new Promise((resolve) => setTimeout(resolve, 50));
  }
  throw new Error(
    `Server ${handle.port} timed out waiting for "${marker}": ${handle.output.slice(-4000)}`,
  );
}

async function createDatabase(database: Database, name: string): Promise<void> {
  if (database === 'mongodb') return;
  const admin = sqlClient(database);
  try {
    await admin.raw('CREATE DATABASE ??', [name]);
  } finally {
    await admin.destroy();
  }
}

async function dropDatabase(database: Database, name: string): Promise<void> {
  if (database === 'mongodb') {
    const client = new MongoClient(mongoUri(name));
    try {
      await client.connect();
      await client.db(name).dropDatabase();
    } finally {
      await client.close();
    }
    return;
  }
  const admin = sqlClient(database);
  try {
    if (database === 'postgres') {
      await admin.raw('DROP DATABASE IF EXISTS ?? WITH (FORCE)', [name]);
    } else {
      await admin.raw('DROP DATABASE IF EXISTS ??', [name]);
    }
  } finally {
    await admin.destroy();
  }
}

async function assertInitialized(
  database: Database,
  name: string,
): Promise<void> {
  if (database === 'mongodb') {
    const client = new MongoClient(mongoUri(name));
    try {
      await client.connect();
      const setting = await client
        .db(name)
        .collection('enfyra_setting')
        .findOne({});
      assert.equal(
        setting?.isInit,
        true,
        `${database} did not publish isInit=true`,
      );
      assert.ok(
        (await client.db(name).collection('enfyra_table').countDocuments({})) >
          0,
        `${database} did not provision metadata`,
      );
    } finally {
      await client.close();
    }
    return;
  }
  const db = sqlClient(database, name);
  try {
    const setting = await db('enfyra_setting').first();
    assert.equal(
      setting?.isInit === true || setting?.isInit === 1,
      true,
      `${database} did not publish isInit=true`,
    );
    assert.ok(
      Number((await db('enfyra_table').count({ count: '*' }))[0].count) > 0,
      `${database} did not provision metadata`,
    );
  } finally {
    await db.destroy();
  }
}

async function readInitialized(
  database: Database,
  name: string,
): Promise<boolean> {
  if (database === 'mongodb') {
    const client = new MongoClient(mongoUri(name));
    try {
      await client.connect();
      const setting = await client
        .db(name)
        .collection('enfyra_setting')
        .findOne({});
      return setting?.isInit === true;
    } finally {
      await client.close();
    }
  }
  const db = sqlClient(database, name);
  try {
    const setting = await db('enfyra_setting').first();
    return setting?.isInit === true || setting?.isInit === 1;
  } catch (error: any) {
    if (error?.code === '42P01' || error?.code === 'ER_NO_SUCH_TABLE')
      return false;
    throw error;
  } finally {
    await db.destroy();
  }
}

async function clearNamespace(nodeName: string): Promise<void> {
  const redis = new Redis(redisUri(), { maxRetriesPerRequest: 1 });
  try {
    let cursor = '0';
    do {
      const [next, keys] = await redis.scan(
        cursor,
        'MATCH',
        `${nodeName}:*`,
        'COUNT',
        100,
      );
      if (keys.length > 0) await redis.del(...keys);
      cursor = next;
    } while (cursor !== '0');
  } finally {
    await redis.quit();
  }
}

async function assertLockNamespace(nodeName: string): Promise<void> {
  const redis = new Redis(redisUri(), { maxRetriesPerRequest: 1 });
  const otherNode = `${nodeName}-other`;
  const ownerKey = `${nodeName}:${PROVISION_LOCK_KEY}`;
  const otherKey = `${otherNode}:${PROVISION_LOCK_KEY}`;
  try {
    await redis.del(ownerKey, otherKey);
    assert.equal(await redis.set(ownerKey, 'owner', 'PX', 5_000, 'NX'), 'OK');
    assert.equal(
      await redis.set(ownerKey, 'other-owner', 'PX', 5_000, 'NX'),
      null,
    );
    assert.equal(await redis.set(otherKey, 'other', 'PX', 5_000, 'NX'), 'OK');
  } finally {
    await redis.del(ownerKey, otherKey);
    await redis.quit();
  }
}

async function runCase(database: Database, port: number): Promise<void> {
  const suffix = randomUUID().replaceAll('-', '').slice(0, 12);
  const databaseName = `enfyra_boot_race_${database}_${suffix}`;
  const nodeName = `boot-race-${database}-${suffix}`;
  const secretKey = `boot-race-secret-${suffix}`;
  const adminPassword = `boot-race-password-${suffix}`;
  let owner: ServerHandle | null = null;
  let peer: ServerHandle | null = null;
  try {
    await createDatabase(database, databaseName);
    await clearNamespace(nodeName);
    await assertLockNamespace(nodeName);
    const dbUri =
      database === 'mongodb'
        ? mongoUri(databaseName)
        : databaseUri(database, databaseName);

    const primary = spawnServer(
      dbUri,
      port,
      nodeName,
      secretKey,
      adminPassword,
    );
    const secondary = spawnServer(
      dbUri,
      port + 1,
      nodeName,
      secretKey,
      adminPassword,
    );
    owner = primary;
    peer = secondary;
    await Promise.all([
      waitForMarker(primary, `HTTP listening on port ${port}`),
      waitForMarker(secondary, `HTTP listening on port ${port + 1}`),
    ]);

    const replicas: [ServerHandle, ServerHandle] = [primary, secondary];
    const lockOwners = replicas.filter((replica) =>
      replica.output.includes('acquired init lock'),
    );
    assert.equal(
      lockOwners.length,
      1,
      `${database} expected exactly one bootstrap lock owner`,
    );
    const waiter = replicas.find((replica) => !lockOwners.includes(replica));
    assert.ok(waiter);
    const waiterObservedLiveLock = /another instance is running, waiting/.test(
      waiter.output,
    );
    const waiterSkippedAfterPublish =
      !/\b(?:Installing|Upgrading) \[Planning\] starting/.test(waiter.output);
    assert.equal(
      waiterObservedLiveLock || waiterSkippedAfterPublish,
      true,
      `${database} peer neither waited on the live lock nor skipped after initialization`,
    );
    const bootstrapOwner = lockOwners[0];
    assert.ok(
      bootstrapOwner.output.indexOf('publish initialized version') <
        bootstrapOwner.output.indexOf(
          `HTTP listening on port ${bootstrapOwner.port}`,
        ),
      `${database} published HTTP readiness before bootstrap attestation`,
    );
    await assertInitialized(database, databaseName);

    const redis = new Redis(redisUri(), { maxRetriesPerRequest: 1 });
    try {
      assert.equal(await redis.get(`${nodeName}:${PROVISION_LOCK_KEY}`), null);
    } finally {
      await redis.quit();
    }
    console.log(
      `[boot-race] PASS database=${database} node=${nodeName} owner=${port} peer=${port + 1}`,
    );
  } finally {
    await Promise.all([stopServer(peer), stopServer(owner)]);
    await clearNamespace(nodeName).catch(() => undefined);
    await dropDatabase(database, databaseName).catch((error) => {
      console.error(
        `[boot-race] cleanup failed database=${databaseName}`,
        error,
      );
    });
  }
}

async function runLeaseLossCase(
  database: Database,
  port: number,
): Promise<void> {
  const suffix = randomUUID().replaceAll('-', '').slice(0, 12);
  const databaseName = `enfyra_lease_loss_${database}_${suffix}`;
  const nodeName = `lease-loss-${database}-${suffix}`;
  const secretKey = `lease-loss-secret-${suffix}`;
  const adminPassword = `lease-loss-password-${suffix}`;
  const latchDirectory = await mkdtemp(join(tmpdir(), 'enfyra-lease-loss-'));
  const markerFile = join(latchDirectory, 'publication-blocked');
  const releaseFile = join(latchDirectory, 'release');
  let owner: ServerHandle | null = null;
  let follower: ServerHandle | null = null;
  try {
    await createDatabase(database, databaseName);
    await clearNamespace(nodeName);
    const dbUri =
      database === 'mongodb'
        ? mongoUri(databaseName)
        : databaseUri(database, databaseName);
    owner = spawnServer(dbUri, port, nodeName, secretKey, adminPassword, {
      entrypoint: 'test/e2e/fixtures/bootstrap-lease-loss-server.ts',
      env: {
        BOOTSTRAP_LEASE_TEST_MARKER: markerFile,
        BOOTSTRAP_LEASE_TEST_RELEASE: releaseFile,
      },
    });
    await waitForMarker(owner, '[lease-loss] publication-blocked');

    const redis = new Redis(redisUri(), { maxRetriesPerRequest: 1 });
    try {
      const lockKey = `${nodeName}:${PROVISION_LOCK_KEY}`;
      assert.ok(
        await redis.get(lockKey),
        `${database} owner never held the provision lease`,
      );
      await redis.set(lockKey, `replacement-${suffix}`, 'PX', 30_000);
    } finally {
      await redis.quit();
    }
    await writeFile(releaseFile, 'release');
    await waitForExit(owner);
    assert.notEqual(
      owner.child.exitCode,
      0,
      `${database} stale owner exited successfully`,
    );
    assert.match(owner.output, /lost the provision lease/i);
    assert.equal(
      await readInitialized(database, databaseName),
      false,
      `${database} stale owner published isInit=true`,
    );

    await clearNamespace(nodeName);
    follower = spawnServer(dbUri, port + 1, nodeName, secretKey, adminPassword);
    await waitForMarker(follower, `HTTP listening on port ${port + 1}`);
    await assertInitialized(database, databaseName);
    console.log(
      `[boot-race] PASS lease-loss database=${database} node=${nodeName}`,
    );
  } finally {
    await Promise.all([stopServer(follower), stopServer(owner)]);
    await clearNamespace(nodeName).catch(() => undefined);
    await dropDatabase(database, databaseName).catch((error) => {
      console.error(
        `[boot-race] cleanup failed database=${databaseName}`,
        error,
      );
    });
    await rm(latchDirectory, { recursive: true, force: true });
  }
}

async function runMongoJunctionConflictCase(
  database: Database,
  port: number,
): Promise<void> {
  assert.equal(
    database,
    'mongodb',
    'mongo-junction-conflict mode only supports MongoDB',
  );
  const suffix = randomUUID().replaceAll('-', '').slice(0, 12);
  const databaseName = `enfyra_mongo_junction_conflict_${suffix}`;
  const nodeName = `mongo-junction-conflict-${suffix}`;
  const secretKey = `mongo-junction-conflict-secret-${suffix}`;
  const adminPassword = `mongo-junction-conflict-password-${suffix}`;
  const dbUri = mongoUri(databaseName);
  let server: ServerHandle | null = null;
  const client = new MongoClient(dbUri);
  try {
    await clearNamespace(nodeName);
    server = spawnServer(dbUri, port, nodeName, secretKey, adminPassword);
    await waitForMarker(server, `HTTP listening on port ${port}`);
    await stopServer(server);
    server = null;

    await client.connect();
    const db = client.db(databaseName);
    const routeTable = await db
      .collection('enfyra_table')
      .findOne({ name: 'enfyra_route' });
    const methodTable = await db
      .collection('enfyra_table')
      .findOne({ name: 'enfyra_method' });
    assert.ok(routeTable?._id);
    assert.ok(methodTable?._id);
    const relation = await db.collection('enfyra_relation').findOne({
      sourceTable: routeTable._id,
      targetTable: methodTable._id,
      propertyName: 'availableMethods',
      type: 'many-to-many',
    });
    assert.ok(relation?._id);
    const canonicalCollectionName = String(relation.junctionTableName);
    const canonicalSourceColumn = String(relation.junctionSourceColumn);
    const canonicalTargetColumn = String(relation.junctionTargetColumn);
    const legacyCollectionName = 'enfyra_route_availableMethods_enfyra_method';
    const legacySourceColumn = 'enfyra_routeId';
    const legacyTargetColumn = 'enfyra_methodId';
    assert.notEqual(canonicalCollectionName, legacyCollectionName);
    assert.notEqual(canonicalSourceColumn, legacySourceColumn);

    const canonicalRowsBefore = await db
      .collection(canonicalCollectionName)
      .find({})
      .toArray();
    assert.ok(canonicalRowsBefore.length > 0);
    const oldRouteId = canonicalRowsBefore[0][canonicalSourceColumn];
    const conflictingRouteId = new ObjectId();
    const methodId = canonicalRowsBefore[0][canonicalTargetColumn];
    const conflictingDocument = {
      _id: new ObjectId(),
      [legacySourceColumn]: oldRouteId,
      [canonicalSourceColumn]: conflictingRouteId,
      [legacyTargetColumn]: methodId,
      [canonicalTargetColumn]: methodId,
    };
    await db.createCollection(legacyCollectionName);
    await db.collection(legacyCollectionName).insertOne(conflictingDocument);
    await db.collection('enfyra_relation').updateOne(
      { _id: relation._id },
      {
        $set: {
          junctionTableName: legacyCollectionName,
          junctionSourceColumn: legacySourceColumn,
          junctionTargetColumn: legacyTargetColumn,
        },
      },
    );
    await db
      .collection('enfyra_setting')
      .updateMany({}, { $set: { isInit: false } });
    await clearNamespace(nodeName);

    server = spawnServer(dbUri, port, nodeName, secretKey, adminPassword);
    await waitForExit(server);
    assert.notEqual(server.child.exitCode, 0);
    assert.match(server.output, /unmappable|blocked/i);
    server = null;
    assert.equal(await readInitialized('mongodb', databaseName), false);
    assert.deepEqual(
      await db
        .collection(legacyCollectionName)
        .findOne({ _id: conflictingDocument._id }),
      conflictingDocument,
    );
    assert.deepEqual(
      await db.collection(canonicalCollectionName).find({}).toArray(),
      canonicalRowsBefore,
    );

    await db
      .collection(legacyCollectionName)
      .updateOne(
        { _id: conflictingDocument._id },
        { $set: { [canonicalSourceColumn]: oldRouteId } },
      );
    await clearNamespace(nodeName);
    server = spawnServer(dbUri, port, nodeName, secretKey, adminPassword);
    await waitForMarker(server, `HTTP listening on port ${port}`);
    await assertInitialized('mongodb', databaseName);
    assert.equal(
      (await db.listCollections({ name: legacyCollectionName }).toArray())
        .length,
      0,
    );
    assert.ok(
      await db.collection(canonicalCollectionName).findOne({
        [canonicalSourceColumn]: oldRouteId,
        [canonicalTargetColumn]: methodId,
      }),
    );
    console.log(
      `[boot-race] PASS mongo junction conflict database=${databaseName} node=${nodeName}`,
    );
  } finally {
    await stopServer(server);
    await client.close();
    await clearNamespace(nodeName).catch(() => undefined);
    await dropDatabase('mongodb', databaseName).catch((error) => {
      console.error(
        `[boot-race] cleanup failed database=${databaseName}`,
        error,
      );
    });
  }
}

async function main(): Promise<void> {
  const databases = selectedDatabases();
  const basePort = Number(process.env.BOOT_RACE_BASE_PORT || 18300);
  assert.ok(Number.isInteger(basePort) && basePort > 0);
  const redis = new Redis(redisUri(), { maxRetriesPerRequest: 1 });
  const rawLockSentinel = `boot-race-raw-${randomUUID()}`;
  console.log(
    `[boot-race] start databases=${databases.join(',')} redis=${redisUri()} basePort=${basePort}`,
  );
  let results: PromiseSettledResult<void>[];
  try {
    await redis.set(PROVISION_LOCK_KEY, rawLockSentinel, 'PX', 180_000);
    results = await Promise.allSettled(
      databases.map((database, index) =>
        process.env.BOOT_RACE_MODE === 'lease-loss'
          ? runLeaseLossCase(database, basePort + index * 10)
          : process.env.BOOT_RACE_MODE === 'mongo-junction-conflict'
            ? runMongoJunctionConflictCase(database, basePort + index * 10)
            : runCase(database, basePort + index * 10),
      ),
    );
  } finally {
    await redis.del(PROVISION_LOCK_KEY);
    await redis.quit();
  }
  const failures = results.filter(
    (result): result is PromiseRejectedResult => result.status === 'rejected',
  );
  if (failures.length > 0) {
    const details = failures
      .map((failure) =>
        failure.reason instanceof Error
          ? failure.reason.stack || failure.reason.message
          : String(failure.reason),
      )
      .join('\n---\n');
    throw new Error(`${failures.length} boot race case(s) failed\n${details}`);
  }
  console.log('[boot-race] all selected databases passed');
}

main().catch((error) => {
  console.error(
    error instanceof Error ? error.stack || error.message : String(error),
  );
  process.exitCode = 1;
});
