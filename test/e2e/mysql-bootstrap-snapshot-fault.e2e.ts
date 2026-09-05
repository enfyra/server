import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { spawn, type ChildProcess } from 'node:child_process';
import Redis from 'ioredis';
import knex, { type Knex } from 'knex';
import { MySqlBootstrapSnapshotService } from '../../src/engines/bootstrap';

const FAULT_ENTRYPOINT =
  'test/e2e/fixtures/mysql-bootstrap-snapshot-fault-server.ts';

type ServerHandle = {
  child: ChildProcess;
  output: string;
};

type CaseContext = {
  databaseName: string;
  dbUri: string;
  markerDirectory: string;
  nodeName: string;
  port: number;
  target: Knex;
  spawnServer: (
    entrypoint?: string,
    extraEnvironment?: NodeJS.ProcessEnv,
  ) => ServerHandle;
};

function required(name: string): string {
  const value = process.env[name];
  if (!value) throw new Error(`${name} is required`);
  return value;
}

function mysqlUri(databaseName?: string): string {
  const uri = new URL(required('MATRIX_MYSQL_URI'));
  if (databaseName) uri.pathname = `/${databaseName}`;
  return uri.toString();
}

function createKnex(databaseName?: string): Knex {
  return knex({ client: 'mysql2', connection: mysqlUri(databaseName) });
}

function redisUri(): string {
  const uri = new URL(process.env.MATRIX_REDIS_URI || 'redis://127.0.0.1:6379');
  uri.pathname = `/${process.env.MATRIX_REDIS_DB || '14'}`;
  return uri.toString();
}

function createServerHandle(
  dbUri: string,
  port: number,
  nodeName: string,
  entrypoint = 'src/main.ts',
  extraEnvironment: NodeJS.ProcessEnv = {},
): ServerHandle {
  const handle: ServerHandle = {
    child: spawn('yarn', ['tsx', entrypoint], {
      cwd: process.cwd(),
      detached: process.platform !== 'win32',
      env: {
        ...process.env,
        DB_URI: dbUri,
        REDIS_URI: redisUri(),
        PORT: String(port),
        SECRET_KEY: `mysql-snapshot-e2e-${nodeName}`,
        ADMIN_EMAIL: 'mysql-snapshot-e2e@localhost.test',
        ADMIN_PASSWORD: `password-${nodeName}`,
        NODE_ENV: 'test',
        NODE_NAME: nodeName,
        BOOTSTRAP_VERBOSE: '0',
        ...extraEnvironment,
      },
      stdio: ['ignore', 'pipe', 'pipe'],
    }),
    output: '',
  };
  const onData = (chunk: Buffer) => {
    handle.output = (handle.output + chunk.toString()).slice(-80_000);
  };
  handle.child.stdout?.on('data', onData);
  handle.child.stderr?.on('data', onData);
  return handle;
}

function killServer(handle: ServerHandle, signal: NodeJS.Signals): void {
  if (handle.child.exitCode !== null || !handle.child.pid) return;
  try {
    if (process.platform !== 'win32') {
      process.kill(-handle.child.pid, signal);
    } else {
      handle.child.kill(signal);
    }
  } catch {}
}

async function waitForMarker(
  handle: ServerHandle,
  marker: string,
  timeoutMs = 120_000,
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (handle.output.includes(marker)) return;
    if (handle.child.exitCode !== null) {
      throw new Error(
        `Server exited before marker '${marker}' with code ${handle.child.exitCode}: ${handle.output.slice(-4000)}`,
      );
    }
    await new Promise((resolve) => setTimeout(resolve, 20));
  }
  killServer(handle, 'SIGKILL');
  throw new Error(
    `Server did not reach marker '${marker}': ${handle.output.slice(-4000)}`,
  );
}

async function waitForExit(
  handle: ServerHandle,
  timeoutMs = 120_000,
): Promise<void> {
  if (handle.child.exitCode !== null) return;
  await new Promise<void>((resolve, reject) => {
    const timeout = setTimeout(() => {
      killServer(handle, 'SIGKILL');
      reject(new Error(`Server did not exit: ${handle.output.slice(-4000)}`));
    }, timeoutMs);
    handle.child.once('exit', () => {
      clearTimeout(timeout);
      resolve();
    });
  });
}

async function stopServer(handle: ServerHandle): Promise<void> {
  if (handle.child.exitCode !== null) return;
  killServer(handle, 'SIGTERM');
  try {
    await waitForExit(handle, 15_000);
  } catch {
    killServer(handle, 'SIGKILL');
    await waitForExit(handle, 5_000).catch(() => undefined);
  }
}

async function clearNamespace(nodeName: string): Promise<void> {
  const redis = new Redis(redisUri(), { maxRetriesPerRequest: 1 });
  try {
    let cursor = '0';
    do {
      const [nextCursor, keys] = await redis.scan(
        cursor,
        'MATCH',
        `${nodeName}:*`,
        'COUNT',
        100,
      );
      if (keys.length > 0) await redis.del(...keys);
      cursor = nextCursor;
    } while (cursor !== '0');
  } finally {
    await redis.quit();
  }
}

async function assertInitialized(
  target: Knex,
  expected: boolean,
): Promise<void> {
  const setting = await target('enfyra_setting').first('isInit');
  assert.equal(setting.isInit === true || setting.isInit === 1, expected);
}

async function countBackups(target: Knex): Promise<number> {
  const result = await target.raw(
    `SELECT COUNT(*) AS count FROM information_schema.TABLES
     WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME LIKE 'system_bootstrap_backup_%'`,
  );
  return Number(result[0][0].count);
}

async function withInitializedDatabase(
  label: string,
  port: number,
  callback: (context: CaseContext) => Promise<void>,
): Promise<void> {
  const suffix = randomUUID().replaceAll('-', '').slice(0, 12);
  const databaseName = `enfyra_mysql_snapshot_${label}_${suffix}`;
  const nodeName = `mysql-snapshot-${label}-${suffix}`;
  const dbUri = mysqlUri(databaseName);
  const admin = createKnex();
  const markerDirectory = await mkdtemp(
    join(tmpdir(), 'enfyra-mysql-snapshot-'),
  );
  const handles = new Set<ServerHandle>();
  let target: Knex | null = null;
  try {
    await admin.raw('CREATE DATABASE ??', [databaseName]);
    target = createKnex(databaseName);
    await clearNamespace(nodeName);
    const initial = createServerHandle(dbUri, port, nodeName);
    handles.add(initial);
    await waitForMarker(initial, `HTTP listening on port ${port}`);
    await stopServer(initial);
    handles.delete(initial);
    await assertInitialized(target, true);

    await callback({
      databaseName,
      dbUri,
      markerDirectory,
      nodeName,
      port,
      target,
      spawnServer: (entrypoint, extraEnvironment) => {
        const handle = createServerHandle(
          dbUri,
          port,
          nodeName,
          entrypoint,
          extraEnvironment,
        );
        handles.add(handle);
        return handle;
      },
    });
  } finally {
    await Promise.all([...handles].map((handle) => stopServer(handle)));
    await target?.destroy();
    await clearNamespace(nodeName).catch(() => undefined);
    await admin.raw('DROP DATABASE IF EXISTS ??', [databaseName]);
    await admin.destroy();
    await rm(markerDirectory, { recursive: true, force: true });
  }
}

async function runPlanningCrashCase(port: number): Promise<void> {
  await withInitializedDatabase('planning', port, async (context) => {
    const { target } = context;
    await target.schema.createTable('e2e_m1_alpha', (table) => {
      table.increments('id').primary();
      table.string('value').notNullable();
    });
    await target.schema.createTable('e2e_m1_beta', (table) => {
      table.increments('id').primary();
      table.string('value').notNullable();
    });
    await target('e2e_m1_alpha').insert({ value: 'alpha-before' });
    await target('e2e_m1_beta').insert({ value: 'beta-before' });
    await target('enfyra_setting').update({ isInit: false });
    const markerFile = join(context.markerDirectory, 'planning');
    const fault = context.spawnServer(FAULT_ENTRYPOINT, {
      MYSQL_SNAPSHOT_FAULT_MODE: 'planning-crash',
      MYSQL_SNAPSHOT_FAULT_MARKER: markerFile,
    });
    await waitForMarker(
      fault,
      '[mysql-snapshot-fault] planning-backup-captured',
    );
    killServer(fault, 'SIGKILL');
    await waitForExit(fault);

    const transaction = await target('system_bootstrap_transactions')
      .where({ status: 'planning' })
      .orderBy('createdAt', 'desc')
      .first();
    assert.ok(transaction);
    const snapshotCount = Number(
      (
        await target('system_bootstrap_snapshots')
          .where({ txId: transaction.txId })
          .count({ count: '*' })
      )[0].count,
    );
    assert.ok(snapshotCount > 0);
    assert.ok(snapshotCount < Number(transaction.snapshotTableCount));
    assert.equal((await target('e2e_m1_alpha').first()).value, 'alpha-before');
    assert.equal((await target('e2e_m1_beta').first()).value, 'beta-before');

    await clearNamespace(context.nodeName);
    const resumed = context.spawnServer();
    await waitForMarker(resumed, `HTTP listening on port ${port}`);
    await stopServer(resumed);
    await assertInitialized(target, true);
    assert.equal((await target('e2e_m1_alpha').first()).value, 'alpha-before');
    assert.equal((await target('e2e_m1_beta').first()).value, 'beta-before');
    assert.equal(
      Number(
        (
          await target('system_bootstrap_snapshots')
            .where({ txId: transaction.txId })
            .count({ count: '*' })
        )[0].count,
      ),
      0,
    );
    assert.equal(
      (
        await target('system_bootstrap_transactions')
          .where({ txId: transaction.txId })
          .first('status')
      ).status,
      'rolled_back',
    );
    console.log('[mysql-snapshot-e2e] PASS M1 planning crash recovery');
  });
}

async function runCommittedCleanupCrashCase(port: number): Promise<void> {
  await withInitializedDatabase('committed', port, async (context) => {
    const { target } = context;
    await target.schema.createTable('e2e_m2_committed', (table) => {
      table.increments('id').primary();
      table.string('value').notNullable();
    });
    await target('e2e_m2_committed').insert({ value: 'committed-state' });
    await target('enfyra_setting').update({ isInit: false });
    const markerFile = join(context.markerDirectory, 'committed');
    const fault = context.spawnServer(FAULT_ENTRYPOINT, {
      MYSQL_SNAPSHOT_FAULT_MODE: 'committed-cleanup-crash',
      MYSQL_SNAPSHOT_FAULT_MARKER: markerFile,
    });
    await waitForMarker(
      fault,
      '[mysql-snapshot-fault] committed-cleanup-partial',
    );
    killServer(fault, 'SIGKILL');
    await waitForExit(fault);

    const transaction = await target('system_bootstrap_transactions')
      .where({ status: 'committed' })
      .orderBy('createdAt', 'desc')
      .first();
    assert.ok(transaction);
    await assertInitialized(target, true);
    assert.equal(
      (await target('e2e_m2_committed').first()).value,
      'committed-state',
    );
    assert.ok(await countBackups(target));

    await clearNamespace(context.nodeName);
    const resumed = context.spawnServer();
    await waitForMarker(resumed, `HTTP listening on port ${port}`);
    await stopServer(resumed);
    await assertInitialized(target, true);
    assert.equal(
      (await target('e2e_m2_committed').first()).value,
      'committed-state',
    );
    assert.equal(
      Number(
        (
          await target('system_bootstrap_snapshots')
            .where({ txId: transaction.txId })
            .count({ count: '*' })
        )[0].count,
      ),
      0,
    );
    assert.equal(await countBackups(target), 0);
    console.log(
      '[mysql-snapshot-e2e] PASS M2 committed cleanup crash recovery',
    );
  });
}

async function runMissingBackupBootCase(port: number): Promise<void> {
  await withInitializedDatabase('missing', port, async (context) => {
    const { target } = context;
    await target.schema.createTable('e2e_m3_live', (table) => {
      table.increments('id').primary();
      table.string('value').notNullable();
    });
    await target('e2e_m3_live').insert({ value: 'live-before' });
    const service = new MySqlBootstrapSnapshotService({
      knexService: { getKnex: () => target },
    } as any);
    (service as any).restore = async () => undefined;
    await assert.rejects(
      service.run(async () => {
        throw new Error('leave recovery snapshot');
      }),
      /leave recovery snapshot/,
    );
    const transaction = await target('system_bootstrap_transactions')
      .where({ status: 'rolling_back' })
      .orderBy('createdAt', 'desc')
      .first();
    const entries = await target('system_bootstrap_snapshots')
      .where({ txId: transaction.txId })
      .orderBy('ordinal', 'asc');
    assert.ok(entries.length > 1);
    await target.schema.dropTable(entries[0].backupTableName);

    const failedBoot = context.spawnServer();
    await waitForExit(failedBoot);
    assert.notEqual(failedBoot.child.exitCode, 0);
    assert.match(failedBoot.output, /missing backup/i);
    assert.equal((await target('e2e_m3_live').first()).value, 'live-before');
    await assertInitialized(target, true);

    for (const entry of entries) {
      await target.schema.dropTableIfExists(entry.backupTableName);
    }
    await target('system_bootstrap_snapshots')
      .where({ txId: transaction.txId })
      .delete();
    await target('system_bootstrap_transactions')
      .where({ txId: transaction.txId })
      .delete();
    await clearNamespace(context.nodeName);
    const resumed = context.spawnServer();
    await waitForMarker(resumed, `HTTP listening on port ${port}`);
    await stopServer(resumed);
    assert.equal((await target('e2e_m3_live').first()).value, 'live-before');
    console.log('[mysql-snapshot-e2e] PASS M3 full-boot fail-closed recovery');
  });
}

async function runExpressionRestoreCase(port: number): Promise<void> {
  await withInitializedDatabase('expression', port, async (context) => {
    const { target } = context;
    await target.raw(
      `CREATE TABLE e2e_expression_defaults (
        id INT AUTO_INCREMENT PRIMARY KEY,
        amount INT NOT NULL,
        scalarDefault INT NOT NULL DEFAULT (ABS(-7)),
        createdAt TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
        updatedAt TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
        storedTotal INT GENERATED ALWAYS AS (amount * 2) STORED,
        virtualTotal INT GENERATED ALWAYS AS (amount * 3) VIRTUAL
      )`,
    );
    await target('e2e_expression_defaults').insert({
      amount: 11,
      scalarDefault: 19,
      createdAt: '2001-02-03 04:05:06.123456',
      updatedAt: '2002-03-04 05:06:07.234567',
    });
    const before = await target('e2e_expression_defaults').first();
    await target('enfyra_setting').update({ isInit: false });
    const markerFile = join(context.markerDirectory, 'expression');
    const fault = context.spawnServer(FAULT_ENTRYPOINT, {
      MYSQL_SNAPSHOT_FAULT_MODE: 'expression-mutation-failure',
      MYSQL_SNAPSHOT_FAULT_MARKER: markerFile,
    });
    await waitForExit(fault);
    assert.notEqual(fault.child.exitCode, 0);
    assert.match(fault.output, /injected expression mutation failure/);
    assert.match(fault.output, /\[mysql-snapshot-fault] expression-mutated/);
    const after = await target('e2e_expression_defaults').first();
    assert.deepEqual(
      {
        amount: after.amount,
        scalarDefault: after.scalarDefault,
        createdAt: new Date(after.createdAt).toISOString(),
        updatedAt: new Date(after.updatedAt).toISOString(),
        storedTotal: after.storedTotal,
        virtualTotal: after.virtualTotal,
      },
      {
        amount: 11,
        scalarDefault: 19,
        createdAt: new Date(before.createdAt).toISOString(),
        updatedAt: new Date(before.updatedAt).toISOString(),
        storedTotal: 22,
        virtualTotal: 33,
      },
    );
    await assertInitialized(target, false);

    await clearNamespace(context.nodeName);
    const resumed = context.spawnServer();
    await waitForMarker(resumed, `HTTP listening on port ${port}`);
    await stopServer(resumed);
    await assertInitialized(target, true);
    console.log('[mysql-snapshot-e2e] PASS M4 full-boot expression restore');
  });
}

async function main(): Promise<void> {
  const basePort = Number(process.env.MYSQL_SNAPSHOT_E2E_BASE_PORT || 18410);
  assert.ok(Number.isInteger(basePort) && basePort > 0);
  console.log(
    `[mysql-snapshot-e2e] start redis=${redisUri()} basePort=${basePort}`,
  );
  await runPlanningCrashCase(basePort);
  await runCommittedCleanupCrashCase(basePort + 1);
  await runMissingBackupBootCase(basePort + 2);
  await runExpressionRestoreCase(basePort + 3);
  console.log('[mysql-snapshot-e2e] all cases passed');
}

main().catch((error) => {
  console.error(
    error instanceof Error ? error.stack || error.message : String(error),
  );
  process.exitCode = 1;
});
