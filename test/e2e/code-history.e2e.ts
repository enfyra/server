import assert from 'node:assert/strict';
import { execFileSync } from 'node:child_process';
import { randomUUID } from 'node:crypto';
import { spawn, type ChildProcess } from 'node:child_process';
import { existsSync, readFileSync, symlinkSync } from 'node:fs';
import { join } from 'node:path';
import { Redis } from 'ioredis';
import { MongoClient } from 'mongodb';
import mysql from 'mysql2/promise';
import pg from 'pg';

type Backend = 'postgres' | 'mysql' | 'mongodb';

const REDIS_DB = process.env.CODE_HISTORY_E2E_REDIS_DB || '15';
const BASE_VERSION_WORKTREE =
  process.env.CODE_HISTORY_E2E_BASE_WORKTREE || '/tmp/enfyra-server-baseline';
const BASE_VERSION_REF =
  process.env.CODE_HISTORY_E2E_BASE_REF || 'v2.3.1-patch-1';
let createdBaselineWorktree = false;

function ensureBaselineWorktree(): void {
  if (existsSync(join(BASE_VERSION_WORKTREE, 'src/main.ts'))) return;
  execFileSync(
    'git',
    ['worktree', 'add', '--detach', BASE_VERSION_WORKTREE, BASE_VERSION_REF],
    { stdio: 'inherit' },
  );
  symlinkSync(
    join(process.cwd(), 'node_modules'),
    join(BASE_VERSION_WORKTREE, 'node_modules'),
  );
  createdBaselineWorktree = true;
}

function removeBaselineWorktree(): void {
  if (!createdBaselineWorktree) return;
  try {
    execFileSync(
      'git',
      ['worktree', 'remove', '--force', BASE_VERSION_WORKTREE],
      { stdio: 'ignore' },
    );
  } catch {}
  createdBaselineWorktree = false;
}
const PORTS: Record<Backend, [number, number]> = {
  postgres: [4311, 4312],
  mysql: [4313, 4314],
  mongodb: [4315, 4316],
};

type Booted = { child: ChildProcess; baseUrl: string };

function env(name: string): string {
  const value = process.env[name];
  if (!value) throw new Error(`${name} is required`);
  return value;
}

function sqlAdminUri(backend: 'postgres' | 'mysql'): string {
  const prefix = backend === 'postgres' ? 'POSTGRES' : 'MYSQL';
  const fallback =
    backend === 'postgres' && process.env.DB_URI
      ? new URL(process.env.DB_URI)
      : undefined;
  const url = new URL(
    `${backend === 'postgres' ? 'postgresql' : 'mysql'}://${process.env[`MATRIX_${prefix}_HOST`] || fallback?.hostname || '127.0.0.1'}`,
  );
  url.port =
    process.env[`MATRIX_${prefix}_PORT`] ||
    fallback?.port ||
    (backend === 'postgres' ? '5432' : '3306');
  url.username =
    process.env[`MATRIX_${prefix}_USER`] ||
    fallback?.username ||
    env(`MATRIX_${prefix}_USER`);
  url.password =
    process.env[`MATRIX_${prefix}_PASSWORD`] ||
    fallback?.password ||
    env(`MATRIX_${prefix}_PASSWORD`);
  url.pathname = '/';
  return url.toString();
}

function mongoClientUri(database?: string): string {
  const url = new URL(
    `mongodb://${process.env.MATRIX_MONGO_HOST || '127.0.0.1'}`,
  );
  url.port = process.env.MATRIX_MONGO_PORT || '27017';
  url.username = env('MATRIX_MONGO_USER');
  url.password = env('MATRIX_MONGO_PASSWORD');
  url.pathname = `/${database || ''}`;
  url.searchParams.set(
    'authSource',
    process.env.MATRIX_MONGO_AUTH_DATABASE || 'admin',
  );
  return url.toString();
}

function databaseUri(backend: Backend, name: string): string {
  if (backend === 'mongodb') return mongoClientUri(name);
  const url = new URL(sqlAdminUri(backend));
  url.pathname = `/${name}`;
  return url.toString();
}

function redisUri(): string {
  const url = new URL(process.env.MATRIX_REDIS_URI || 'redis://127.0.0.1:6379');
  url.pathname = `/${REDIS_DB}`;
  return url.toString();
}

async function createDatabase(backend: Backend, name: string): Promise<void> {
  if (backend === 'postgres') {
    const url = new URL(sqlAdminUri('postgres'));
    url.pathname = '/postgres';
    const client = new pg.Client({ connectionString: url.toString() });
    await client.connect();
    try {
      await client.query(`create database "${name}"`);
    } finally {
      await client.end();
    }
    return;
  }
  if (backend === 'mysql') {
    const connection = await mysql.createConnection(sqlAdminUri('mysql'));
    try {
      await connection.query(`create database \`${name}\``);
    } finally {
      await connection.end();
    }
    return;
  }
  const client = new MongoClient(mongoClientUri());
  await client.connect();
  try {
    await client.db(name).dropDatabase();
  } finally {
    await client.close();
  }
}

async function dropDatabase(backend: Backend, name: string): Promise<void> {
  if (backend === 'postgres') {
    const url = new URL(sqlAdminUri('postgres'));
    url.pathname = '/postgres';
    const client = new pg.Client({ connectionString: url.toString() });
    await client.connect();
    try {
      await client.query(`drop database if exists "${name}" with (force)`);
    } finally {
      await client.end();
    }
    return;
  }
  if (backend === 'mysql') {
    const connection = await mysql.createConnection(sqlAdminUri('mysql'));
    try {
      await connection.query(`drop database if exists \`${name}\``);
    } finally {
      await connection.end();
    }
    return;
  }
  const client = new MongoClient(mongoClientUri());
  await client.connect();
  try {
    await client.db(name).dropDatabase();
  } finally {
    await client.close();
  }
}

function killServer(child: ChildProcess, signal: NodeJS.Signals): void {
  try {
    if (process.platform !== 'win32' && child.pid) {
      process.kill(-child.pid, signal);
      return;
    }
  } catch {}
  child.kill(signal);
}

async function stopServer(child: ChildProcess): Promise<void> {
  if (child.exitCode !== null || child.signalCode !== null) return;
  await new Promise<void>((resolve) => {
    const timer = setTimeout(() => {
      if (child.exitCode === null) killServer(child, 'SIGKILL');
    }, 10_000);
    child.once('exit', () => {
      clearTimeout(timer);
      resolve();
    });
    killServer(child, 'SIGTERM');
  });
}

async function boot(
  cwd: string,
  backend: Backend,
  database: string,
  port: number,
  nodeName: string,
  adminPassword: string,
): Promise<Booted> {
  const child = spawn(join(cwd, 'node_modules/.bin/tsx'), ['src/main.ts'], {
    cwd,
    detached: process.platform !== 'win32',
    env: {
      ...process.env,
      ADMIN_EMAIL: 'code-history-e2e@localhost.test',
      ADMIN_PASSWORD: adminPassword,
      BOOTSTRAP_VERBOSE: '0',
      DB_URI: databaseUri(backend, database),
      MONGO_FORCE_APP_TRANSACTION: '0',
      NODE_ENV: 'test',
      NODE_NAME: nodeName,
      PORT: String(port),
      REDIS_URI: redisUri(),
      SECRET_KEY: randomUUID(),
      STARTUP_VERBOSE: '1',
    },
    stdio: ['ignore', 'pipe', 'pipe'],
  });
  let output = '';
  child.stdout?.on('data', (chunk: Buffer) => {
    output += chunk.toString();
  });
  child.stderr?.on('data', (chunk: Buffer) => {
    output += chunk.toString();
  });
  const baseUrl = `http://127.0.0.1:${port}`;
  const deadline = Date.now() + 180_000;
  while (Date.now() < deadline) {
    if (child.exitCode !== null) {
      throw new Error(
        `${backend} server exited during boot:\n${output.slice(-6000)}`,
      );
    }
    try {
      const response = await fetch(`${baseUrl}/health/ready`);
      if (response.status === 200) return { child, baseUrl };
    } catch {}
    await new Promise((resolve) => setTimeout(resolve, 500));
  }
  killServer(child, 'SIGKILL');
  throw new Error(
    `${backend} server boot timed out on port ${port}:\n${output.slice(-6000)}`,
  );
}

async function login(baseUrl: string, adminPassword: string): Promise<string> {
  const response = await fetch(`${baseUrl}/auth/login`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      email: 'code-history-e2e@localhost.test',
      password: adminPassword,
    }),
  });
  const body: any = await response.json();
  assert.equal(response.status, 200, `login failed: ${JSON.stringify(body)}`);
  assert.ok(body.accessToken, 'login returned no access token');
  return body.accessToken;
}

function api(baseUrl: string, token: string) {
  return async (path: string, init: RequestInit = {}) => {
    const response = await fetch(`${baseUrl}${path}`, {
      ...init,
      headers: {
        'Content-Type': 'application/json',
        Authorization: `Bearer ${token}`,
        ...(init.headers || {}),
      },
    });
    const text = await response.text();
    const body = text ? JSON.parse(text) : null;
    return { status: response.status, body };
  };
}

function rows(body: any): any[] {
  if (!body) return [];
  if (Array.isArray(body.data)) return body.data;
  if (body.data && typeof body.data === 'object') return [body.data];
  return [];
}

function firstRow(body: any): any {
  return rows(body)[0];
}

function idOf(record: any): string {
  return String(record?.id ?? record?._id ?? '');
}

function pkOf(backend: Backend): string {
  return backend === 'mongodb' ? '_id' : 'id';
}

function filterQuery(
  filter: Record<string, unknown>,
  fields: string,
  extra: Record<string, string> = {},
) {
  const query = new URLSearchParams({
    filter: JSON.stringify(filter),
    fields,
    ...extra,
  });
  return `?${query.toString()}`;
}

async function assertCodeHistoryProvisioned(
  call: ReturnType<typeof api>,
  label: string,
) {
  const history = await call('/enfyra_audit?limit=1');
  assert.equal(
    history.status,
    200,
    `${label}: history route unavailable: ${JSON.stringify(history.body)}`,
  );

  const menu = await call(
    `/enfyra_menu${filterQuery({ path: { _eq: '/settings/audit' } }, 'id,path,isEnabled,isPublic,isSystem,permission,parent.label')}`,
  );
  const menuRow = firstRow(menu.body);
  assert.ok(menuRow, `${label}: Audit menu missing`);
  assert.equal(
    menuRow.isPublic,
    false,
    `${label}: Audit menu must stay private`,
  );
  assert.equal(menuRow.isEnabled, true, `${label}: Audit menu disabled`);
  assert.equal(
    menuRow.isSystem,
    true,
    `${label}: Audit menu must be system-owned`,
  );
  assert.deepEqual(
    menuRow.permission,
    { and: [{ route: '/enfyra_audit', methods: ['GET'] }] },
    `${label}: Audit menu visibility condition changed`,
  );
  assert.equal(
    menuRow.parent?.label,
    'Settings',
    `${label}: Audit menu parent changed`,
  );

  const logsMenu = await call(
    `/enfyra_menu${filterQuery({ path: { _eq: '/settings/logs' } }, 'id,path,parent.label')}`,
  );
  assert.ok(
    firstRow(logsMenu.body),
    `${label}: Server Logs menu missing at /settings/logs`,
  );
  const nestedMenus = await call(
    `/enfyra_menu${filterQuery({ path: { _contains: '/settings/admin' } }, 'id,path', { limit: '10' })}`,
  );
  assert.deepEqual(
    rows(nestedMenus.body),
    [],
    `${label}: Settings menus must not use /settings/admin paths`,
  );

  const route = await call(
    `/enfyra_route${filterQuery({ path: { _eq: '/enfyra_audit' } }, 'path,isEnabled,methodConfigs.method.name,methodConfigs.available')}`,
  );
  const routeRow = firstRow(route.body);
  assert.ok(routeRow, `${label}: history route metadata missing`);
  assert.equal(routeRow.isEnabled, true, `${label}: history route disabled`);
  const methods = (routeRow.methodConfigs || [])
    .filter((config: any) => config.available === true)
    .map((config: any) => config.method?.name);
  assert.deepEqual(
    methods,
    ['GET', 'DELETE'],
    `${label}: history route must expose GET and DELETE`,
  );

  const revert = await call(
    `/enfyra_route${filterQuery({ path: { _eq: '/admin/code-audit/revert' } }, 'path,isEnabled,methodConfigs.method.name,methodConfigs.available')}`,
  );
  const revertRow = firstRow(revert.body);
  assert.ok(revertRow, `${label}: revert route metadata missing`);
  assert.deepEqual(
    (revertRow.methodConfigs || [])
      .filter((config: any) => config.available === true)
      .map((config: any) => config.method?.name),
    ['POST'],
  );
}

async function assertVersionAndHistoryFlow(
  call: ReturnType<typeof api>,
  backend: Backend,
  label: string,
) {
  const name = `e2e_history_${randomUUID().replace(/-/g, '').slice(0, 12)}`;
  const created = await call('/enfyra_helper', {
    method: 'POST',
    body: JSON.stringify({
      name,
      description: 'code history e2e',
      scriptLanguage: 'typescript',
      sourceCode: 'return 1;',
    }),
  });
  assert.equal(
    created.status,
    200,
    `${label}: helper create failed: ${JSON.stringify(created.body)}`,
  );
  const record = firstRow(created.body);
  const recordId = idOf(record);
  assert.ok(recordId, `${label}: create returned no record id`);
  assert.equal(
    record.version,
    '1.0.0',
    `${label}: create must default to 1.0.0`,
  );

  const missing = await call(`/enfyra_helper/${recordId}`, {
    method: 'PATCH',
    body: JSON.stringify({ sourceCode: 'return 2;' }),
  });
  assert.equal(
    missing.status,
    400,
    `${label}: update without a version must be rejected`,
  );

  const equal = await call(`/enfyra_helper/${recordId}`, {
    method: 'PATCH',
    body: JSON.stringify({ sourceCode: 'return 2;', version: '1.0.0' }),
  });
  assert.equal(
    equal.status,
    400,
    `${label}: non-advancing version must be rejected`,
  );

  const updated = await call(`/enfyra_helper/${recordId}`, {
    method: 'PATCH',
    body: JSON.stringify({ sourceCode: 'return 2;', version: '1.0.1' }),
  });
  assert.equal(
    updated.status,
    200,
    `${label}: versioned update failed: ${JSON.stringify(updated.body)}`,
  );

  const read = await call(
    `/enfyra_helper${filterQuery({ [pkOf(backend)]: { _eq: recordId } }, `${pkOf(backend)},name,version,sourceCode`, { limit: '1' })}`,
  );
  assert.equal(
    firstRow(read.body)?.version,
    '1.0.1',
    `${label}: saved version not persisted`,
  );

  const history = await call(
    `/enfyra_audit${filterQuery({ tableName: { _eq: 'enfyra_helper' }, recordId: { _eq: recordId } }, 'id,action,fromVersion,toVersion,previous', { sort: '-id', limit: '20' })}`,
  );
  assert.equal(
    history.status,
    200,
    `${label}: history read failed: ${JSON.stringify(history.body)}`,
  );
  const entries = rows(history.body);
  const actions = entries.map((entry) => entry.action);
  assert.ok(actions.includes('create'), `${label}: create audit entry missing`);
  const updateEntry = entries.find((entry) => entry.action === 'update');
  assert.ok(updateEntry, `${label}: update audit entry missing`);
  assert.equal(updateEntry.fromVersion, '1.0.0');
  assert.equal(updateEntry.toVersion, '1.0.1');
  assert.equal(
    updateEntry.previous?.sourceCode,
    'return 1;',
    `${label}: pre-image did not store the previous source`,
  );

  const revert = await call('/admin/code-audit/revert', {
    method: 'POST',
    body: JSON.stringify({ auditId: idOf(updateEntry), version: '1.0.2' }),
  });
  assert.equal(
    revert.status,
    200,
    `${label}: revert failed: ${JSON.stringify(revert.body)}`,
  );

  const restored = await call(
    `/enfyra_helper${filterQuery({ [pkOf(backend)]: { _eq: recordId } }, 'version,sourceCode', { limit: '1' })}`,
  );
  const restoredRow = firstRow(restored.body);
  assert.equal(
    restoredRow.version,
    '1.0.2',
    `${label}: revert must create a new version`,
  );
  assert.equal(
    restoredRow.sourceCode,
    'return 1;',
    `${label}: revert must restore the pre-image source`,
  );

  const afterRevert = await call(
    `/enfyra_audit${filterQuery({ tableName: { _eq: 'enfyra_helper' }, recordId: { _eq: recordId } }, 'id,action', { sort: '-id', limit: '1' })}`,
  );
  assert.equal(
    firstRow(afterRevert.body)?.action,
    'revert',
    `${label}: revert audit entry missing`,
  );

  const removed = await call(`/enfyra_helper/${recordId}`, {
    method: 'DELETE',
  });
  assert.equal(
    removed.status,
    200,
    `${label}: delete failed: ${JSON.stringify(removed.body)}`,
  );
  const deleteEntry = firstRow(
    (
      await call(
        `/enfyra_audit${filterQuery({ tableName: { _eq: 'enfyra_helper' }, recordId: { _eq: recordId } }, 'id,action', { sort: '-id', limit: '1' })}`,
      )
    ).body,
  );
  assert.equal(
    deleteEntry?.action,
    'delete',
    `${label}: delete audit entry missing`,
  );

  const recreate = await call('/admin/code-audit/revert', {
    method: 'POST',
    body: JSON.stringify({ auditId: idOf(deleteEntry), version: '1.0.3' }),
  });
  assert.equal(
    recreate.status,
    400,
    `${label}: deleted records must not be restorable`,
  );
}

async function flushRedis() {
  const redis = new Redis(redisUri(), { maxRetriesPerRequest: 1 });
  try {
    await redis.flushdb();
  } finally {
    await redis.quit();
  }
}

async function runBackend(backend: Backend, runId: string) {
  const freshDatabase = `enfyra_code_history_fresh_${runId}`;
  const upgradeDatabase = `enfyra_code_history_upgrade_${runId}`;
  const adminPassword = `E2e-${randomUUID()}`;
  const [freshPort, upgradePort] = PORTS[backend];
  const booted: ChildProcess[] = [];
  try {
    await flushRedis();
    await createDatabase(backend, freshDatabase);
    const fresh = await boot(
      process.cwd(),
      backend,
      freshDatabase,
      freshPort,
      `ch-fresh-${runId}`,
      adminPassword,
    );
    booted.push(fresh.child);
    const freshCall = api(
      fresh.baseUrl,
      await login(fresh.baseUrl, adminPassword),
    );
    await assertCodeHistoryProvisioned(freshCall, `${backend} fresh install`);
    await assertVersionAndHistoryFlow(
      freshCall,
      backend,
      `${backend} fresh install`,
    );
    await stopServer(fresh.child);
    console.log(
      `PASS ${backend} fresh install: schema, routes, menu, versioning, audit, revert, delete protection`,
    );

    ensureBaselineWorktree();
    const baselineVersion = JSON.parse(
      readFileSync(join(BASE_VERSION_WORKTREE, 'package.json'), 'utf8'),
    ).version;
    const runningVersion = JSON.parse(
      readFileSync(join(process.cwd(), 'package.json'), 'utf8'),
    ).version;
    assert.notEqual(
      baselineVersion,
      runningVersion,
      'baseline worktree must predate the running version',
    );
    await createDatabase(backend, upgradeDatabase);
    const base = await boot(
      BASE_VERSION_WORKTREE,
      backend,
      upgradeDatabase,
      freshPort,
      `ch-base-${runId}`,
      adminPassword,
    );
    booted.push(base.child);
    const baseCall = api(
      base.baseUrl,
      await login(base.baseUrl, adminPassword),
    );
    const baseHistory = await baseCall('/enfyra_audit?limit=1');
    assert.equal(
      baseHistory.status,
      404,
      `${backend} baseline build must not expose code history, got ${baseHistory.status}`,
    );
    await stopServer(base.child);
    await flushRedis();

    const upgraded = await boot(
      process.cwd(),
      backend,
      upgradeDatabase,
      upgradePort,
      `ch-upgrade-${runId}`,
      adminPassword,
    );
    booted.push(upgraded.child);
    const upgradedCall = api(
      upgraded.baseUrl,
      await login(upgraded.baseUrl, adminPassword),
    );
    await assertCodeHistoryProvisioned(upgradedCall, `${backend} upgrade`);
    await assertVersionAndHistoryFlow(
      upgradedCall,
      backend,
      `${backend} upgrade`,
    );
    const setting = await upgradedCall(
      '/enfyra_setting?fields=enfyraVersion,isInit&limit=1',
    );
    assert.equal(
      firstRow(setting.body)?.enfyraVersion,
      runningVersion,
      `${backend} upgrade must record the running version`,
    );
    await stopServer(upgraded.child);
    console.log(
      `PASS ${backend} upgrade from ${baselineVersion}: additive schema, route, menu, versioning, audit, revert`,
    );
  } finally {
    for (const child of booted) await stopServer(child);
    await dropDatabase(backend, freshDatabase);
    await dropDatabase(backend, upgradeDatabase);
    await flushRedis();
  }
}

async function main() {
  const runId = `${Date.now().toString(36)}${randomUUID().replace(/-/g, '').slice(0, 6)}`;
  const requested = (
    process.env.CODE_HISTORY_E2E_DATABASES || 'postgres,mysql,mongodb'
  )
    .split(',')
    .map((value) => value.trim())
    .filter(Boolean) as Backend[];
  for (const backend of requested) {
    await runBackend(
      backend,
      `${backend}${runId}`.replace(/[^a-z0-9]/g, '').slice(0, 20),
    );
  }
  removeBaselineWorktree();
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
