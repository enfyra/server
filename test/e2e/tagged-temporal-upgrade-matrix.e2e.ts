import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { randomUUID } from 'node:crypto';
import { constants, copyFileSync, existsSync, mkdtempSync, readFileSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { knex } from 'knex';
import { MongoClient } from 'mongodb';
import Redis from 'ioredis';

const root = resolve(import.meta.dirname, '../..');
const sourceRoot = process.env.TAGGED_SERVER_ROOT;
assert.ok(sourceRoot, 'TAGGED_SERVER_ROOT must select an isolated v2.3.3 checkout');
const fixture = 'test/e2e/fixtures/tagged-temporal-upgrade.ts';
if (existsSync(join(sourceRoot, fixture))) assert.deepEqual(readFileSync(join(sourceRoot, fixture)), readFileSync(join(root, fixture)), 'Existing historical fixture differs');
else copyFileSync(join(root, fixture), join(sourceRoot, fixture), constants.COPYFILE_EXCL);
const logs = mkdtempSync(join(tmpdir(), 'enfyra-tagged-upgrade-'));
const backends = (process.env.MATRIX_DATABASES || 'postgres,mysql,mariadb,mongodb').split(',');

async function runStage(backend: string, uri: string, namespace: string, stage: 'source' | 'target' | 'stable') {
  const cwd = stage === 'source' ? sourceRoot! : root;
  const child = spawn('yarn', ['tsx', fixture], {
    cwd, detached: true,
    env: { ...process.env, NODE_ENV: 'test', DB_URI: uri, DB_REPLICA_URIS: '', REDIS_URI: process.env.MATRIX_REDIS_URI!, NODE_NAME: namespace, INSTANCE_ID: namespace, SECRET_KEY: `tagged-upgrade-${namespace}`, ADMIN_EMAIL: '', ADMIN_PASSWORD: '', DOTENV_CONFIG_PATH: '/dev/null', TAG_UPGRADE_STAGE: stage, NODE_OPTIONS: '--no-node-snapshot', REDIS_RUNTIME_CACHE: 'false', SQL_POOL_MIN: '0', SQL_POOL_MAX: '4', STARTUP_VERBOSE: '1', BOOTSTRAP_VERBOSE: '1' },
    stdio: ['ignore', 'pipe', 'pipe'],
  });
  let output = '';
  child.stdout.on('data', (chunk) => { output += chunk; });
  child.stderr.on('data', (chunk) => { output += chunk; });
  const stop = () => { if (child.pid) { try { process.kill(-child.pid, 'SIGKILL'); } catch (error) { if ((error as NodeJS.ErrnoException).code !== 'ESRCH') throw error; } } };
  const timer = setTimeout(stop, 120000);
  try {
    const code = await new Promise<number | null>((resolve, reject) => { child.once('error', reject); child.once('close', resolve); });
    const log = join(logs, `${backend}-${stage}.log`);
    writeFileSync(log, output.replace(/((?:mysql|postgres(?:ql)?|mongodb|redis):\/\/)[^\s"']+/g, '$1[REDACTED]'), { mode: 0o600, flag: 'wx' });
    assert.equal(code, 0, `${backend} ${stage} failed; see ${log}`);
    const resultLine = output.split(/\r?\n/).find((line) => line.startsWith('TAG_UPGRADE_RESULT '));
    assert.ok(resultLine, `${backend} ${stage} returned no result`);
    const result = JSON.parse(resultLine.slice('TAG_UPGRADE_RESULT '.length));
    assert.equal(result.stage, stage);
    assert.equal(result.backend, backend === 'mariadb' ? 'mysql' : backend);
    assert.equal(result.version, stage === 'source' ? '2.3.3' : '2.4.0');
    assert.equal(result.bootstrapRuns, stage === 'stable' ? 0 : 1);
    assert.equal(result.passed, true);
    console.log('PASS', backend, stage, `bootstrapRuns=${result.bootstrapRuns}`);
  } finally { clearTimeout(timer); stop(); }
}

async function main() {
  for (const backend of backends) {
    assert.ok(['postgres', 'mysql', 'mariadb', 'mongodb'].includes(backend));
    const configured = process.env[`MATRIX_${backend.toUpperCase()}_URI`];
    assert.ok(configured);
    const adminUri = new URL(configured);
    assert.ok(['localhost', '127.0.0.1'].includes(adminUri.hostname));
    const database = `enfyra_tag_upgrade_${randomUUID().replaceAll('-', '').slice(0, 24)}`;
    const namespace = `tag-upgrade-${backend}-${randomUUID()}`;
    const uri = new URL(adminUri); uri.pathname = `/${database}`;
    const client = backend === 'mongodb' ? new MongoClient(configured) : null;
    const sql = client ? null : knex({ client: backend === 'postgres' ? 'pg' : 'mysql2', connection: configured, pool: { min: 0, max: 1 } });
    let created = false;
    try {
      if (client) { await client.connect(); await client.db(database).createCollection('fixture_probe'); await client.db(database).collection('fixture_probe').drop(); }
      else await sql!.raw('CREATE DATABASE ??', [database]);
      created = true;
      await runStage(backend, uri.href, namespace, 'source');
      await runStage(backend, uri.href, namespace, 'target');
      await runStage(backend, uri.href, namespace, 'stable');
    } finally {
      try {
        if (created) {
          if (client) await client.db(database).dropDatabase();
          else await sql!.raw(backend === 'postgres' ? 'DROP DATABASE ?? WITH (FORCE)' : 'DROP DATABASE ??', [database]);
        }
      } finally {
        await client?.close(); await sql?.destroy();
        const redis = new Redis(process.env.MATRIX_REDIS_URI!, { maxRetriesPerRequest: 1 });
        try {
          let cursor = '0'; do {
            const page = await redis.scan(cursor, 'MATCH', `${namespace}:*`, 'COUNT', 100); cursor = page[0];
            if (page[1].length) await redis.unlink(...page[1]);
          } while (cursor !== '0');
        } finally { redis.disconnect(); }
      }
    }
  }
  console.log('TAGGED_UPGRADE_MATRIX_RESULT', JSON.stringify({ backends, logs, passed: true }));
}
void main().catch((error) => { console.error(error); process.exitCode = 1; });
