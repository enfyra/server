import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { createHash, randomUUID } from 'node:crypto';
import { writeFileSync } from 'node:fs';
import { resolve } from 'node:path';
import { knex, type Knex } from 'knex';
import { MongoClient } from 'mongodb';
import Redis from 'ioredis';

const root = resolve(import.meta.dirname, '../..');
const LARGE_JSON = JSON.stringify({ payload: 'x'.repeat(70 * 1024) });

type BootResult = { bootstrapRuns: number; version: string };

async function boot(uri: string, namespace: string, expected: number, backend: string, label: string): Promise<BootResult> {
  const result = await new Promise<{ code: number | null; output: string }>((fulfill, reject) => {
    const child = spawn('yarn', ['tsx', 'test/e2e/fixtures/storage-contract-boot.ts'], {
      cwd: root, detached: true, env: {
        ...process.env, DOTENV_CONFIG_PATH: '/dev/null', DB_URI: uri,
        REDIS_URI: process.env.MATRIX_REDIS_URI || 'redis://127.0.0.1:6379/13', NODE_NAME: namespace,
        NODE_ENV: 'test', NODE_OPTIONS: '--no-node-snapshot', ADMIN_EMAIL: 'storage-contract@localhost.test',
        ADMIN_PASSWORD: randomUUID(), SECRET_KEY: 'storage-contract-test-secret',
        STARTUP_VERBOSE: '1', BOOTSTRAP_VERBOSE: '0', REDIS_RUNTIME_CACHE: 'false',
        MONGO_FORCE_APP_TRANSACTION: '0', EXPECTED_BOOTSTRAP_RUNS: String(expected),
      }, stdio: ['ignore', 'pipe', 'pipe'],
    });
    const stop = () => { if (child.pid) { try { process.kill(-child.pid, 'SIGKILL'); } catch {} } };
    const timer = setTimeout(stop, 120000);
    let output = '';
    child.stdout.on('data', (chunk) => { output += chunk; });
    child.stderr.on('data', (chunk) => { output += chunk; });
    child.on('error', (error) => { clearTimeout(timer); stop(); reject(error); });
    child.on('close', (code) => { clearTimeout(timer); stop(); fulfill({ code, output }); });
  });
  const log = `/tmp/enfyra-storage-${backend}-${label}-${randomUUID()}.log`;
  writeFileSync(log, result.output, { flag: 'wx', mode: 0o600 });
  assert.equal(result.code, 0, `${backend} ${label} failed; see ${log}: ${result.output.slice(-1500)}`);
  const marker = result.output.match(/STORAGE_BOOT_RESULT (\{[^\n]+\})/);
  assert.ok(marker, `${backend} ${label} missing boot result`);
  return JSON.parse(marker[1]);
}

async function jsonColumns(client: Knex, backend: 'postgres' | 'mysql') {
  const rows = backend === 'postgres'
    ? (await client.raw(`SELECT c.table_name AS "tableName", c.column_name AS "columnName", c.data_type AS "dataType" FROM information_schema.columns c JOIN enfyra_column col ON col.name = c.column_name JOIN enfyra_table tbl ON tbl.id = col."tableId" AND tbl.name = c.table_name WHERE c.table_schema = current_schema() AND col.type = 'simple-json'`)).rows
    : (await client.raw(`SELECT c.TABLE_NAME AS tableName, c.COLUMN_NAME AS columnName, c.DATA_TYPE AS dataType FROM information_schema.COLUMNS c JOIN enfyra_column col ON col.name = c.COLUMN_NAME JOIN enfyra_table tbl ON tbl.id = col.tableId AND tbl.name = c.TABLE_NAME WHERE c.TABLE_SCHEMA = DATABASE() AND col.type = 'simple-json'`))[0];
  return rows as Array<{ tableName: string; columnName: string; dataType: string }>;
}

const fingerprint = (value: unknown) => createHash('sha256').update(JSON.stringify(value)).digest('hex');

function parseConnection(uri: string) {
  const parsed = new URL(uri);
  return {
    host: parsed.hostname,
    port: Number(parsed.port || (parsed.protocol === 'mysql:' ? 3306 : 5432)),
    user: decodeURIComponent(parsed.username),
    password: decodeURIComponent(parsed.password),
  };
}

async function main() {
  for (const backend of (process.env.MATRIX_DATABASES || 'postgres,mysql,mongodb').split(',') as Array<'postgres' | 'mysql' | 'mongodb'>) {
    assert.ok(['postgres', 'mysql', 'mongodb'].includes(backend));
    const database = `enfyra_storage_contract_${randomUUID().replaceAll('-', '').slice(0, 12)}`;
    const namespace = `storage-contract-${randomUUID()}`;
    const configured = process.env[`MATRIX_${backend.toUpperCase()}_URI`];
    assert.ok(configured, `MATRIX_${backend.toUpperCase()}_URI is required`);
    const uri = new URL(configured);
    uri.pathname = `/${database}`;
    const mongo = backend === 'mongodb' ? new MongoClient(configured) : null;
    const sql = backend === 'mongodb' ? null : knex({ client: backend === 'postgres' ? 'pg' : 'mysql2', connection: { ...parseConnection(configured), database }, pool: { min: 0, max: 1 }, acquireConnectionTimeout: 5000 });
    const admin = backend === 'mongodb' ? null : knex({ client: backend === 'postgres' ? 'pg' : 'mysql2', connection: configured, pool: { min: 0, max: 1 }, acquireConnectionTimeout: 5000 });
    let created = false;
    try {
      if (mongo) await mongo.connect();
      else await admin!.raw('CREATE DATABASE ??', [database]);
      created = true;
      const targetType = backend === 'mysql' ? 'longtext' : 'text';

      const fresh = await boot(uri.href, namespace, 1, backend, 'fresh');
      const stable = await boot(uri.href, namespace, 0, backend, 'stable');
      assert.equal(stable.bootstrapRuns, 0);

      if (sql) {
        const columns = await jsonColumns(sql, backend);
        assert.ok(columns.length > 0, `${backend}: no metadata-declared simple-json columns`);
        for (const column of columns) {
          assert.equal(String(column.dataType).toLowerCase(), targetType, `${backend}: ${column.tableName}.${column.columnName} is ${column.dataType}`);
        }

        const trx = await sql.transaction();
        try {
          const row = await trx('enfyra_column').select('id', 'options').whereNotNull('options').first();
          if (row) {
            await trx('enfyra_column').where({ id: row.id }).update({ options: LARGE_JSON });
            assert.equal((await trx('enfyra_column').where({ id: row.id }).first('options')).options, LARGE_JSON);
          }
        } finally { await trx.rollback(); }

        const fingerprints = new Map<string, Array<{ id: unknown; value: string }>>();
        for (const column of columns) {
          if (column.columnName === 'metadata' || column.columnName === 'options') continue;
          const rows = await sql(column.tableName).select('id', column.columnName).orderBy('id');
          fingerprints.set(`${column.tableName}.${column.columnName}`, rows.map((entry) => ({ id: entry.id, value: fingerprint(entry[column.columnName]) })));
        }

        for (const column of columns) {
          if (backend === 'postgres') {
            await sql.raw('ALTER TABLE ?? ALTER COLUMN ?? TYPE VARCHAR USING ??::text::varchar', [column.tableName, column.columnName, column.columnName]);
          } else {
            await sql.raw('ALTER TABLE ?? MODIFY COLUMN ?? TEXT NULL', [column.tableName, column.columnName]);
          }
        }
        const legacy = await jsonColumns(sql, backend);
        assert.equal(legacy.length, columns.length);
        assert.ok(legacy.every((column) => column.dataType.toLowerCase() !== targetType));

        await sql('enfyra_setting').update({ enfyraVersion: '2.3.2', isInit: true });
        const upgrade = await boot(uri.href, namespace, 1, backend, 'legacy-upgrade');
        assert.equal(upgrade.bootstrapRuns, 1);
        const migrated = await jsonColumns(sql, backend);
        for (const column of migrated) {
          assert.equal(String(column.dataType).toLowerCase(), targetType, `${backend}: ${column.tableName}.${column.columnName} stayed ${column.dataType}`);
        }
        for (const [key, before] of fingerprints) {
          const [tableName, columnName] = key.split('.');
          const rows = await sql(tableName).select('id', columnName).whereIn('id', before.map((entry) => entry.id));
          const retained = new Set(rows.map((entry) => entry.id));
          for (const entry of before.filter((item) => retained.has(item.id))) {
            assert.equal(fingerprint(rows.find((row) => row.id === entry.id)?.[columnName]), entry.value, `${backend}: ${key} changed for id ${entry.id}`);
          }
        }
        await boot(uri.href, namespace, 0, backend, 'post-upgrade-stable');

        // A database can also hold native JSON columns: Enfyra has one JSON
        // storage shape per SQL backend, so healing must converge them.
        const userTable = 'storage_json_user_table';
        await sql.raw(`CREATE TABLE ?? (id INTEGER PRIMARY KEY, payload ${backend === 'postgres' ? 'json' : 'JSON'} NULL)`, [userTable]);
        await sql(userTable).insert({ id: 1, payload: JSON.stringify({ kept: 'yes' }) });
        await sql('enfyra_table').insert({
          name: userTable, isSystem: false, isSingleRecord: false,
          uniques: '[]', indexes: '[]', metadata: '{}', validateBody: true,
        });
        const userTableRow = await sql('enfyra_table').where({ name: userTable }).first('id');
        await sql('enfyra_column').insert([
          { tableId: userTableRow.id, name: 'id', type: 'int', isPrimary: true, isNullable: false },
          { tableId: userTableRow.id, name: 'payload', type: 'simple-json', isNullable: true },
        ]);

        const nativeJsonColumns = [
          ['enfyra_table', 'uniques'],
          ['enfyra_table', 'indexes'],
          ['enfyra_table', 'metadata'],
          ['enfyra_relation', 'metadata'],
        ];
        for (const [tableName, columnName] of nativeJsonColumns) {
          const values = await sql(tableName).select(columnName).whereNotNull(columnName);
          const allJson = values.every((row) => {
            try { JSON.parse(String(row[columnName])); return true; } catch { return false; }
          });
          if (!allJson) continue;
          if (backend === 'postgres') {
            await sql.raw('ALTER TABLE ?? ALTER COLUMN ?? TYPE json USING ??::json', [tableName, columnName, columnName]);
          } else {
            await sql.raw('ALTER TABLE ?? MODIFY COLUMN ?? JSON NULL', [tableName, columnName]);
          }
        }
        await sql('enfyra_setting').update({ enfyraVersion: '2.3.2', isInit: true });
        await boot(uri.href, namespace, 1, backend, 'native-json-heal');

        const healed = await jsonColumns(sql, backend);
        for (const column of healed) {
          assert.equal(String(column.dataType).toLowerCase(), targetType, `${backend}: ${column.tableName}.${column.columnName} stayed ${column.dataType}`);
        }
        assert.deepEqual(JSON.parse(String((await sql(userTable).where({ id: 1 }).first('payload')).payload)), { kept: 'yes' });
      }

      console.log(JSON.stringify({ backend, freshInstall: fresh.bootstrapRuns, stableBoot: stable.bootstrapRuns, legacyUpgrade: sql ? 1 : 'n/a', jsonStorage: sql ? targetType : 'native-json', passed: true }));
    } finally {
      if (created) {
        if (mongo) await mongo.db(database).dropDatabase();
        else await admin!.raw(backend === 'postgres' ? 'DROP DATABASE ?? WITH (FORCE)' : 'DROP DATABASE ??', [database]);
      }
      await mongo?.close();
      await sql?.destroy();
      await admin?.destroy();
      const redis = new Redis(process.env.MATRIX_REDIS_URI || 'redis://127.0.0.1:6379/13', { maxRetriesPerRequest: 1 });
      try {
        let cursor = '0';
        do {
          const page = await redis.scan(cursor, 'MATCH', `${namespace}:*`, 'COUNT', 500);
          cursor = page[0];
          if (page[1].length) await redis.del(...page[1]);
        } while (cursor !== '0');
      } finally { await redis.quit(); }
      console.log(JSON.stringify({ backend, cleaned: database }));
    }
  }
}
main().catch((error) => { console.error(error); process.exitCode = 1; });
