import knex from 'knex';
import { MongoClient } from 'mongodb';
import { randomUUID } from 'node:crypto';
import { describe, expect, it } from 'vitest';
import snapshot from '../../src/data/snapshot';
import { parseSnapshotToSchema } from '../../src/engines/knex/utils/provision/schema-parser';
import { createTable } from '../../src/engines/knex/utils/provision/table-builder';
import { RuntimeLogWriterService } from '../../src/modules/admin';
import { acknowledgeRuntimeLog, peekRuntimeLogs, recordSystemError, recordUserLog } from '../../src/shared/runtime-log-buffer';

const names = ['enfyra_system_error', 'enfyra_user_log'];
const tables = Object.fromEntries(names.map((name) => [name, snapshot[name]]));
function reset() { for (const item of peekRuntimeLogs(3000)) acknowledgeRuntimeLog(item.record.eventId); }

describe('runtime logs on real databases', () => {
  for (const backend of ['postgres', 'mysql']) {
    it(`provisions both tables and persists outside rollback on ${backend}`, async () => {
      reset();
      const connection = backend === 'postgres' ? process.env.PG_TEST_URI : process.env.MYSQL_TEST_URI;
      if (!connection) throw new Error('Set PG_TEST_URI and MYSQL_TEST_URI to isolated local test servers');
      const client = backend === 'postgres' ? 'pg' : 'mysql2';
      const scope = 'runtime_logs_test_' + randomUUID().replaceAll('-', '');
      const admin = knex({ client, connection });
      await admin.raw(backend === 'postgres' ? 'create schema ??' : 'create database ??', [scope]);
      const url = new URL(connection);
      if (backend === 'mysql') url.pathname = '/' + scope;
      const db = knex({ client, connection: url.toString(), ...(backend === 'postgres' ? { searchPath: [scope] } : {}) });
      try {
        const schemas = parseSnapshotToSchema(tables);
        for (const schema of schemas) await createTable(db, schema, backend, schemas);
        const writer = new RuntimeLogWriterService({ databaseConfigService: { isMongoDb: () => false }, knexService: { getUnscopedWriteKnex: () => db }, mongoService: {}, instanceService: {} } as any);
        await writer.assertReady();
        await expect(db.transaction(async () => { recordSystemError('rollback evidence', { correlationId: 'req_rollback' }); recordUserLog(['before rollback'], { correlationId: 'req_rollback' }); await writer.flush(); throw new Error('rollback'); })).rejects.toThrow('rollback');
        expect(peekRuntimeLogs()).toHaveLength(0);
        const errors = await db(names[0]).select('*');
        const logs = await db(names[1]).select('*');
        expect(errors).toHaveLength(1); expect(logs).toHaveLength(1);
        expect(JSON.parse(logs[0].entries)).toEqual(['before rollback']);
        await db(names[0]).insert({ ...errors[0], id: undefined }).onConflict('eventId').ignore();
        expect(await db(names[0]).select('eventId')).toHaveLength(1);
        await db(names[0]).update({ occurredAt: new Date(Date.now() - 31 * 86400_000) });
        (writer as any).lastCleanup = 0;
        await writer.flush();
        expect(await db(names[0]).select('eventId')).toHaveLength(0);
      } finally {
        await db.destroy();
        await admin.raw(backend === 'postgres' ? 'drop schema ?? cascade' : 'drop database ??', [scope]);
        await admin.destroy(); reset();
      }
    }, 30_000);
  }

  it('persists and expires Mongo records with unique event IDs', async () => {
    reset();
    if (!process.env.MONGO_TEST_URI) throw new Error('Set MONGO_TEST_URI to a local test server');
    const client = await new MongoClient(process.env.MONGO_TEST_URI).connect();
    const db = client.db('runtime_logs_test_' + randomUUID().replaceAll('-', ''));
    try {
      for (const name of names) { await db.createCollection(name); await db.collection(name).createIndex({ eventId: 1 }, { unique: true }); }
      const writer = new RuntimeLogWriterService({ databaseConfigService: { isMongoDb: () => true }, mongoService: { getRawDb: () => db }, knexService: {}, instanceService: {} } as any);
      await writer.assertReady(); recordSystemError('mongo evidence'); recordUserLog(['one']);
      await writer.flush(); expect(peekRuntimeLogs()).toHaveLength(0);
      expect(await db.collection(names[0]).countDocuments()).toBe(1);
      expect((await db.collection(names[1]).findOne())?.entries).toEqual(['one']);
      await db.collection(names[1]).updateMany({}, { $set: { occurredAt: new Date(Date.now() - 31 * 86400_000) } });
      (writer as any).lastCleanup = 0; await writer.flush();
      expect(await db.collection(names[1]).countDocuments()).toBe(0);
    } finally { await db.dropDatabase(); await client.close(); reset(); }
  }, 30_000);
});
