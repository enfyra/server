import knex from 'knex';
import { MongoClient } from 'mongodb';
import { randomUUID } from 'node:crypto';
import { afterEach, describe, expect, it, vi } from 'vitest';
import dataMigration from '../../src/data/data-migration';
import { DataMigrationService } from '../../src/engines/bootstrap/services/data-migration.service';
import { DatabaseConfigService } from '../../src/shared/services';

const paths = ['/logs', '/logs/stats', '/logs/:filename', '/logs/:filename/tail'];
const deletions = dataMigration._deletedRecords.filter((record) => record.table === 'enfyra_route' && paths.includes((record.filter.path as any)?._eq));

afterEach(() => vi.restoreAllMocks());

async function verifyDeletion(queryBuilderService: any, remaining: () => Promise<string[]>) {
  const service = new DataMigrationService({ queryBuilderService, bootstrapDefinitionService: { getDataMigration: () => ({ _deletedRecords: deletions }) } as any });
  expect(deletions).toHaveLength(4);
  await expect(service.assertTargetState()).rejects.toThrow(/still contains .* deleted record/);
  await (service as any).deleteRecords(deletions);
  await service.assertTargetState();
  await (service as any).deleteRecords(deletions);
  await service.assertTargetState();
  expect(await remaining()).toEqual(['/logs-custom']);
}

describe('retired log route deletion and target attestation on real databases', () => {
  for (const backend of ['postgres', 'mysql']) {
    it(`deletes only exact routes and supports retry on ${backend}`, async () => {
      vi.spyOn(DatabaseConfigService, 'instanceIsMongoDb').mockReturnValue(false);
      const connection = backend === 'postgres' ? process.env.PG_TEST_URI : process.env.MYSQL_TEST_URI;
      if (!connection) throw new Error('Local database test URI required');
      const client = backend === 'postgres' ? 'pg' : 'mysql2';
      const scope = 'migration_delete_test_' + randomUUID().replaceAll('-', '');
      const admin = knex({ client, connection });
      await admin.raw(backend === 'postgres' ? 'create schema ??' : 'create database ??', [scope]);
      const url = new URL(connection);
      if (backend === 'mysql') url.pathname = '/' + scope;
      const db = knex({ client, connection: url.toString(), ...(backend === 'postgres' ? { searchPath: [scope] } : {}) });
      try {
        await db.schema.createTable('enfyra_route', (table) => { table.increments('id'); table.string('path'); table.integer('mainTableId').nullable(); });
        await db.schema.createTable('enfyra_table', (table) => { table.increments('id'); table.string('name'); });
        await db('enfyra_route').insert([...paths, '/logs-custom'].map((path) => ({ path })));
        await verifyDeletion({ getKnex: () => db }, async () => (await db('enfyra_route').select('path')).map((row) => row.path));
      } finally {
        await db.destroy();
        await admin.raw(backend === 'postgres' ? 'drop schema ?? cascade' : 'drop database ??', [scope]);
        await admin.destroy();
      }
    }, 30_000);
  }

  it('deletes only exact routes and supports retry on MongoDB', async () => {
    vi.spyOn(DatabaseConfigService, 'instanceIsMongoDb').mockReturnValue(true);
    if (!process.env.MONGO_TEST_URI) throw new Error('Local MongoDB test URI required');
    const client = await new MongoClient(process.env.MONGO_TEST_URI).connect();
    const db = client.db('migration_delete_test_' + randomUUID().replaceAll('-', ''));
    try {
      await db.collection('enfyra_route').insertMany([...paths, '/logs-custom'].map((path) => ({ path })));
      await verifyDeletion({ getMongoDb: () => db }, async () => (await db.collection('enfyra_route').find().toArray()).map((row) => row.path));
    } finally { await db.dropDatabase(); await client.close(); }
  }, 30_000);
});
