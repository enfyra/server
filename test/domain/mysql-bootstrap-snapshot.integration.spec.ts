import knex, { type Knex } from 'knex';
import { MySqlBootstrapSnapshotService } from '../../src/engines/bootstrap';

const MYSQL_URI =
  process.env.MATRIX_MYSQL_URI ??
  process.env.MYSQL_TEST_URI ??
  'mysql://root:1234@localhost:3306/mysql';

describe('MySQL bootstrap snapshot compensation', () => {
  let admin: Knex;
  let db: Knex;
  let databaseName: string;

  beforeAll(async () => {
    admin = knex({ client: 'mysql2', connection: MYSQL_URI });
    await admin.raw('select 1');
    databaseName = `bootstrap_uow_${Date.now()}`;
    await admin.raw('create database ??', [databaseName]);
    const url = new URL(MYSQL_URI);
    url.pathname = `/${databaseName}`;
    db = knex({ client: 'mysql2', connection: url.toString() });
  });

  afterAll(async () => {
    await db?.destroy();
    if (databaseName) {
      await admin.raw('drop database if exists ??', [databaseName]);
    }
    await admin?.destroy();
  });

  it('restores schema, data, indexes, and foreign keys after bootstrap failure', async () => {
    await db.schema.createTable('parents', (table) => {
      table.increments('id').primary();
      table.string('name').notNullable().index('parents_name_idx');
      table.string('legacy').nullable();
    });
    await db.schema.createTable('children', (table) => {
      table.increments('id').primary();
      table
        .integer('parentId')
        .unsigned()
        .notNullable()
        .references('id')
        .inTable('parents')
        .onDelete('CASCADE');
      table.string('value').notNullable();
    });
    const [parentId] = await db('parents').insert({
      name: 'before',
      legacy: 'keep',
    });
    await db('children').insert({ parentId, value: 'child-before' });

    const service = new MySqlBootstrapSnapshotService({
      knexService: { getKnex: () => db },
    } as any);

    await expect(
      service.run(async () => {
        await db('parents').where({ id: parentId }).update({ name: 'after' });
        await db.schema.alterTable('parents', (table) => {
          table.dropColumn('legacy');
          table.string('nextVersionOnly');
        });
        await db.schema.dropTable('children');
        await db.schema.createTable('new_version_table', (table) => {
          table.increments('id').primary();
        });
        throw new Error('fail after MySQL DDL');
      }),
    ).rejects.toThrow('fail after MySQL DDL');

    expect(await db.schema.hasTable('children')).toBe(true);
    expect(await db.schema.hasTable('new_version_table')).toBe(false);
    expect(await db.schema.hasColumn('parents', 'legacy')).toBe(true);
    expect(await db.schema.hasColumn('parents', 'nextVersionOnly')).toBe(false);
    await expect(
      db('parents').where({ id: parentId }).first(),
    ).resolves.toMatchObject({ name: 'before', legacy: 'keep' });
    await expect(
      db('children').where({ parentId }).first(),
    ).resolves.toMatchObject({ value: 'child-before' });
    const indexes = await db.raw('SHOW INDEX FROM ??', ['parents']);
    expect(indexes[0].map((row: any) => row.Key_name)).toContain(
      'parents_name_idx',
    );
    const foreignKeys = await db.raw(
      `SELECT REFERENCED_TABLE_NAME AS referencedTable
       FROM information_schema.KEY_COLUMN_USAGE
       WHERE TABLE_SCHEMA = DATABASE()
         AND TABLE_NAME = 'children'
         AND COLUMN_NAME = 'parentId'`,
    );
    expect(foreignKeys[0]).toContainEqual(
      expect.objectContaining({ referencedTable: 'parents' }),
    );
  }, 120000);

  it('discards an incomplete planning snapshot without restoring partial state', async () => {
    const service = new MySqlBootstrapSnapshotService({
      knexService: { getKnex: () => db },
    } as any);
    await service.recoverPending();

    const txId = 'bootstrap-incomplete-planning';
    const backupTableName = 'system_bootstrap_backup_0_incomplete';
    await db('system_bootstrap_transactions').insert({
      txId,
      status: 'planning',
      mutationId: 'runtime-schema:incomplete-planning',
      errorMessage: null,
      createdAt: new Date(),
      updatedAt: new Date(),
    });
    await db.schema.createTable(backupTableName, (table) => {
      table.integer('id');
    });
    await db('system_bootstrap_snapshots').insert({
      txId,
      tableName: 'parents',
      backupTableName,
      createSql: 'CREATE TABLE `parents` (`id` int)',
      columnsJson: '["id"]',
      ordinal: 0,
    });

    const recovery = await service.recoverPending();

    expect(recovery.rolledBackMutationIds).toContain(
      'runtime-schema:incomplete-planning',
    );

    expect(await db.schema.hasTable('parents')).toBe(true);
    expect(await db.schema.hasTable('children')).toBe(true);
    expect(await db.schema.hasTable(backupTableName)).toBe(false);
    await expect(
      db('system_bootstrap_transactions').where({ txId }).first(),
    ).resolves.toMatchObject({ status: 'rolled_back' });
  }, 120000);
});
