import knex, { type Knex } from 'knex';
import { BootstrapUnitOfWorkService } from '../../src/engines/bootstrap';

const POSTGRES_URI =
  process.env.MATRIX_POSTGRES_URI ??
  process.env.PG_TEST_URI ??
  'postgresql://root:1234@localhost:5432/postgres';

describe('PostgreSQL bootstrap unit of work', () => {
  let db: Knex;
  let admin: Knex;
  let schemaName: string;

  beforeAll(async () => {
    admin = knex({ client: 'pg', connection: POSTGRES_URI });
    await admin.raw('select 1');
    schemaName = `bootstrap_uow_${Date.now()}`;
    await admin.raw('create schema ??', [schemaName]);
    db = knex({
      client: 'pg',
      connection: POSTGRES_URI,
      searchPath: [schemaName, 'public'],
    });
  });

  afterAll(async () => {
    await db?.destroy();
    if (schemaName) {
      await admin.raw('drop schema if exists ?? cascade', [schemaName]);
    }
    await admin?.destroy();
  });

  it('rolls all PostgreSQL DDL and data changes back together', async () => {
    await db.schema.createTable('records', (table) => {
      table.increments('id').primary();
      table.string('value').notNullable();
      table.string('legacy');
    });
    const [record] = await db('records')
      .insert({ value: 'before', legacy: 'keep' })
      .returning('id');
    let activeConnection: Knex | Knex.Transaction = db;
    const knexService = {
      transaction: (callback: (trx: Knex.Transaction) => Promise<unknown>) =>
        db.transaction(async (trx) => {
          activeConnection = trx;
          try {
            return await callback(trx);
          } finally {
            activeConnection = db;
          }
        }),
    };
    const service = new BootstrapUnitOfWorkService({
      databaseConfigService: {
        isMongoDb: () => false,
        getDbType: () => 'postgres',
      },
      knexService,
      mongoService: {},
      mySqlBootstrapSnapshotService: {},
    } as any);

    await expect(
      service.run(async () => {
        await activeConnection('records')
          .where({ id: record.id })
          .update({ value: 'after' });
        await activeConnection.schema.alterTable('records', (table) => {
          table.dropColumn('legacy');
          table.string('nextVersionOnly');
        });
        await activeConnection.schema.createTable(
          'new_version_table',
          (table) => {
            table.increments('id').primary();
          },
        );
        throw new Error('fail after PostgreSQL DDL');
      }),
    ).rejects.toThrow('fail after PostgreSQL DDL');

    expect(await db.schema.hasTable('new_version_table')).toBe(false);
    expect(await db.schema.hasColumn('records', 'legacy')).toBe(true);
    expect(await db.schema.hasColumn('records', 'nextVersionOnly')).toBe(false);
    await expect(
      db('records').where({ id: record.id }).first(),
    ).resolves.toMatchObject({ value: 'before', legacy: 'keep' });
  }, 120000);
});
