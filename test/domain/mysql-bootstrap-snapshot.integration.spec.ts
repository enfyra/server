import knex, { type Knex } from 'knex';
import { MySqlBootstrapSnapshotService } from '../../src/engines/bootstrap';
import { RuntimeSchemaUnitOfWorkService } from '../../src/modules/table-management/services/runtime-schema-unit-of-work.service';

const MYSQL_URI =
  process.env.MATRIX_MYSQL_URI ??
  process.env.MYSQL_TEST_URI ??
  'mysql://root:1234@localhost:3306/mysql';

describe('MySQL bootstrap snapshot compensation', () => {
  let admin: Knex;
  let db: Knex;
  let databaseName: string;

  async function leavePendingSnapshot() {
    const service = new MySqlBootstrapSnapshotService({
      knexService: { getKnex: () => db },
    } as any);
    (service as any).restore = async () => undefined;
    await expect(
      service.run(async () => {
        throw new Error('leave captured snapshot for recovery');
      }),
    ).rejects.toThrow('leave captured snapshot for recovery');
    const transaction = await db('system_bootstrap_transactions')
      .where({ status: 'rolling_back' })
      .orderBy('createdAt', 'desc')
      .first();
    const entries = await db('system_bootstrap_snapshots')
      .where({ txId: transaction.txId })
      .orderBy('ordinal', 'asc');
    return { transaction, entries };
  }

  async function cleanupPendingSnapshot(txId: string, entries: any[]) {
    for (const entry of entries) {
      await db.schema.dropTableIfExists(entry.backupTableName);
    }
    await db('system_bootstrap_snapshots').where({ txId }).delete();
    await db('system_bootstrap_transactions').where({ txId }).delete();
  }

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

  it('does not restore live tables when a real backup copy fails during planning', async () => {
    const service = new MySqlBootstrapSnapshotService({
      knexService: {
        getKnex: () =>
          new Proxy(db, {
            apply: (target, _thisArg, args) =>
              Reflect.apply(target, target, args),
            get: (target, property) => {
              if (property === 'raw') {
                return (sql: string, bindings?: readonly Knex.RawBinding[]) => {
                  if (
                    sql.startsWith('INSERT INTO ??') &&
                    ++backupCopies === 2
                  ) {
                    throw new Error('injected real backup copy failure');
                  }
                  return target.raw(sql, bindings as Knex.RawBinding[]);
                };
              }
              const value = Reflect.get(target, property, target);
              return typeof value === 'function' ? value.bind(target) : value;
            },
          }),
      },
    } as any);
    let backupCopies = 0;

    let mutated = false;
    await expect(
      service.run(async () => {
        mutated = true;
      }),
    ).rejects.toThrow('injected real backup copy failure');

    expect(mutated).toBe(false);
    expect(await db.schema.hasTable('parents')).toBe(true);
    expect(await db.schema.hasTable('children')).toBe(true);
    const backups = await db.raw(
      `SELECT TABLE_NAME AS tableName FROM information_schema.TABLES
       WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME LIKE 'system_bootstrap_backup_%'`,
    );
    expect(backups[0]).toEqual([]);
  }, 120000);

  it('keeps committed state when cleanup fails and completes cleanup on recovery', async () => {
    const service = new MySqlBootstrapSnapshotService({
      knexService: { getKnex: () => db },
    } as any);
    const boundary = service as any;
    const originalCleanup = boundary.cleanup.bind(service);
    let faultArmed = false;
    boundary.cleanup = async (_knex: Knex, txId: string) => {
      if (!faultArmed) return originalCleanup(_knex, txId);
      const entries = await db('system_bootstrap_snapshots')
        .where({ txId })
        .orderBy('ordinal', 'asc');
      expect(entries.length).toBeGreaterThan(1);
      await db.schema.dropTableIfExists(entries[0].backupTableName);
      throw new Error('injected committed cleanup failure');
    };

    await expect(
      service.run(async () => {
        faultArmed = true;
        await db('parents').where({ id: 1 }).update({ name: 'committed-B' });
      }),
    ).rejects.toThrow('injected committed cleanup failure');
    await expect(db('parents').where({ id: 1 }).first()).resolves.toMatchObject(
      {
        name: 'committed-B',
      },
    );

    const recoveryService = new MySqlBootstrapSnapshotService({
      knexService: { getKnex: () => db },
    } as any);
    await recoveryService.recoverPending();
    await expect(db('parents').where({ id: 1 }).first()).resolves.toMatchObject(
      {
        name: 'committed-B',
      },
    );
  }, 120000);

  it('restores expression-default values while recomputing generated columns', async () => {
    await db.raw(
      `CREATE TABLE expression_defaults (
        id INT AUTO_INCREMENT PRIMARY KEY,
        amount INT NOT NULL,
        scalarDefault INT NOT NULL DEFAULT (ABS(-7)),
        createdAt TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
        updatedAt TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
        storedTotal INT GENERATED ALWAYS AS (amount * 2) STORED,
        virtualTotal INT GENERATED ALWAYS AS (amount * 3) VIRTUAL
      )`,
    );
    await db('expression_defaults').insert({
      amount: 11,
      scalarDefault: 19,
      createdAt: '2001-02-03 04:05:06.123456',
      updatedAt: '2002-03-04 05:06:07.234567',
    });
    const before = await db('expression_defaults').first();
    const service = new MySqlBootstrapSnapshotService({
      knexService: { getKnex: () => db },
    } as any);

    await expect(
      service.run(async () => {
        await db('expression_defaults').where({ id: before.id }).update({
          amount: 50,
          scalarDefault: 7,
          createdAt: '2010-01-01 00:00:00.000000',
        });
        throw new Error('restore expression defaults');
      }),
    ).rejects.toThrow('restore expression defaults');

    const after = await db('expression_defaults')
      .where({ id: before.id })
      .first();
    expect({
      amount: after.amount,
      scalarDefault: after.scalarDefault,
      createdAt: new Date(after.createdAt).toISOString(),
      updatedAt: new Date(after.updatedAt).toISOString(),
      storedTotal: after.storedTotal,
      virtualTotal: after.virtualTotal,
    }).toEqual({
      amount: 11,
      scalarDefault: 19,
      createdAt: new Date(before.createdAt).toISOString(),
      updatedAt: new Date(before.updatedAt).toISOString(),
      storedTotal: 22,
      virtualTotal: 33,
    });
  }, 120000);

  it('uses the durable snapshot through the runtime schema unit-of-work path', async () => {
    const snapshotService = new MySqlBootstrapSnapshotService({
      knexService: { getKnex: () => db },
    } as any);
    const service = new RuntimeSchemaUnitOfWorkService({
      databaseConfigService: {
        isMongoDb: () => false,
        getDbType: () => 'mysql',
      } as any,
      knexService: { getKnex: () => db } as any,
      mongoService: {} as any,
      mySqlBootstrapSnapshotService: snapshotService,
      mySqlRuntimeWriteBarrierService: {
        runExclusive: async (
          _context: unknown,
          callback: () => Promise<unknown>,
        ) => callback(),
      } as any,
    });
    const before = await db('parents').where({ id: 1 }).first();

    await expect(
      service.run(
        async () => {
          await db('parents').where({ id: 1 }).update({ name: 'runtime-uow' });
          throw new Error('runtime schema mutation failed');
        },
        { mutationId: 'runtime-schema:real-db' } as any,
      ),
    ).rejects.toThrow('runtime schema mutation failed');

    await expect(db('parents').where({ id: 1 }).first()).resolves.toMatchObject(
      before,
    );
  }, 120000);

  it('rejects a missing required backup before dropping any live table', async () => {
    const { transaction, entries } = await leavePendingSnapshot();
    expect(entries.length).toBeGreaterThan(1);
    await db.schema.dropTable(entries[0].backupTableName);
    const before = await db('parents').where({ id: 1 }).first();

    const recoveryService = new MySqlBootstrapSnapshotService({
      knexService: { getKnex: () => db },
    } as any);
    await expect(recoveryService.recoverPending()).rejects.toThrow(
      /missing backup/,
    );
    await expect(db('parents').where({ id: 1 }).first()).resolves.toMatchObject(
      before,
    );
    expect(await db.schema.hasTable('children')).toBe(true);

    await cleanupPendingSnapshot(transaction.txId, entries);
  }, 120000);

  it('rejects invalid snapshot column metadata before dropping live tables', async () => {
    const { transaction, entries } = await leavePendingSnapshot();
    const before = await db('parents').where({ id: 1 }).first();
    await db('system_bootstrap_snapshots')
      .where({ txId: transaction.txId, ordinal: 0 })
      .update({ columnsJson: '{invalid' });

    const recoveryService = new MySqlBootstrapSnapshotService({
      knexService: { getKnex: () => db },
    } as any);
    await expect(recoveryService.recoverPending()).rejects.toThrow(
      /invalid column metadata/,
    );
    await expect(db('parents').where({ id: 1 }).first()).resolves.toMatchObject(
      before,
    );
    expect(await db.schema.hasTable('children')).toBe(true);

    await cleanupPendingSnapshot(transaction.txId, entries);
  }, 120000);

  it('rejects a backup with missing writable columns before dropping live tables', async () => {
    const { transaction, entries } = await leavePendingSnapshot();
    const parentEntry = entries.find((entry) => entry.tableName === 'parents');
    expect(parentEntry).toBeTruthy();
    const before = await db('parents').where({ id: 1 }).first();
    await db.schema.alterTable(parentEntry.backupTableName, (table) => {
      table.dropColumn('legacy');
    });

    const recoveryService = new MySqlBootstrapSnapshotService({
      knexService: { getKnex: () => db },
    } as any);
    await expect(recoveryService.recoverPending()).rejects.toThrow(
      /missing column\(s\): legacy/,
    );
    await expect(db('parents').where({ id: 1 }).first()).resolves.toMatchObject(
      before,
    );
    expect(await db.schema.hasTable('children')).toBe(true);

    await cleanupPendingSnapshot(transaction.txId, entries);
  }, 120000);

  it('rejects a backup with missing rows before dropping live tables', async () => {
    const { transaction, entries } = await leavePendingSnapshot();
    const parentEntry = entries.find((entry) => entry.tableName === 'parents');
    expect(parentEntry).toBeTruthy();
    const before = await db('parents').where({ id: 1 }).first();
    await db(parentEntry.backupTableName).delete();

    const recoveryService = new MySqlBootstrapSnapshotService({
      knexService: { getKnex: () => db },
    } as any);
    await expect(recoveryService.recoverPending()).rejects.toThrow(
      /row-count attestation failed/,
    );
    await expect(db('parents').where({ id: 1 }).first()).resolves.toMatchObject(
      before,
    );
    expect(await db.schema.hasTable('children')).toBe(true);

    await cleanupPendingSnapshot(transaction.txId, entries);
  }, 120000);
});
