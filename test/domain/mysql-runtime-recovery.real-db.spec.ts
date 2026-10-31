import { randomUUID } from 'node:crypto';
import knex, { type Knex } from 'knex';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { MySqlBootstrapSnapshotService } from '../../src/engines/bootstrap/services/mysql-bootstrap-snapshot.service';
import type { MySqlBootstrapSnapshotContext } from '../../src/engines/bootstrap/types/mysql-bootstrap-snapshot.types';

function contract(
  operation: 'create' | 'update',
  source: string | null,
  target: string,
): MySqlBootstrapSnapshotContext {
  return {
    mutationId: `test-${randomUUID()}`,
    runtimeContract: {
      context: {
        operation,
        source: source ? { name: source } : null,
        target: { name: target, relations: [] },
        diff: { policyMetadataChanged: false },
      },
    } as NonNullable<MySqlBootstrapSnapshotContext['runtimeContract']>,
  };
}

describe('MySQL/MariaDB durable runtime snapshot recovery', () => {
  let admin: Knex;
  let db: Knex;
  let database: string;
  let created: boolean;
  const makeService = () =>
    new MySqlBootstrapSnapshotService({
      knexService: { getKnex: () => db },
    } as never);

  beforeEach(async () => {
    const uri = process.env.MYSQL_TEST_URI;
    if (!uri) throw new Error('MYSQL_TEST_URI is required');
    const connection = new URL(uri);
    if (!['localhost', '127.0.0.1'].includes(connection.hostname))
      throw new Error('MYSQL_TEST_URI must be local');
    database = `runtime_recovery_${randomUUID().replaceAll('-', '')}`;
    created = false;
    admin = knex({
      client: 'mysql2',
      connection: uri,
      pool: { min: 0, max: 1 },
    });
    await admin.raw('CREATE DATABASE ??', [database]);
    created = true;
    connection.pathname = `/${database}`;
    db = knex({
      client: 'mysql2',
      connection: connection.href,
      pool: { min: 0, max: 3 },
      acquireConnectionTimeout: 5000,
    });
    await db.schema.createTable('orders', (table) => {
      table.increments('id');
      table.string('marker');
    });
    await db('orders').insert({ id: 7, marker: 'preserved' });
  });

  afterEach(async () => {
    try {
      if (db) await db.destroy();
      if (created) await admin.raw('DROP DATABASE ??', [database]);
    } finally {
      if (admin) await admin.destroy();
    }
  });

  it('recovers a failed rename in a new service without the in-memory contract', async () => {
    const service = makeService();
    const restore = vi
      .spyOn(service as unknown as { restore(): Promise<void> }, 'restore')
      .mockRejectedValueOnce(
        new Error('simulated process loss before restore'),
      );
    await expect(
      service.run(
        async () => {
          await db.schema.renameTable('orders', 'renamed_orders');
          await db('renamed_orders')
            .where({ id: 7 })
            .update({ marker: 'changed' });
          throw new Error('injected after rename');
        },
        contract('update', 'orders', 'renamed_orders'),
      ),
    ).rejects.toThrow('simulated process loss');
    restore.mockRestore();
    const journal = await db('system_bootstrap_transactions')
      .where({ status: 'rolling_back' })
      .first();
    expect(JSON.parse(journal.runtimeCreatedTablesJson)).toEqual([
      'renamed_orders',
    ]);

    await makeService().recoverPending();
    await makeService().recoverPending();

    expect(await db.schema.hasTable('renamed_orders')).toBe(false);
    expect(await db('orders').select('*')).toEqual([
      { id: 7, marker: 'preserved' },
    ]);
    expect(
      await db('system_bootstrap_snapshots').count({ count: '*' }).first(),
    ).toMatchObject({ count: 0 });
  }, 20000);

  it('removes a created-only table during fresh-service recovery without touching unrelated data', async () => {
    const service = makeService();
    vi.spyOn(
      service as unknown as { restore(): Promise<void> },
      'restore',
    ).mockRejectedValueOnce(new Error('simulated loss'));
    await expect(
      service.run(
        async () => {
          await db.schema.createTable('new_orders', (table) =>
            table.increments('id'),
          );
          throw new Error('injected after create');
        },
        contract('create', null, 'new_orders'),
      ),
    ).rejects.toThrow('simulated loss');

    await makeService().recoverPending();

    expect(await db.schema.hasTable('new_orders')).toBe(false);
    expect(await db('orders').select('*')).toEqual([
      { id: 7, marker: 'preserved' },
    ]);
  }, 20000);

  it('rejects invalid cleanup metadata before dropping any table', async () => {
    const service = makeService();
    vi.spyOn(
      service as unknown as { restore(): Promise<void> },
      'restore',
    ).mockRejectedValueOnce(new Error('simulated loss'));
    await expect(
      service.run(
        async () => {
          throw new Error('injected');
        },
        contract('update', 'orders', 'renamed_orders'),
      ),
    ).rejects.toThrow('simulated loss');
    await db('system_bootstrap_transactions')
      .where({ status: 'rolling_back' })
      .update({ runtimeCreatedTablesJson: JSON.stringify(['orders']) });

    await expect(makeService().recoverPending()).rejects.toThrow(
      /invalid created-table metadata/,
    );
    expect(await db('orders').select('*')).toEqual([
      { id: 7, marker: 'preserved' },
    ]);
  }, 20000);
});
