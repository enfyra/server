import knex, { Knex } from 'knex';
import { describe, expect, it, beforeAll, afterAll } from 'vitest';
import { KnexEntityManager } from '../../src/engines/knex/entity-manager';
import { runWithIoAbortSignal } from '@enfyra/kernel';

const PG_URI = 'postgres://root:1234@localhost:5432/enfyra';

describe('Knex IO Abort E2E (PostgreSQL)', () => {
  let db: Knex;

  beforeAll(async () => {
    db = knex({ client: 'pg', connection: PG_URI, pool: { min: 0, max: 5 } });
    await db.raw('CREATE TABLE IF NOT EXISTS _abort_test (id serial primary key, name text, value int)');
    await db('_abort_test').delete({});
  });

  afterAll(async () => {
    await db.raw('DROP TABLE IF EXISTS _abort_test');
    await db.destroy();
  });

  it('entity manager insert succeeds without abort signal', async () => {
    const em = new KnexEntityManager(db, { beforeInsert: [], afterInsert: [], afterInsertMany: [] }, 'pg');
    const id = await runWithIoAbortSignal(undefined, () =>
      em.insert('_abort_test', { name: 'ok', value: 1 }),
    );
    expect(id).toBeDefined();
    const row = await db('_abort_test').where('name', 'ok').first();
    expect(row.value).toBe(1);
  });

  it('entity manager insert throws when signal is already aborted', async () => {
    const controller = new AbortController();
    controller.abort();
    const em = new KnexEntityManager(db, { beforeInsert: [], afterInsert: [], afterInsertMany: [] }, 'pg');
    await expect(
      runWithIoAbortSignal(controller.signal, () =>
        em.insert('_abort_test', { name: 'should-not-exist', value: 99 }),
      ),
    ).rejects.toThrow('Operation aborted');

    const row = await db('_abort_test').where('name', 'should-not-exist').first();
    expect(row).toBeUndefined();
  });

  it('entity manager update throws when signal is already aborted', async () => {
    const controller = new AbortController();
    controller.abort();
    const em = new KnexEntityManager(db, { beforeUpdate: [], afterUpdate: [] }, 'pg');
    await expect(
      runWithIoAbortSignal(controller.signal, () =>
        em.update('_abort_test', 1, { value: 999 }),
      ),
    ).rejects.toThrow('Operation aborted');

    const row = await db('_abort_test').where('name', 'ok').first();
    expect(row.value).toBe(1);
  });

  it('entity manager insertMany throws when signal is already aborted', async () => {
    const controller = new AbortController();
    controller.abort();
    const em = new KnexEntityManager(db, { beforeInsert: [], afterInsert: [], afterInsertMany: [] }, 'pg');
    await expect(
      runWithIoAbortSignal(controller.signal, () =>
        em.insertMany('_abort_test', [{ name: 'batch-1', value: 10 }, { name: 'batch-2', value: 20 }]),
      ),
    ).rejects.toThrow('Operation aborted');

    const rows = await db('_abort_test').where('name', 'batch-1').orWhere('name', 'batch-2');
    expect(rows.length).toBe(0);
  });

  it('abort during hook prevents SQL execution', async () => {
    const controller = new AbortController();

    const em = new KnexEntityManager(db, {
      beforeInsert: [
        async () => {
          controller.abort();
          return { name: 'slow', value: 1 };
        },
      ],
      afterInsert: [],
      afterInsertMany: [],
    }, 'pg');

    await expect(
      runWithIoAbortSignal(controller.signal, () =>
        em.insert('_abort_test', { name: 'slow', value: 1 }),
      ),
    ).rejects.toThrow('Operation aborted');

    const row = await db('_abort_test').where('name', 'slow').first();
    expect(row).toBeUndefined();
  });

  it('transaction rollback on abort via hook manager pattern', async () => {
    const controller = new AbortController();

    let outcome: string;
    try {
      await db.transaction(async (trx) => {
        const signal = controller.signal;
        if (signal.aborted) throw new Error('Operation aborted');
        const onAbort = () => {
          if (!trx.isCompleted()) trx.rollback(new Error('Operation aborted')).catch(() => {});
        };
        signal.addEventListener('abort', onAbort, { once: true });

        await trx('_abort_test').insert({ name: 'trx-test', value: 42 });

        controller.abort();
        await new Promise((r) => setTimeout(r, 100));

        signal.removeEventListener('abort', onAbort);
      });
      outcome = 'committed';
    } catch {
      outcome = 'rolled-back';
    }

    expect(outcome).toBe('rolled-back');
    const row = await db('_abort_test').where('name', 'trx-test').first();
    expect(row).toBeUndefined();
  });
});
