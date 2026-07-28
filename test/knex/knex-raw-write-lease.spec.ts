import knex from 'knex';
import { describe, expect, it, vi } from 'vitest';
import { KnexService } from '../../src/engines/knex';

describe('KnexService raw write lease boundary', () => {
  it('keeps non-query raw SQL fragments synchronous for schema builders', async () => {
    const base = knex({ client: 'pg' });
    const service = new KnexService({
      databaseConfigService: {} as any,
      knexHookManagerService: {} as any,
      lazyRef: {} as any,
      envService: {} as any,
    });
    Object.assign(service as any, {
      knexInstance: base,
      dbType: 'postgres',
    });

    const fragment = service.getKnex().raw('gen_random_uuid()');

    expect(fragment).not.toBeInstanceOf(Promise);
    expect(fragment.toSQL().sql).toBe('gen_random_uuid()');
    await base.destroy();
  });

  it('acquires the MySQL write lease only when an executable raw is awaited', async () => {
    const base = knex({ client: 'mysql2' });
    const runWithWriteLease = vi.fn(async (callback: () => Promise<unknown>) =>
      callback(),
    );
    const service = new KnexService({
      databaseConfigService: {} as any,
      knexHookManagerService: {} as any,
      lazyRef: {
        mySqlRuntimeWriteBarrierService: { runWithWriteLease },
      } as any,
      envService: {} as any,
    });
    Object.assign(service as any, {
      knexInstance: base,
      dbType: 'mysql',
    });

    const raw = service.getKnex().raw('SET @enfyra_test = 1');
    expect(runWithWriteLease).not.toHaveBeenCalled();
    await expect(raw).rejects.toThrow();
    expect(runWithWriteLease).toHaveBeenCalledOnce();
    await base.destroy();
  });
});
