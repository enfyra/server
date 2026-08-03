import { describe, expect, it, vi } from 'vitest';
import { MySqlRuntimeWriteBarrierService } from '../../src/engines/knex';

describe('MySqlRuntimeWriteBarrierService control transactions', () => {
  it('retries a deadlocked exclusive-fence acquisition before running the callback', async () => {
    const fence = {
      id: 'global',
      isFenced: false,
      fenceEpoch: 0,
      fenceToken: null,
      ownerInstanceId: null,
      mutationId: null,
      leaseExpiresAt: null,
    };
    const makeBuilder = (tableName: string) => {
      const builder: any = {
        where: vi.fn(() => builder),
        forUpdate: vi.fn(() => builder),
        count: vi.fn(() => builder),
        delete: vi.fn(async () => 0),
        first: vi.fn(async () =>
          tableName === 'system_runtime_write_fence' ? { ...fence } : { count: 0 },
        ),
        update: vi.fn(async (value: Record<string, unknown>) => {
          if (tableName === 'system_runtime_write_fence') Object.assign(fence, value);
          return 1;
        }),
      };
      return builder;
    };
    const knex: any = vi.fn((tableName: string) => makeBuilder(tableName));
    const deadlock = Object.assign(new Error('Deadlock found when trying to get lock'), {
      code: 'ER_LOCK_DEADLOCK',
      errno: 1213,
    });
    knex.transaction = vi
      .fn()
      .mockRejectedValueOnce(deadlock)
      .mockImplementation(async (callback: (trx: any) => Promise<unknown>) => callback(knex));
    const service = new MySqlRuntimeWriteBarrierService({
      knexService: { getSystemKnex: () => knex } as any,
      instanceService: { getInstanceId: () => 'instance-a' } as any,
    });
    (service as any).readyPromise = Promise.resolve();

    await expect(
      service.runExclusive(
        { mutationId: 'runtime-schema:retry-deadlock' },
        async () => 'done',
      ),
    ).resolves.toBe('done');
    expect(knex.transaction).toHaveBeenCalledTimes(2);
  });
});
