import { describe, expect, it, vi } from 'vitest';
import { RedisCacheService } from '../../src/engines/cache/services/redis-cache.service';
import type { RedisCachePolicy } from '../../src/engines/cache/types/redis-cache-policy.types';

const SYSTEM_POLICY: RedisCachePolicy = {
  keyPrefix: '',
  clearAllMode: 'namespace',
};

function makeSystemCache(redis: any, nodeName: string | null) {
  return new RedisCacheService({
    redis,
    envService: {
      get: (key: string) => (key === 'NODE_NAME' ? nodeName : null),
    } as any,
    runtimeNamespaceLifecycleService: {
      getKeyTtlMs: () => 7000,
      registerManagedKey: vi.fn(async () => {}),
      unregisterManagedKey: vi.fn(async () => {}),
    } as any,
    policy: SYSTEM_POLICY,
  });
}

describe('RedisCacheService — system policy', () => {
  it('stores zero-ttl values with namespace lifecycle ttl when available', async () => {
    const redis = {
      set: vi.fn(async () => 'OK'),
    };
    const service = makeSystemCache(redis, 'app-a');

    await service.set('auth:oauth-exchange:pending:code', { ok: true }, 0);

    expect(redis.set).toHaveBeenCalledWith(
      'app-a:auth:oauth-exchange:pending:code',
      JSON.stringify({ ok: true }),
      'PX',
      7000,
    );
  });

  it('acquires zero-ttl locks with namespace lifecycle ttl when available', async () => {
    const redis = {
      set: vi.fn(async () => 'OK'),
    };
    const service = makeSystemCache(redis, 'app-a');

    await service.acquire('lock:boot', 'token', 0);

    expect(redis.set).toHaveBeenCalledWith(
      'app-a:lock:boot',
      'token',
      'PX',
      7000,
      'NX',
    );
  });

  it('scopes coordination keys to the app namespace', async () => {
    const redis = {
      set: vi.fn(async () => 'OK'),
    };

    await makeSystemCache(redis, 'app-a').acquire(
      'sys:provision_init_lock',
      'token-a',
      7000,
    );
    await makeSystemCache(redis, 'app-a').acquire(
      'sys:provision_init_lock',
      'token-b',
      7000,
    );
    await makeSystemCache(redis, 'app-b').acquire(
      'sys:provision_init_lock',
      'token-c',
      7000,
    );

    expect(redis.set.mock.calls.map(([key]) => key)).toEqual([
      'app-a:sys:provision_init_lock',
      'app-a:sys:provision_init_lock',
      'app-b:sys:provision_init_lock',
    ]);
  });

  it('renews a lock only while the caller still owns it', async () => {
    const redis = {
      eval: vi.fn(async () => 1),
    };
    const service = makeSystemCache(redis, 'app-a');

    await expect(service.renew('lock:boot', 'token', 7000)).resolves.toBe(true);

    expect(redis.eval).toHaveBeenCalledWith(
      expect.stringContaining('pexpire'),
      1,
      'app-a:lock:boot',
      'token',
      7000,
    );
  });

  it('reports a lost lock instead of recreating it during renewal', async () => {
    const service = new RedisCacheService({
      redis: {
        eval: vi.fn(async () => 0),
      } as any,
      envService: {
        get: () => null,
      } as any,
      policy: SYSTEM_POLICY,
    });

    await expect(service.renew('lock:boot', 'stale-token', 7000)).resolves.toBe(
      false,
    );
  });

  it('refreshes a cache value only while it still matches the expected state', async () => {
    const redis = {
      eval: vi.fn(async () => 1),
    };
    const service = makeSystemCache(redis, 'app-a');

    await expect(
      service.compareAndSet('auth:api-token:token-1', { version: 1 }, { version: 2 }, 60000),
    ).resolves.toBe(true);

    expect(redis.eval).toHaveBeenCalledWith(
      expect.stringContaining('redis.call("set"'),
      1,
      'app-a:auth:api-token:token-1',
      JSON.stringify({ version: 1 }),
      JSON.stringify({ version: 2 }),
      60000,
    );
  });

  it('writes related cache entries only while the revocation guard is absent', async () => {
    const redis = {
      eval: vi.fn(async () => 1),
    };
    const service = makeSystemCache(redis, 'app-a');

    await expect(
      service.setManyIfKeyAbsent('auth:api-token:revoked:token-1', [
        {
          key: 'auth:api-token:token-1',
          value: { id: 'token-1' },
          ttlMs: 60000,
        },
        {
          key: 'auth:api-token:hash:hash-1',
          value: { id: 'token-1' },
          ttlMs: 60000,
        },
      ]),
    ).resolves.toBe(true);

    expect(redis.eval).toHaveBeenCalledWith(
      expect.stringContaining('redis.call("exists"'),
      3,
      'app-a:auth:api-token:revoked:token-1',
      'app-a:auth:api-token:token-1',
      'app-a:auth:api-token:hash:hash-1',
      JSON.stringify({ id: 'token-1' }),
      60000,
      JSON.stringify({ id: 'token-1' }),
      60000,
    );
  });

  it('marks revocation and deletes token states in one Redis script', async () => {
    const redis = {
      eval: vi.fn(async () => null),
    };
    const service = makeSystemCache(redis, 'app-a');

    await service.setManyAndDelete(
      [
        {
          key: 'auth:api-token:revoked:token-1',
          value: true,
          ttlMs: 900000,
        },
      ],
      ['auth:api-token:token-1', 'auth:api-token:hash:hash-1'],
    );

    expect(redis.eval).toHaveBeenCalledWith(
      expect.stringContaining('redis.call("del"'),
      3,
      'app-a:auth:api-token:revoked:token-1',
      'app-a:auth:api-token:token-1',
      'app-a:auth:api-token:hash:hash-1',
      1,
      'true',
      900000,
    );
  });
});
