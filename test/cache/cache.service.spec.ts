import { describe, expect, it, vi } from 'vitest';
import { CacheService } from '../../src/engines/cache/services/cache.service';

describe('CacheService', () => {
  it('stores zero-ttl values with namespace lifecycle ttl when available', async () => {
    const redis = {
      set: vi.fn(async () => 'OK'),
    };
    const service = new CacheService({
      redis: redis as any,
      envService: {
        get: (key: string) => (key === 'NODE_NAME' ? 'app-a' : null),
      } as any,
      runtimeNamespaceLifecycleService: {
        getKeyTtlMs: () => 7000,
      } as any,
    });

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
    const service = new CacheService({
      redis: redis as any,
      envService: {
        get: (key: string) => (key === 'NODE_NAME' ? 'app-a' : null),
      } as any,
      runtimeNamespaceLifecycleService: {
        getKeyTtlMs: () => 7000,
      } as any,
    });

    await service.acquire('lock:boot', 'token', 0);

    expect(redis.set).toHaveBeenCalledWith(
      'app-a:lock:boot',
      'token',
      'PX',
      7000,
      'NX',
    );
  });
});
