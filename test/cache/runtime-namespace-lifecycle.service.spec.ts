import { describe, expect, it } from 'vitest';
import { RuntimeNamespaceLifecycleService } from '../../src/engines/cache';

class FakePipeline {
  private readonly ops: Array<() => void> = [];

  constructor(private readonly redis: FakeRedis) {}

  set(key: string, value: string, mode?: string, ttlMs?: number) {
    this.ops.push(() => this.redis.setSync(key, value, mode, ttlMs));
    return this;
  }

  hset(key: string, field: string, value: string) {
    this.ops.push(() => this.redis.hsetSync(key, field, value));
    return this;
  }

  hdel(key: string, field: string) {
    this.ops.push(() => this.redis.hdelSync(key, field));
    return this;
  }

  pexpire(key: string, ttlMs: number) {
    this.ops.push(() => this.redis.pexpireSync(key, ttlMs));
    return this;
  }

  async exec() {
    for (const operation of this.ops) operation();
    return [];
  }
}

class FakeRedis {
  values = new Map<string, string>();
  hashes = new Map<string, Map<string, string>>();
  expiries = new Map<string, number>();

  pipeline() {
    return new FakePipeline(this);
  }

  multi() {
    return new FakePipeline(this);
  }

  async set(key: string, value: string, mode?: string, ttlMs?: number) {
    this.setSync(key, value, mode, ttlMs);
    return 'OK';
  }

  async del(...keys: string[]) {
    return keys.reduce((count, key) => count + this.delSync(key), 0);
  }

  async pexpire(key: string, ttlMs: number) {
    this.pexpireSync(key, ttlMs);
    return this.hasKey(key) ? 1 : 0;
  }

  async pttl(key: string) {
    if (!this.hasKey(key)) return -2;
    return this.expiries.get(key) ?? -1;
  }

  async hset(key: string, field: string, value: string) {
    this.hsetSync(key, field, value);
    return 1;
  }

  async hdel(key: string, field: string) {
    return this.hdelSync(key, field);
  }

  async hscan(key: string, cursor: string) {
    const fields = [...(this.hashes.get(key)?.entries() ?? [])].flatMap(
      ([field, value]) => [field, value],
    );
    return [cursor === '0' ? '0' : cursor, cursor === '0' ? fields : []];
  }

  setSync(key: string, value: string, mode?: string, ttlMs?: number) {
    this.values.set(key, value);
    if (mode === 'PX' && typeof ttlMs === 'number') {
      this.pexpireSync(key, ttlMs);
    }
  }

  hsetSync(key: string, field: string, value: string) {
    const hash = this.hashes.get(key) ?? new Map<string, string>();
    hash.set(field, value);
    this.hashes.set(key, hash);
  }

  hdelSync(key: string, field: string) {
    return this.hashes.get(key)?.delete(field) ? 1 : 0;
  }

  pexpireSync(key: string, ttlMs: number) {
    if (this.hasKey(key)) this.expiries.set(key, ttlMs);
  }

  delSync(key: string) {
    const existed = this.values.delete(key) || this.hashes.delete(key);
    this.expiries.delete(key);
    return existed ? 1 : 0;
  }

  hasKey(key: string) {
    return this.values.has(key) || this.hashes.has(key);
  }
}

function createService(redis: FakeRedis) {
  return new RuntimeNamespaceLifecycleService({
    redis: redis as any,
    instanceService: { getInstanceId: () => 'inst-a' } as any,
    envService: {
      get: (key: string) => {
        if (key === 'NODE_NAME') return 'app-a';
        if (key === 'REDIS_NAMESPACE_KEY_TTL_MS') return 10000;
        if (key === 'REDIS_NAMESPACE_LEASE_TTL_MS') return 1000;
        if (key === 'REDIS_NAMESPACE_RENEW_INTERVAL_MS') return 1000000;
        if (key === 'NODE_ENV') return 'test';
        return undefined;
      },
    } as any,
  });
}

describe('RuntimeNamespaceLifecycleService', () => {
  it('renews registered lifecycle-managed keys only', async () => {
    const redis = new FakeRedis();
    const service = createService(redis);
    const managedKey = 'app-a:runtime_cache:route';
    const explicitTtlKey = 'app-a:runtime_cache:route:lock';
    const bullMqKey = 'app-a:sys_flow-execution:repeat:flow-schedule-13';

    redis.values.set(managedKey, 'snapshot');
    redis.values.set(explicitTtlKey, 'lock-token');
    redis.values.set(bullMqKey, 'scheduler');
    redis.expiries.set(managedKey, 100);
    redis.expiries.set(explicitTtlKey, 30000);
    await service.registerManagedKey(managedKey);

    await service.renewCurrentNamespaceKeys();

    expect((service as any).renewSystemQueueKeys).toBeUndefined();
    expect(redis.expiries.get(managedKey)).toBe(10000);
    expect(redis.expiries.get(explicitTtlKey)).toBe(30000);
    expect(redis.expiries.has(bullMqKey)).toBe(false);
  });

  it('removes expired and manually deleted entries from the managed registry', async () => {
    const redis = new FakeRedis();
    const service = createService(redis);
    const deletedKey = 'app-a:runtime_cache:metadata';

    await service.registerManagedKey(deletedKey);
    await service.renewCurrentNamespaceKeys();

    expect(
      redis.hashes
        .get('app-a:runtime_lifecycle:managed_cache_keys')
        ?.has(deletedKey),
    ).toBe(false);
  });

  it('does not extend a lease from a previous process', async () => {
    const redis = new FakeRedis();
    redis.values.set('app-a:runtime_lifecycle:lease:previous-instance', 'old');
    redis.expiries.set('app-a:runtime_lifecycle:lease:previous-instance', 100);

    const service = createService(redis);
    await service.renewCurrentNamespaceKeys();

    expect(redis.expiries.get('app-a:runtime_lifecycle:lease:inst-a')).toBe(
      1000,
    );
    expect(
      redis.expiries.get('app-a:runtime_lifecycle:lease:previous-instance'),
    ).toBe(100);
  });
});
