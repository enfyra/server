import { describe, expect, it } from 'vitest';
import { RuntimeNamespaceLifecycleService } from '../../src/engines/cache';

class FakePipeline {
  private readonly ops: Array<() => void> = [];

  constructor(private readonly redis: FakeRedis) {}

  set(key: string, value: string, mode?: string, ttlMs?: number) {
    this.ops.push(() => this.redis.setSync(key, value, mode, ttlMs));
    return this;
  }

  zadd(key: string, score: number, member: string) {
    this.ops.push(() => this.redis.zaddSync(key, score, member));
    return this;
  }

  hset(key: string, value: Record<string, string>) {
    this.ops.push(() => this.redis.hsetSync(key, value));
    return this;
  }

  pexpire(key: string, ttlMs: number) {
    this.ops.push(() => this.redis.pexpireSync(key, ttlMs));
    return this;
  }

  async exec() {
    for (const op of this.ops) op();
    return [];
  }
}

class FakeRedis {
  values = new Map<string, string>();
  hashes = new Map<string, Map<string, string>>();
  zsets = new Map<string, Map<string, number>>();
  expiries = new Map<string, number>();

  pipeline() {
    return new FakePipeline(this);
  }

  async set(key: string, value: string, mode?: string, ttlMs?: number) {
    this.setSync(key, value, mode, ttlMs);
    return 'OK';
  }

  async del(...keys: string[]) {
    let deleted = 0;
    for (const key of keys) deleted += this.delSync(key);
    return deleted;
  }

  async unlink(...keys: string[]) {
    return this.del(...keys);
  }

  async pexpire(key: string, ttlMs: number) {
    this.pexpireSync(key, ttlMs);
    return 1;
  }

  async scan(cursor: string, _match: string, pattern: string) {
    const prefix = pattern.endsWith('*') ? pattern.slice(0, -1) : pattern;
    const keys = [
      ...this.values.keys(),
      ...this.hashes.keys(),
      ...this.zsets.keys(),
    ].filter((key, index, all) => all.indexOf(key) === index);
    return [
      '0',
      cursor === '0' ? keys.filter((key) => key.startsWith(prefix)) : [],
    ];
  }

  async zrangebyscore(key: string, min: number, max: number) {
    return [...(this.zsets.get(key)?.entries() ?? [])]
      .filter(([, score]) => score >= min && score <= max)
      .sort((a, b) => a[1] - b[1])
      .map(([member]) => member);
  }

  async zrem(key: string, member: string) {
    return this.zsets.get(key)?.delete(member) ? 1 : 0;
  }

  setSync(key: string, value: string, mode?: string, ttlMs?: number) {
    this.values.set(key, value);
    if (mode === 'PX' && typeof ttlMs === 'number') {
      this.pexpireSync(key, ttlMs);
    }
  }

  zaddSync(key: string, score: number, member: string) {
    const zset = this.zsets.get(key) ?? new Map<string, number>();
    zset.set(member, score);
    this.zsets.set(key, zset);
  }

  hsetSync(key: string, value: Record<string, string>) {
    const hash = this.hashes.get(key) ?? new Map<string, string>();
    for (const [field, fieldValue] of Object.entries(value)) {
      hash.set(field, fieldValue);
    }
    this.hashes.set(key, hash);
  }

  pexpireSync(key: string, ttlMs: number) {
    this.expiries.set(key, ttlMs);
  }

  delSync(key: string) {
    const existed =
      this.values.delete(key) ||
      this.hashes.delete(key) ||
      this.zsets.delete(key);
    this.expiries.delete(key);
    return existed ? 1 : 0;
  }
}

function createService(redis: FakeRedis, nodeName = 'app-a') {
  return new RuntimeNamespaceLifecycleService({
    redis: redis as any,
    instanceService: { getInstanceId: () => 'inst-a' } as any,
    envService: {
      get: (key: string) => {
        if (key === 'NODE_NAME') return nodeName;
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
  it('renews TTL for safe runtime keys in the current namespace', async () => {
    const redis = new FakeRedis();
    redis.values.set('app-a:runtime_cache:metadata', 'cache');
    redis.values.set('app-a:sys_flow-execution:completed', 'queue');
    redis.values.set('other:runtime_cache:metadata', 'other');

    const service = createService(redis);

    await service.renewCurrentNamespaceKeys();

    expect(redis.expiries.get('app-a:runtime_cache:metadata')).toBe(10000);
    expect(redis.expiries.get('app-a:sys_flow-execution:completed')).toBe(
      10000,
    );
    expect(redis.expiries.has('other:runtime_cache:metadata')).toBe(false);
    expect(redis.values.has('app-a:runtime_lifecycle:lease:inst-a')).toBe(true);
  });

  it('renews existing lifecycle keys for the same namespace after restart', async () => {
    const redis = new FakeRedis();
    redis.values.set('app-a:runtime_cache:metadata', 'old-cache');
    redis.values.set('app-a:sys_session-cleanup:completed', 'old-queue');
    redis.expiries.set('app-a:runtime_cache:metadata', 100);
    redis.expiries.set('app-a:sys_session-cleanup:completed', 100);

    const restartedService = createService(redis, 'app-a');
    await restartedService.renewCurrentNamespaceKeys();

    expect(redis.expiries.get('app-a:runtime_cache:metadata')).toBe(10000);
    expect(redis.expiries.get('app-a:sys_session-cleanup:completed')).toBe(
      10000,
    );
  });

  it('renews one system queue namespace on demand', async () => {
    const redis = new FakeRedis();
    redis.values.set('app-a:sys_flow-execution:wait', 'queue');
    redis.values.set('app-a:sys_session-cleanup:wait', 'queue');

    const service = createService(redis);

    await service.renewSystemQueueKeys('sys_flow-execution');

    expect(redis.expiries.get('app-a:sys_flow-execution:wait')).toBe(10000);
    expect(redis.expiries.has('app-a:sys_session-cleanup:wait')).toBe(false);
  });
});
