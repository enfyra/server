import { describe, expect, it } from 'vitest';
import { UserCacheService } from '../../src/engines/cache';

class FakePipeline {
  private readonly ops: Array<() => void> = [];

  constructor(private readonly redis: FakeRedis) {}

  hset(key: string, field: string, value: any) {
    this.ops.push(() => this.redis.hsetSync(key, field, String(value)));
    return this;
  }

  hdel(key: string, field: string) {
    this.ops.push(() => this.redis.hdelSync(key, field));
    return this;
  }

  zadd(key: string, score: number, value: string) {
    this.ops.push(() => this.redis.zaddSync(key, score, value));
    return this;
  }

  zrem(key: string, value: string) {
    this.ops.push(() => this.redis.zremSync(key, value));
    return this;
  }

  incrby(key: string, amount: number) {
    this.ops.push(() => this.redis.incrbySync(key, amount));
    return this;
  }

  del(...keys: string[]) {
    this.ops.push(() => {
      for (const key of keys) this.redis.delSync(key);
    });
    return this;
  }

  async exec() {
    for (const op of this.ops) op();
    return [];
  }
}

class FakeRedis {
  strings = new Map<string, string>();
  hashes = new Map<string, Map<string, string>>();
  zsets = new Map<string, Map<string, number>>();

  pipeline() {
    return new FakePipeline(this);
  }

  async set(key: string, value: string, ...args: any[]) {
    if (args.includes('NX') && this.strings.has(key)) return null;
    this.strings.set(key, value);
    return 'OK';
  }

  async get(key: string) {
    return this.strings.get(key) ?? null;
  }

  async del(...keys: string[]) {
    let deleted = 0;
    for (const key of keys) deleted += this.delSync(key);
    return deleted;
  }

  async hget(key: string, field: string) {
    return this.hashes.get(key)?.get(field) ?? null;
  }

  async zadd(key: string, score: number, value: string) {
    this.zaddSync(key, score, value);
    return 1;
  }

  async zrange(key: string) {
    return [...(this.zsets.get(key)?.entries() ?? [])]
      .sort((a, b) => a[1] - b[1])
      .map(([value]) => value);
  }

  async scan(cursor: string, _match: string, pattern: string) {
    const prefix = pattern.replace('*', '');
    return [
      '0',
      cursor === '0'
        ? [...this.strings.keys()].filter((key) => key.startsWith(prefix))
        : [],
    ];
  }

  async eval(_lua: string, _keyCount: number, key: string, expected: string) {
    if (this.strings.get(key) !== expected) return 0;
    return this.delSync(key);
  }

  delSync(key: string) {
    const existed =
      this.strings.delete(key) ||
      this.hashes.delete(key) ||
      this.zsets.delete(key);
    return existed ? 1 : 0;
  }

  hsetSync(key: string, field: string, value: string) {
    const hash = this.hashes.get(key) ?? new Map<string, string>();
    hash.set(field, value);
    this.hashes.set(key, hash);
  }

  hdelSync(key: string, field: string) {
    this.hashes.get(key)?.delete(field);
  }

  zaddSync(key: string, score: number, value: string) {
    const zset = this.zsets.get(key) ?? new Map<string, number>();
    zset.set(value, score);
    this.zsets.set(key, zset);
  }

  zremSync(key: string, value: string) {
    this.zsets.get(key)?.delete(value);
  }

  incrbySync(key: string, amount: number) {
    const next = Number(this.strings.get(key) ?? 0) + amount;
    this.strings.set(key, String(next));
  }
}

function makeService(limitMb: number, maxValueBytes = 0) {
  const redis = new FakeRedis();
  return {
    redis,
    service: new UserCacheService({
      redis: redis as any,
      envService: {
        get: (key: string) => {
          if (key === 'NODE_NAME') return 'app-a';
          if (key === 'REDIS_USER_CACHE_LIMIT_MB') return limitMb;
          if (key === 'REDIS_USER_CACHE_MAX_VALUE_BYTES') return maxValueBytes;
          return undefined;
        },
      } as any,
    }),
  };
}

describe('UserCacheService', () => {
  it('stores $cache keys under the user_cache namespace', async () => {
    const { redis, service } = makeService(1);

    await service.set('feature', { enabled: true }, 1000);

    expect(redis.strings.get('app-a:user_cache:feature')).toBe(
      '{"enabled":true}',
    );
    expect(await service.get('feature')).toEqual({ enabled: true });
  });

  it('evicts least recently used keys when the user cache quota is exceeded', async () => {
    const { redis, service } = makeService(0.000015);

    await service.set('old', '1234567890', 0);
    await service.set('new', '1234567890', 0);

    expect(redis.strings.has('app-a:user_cache:old')).toBe(false);
    expect(redis.strings.get('app-a:user_cache:new')).toBe('1234567890');
  });

  it('rejects a single value larger than the configured cache value limit', async () => {
    const { service } = makeService(1, 4);

    await expect(service.set('too-big', '12345', 0)).rejects.toThrow(
      'REDIS_USER_CACHE_MAX_VALUE_BYTES',
    );
  });

  it('does not overwrite an existing key on acquire and touches LRU metadata', async () => {
    const { service } = makeService(1);

    expect(await service.acquire('lock', 'a', 1000)).toBe(true);
    expect(await service.acquire('lock', 'b', 1000)).toBe(false);
    expect(await service.get('lock')).toBe('a');
    expect(await service.release('lock', 'a')).toBe(true);
    expect(await service.get('lock')).toBeNull();
  });
});
