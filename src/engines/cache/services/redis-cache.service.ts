import { Redis } from 'ioredis';
import type { EnvService } from '../../../shared/services';
import type { ICache } from '../../../domain/shared/interfaces/cache.interface';
import type { RuntimeNamespaceLifecycleService } from './runtime-namespace-lifecycle.service';
import type { RedisCachePolicy } from '../types/redis-cache-policy.types';

export class RedisCacheService implements ICache {
  private readonly redis: Redis;
  private readonly namespace: string | null;
  private readonly policy: RedisCachePolicy;
  private readonly runtimeNamespaceLifecycleService?: RuntimeNamespaceLifecycleService;

  constructor(deps: {
    redis: Redis;
    envService: EnvService;
    runtimeNamespaceLifecycleService?: RuntimeNamespaceLifecycleService;
    policy: RedisCachePolicy;
  }) {
    this.redis = deps.redis;
    if (!this.redis) {
      throw new Error(
        'Redis connection not available - RedisCacheService cannot initialize',
      );
    }
    this.policy = deps.policy;
    this.runtimeNamespaceLifecycleService =
      deps.runtimeNamespaceLifecycleService;
    const nodeName = deps.envService.get('NODE_NAME') || '';
    this.namespace =
      nodeName || (deps.policy.requireNamespace ? 'enfyra' : null);
  }

  getQuota(): { limitBytes: number; maxValueBytes: number } | null {
    return this.policy.quota ?? null;
  }

  async acquire(key: string, value: any, ttlMs: number): Promise<boolean> {
    const decoratedKey = this.decorateLockKey(key);
    const result = await this.redis.set(
      decoratedKey,
      this.serialize(value),
      'PX',
      ttlMs > 0 ? ttlMs : this.lifecycleTtlMs(),
      'NX',
    );
    return result === 'OK';
  }

  async renew(key: string, value: any, ttlMs: number): Promise<boolean> {
    const decoratedKey = this.decorateLockKey(key);
    const lua = `
      if redis.call("get", KEYS[1]) == ARGV[1] then
        return redis.call("pexpire", KEYS[1], ARGV[2])
      else
        return 0
      end`;
    try {
      const renewed = await this.redis.eval(
        lua,
        1,
        decoratedKey,
        this.serialize(value),
        ttlMs > 0 ? ttlMs : this.lifecycleTtlMs(),
      );
      return renewed === 1;
    } catch {
      return false;
    }
  }

  async release(key: string, value: any): Promise<boolean> {
    const decoratedKey = this.decorateLockKey(key);
    const serializedValue = this.serialize(value);
    const lua = `
      if redis.call("get", KEYS[1]) == ARGV[1] then
        return redis.call("del", KEYS[1])
      else
        return 0
      end`;
    try {
      const deleted = await this.redis.eval(
        lua,
        1,
        decoratedKey,
        serializedValue,
      );
      return deleted === 1;
    } catch {
      return false;
    }
  }

  async get<T = any>(key: string): Promise<T | null> {
    const decoratedKey = this.decorateKey(key);
    const current = await this.redis.get(decoratedKey);
    if (this.policy.quota) {
      if (current === null) {
        await this.untrack(decoratedKey);
        return null;
      }
      await this.touch(decoratedKey);
    }
    return this.deserialize(current);
  }

  async set<T = any>(key: string, value: T, ttlMs: number): Promise<void> {
    const decoratedKey = this.decorateKey(key);
    const serializedValue = this.serialize(value);
    const size = this.valueSize(serializedValue);
    if (this.policy.quota) {
      this.assertValueSize(size);
      this.assertFitsLimit(size);
    }
    if (ttlMs > 0) {
      await this.redis.set(decoratedKey, serializedValue, 'PX', ttlMs);
    } else {
      await this.redis.set(
        decoratedKey,
        serializedValue,
        'PX',
        this.lifecycleTtlMs(),
      );
    }
    if (this.policy.quota) {
      await this.track(decoratedKey, size);
      await this.evictIfNeeded();
    }
  }

  async exists(key: string, value: any): Promise<boolean> {
    const decoratedKey = this.decorateKey(key);
    const current = await this.redis.get(decoratedKey);
    if (this.policy.quota) {
      if (current === null) {
        await this.untrack(decoratedKey);
        return false;
      }
      await this.touch(decoratedKey);
    }
    const parsed = this.deserialize(current);
    const checkValue = this.deserialize(this.serialize(value));
    return JSON.stringify(parsed) === JSON.stringify(checkValue);
  }

  async deleteKey(key: string): Promise<void> {
    const decoratedKey = this.decorateKey(key);
    await this.redis.del(decoratedKey);
    if (this.policy.quota) await this.untrack(decoratedKey);
  }

  async setNoExpire<T = any>(key: string, value: T): Promise<void> {
    await this.set(key, value, 0);
  }

  async clearAll(): Promise<void> {
    if (this.policy.clearAllMode === 'prefix') {
      let cursor = '0';
      do {
        const [nextCursor, keys] = await this.redis.scan(
          cursor,
          'MATCH',
          `${this.dataPrefix()}*`,
          'COUNT',
          100,
        );
        if (keys.length > 0) await this.redis.del(...keys);
        cursor = nextCursor;
      } while (cursor !== '0');
      await this.redis.del(this.lruKey(), this.sizesKey(), this.totalKey());
      return;
    }
    if (!this.namespace) {
      await this.redis.flushdb();
      return;
    }
    const pattern = `${this.namespace}:*`;
    const runtimeCachePrefix = `${this.namespace}:runtime_cache:`;
    let cursor = '0';
    do {
      const [nextCursor, keys] = await this.redis.scan(
        cursor,
        'MATCH',
        pattern,
        'COUNT',
        100,
      );
      const deletableKeys = keys.filter(
        (key) => !key.startsWith(runtimeCachePrefix),
      );
      if (deletableKeys.length > 0) {
        await this.redis.del(...deletableKeys);
      }
      cursor = nextCursor;
    } while (cursor !== '0');
  }

  private decorateKey(key: string): string {
    if (this.policy.quota) {
      if (!key || typeof key !== 'string')
        throw new Error('cache key is required');
      if (key.startsWith(this.dataPrefix())) return key;
      return `${this.dataPrefix()}${key}`;
    }
    if (!this.namespace) return key;
    return `${this.namespace}:${key}`;
  }

  private decorateLockKey(key: string): string {
    if (!this.policy.quota) return this.decorateKey(key);
    if (!key || typeof key !== 'string') {
      throw new Error('cache key is required');
    }
    const prefix = `${this.namespace}:${this.policy.keyPrefix.replace(/:$/, '')}_lock:`;
    return key.startsWith(prefix) ? key : `${prefix}${key}`;
  }

  private dataPrefix(): string {
    return `${this.namespace}:${this.policy.keyPrefix}`;
  }

  private metaPrefix(): string {
    return `${this.namespace}:${this.policy.keyPrefix.replace(/:$/, '')}_meta:`;
  }

  private lruKey(): string {
    return `${this.metaPrefix()}lru`;
  }

  private sizesKey(): string {
    return `${this.metaPrefix()}sizes`;
  }

  private totalKey(): string {
    return `${this.metaPrefix()}total_bytes`;
  }

  private serialize(value: any): string {
    return typeof value === 'string' ? value : JSON.stringify(value);
  }

  private deserialize(value: string | null): any {
    if (value === null) return null;
    try {
      return JSON.parse(value);
    } catch {
      return value;
    }
  }

  private valueSize(value: string): number {
    return Buffer.byteLength(value);
  }

  private assertValueSize(size: number): void {
    const maxValueBytes = this.policy.quota?.maxValueBytes ?? 0;
    if (maxValueBytes > 0 && size > maxValueBytes) {
      throw new Error(
        `$cache value is ${size} bytes, above REDIS_USER_CACHE_MAX_VALUE_BYTES=${maxValueBytes}`,
      );
    }
  }

  private assertFitsLimit(size: number): void {
    const limitBytes = this.policy.quota?.limitBytes ?? 0;
    if (limitBytes > 0 && size > limitBytes) {
      throw new Error(
        `$cache value is ${size} bytes, above REDIS_USER_CACHE_LIMIT_MB capacity ${limitBytes} bytes`,
      );
    }
  }

  private async touch(key: string): Promise<void> {
    const pipeline = this.redisTransaction();
    pipeline.zadd(this.lruKey(), Date.now(), key);
    this.touchMetadataKeys(pipeline);
    await pipeline.exec();
  }

  private async track(key: string, size: number): Promise<void> {
    const oldSize = Number((await this.redis.hget(this.sizesKey(), key)) ?? 0);
    const pipeline = this.redisTransaction();
    pipeline.hset(this.sizesKey(), key, size);
    pipeline.incrby(this.totalKey(), size - oldSize);
    pipeline.zadd(this.lruKey(), Date.now(), key);
    this.touchMetadataKeys(pipeline);
    await pipeline.exec();
  }

  private async untrack(key: string): Promise<void> {
    const oldSize = Number((await this.redis.hget(this.sizesKey(), key)) ?? 0);
    const pipeline = this.redisTransaction();
    pipeline.hdel(this.sizesKey(), key);
    pipeline.zrem(this.lruKey(), key);
    if (oldSize !== 0) pipeline.incrby(this.totalKey(), -oldSize);
    this.touchMetadataKeys(pipeline);
    await pipeline.exec();
  }

  private async evictIfNeeded(): Promise<void> {
    const limitBytes = this.policy.quota?.limitBytes ?? 0;
    if (limitBytes <= 0) return;
    let total = Number((await this.redis.get(this.totalKey())) ?? 0);
    while (total > limitBytes) {
      const [oldest] = await this.redis.zrange(this.lruKey(), 0, 0);
      if (!oldest) break;
      const size = Number(
        (await this.redis.hget(this.sizesKey(), oldest)) ?? 0,
      );
      const pipeline = this.redisTransaction();
      pipeline.del(oldest);
      pipeline.hdel(this.sizesKey(), oldest);
      pipeline.zrem(this.lruKey(), oldest);
      if (size !== 0) pipeline.incrby(this.totalKey(), -size);
      this.touchMetadataKeys(pipeline);
      await pipeline.exec();
      total -= size;
    }
  }

  private lifecycleTtlMs(): number {
    const ttlMs = this.runtimeNamespaceLifecycleService?.getKeyTtlMs();
    if (!ttlMs || ttlMs <= 0) {
      throw new Error('Runtime namespace lifecycle TTL is required');
    }
    return ttlMs;
  }

  private redisTransaction(): ReturnType<Redis['pipeline']> {
    const redis = this.redis as Redis & {
      multi?: () => ReturnType<Redis['pipeline']>;
    };
    return typeof redis.multi === 'function'
      ? redis.multi()
      : this.redis.pipeline();
  }

  private touchMetadataKeys(pipeline: ReturnType<Redis['pipeline']>): void {
    if (!this.runtimeNamespaceLifecycleService) {
      throw new Error('Runtime namespace lifecycle TTL is required');
    }
    const ttlMs = this.lifecycleTtlMs();
    pipeline.pexpire(this.lruKey(), ttlMs);
    pipeline.pexpire(this.sizesKey(), ttlMs);
    pipeline.pexpire(this.totalKey(), ttlMs);
  }
}
