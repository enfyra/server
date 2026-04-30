import { Redis } from 'ioredis';
import { EnvService } from '../../../shared/services';
import { ICache } from '../../../domain/shared/interfaces/cache.interface';

export class UserCacheService implements ICache {
  private readonly redis: Redis;
  private readonly nodeName: string;
  private readonly limitBytes: number;
  private readonly maxValueBytes: number;

  constructor(deps: { redis: Redis; envService: EnvService }) {
    this.redis = deps.redis;
    this.nodeName = deps.envService.get('NODE_NAME') || 'enfyra';
    this.limitBytes =
      Number(deps.envService.get('REDIS_USER_CACHE_LIMIT_MB') || 0) *
      1024 *
      1024;
    this.maxValueBytes = Number(
      deps.envService.get('REDIS_USER_CACHE_MAX_VALUE_BYTES') || 0,
    );
  }

  async acquire(key: string, value: any, ttlMs: number): Promise<boolean> {
    const decoratedKey = this.decorateKey(key);
    const serializedValue = this.serialize(value);
    const size = this.valueSize(serializedValue);
    this.assertValueSize(size);
    this.assertFitsLimit(size);
    const result = await this.redis.set(
      decoratedKey,
      serializedValue,
      'PX',
      ttlMs,
      'NX',
    );
    if (result !== 'OK') {
      await this.touch(decoratedKey);
      return false;
    }
    await this.track(decoratedKey, size);
    await this.evictIfNeeded();
    return true;
  }

  async release(key: string, value: any): Promise<boolean> {
    const decoratedKey = this.decorateKey(key);
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
      if (deleted === 1) await this.untrack(decoratedKey);
      return deleted === 1;
    } catch {
      return false;
    }
  }

  async get<T = any>(key: string): Promise<T | null> {
    const decoratedKey = this.decorateKey(key);
    const current = await this.redis.get(decoratedKey);
    if (current === null) {
      await this.untrack(decoratedKey);
      return null;
    }
    await this.touch(decoratedKey);
    return this.deserialize(current);
  }

  async set<T = any>(key: string, value: T, ttlMs: number): Promise<void> {
    const decoratedKey = this.decorateKey(key);
    const serializedValue = this.serialize(value);
    const size = this.valueSize(serializedValue);
    this.assertValueSize(size);
    this.assertFitsLimit(size);
    if (ttlMs > 0) {
      await this.redis.set(decoratedKey, serializedValue, 'PX', ttlMs);
    } else {
      await this.redis.set(decoratedKey, serializedValue);
    }
    await this.track(decoratedKey, size);
    await this.evictIfNeeded();
  }

  async exists(key: string, value: any): Promise<boolean> {
    const decoratedKey = this.decorateKey(key);
    const current = await this.redis.get(decoratedKey);
    if (current === null) {
      await this.untrack(decoratedKey);
      return false;
    }
    await this.touch(decoratedKey);
    const parsed = this.deserialize(current);
    const checkValue = this.deserialize(this.serialize(value));
    return JSON.stringify(parsed) === JSON.stringify(checkValue);
  }

  async deleteKey(key: string): Promise<void> {
    const decoratedKey = this.decorateKey(key);
    await this.redis.del(decoratedKey);
    await this.untrack(decoratedKey);
  }

  async setNoExpire<T = any>(key: string, val: T): Promise<void> {
    await this.set(key, val, 0);
  }

  async clearAll(): Promise<void> {
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
  }

  private decorateKey(key: string): string {
    if (!key || typeof key !== 'string') throw new Error('cache key is required');
    if (key.startsWith(this.dataPrefix())) return key;
    return `${this.dataPrefix()}${key}`;
  }

  private dataPrefix(): string {
    return `${this.nodeName}:user_cache:`;
  }

  private lruKey(): string {
    return `${this.nodeName}:user_cache_meta:lru`;
  }

  private sizesKey(): string {
    return `${this.nodeName}:user_cache_meta:sizes`;
  }

  private totalKey(): string {
    return `${this.nodeName}:user_cache_meta:total_bytes`;
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
    if (this.maxValueBytes > 0 && size > this.maxValueBytes) {
      throw new Error(
        `$cache value is ${size} bytes, above REDIS_USER_CACHE_MAX_VALUE_BYTES=${this.maxValueBytes}`,
      );
    }
  }

  private assertFitsLimit(size: number): void {
    if (this.limitBytes > 0 && size > this.limitBytes) {
      throw new Error(
        `$cache value is ${size} bytes, above REDIS_USER_CACHE_LIMIT_MB capacity ${this.limitBytes} bytes`,
      );
    }
  }

  private async touch(key: string): Promise<void> {
    await this.redis.zadd(this.lruKey(), Date.now(), key);
  }

  private async track(key: string, size: number): Promise<void> {
    const oldSize = Number((await this.redis.hget(this.sizesKey(), key)) ?? 0);
    await this.redis
      .pipeline()
      .hset(this.sizesKey(), key, size)
      .incrby(this.totalKey(), size - oldSize)
      .zadd(this.lruKey(), Date.now(), key)
      .exec();
  }

  private async untrack(key: string): Promise<void> {
    const oldSize = Number((await this.redis.hget(this.sizesKey(), key)) ?? 0);
    await this.redis
      .pipeline()
      .hdel(this.sizesKey(), key)
      .zrem(this.lruKey(), key)
      .incrby(this.totalKey(), -oldSize)
      .exec();
  }

  private async evictIfNeeded(): Promise<void> {
    if (this.limitBytes <= 0) return;
    let total = Number((await this.redis.get(this.totalKey())) ?? 0);
    while (total > this.limitBytes) {
      const [oldest] = await this.redis.zrange(this.lruKey(), 0, 0);
      if (!oldest) break;
      const size = Number((await this.redis.hget(this.sizesKey(), oldest)) ?? 0);
      await this.redis
        .pipeline()
        .del(oldest)
        .hdel(this.sizesKey(), oldest)
        .zrem(this.lruKey(), oldest)
        .incrby(this.totalKey(), -size)
        .exec();
      total -= size;
    }
  }
}
