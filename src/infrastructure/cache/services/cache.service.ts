import { Redis } from 'ioredis';
import { EnvService } from '../../../shared/services/env.service';

export class CacheService {
  private readonly redis: Redis;
  private readonly nodeName: string | null;
  private readonly envService: EnvService;
  constructor(deps: { redis: Redis; envService: EnvService }) {
    this.redis = deps.redis;
    this.envService = deps.envService;
    if (!this.redis) {
      throw new Error(
        'Redis connection not available - CacheService cannot initialize',
      );
    }
    this.nodeName = this.envService.get('NODE_NAME') || null;
  }
  private decorateKey(key: string): string {
    if (!this.nodeName) {
      return key;
    }
    return `${this.nodeName}:${key}`;
  }
  private serialize(value: any): string {
    if (typeof value === 'string') return value;
    return JSON.stringify(value);
  }
  private deserialize(value: string | null): any {
    if (value === null) return null;
    try {
      return JSON.parse(value);
    } catch {
      return value;
    }
  }
  async acquire(key: string, value: any, ttlMs: number): Promise<boolean> {
    const decoratedKey = this.decorateKey(key);
    const serializedValue = this.serialize(value);
    const result = await this.redis.set(
      decoratedKey,
      serializedValue,
      'PX',
      ttlMs,
      'NX',
    );
    return result === 'OK';
  }
  async release(key: string, value: any): Promise<boolean> {
    const decoratedKey = this.decorateKey(key);
    const lua = `
      if redis.call("get", KEYS[1]) == ARGV[1] then
        return redis.call("del", KEYS[1])
      else
        return 0
      end`;
    const serializedValue = this.serialize(value);
    try {
      const deleted = await this.redis.eval(
        lua,
        1,
        decoratedKey,
        serializedValue,
      );
      return deleted === 1;
    } catch (error) {
      return false;
    }
  }
  async get<T = any>(key: string): Promise<T | null> {
    const decoratedKey = this.decorateKey(key);
    const current = await this.redis.get(decoratedKey);
    const parsed = this.deserialize(current);
    return parsed;
  }
  async set<T = any>(key: string, value: T, ttlMs: number): Promise<void> {
    const decoratedKey = this.decorateKey(key);
    const serializedValue = this.serialize(value);
    if (ttlMs > 0) {
      await this.redis.set(decoratedKey, serializedValue, 'PX', ttlMs);
    } else {
      await this.redis.set(decoratedKey, serializedValue);
    }
  }
  async exists(key: string, value: any): Promise<boolean> {
    const decoratedKey = this.decorateKey(key);
    const current = await this.redis.get(decoratedKey);
    const parsed = this.deserialize(current);
    const checkValue = this.deserialize(this.serialize(value));
    const isEqual = JSON.stringify(parsed) === JSON.stringify(checkValue);
    return isEqual;
  }
  async deleteKey(key: string): Promise<void> {
    const decoratedKey = this.decorateKey(key);
    await this.redis.del(decoratedKey);
  }
  async setNoExpire<T = any>(key: string, val: T): Promise<void> {
    const decoratedKey = this.decorateKey(key);
    await this.redis.set(decoratedKey, JSON.stringify(val));
  }
  async clearAll(): Promise<void> {
    if (!this.nodeName) {
      await this.redis.flushdb();
      return;
    }
    const pattern = `${this.nodeName}:*`;
    let cursor = '0';
    do {
      const [nextCursor, keys] = await this.redis.scan(
        cursor,
        'MATCH',
        pattern,
        'COUNT',
        100,
      );
      if (keys.length > 0) {
        await this.redis.del(...keys);
      }
      cursor = nextCursor;
    } while (cursor !== '0');
  }
}
