import { randomUUID } from 'crypto';
import { serialize, deserialize } from 'node:v8';
import type { Redis } from 'ioredis';
import { EnvService } from '../../../shared/services';

export interface RedisRuntimeCacheSnapshot<T> {
  cacheIdentifier: string;
  version: number;
  updatedAt: string;
  data: T;
}

function normalizeForSnapshot(value: any): any {
  if (Array.isArray(value)) {
    return value.map((item) => normalizeForSnapshot(item));
  }
  if (value instanceof Map) {
    return new Map(
      Array.from(value.entries()).map(([key, item]) => [
        normalizeForSnapshot(key),
        normalizeForSnapshot(item),
      ]),
    );
  }
  if (value instanceof Set) {
    return new Set(
      Array.from(value.values()).map((item) => normalizeForSnapshot(item)),
    );
  }
  if (value instanceof Date) return value;
  if (
    value &&
    typeof value === 'object' &&
    typeof value.toHexString === 'function' &&
    value._bsontype === 'ObjectId'
  ) {
    return value.toHexString();
  }
  if (value && typeof value === 'object') {
    const out: Record<string, any> = {};
    for (const [key, item] of Object.entries(value)) {
      out[key] = normalizeForSnapshot(item);
    }
    return out;
  }
  return value;
}

function decodeLegacyJsonValue(value: any): any {
  if (Array.isArray(value)) {
    return value.map((item) => decodeLegacyJsonValue(item));
  }
  if (value && typeof value === 'object') {
    if (value.__enfyraType === 'Map' && Array.isArray(value.entries)) {
      return new Map(
        value.entries.map(([key, item]: [any, any]) => [
          decodeLegacyJsonValue(key),
          decodeLegacyJsonValue(item),
        ]),
      );
    }
    if (value.__enfyraType === 'Set' && Array.isArray(value.values)) {
      return new Set(
        value.values.map((item: any) => decodeLegacyJsonValue(item)),
      );
    }
    if (value.__enfyraType === 'Date' && typeof value.value === 'string') {
      return new Date(value.value);
    }
    const out: Record<string, any> = {};
    for (const [key, item] of Object.entries(value)) {
      out[key] = decodeLegacyJsonValue(item);
    }
    return out;
  }
  return value;
}

export class RedisRuntimeCacheStore {
  private readonly redis: Redis;
  private readonly nodeName: string;
  private readonly enabled: boolean;

  constructor(deps: { redis: Redis; envService: EnvService }) {
    this.redis = deps.redis;
    this.nodeName = deps.envService.get('NODE_NAME') || 'enfyra';
    this.enabled = deps.envService.get('REDIS_RUNTIME_CACHE') === true;
  }

  isEnabled(): boolean {
    return this.enabled;
  }

  private keySegment(value: string): string {
    return value.replace(/-/g, '_');
  }

  cacheKey(cacheIdentifier: string): string {
    return `${this.nodeName}:runtime_cache:${this.keySegment(cacheIdentifier)}`;
  }

  auxKey(cacheIdentifier: string, key: string): string {
    return `${this.cacheKey(cacheIdentifier)}:aux:${this.keySegment(key)}`;
  }

  private lockKey(cacheIdentifier: string): string {
    return `${this.cacheKey(cacheIdentifier)}:lock`;
  }

  private async getBuffer(key: string): Promise<Buffer | null> {
    const redisWithBuffer = this.redis as Redis & {
      getBuffer?: (key: string) => Promise<Buffer | null>;
    };
    if (typeof redisWithBuffer.getBuffer === 'function') {
      return redisWithBuffer.getBuffer(key);
    }
    const raw = await this.redis.get(key);
    return raw == null ? null : Buffer.from(raw);
  }

  private encode(value: any): Buffer {
    return serialize(normalizeForSnapshot(value));
  }

  private decode<T>(raw: Buffer): T {
    try {
      return deserialize(raw) as T;
    } catch {
      const parsed = JSON.parse(raw.toString('utf8'));
      return {
        ...parsed,
        data:
          parsed && typeof parsed === 'object' && 'data' in parsed
            ? decodeLegacyJsonValue(parsed.data)
            : decodeLegacyJsonValue(parsed),
      } as T;
    }
  }

  async getSnapshot<T>(
    cacheIdentifier: string,
  ): Promise<RedisRuntimeCacheSnapshot<T> | null> {
    if (!this.enabled) return null;
    const raw = await this.getBuffer(this.cacheKey(cacheIdentifier));
    if (!raw) return null;
    return this.decode<RedisRuntimeCacheSnapshot<T>>(raw);
  }

  async setSnapshot<T>(
    cacheIdentifier: string,
    data: T,
  ): Promise<RedisRuntimeCacheSnapshot<T>> {
    const snapshot: RedisRuntimeCacheSnapshot<T> = {
      cacheIdentifier,
      version: Date.now(),
      updatedAt: new Date().toISOString(),
      data,
    };
    await this.redis.set(this.cacheKey(cacheIdentifier), this.encode(snapshot));
    return snapshot;
  }

  async getAux<T>(cacheIdentifier: string, key: string): Promise<T | null> {
    if (!this.enabled) return null;
    const raw = await this.getBuffer(this.auxKey(cacheIdentifier, key));
    if (!raw) return null;
    return this.decode<T>(raw);
  }

  async setAux<T>(
    cacheIdentifier: string,
    key: string,
    value: T,
  ): Promise<void> {
    if (!this.enabled) return;
    await this.redis.set(
      this.auxKey(cacheIdentifier, key),
      this.encode(value),
    );
  }

  async deleteAuxByPrefix(
    cacheIdentifier: string,
    keyPrefix: string,
  ): Promise<void> {
    if (!this.enabled) return;
    const pattern = this.auxKey(cacheIdentifier, `${keyPrefix}*`);
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

  async acquireRefreshLock(
    cacheIdentifier: string,
    ttlMs = 30000,
  ): Promise<string | null> {
    if (!this.enabled) return null;
    const token = randomUUID();
    const result = await this.redis.set(
      this.lockKey(cacheIdentifier),
      token,
      'PX',
      ttlMs,
      'NX',
    );
    return result === 'OK' ? token : null;
  }

  async acquireRefreshLockWithWait(
    cacheIdentifier: string,
    ttlMs = 30000,
    timeoutMs = 35000,
  ): Promise<string | null> {
    const deadline = Date.now() + timeoutMs;
    do {
      const token = await this.acquireRefreshLock(cacheIdentifier, ttlMs);
      if (token) return token;
      await new Promise((resolve) => setTimeout(resolve, 50));
    } while (Date.now() < deadline);
    return null;
  }

  async releaseRefreshLock(
    cacheIdentifier: string,
    token: string,
  ): Promise<void> {
    if (!this.enabled) return;
    const lua = `
      if redis.call("get", KEYS[1]) == ARGV[1] then
        return redis.call("del", KEYS[1])
      end
      return 0
    `;
    await this.redis.eval(lua, 1, this.lockKey(cacheIdentifier), token);
  }

  async waitForSnapshot<T>(
    cacheIdentifier: string,
    timeoutMs = 10000,
  ): Promise<RedisRuntimeCacheSnapshot<T> | null> {
    const deadline = Date.now() + timeoutMs;
    do {
      const snapshot = await this.getSnapshot<T>(cacheIdentifier);
      if (snapshot) return snapshot;
      await new Promise((resolve) => setTimeout(resolve, 50));
    } while (Date.now() < deadline);
    return null;
  }
}
