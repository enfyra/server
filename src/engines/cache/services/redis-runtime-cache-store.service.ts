import { randomUUID } from 'crypto';
import { AsyncLocalStorage } from 'node:async_hooks';
import type { RuntimeCacheCommittedReadScope, RuntimeCachePublicationScope } from '../types/runtime-cache-publication.types';
import { serialize, deserialize } from 'node:v8';
import type { Redis } from 'ioredis';
import { EnvService } from '../../../shared/services';
import { RuntimeNamespaceLifecycleService } from './runtime-namespace-lifecycle.service';

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

function escapeRedisGlob(value: string): string {
  return value.replace(/[*?[\]\\]/g, (char) => `\\${char}`);
}

export class RedisRuntimeCacheStore {
  private readonly redis: Redis;
  private readonly publication = new AsyncLocalStorage<RuntimeCachePublicationScope>();
  private readonly committedRead = new AsyncLocalStorage<RuntimeCacheCommittedReadScope>();
  private readonly nodeName: string;
  private readonly enabled: boolean;
  private readonly runtimeNamespaceLifecycleService: RuntimeNamespaceLifecycleService | undefined;

  constructor(deps: {
    redis: Redis;
    envService: EnvService;
    runtimeNamespaceLifecycleService?: RuntimeNamespaceLifecycleService;
  }) {
    this.redis = deps.redis;
    this.nodeName = deps.envService.get('NODE_NAME') || 'enfyra';
    this.enabled = deps.envService.get('REDIS_RUNTIME_CACHE') === true;
    this.runtimeNamespaceLifecycleService =
      deps.runtimeNamespaceLifecycleService;
  }

  isEnabled(): boolean {
    return this.enabled;
  }

  private committedKey(): string {
    return `${this.nodeName}:runtime_cache:committed`;
  }

  private generationKey(): string {
    return `${this.nodeName}:runtime_cache:generation`;
  }

  async readCommittedGeneration(): Promise<string | null> {
    if (!this.enabled) return null;
    try {
      const value = await this.redis.get(this.generationKey());
      return value ? String(value) : null;
    } catch {
      return null;
    }
  }

  private getActiveCommittedRead(): RuntimeCacheCommittedReadScope | undefined {
    const scope = this.committedRead.getStore();
    return scope?.active ? scope : undefined;
  }

  async runWithCommittedSnapshots<T>(work: () => Promise<T>): Promise<T> {
    if (!this.enabled || this.getActiveCommittedRead()) return work();
    const values = await this.redis.hgetallBuffer(this.committedKey());
    const scope: RuntimeCacheCommittedReadScope = {
      active: true,
      snapshots: new Map(Object.entries(values)),
    };
    try {
      return await this.committedRead.run(scope, work);
    } finally {
      scope.active = false;
      scope.snapshots.clear();
    }
  }

  private getActivePublication(): RuntimeCachePublicationScope | undefined {
    const scope = this.publication.getStore();
    return scope?.active ? scope : undefined;
  }

  async runWithPublication<T>(work: () => Promise<T>): Promise<T> {
    if (!this.enabled) return work();
    if (this.getActiveCommittedRead()) {
      throw new Error('Runtime cache publication is not allowed during committed snapshot replay');
    }
    if (this.getActivePublication()) return work();
    const token = await this.acquireRefreshLockWithWait('publication');
    if (!token) throw new Error('Runtime cache publication lock timed out');
    const scope: RuntimeCachePublicationScope = {
      active: true,
      token,
      snapshots: new Map(),
      auxiliary: new Map(),
      deletedAuxiliary: new Set(),
    };
    const heartbeat = setInterval(() => {
      void this.redis.eval(`if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('pexpire', KEYS[1], ARGV[2]) end return 0`, 1, this.lockKey('publication'), token, 30000).catch(() => undefined);
    }, 10000);
    heartbeat.unref();
    try {
      return await this.publication.run(scope, async () => {
        const result = await work();
        await this.commitPublication();
        return result;
      });
    } finally {
      scope.active = false;
      scope.snapshots.clear();
      scope.auxiliary.clear();
      scope.deletedAuxiliary.clear();
      clearInterval(heartbeat);
      await this.releaseRefreshLock('publication', token);
    }
  }

  async commitPublication(): Promise<void> {
    const scope = this.getActivePublication();
    if (!scope || scope.committed) return;
    const snapshots = [...scope.snapshots];
    const auxiliary = [...scope.auxiliary];
    const deletedAuxiliary = [...scope.deletedAuxiliary];
    const ttl = this.lifecycleTtlMs();
    const script = `
      if redis.call('get', KEYS[1]) ~= ARGV[1] then return 0 end
      local count = tonumber(ARGV[3])
      local auxiliaryCount = tonumber(ARGV[4])
      for i = 3 + count + auxiliaryCount, #KEYS do
        redis.call('hdel', KEYS[2], 'aux:' .. KEYS[i])
        redis.call('del', KEYS[i])
      end
      for i = 1, count do
        local offset = 5 + (i - 1) * 2
        redis.call('hset', KEYS[2], ARGV[offset], ARGV[offset + 1])
        redis.call('set', KEYS[2 + i], ARGV[offset + 1], 'PX', ARGV[2])
      end
      for i = 1, auxiliaryCount do
        local key = KEYS[2 + count + i]
        local value = ARGV[4 + count * 2 + i]
        redis.call('hset', KEYS[2], 'aux:' .. key, value)
        redis.call('set', key, value, 'PX', ARGV[2])
      end
      redis.call('pexpire', KEYS[2], ARGV[2])
      return 1
    `;
    const writtenKeys = [this.committedKey(), ...snapshots.map(([id]) => this.cacheKey(id)), ...auxiliary.map(([key]) => key)];
    const keys = [this.lockKey('publication'), ...writtenKeys, ...deletedAuxiliary];
    const committed = await this.redis.eval(script, keys.length, ...keys, scope.token!, ttl, snapshots.length, auxiliary.length, ...snapshots.flatMap(([id, data]) => [id, data]), ...auxiliary.map(([, data]) => data));
    if (committed !== 1) throw new Error('Runtime cache publication lease lost');
    scope.committed = true;
    await this.redis
      .set(this.generationKey(), String(Date.now()), 'PX', ttl)
      .catch(() => undefined);
    for (const key of writtenKeys) await this.runtimeNamespaceLifecycleService?.registerManagedKey(key);
    await this.runtimeNamespaceLifecycleService?.registerManagedKey(this.generationKey());
    for (const key of deletedAuxiliary) await this.runtimeNamespaceLifecycleService?.unregisterManagedKey(key);
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
    const key = this.cacheKey(cacheIdentifier);
    const staged = this.getActivePublication()?.snapshots.get(cacheIdentifier);
    const pinned = this.getActiveCommittedRead()?.snapshots;
    const raw = staged ?? (pinned ? pinned.get(cacheIdentifier) : await this.redis.hgetBuffer(this.committedKey(), cacheIdentifier) ?? await this.getBuffer(key));
    if (!raw) return null;
    await this.touchLifecycleKey(key);
    await this.touchLifecycleKey(this.committedKey());
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
    if (!this.enabled) return snapshot;
    const staged = this.getActivePublication();
    if (!staged) return this.runWithPublication(() => this.setSnapshot(cacheIdentifier, data));
    if (staged.committed) throw new Error('Runtime cache publication is already committed');
    staged.snapshots.set(cacheIdentifier, this.encode(snapshot));
    return snapshot;
  }

  async getAux<T>(cacheIdentifier: string, key: string): Promise<T | null> {
    if (!this.enabled) return null;
    const redisKey = this.auxKey(cacheIdentifier, key);
    const scope = this.getActivePublication();
    if (scope?.deletedAuxiliary.has(redisKey)) return null;
    const pinned = this.getActiveCommittedRead()?.snapshots;
    const raw = scope?.auxiliary.get(redisKey) ?? (pinned ? pinned.get(`aux:${redisKey}`) : await this.redis.hgetBuffer(this.committedKey(), `aux:${redisKey}`) ?? await this.getBuffer(redisKey));
    if (!raw) return null;
    await this.touchLifecycleKey(redisKey);
    return this.decode<T>(raw);
  }

  async setAux<T>(
    cacheIdentifier: string,
    key: string,
    value: T,
  ): Promise<void> {
    if (!this.enabled) return;
    const redisKey = this.auxKey(cacheIdentifier, key);
    const staged = this.getActivePublication();
    if (!staged) return this.runWithPublication(() => this.setAux(cacheIdentifier, key, value));
    if (staged.committed) throw new Error('Runtime cache publication is already committed');
    staged.deletedAuxiliary.delete(redisKey);
    staged.auxiliary.set(redisKey, this.encode(value));
  }

  async deleteAuxByPrefix(
    cacheIdentifier: string,
    keyPrefix: string,
  ): Promise<void> {
    if (!this.enabled) return;
    const scope = this.getActivePublication();
    if (!scope) return this.runWithPublication(() => this.deleteAuxByPrefix(cacheIdentifier, keyPrefix));
    if (scope.committed) throw new Error('Runtime cache publication is already committed');
    const prefix = this.auxKey(cacheIdentifier, keyPrefix);
    const committed = await this.redis.hgetallBuffer(this.committedKey());
    for (const field of Object.keys(committed)) {
      if (field.startsWith(`aux:${prefix}`)) scope.deletedAuxiliary.add(field.slice(4));
    }
    for (const key of scope.auxiliary.keys()) {
      if (key.startsWith(prefix)) {
        scope.auxiliary.delete(key);
        scope.deletedAuxiliary.add(key);
      }
    }
    const pattern = this.auxKey(cacheIdentifier, `${escapeRedisGlob(keyPrefix)}*`);
    let cursor = '0';
    do {
      const [nextCursor, keys] = await this.redis.scan(cursor, 'MATCH', pattern, 'COUNT', 100);
      for (const key of keys) scope.deletedAuxiliary.add(key);
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

  private lifecycleTtlMs(): number {
    const ttlMs = this.runtimeNamespaceLifecycleService?.getKeyTtlMs();
    if (!ttlMs || ttlMs <= 0) {
      throw new Error('Runtime namespace lifecycle TTL is required');
    }
    return ttlMs;
  }

  private async touchLifecycleKey(key: string): Promise<void> {
    if (!this.runtimeNamespaceLifecycleService) {
      throw new Error('Runtime namespace lifecycle TTL is required');
    }
    await this.runtimeNamespaceLifecycleService.touchKey(key);
    await this.runtimeNamespaceLifecycleService.registerManagedKey(key);
  }
}
