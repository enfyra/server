import type { Redis } from 'ioredis';
import { EnvService, InstanceService } from '../../../shared/services';
import { Logger } from '../../../shared/logger';
import { SYSTEM_QUEUES } from '../../../shared/utils/constant';

type Timer = ReturnType<typeof setInterval>;

const DEFAULT_KEY_TTL_MS = 7 * 24 * 60 * 60 * 1000;
const DEFAULT_LEASE_TTL_MS = 5 * 60 * 1000;
const DEFAULT_RENEW_INTERVAL_MS = 60 * 1000;
const DEFAULT_JANITOR_INTERVAL_MS = 5 * 60 * 1000;
const DEFAULT_STALE_GRACE_MS = 24 * 60 * 60 * 1000;
const MIN_KEY_TTL_MS = 24 * 60 * 60 * 1000;
const SCAN_COUNT = 250;

export interface RuntimeNamespaceCleanupResult {
  namespace: string;
  deleted: number;
}

export class RuntimeNamespaceLifecycleService {
  private readonly logger = new Logger(RuntimeNamespaceLifecycleService.name);
  private readonly redis: Redis;
  private readonly envService: EnvService;
  private readonly instanceService: InstanceService;
  private readonly nodeName: string;
  private readonly instanceId: string;
  private readonly enabled: boolean;
  private readonly keyTtlMs: number;
  private readonly leaseTtlMs: number;
  private readonly renewIntervalMs: number;
  private readonly janitorIntervalMs: number;
  private readonly staleGraceMs: number;
  private renewTimer?: Timer;
  private janitorTimer?: Timer;
  private running = false;
  private renewing = false;
  private cleaning = false;

  constructor(deps: {
    redis: Redis;
    envService: EnvService;
    instanceService: InstanceService;
  }) {
    this.redis = deps.redis;
    this.envService = deps.envService;
    this.instanceService = deps.instanceService;
    this.nodeName = deps.envService.get('NODE_NAME') || 'enfyra';
    this.instanceId = deps.instanceService.getInstanceId();
    this.enabled =
      deps.envService.get('REDIS_NAMESPACE_LIFECYCLE_ENABLED') !== false;
    this.keyTtlMs = this.readPositiveNumber(
      'REDIS_NAMESPACE_KEY_TTL_MS',
      DEFAULT_KEY_TTL_MS,
      MIN_KEY_TTL_MS,
    );
    this.leaseTtlMs = this.readPositiveNumber(
      'REDIS_NAMESPACE_LEASE_TTL_MS',
      DEFAULT_LEASE_TTL_MS,
    );
    this.renewIntervalMs = Math.min(
      this.readPositiveNumber(
        'REDIS_NAMESPACE_RENEW_INTERVAL_MS',
        Math.min(
          DEFAULT_RENEW_INTERVAL_MS,
          Math.max(1000, this.leaseTtlMs / 3),
        ),
      ),
      Math.max(1000, Math.floor(this.keyTtlMs / 3)),
    );
    this.janitorIntervalMs = this.readPositiveNumber(
      'REDIS_NAMESPACE_JANITOR_INTERVAL_MS',
      DEFAULT_JANITOR_INTERVAL_MS,
    );
    this.staleGraceMs = this.readPositiveNumber(
      'REDIS_NAMESPACE_STALE_GRACE_MS',
      DEFAULT_STALE_GRACE_MS,
    );
  }

  async init(): Promise<void> {
    if (!this.enabled || this.running) return;
    this.running = true;
    await this.heartbeat();
    await this.renewCurrentNamespaceKeys();
    await this.cleanupStaleNamespaces();
    this.renewTimer = setInterval(() => {
      void this.renewCurrentNamespaceKeys();
    }, this.renewIntervalMs);
    this.janitorTimer = setInterval(() => {
      void this.cleanupStaleNamespaces();
    }, this.janitorIntervalMs);
    this.renewTimer.unref?.();
    this.janitorTimer.unref?.();
  }

  async onDestroy(): Promise<void> {
    this.running = false;
    if (this.renewTimer) clearInterval(this.renewTimer);
    if (this.janitorTimer) clearInterval(this.janitorTimer);
    this.renewTimer = undefined;
    this.janitorTimer = undefined;
    if (!this.enabled) return;
    await this.redis.del(this.currentLeaseKey());
  }

  getKeyTtlMs(): number {
    return this.keyTtlMs;
  }

  async touchKey(key: string, ttlMs = this.keyTtlMs): Promise<void> {
    if (!this.enabled || ttlMs <= 0) return;
    await this.redis.pexpire(key, ttlMs);
  }

  async touchKeys(keys: string[], ttlMs = this.keyTtlMs): Promise<void> {
    if (!this.enabled || ttlMs <= 0 || keys.length === 0) return;
    const pipeline = this.redis.pipeline();
    for (const key of keys) pipeline.pexpire(key, ttlMs);
    await pipeline.exec();
  }

  async renewCurrentNamespaceKeys(): Promise<void> {
    if (!this.enabled || this.renewing) return;
    this.renewing = true;
    try {
      await this.heartbeat();
      for (const pattern of this.renewableNamespacePatterns(this.nodeName)) {
        await this.expireByPattern(pattern, this.keyTtlMs);
      }
    } catch (error) {
      this.logger.warn(
        `Runtime namespace renew failed: ${(error as Error).message}`,
      );
    } finally {
      this.renewing = false;
    }
  }

  async cleanupStaleNamespaces(
    now = Date.now(),
  ): Promise<RuntimeNamespaceCleanupResult[]> {
    if (!this.enabled || this.cleaning) return [];
    this.cleaning = true;
    try {
      const staleBefore = now - this.staleGraceMs;
      const namespaces = await this.redis.zrangebyscore(
        this.registryKey(),
        0,
        staleBefore,
      );
      const results: RuntimeNamespaceCleanupResult[] = [];
      for (const namespace of namespaces) {
        if (!namespace || namespace === this.nodeName) continue;
        if (await this.hasActiveLease(namespace)) continue;
        const deleted = await this.cleanupNamespace(namespace);
        await this.redis.zrem(this.registryKey(), namespace);
        await this.redis.del(this.namespaceMetaKey(namespace));
        results.push({ namespace, deleted });
      }
      return results;
    } catch (error) {
      this.logger.warn(
        `Runtime namespace cleanup failed: ${(error as Error).message}`,
      );
      return [];
    } finally {
      this.cleaning = false;
    }
  }

  async cleanupNamespace(namespace: string): Promise<number> {
    if (!this.enabled || !namespace || namespace === this.nodeName) return 0;
    let deleted = 0;
    for (const pattern of this.cleanupNamespacePatterns(namespace)) {
      deleted += await this.unlinkByPattern(pattern);
    }
    return deleted;
  }

  private async heartbeat(): Promise<void> {
    const now = Date.now();
    const payload = JSON.stringify({
      namespace: this.nodeName,
      instanceId: this.instanceId,
      updatedAt: new Date(now).toISOString(),
    });
    await this.redis
      .pipeline()
      .set(this.currentLeaseKey(), payload, 'PX', this.leaseTtlMs)
      .zadd(this.registryKey(), now, this.nodeName)
      .hset(this.namespaceMetaKey(this.nodeName), {
        namespace: this.nodeName,
        updatedAt: new Date(now).toISOString(),
      })
      .pexpire(this.namespaceMetaKey(this.nodeName), this.keyTtlMs)
      .pexpire(this.registryKey(), this.keyTtlMs)
      .exec();
  }

  private async hasActiveLease(namespace: string): Promise<boolean> {
    let cursor = '0';
    const pattern = `${namespace}:runtime_lifecycle:lease:*`;
    do {
      const [nextCursor, keys] = await this.redis.scan(
        cursor,
        'MATCH',
        pattern,
        'COUNT',
        SCAN_COUNT,
      );
      if (keys.length > 0) return true;
      cursor = nextCursor;
    } while (cursor !== '0');
    return false;
  }

  private async expireByPattern(pattern: string, ttlMs: number): Promise<void> {
    let cursor = '0';
    do {
      const [nextCursor, keys] = await this.redis.scan(
        cursor,
        'MATCH',
        pattern,
        'COUNT',
        SCAN_COUNT,
      );
      await this.touchKeys(keys, ttlMs);
      cursor = nextCursor;
    } while (cursor !== '0');
  }

  private async unlinkByPattern(pattern: string): Promise<number> {
    let cursor = '0';
    let deleted = 0;
    do {
      const [nextCursor, keys] = await this.redis.scan(
        cursor,
        'MATCH',
        pattern,
        'COUNT',
        SCAN_COUNT,
      );
      if (keys.length > 0) {
        deleted += await this.unlinkKeys(keys);
      }
      cursor = nextCursor;
    } while (cursor !== '0');
    return deleted;
  }

  private async unlinkKeys(keys: string[]): Promise<number> {
    const redisWithUnlink = this.redis as Redis & {
      unlink?: (...keys: string[]) => Promise<number>;
    };
    if (typeof redisWithUnlink.unlink === 'function') {
      return redisWithUnlink.unlink(...keys);
    }
    return this.redis.del(...keys);
  }

  private renewableNamespacePatterns(namespace: string): string[] {
    return [
      `${namespace}:runtime_lifecycle:*`,
      `${namespace}:runtime_cache:*`,
      `${namespace}:runtime-monitor:*`,
      `${namespace}:cluster-telemetry:*`,
      `${namespace}:user_cache:*`,
      `${namespace}:user_cache_meta:*`,
      `${namespace}:rl:*`,
      `${namespace}:socket.io:*`,
      `${namespace}:coord:sql:*`,
      ...Object.values(SYSTEM_QUEUES).map((queue) => `${namespace}:${queue}:*`),
    ];
  }

  private cleanupNamespacePatterns(namespace: string): string[] {
    return this.renewableNamespacePatterns(namespace);
  }

  private registryKey(): string {
    return 'enfyra:runtime_namespaces';
  }

  private namespaceMetaKey(namespace: string): string {
    return `enfyra:runtime_namespace:${namespace}`;
  }

  private currentLeaseKey(): string {
    return `${this.nodeName}:runtime_lifecycle:lease:${this.instanceId}`;
  }

  private readPositiveNumber(
    key: Parameters<EnvService['get']>[0],
    fallback: number,
    minValue = 1,
  ): number {
    const value = Number(this.envService.get(key));
    if (!Number.isFinite(value) || value <= 0) return fallback;
    if (this.envService.get('NODE_ENV') === 'test') return value;
    return Math.max(value, minValue);
  }
}
