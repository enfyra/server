import type { Redis } from 'ioredis';
import { EnvService, InstanceService } from '../../../shared/services';
import { Logger } from '../../../shared/logger';
type Timer = ReturnType<typeof setInterval>;

const DEFAULT_KEY_TTL_MS = 30 * 60 * 1000;
const DEFAULT_LEASE_TTL_MS = 60 * 1000;
const DEFAULT_RENEW_INTERVAL_MS = 20 * 1000;
const MIN_KEY_TTL_MS = 5 * 60 * 1000;
const SCAN_COUNT = 250;

export class RuntimeNamespaceLifecycleService {
  private readonly logger = new Logger(RuntimeNamespaceLifecycleService.name);
  private readonly redis: Redis;
  private readonly envService: EnvService;
  private readonly instanceService: InstanceService;
  private readonly nodeName: string;
  private readonly instanceId: string;
  private readonly keyTtlMs: number;
  private readonly leaseTtlMs: number;
  private readonly renewIntervalMs: number;
  private renewTimer?: Timer;
  private running = false;
  private renewing = false;

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
  }

  async init(): Promise<void> {
    if (this.running) return;
    this.running = true;
    await this.heartbeat();
    await this.renewCurrentNamespaceKeys();
    this.renewTimer = setInterval(() => {
      void this.renewCurrentNamespaceKeys();
    }, this.renewIntervalMs);
    this.renewTimer.unref?.();
  }

  async onDestroy(): Promise<void> {
    this.running = false;
    if (this.renewTimer) clearInterval(this.renewTimer);
    this.renewTimer = undefined;
    await this.redis.del(this.currentLeaseKey());
  }

  getKeyTtlMs(): number {
    return this.keyTtlMs;
  }

  async touchKey(key: string, ttlMs = this.keyTtlMs): Promise<void> {
    if (ttlMs <= 0) return;
    await this.redis.pexpire(key, ttlMs);
  }

  async registerManagedKey(key: string): Promise<void> {
    if (!key) return;
    await this.redis
      .multi()
      .hset(this.managedKeyRegistry(), key, '1')
      .pexpire(this.managedKeyRegistry(), this.keyTtlMs)
      .exec();
  }

  async unregisterManagedKey(key: string): Promise<void> {
    if (!key) return;
    await this.redis.hdel(this.managedKeyRegistry(), key);
  }

  async renewCurrentNamespaceKeys(): Promise<void> {
    if (this.renewing) return;
    this.renewing = true;
    try {
      await this.heartbeat();
      await this.renewManagedKeys();
    } catch (error) {
      this.logger.warn(
        `Runtime namespace renew failed: ${(error as Error).message}`,
      );
    } finally {
      this.renewing = false;
    }
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
      .exec();
  }

  private async renewManagedKeys(): Promise<void> {
    const registryKey = this.managedKeyRegistry();
    let cursor = '0';
    do {
      const [nextCursor, fields] = await this.redis.hscan(
        registryKey,
        cursor,
        'COUNT',
        SCAN_COUNT,
      );
      const keys = fields.filter((_, index) => index % 2 === 0);
      for (const key of keys) {
        if ((await this.redis.pttl(key)) > 0) {
          await this.redis.pexpire(key, this.keyTtlMs);
        } else {
          await this.redis.hdel(registryKey, key);
        }
      }
      if (keys.length > 0) {
        await this.redis.pexpire(registryKey, this.keyTtlMs);
      }
      cursor = nextCursor;
    } while (cursor !== '0');
  }

  private managedKeyRegistry(): string {
    return `${this.nodeName}:runtime_lifecycle:managed_cache_keys`;
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
