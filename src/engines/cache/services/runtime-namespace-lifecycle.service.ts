import type { Redis } from 'ioredis';
import { EnvService, InstanceService } from '../../../shared/services';
import { Logger } from '../../../shared/logger';
import { SYSTEM_QUEUES } from '../../../shared/utils/constant';

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

  async touchKeys(keys: string[], ttlMs = this.keyTtlMs): Promise<void> {
    if (ttlMs <= 0 || keys.length === 0) return;
    const pipeline = this.redis.pipeline();
    for (const key of keys) pipeline.pexpire(key, ttlMs);
    await pipeline.exec();
  }

  async renewKeysByPattern(
    pattern: string,
    ttlMs = this.keyTtlMs,
  ): Promise<void> {
    if (ttlMs <= 0 || !pattern) return;
    try {
      await this.expireByPattern(pattern, ttlMs);
    } catch (error) {
      this.logger.warn(
        `Runtime namespace pattern renew failed for ${pattern}: ${(error as Error).message}`,
      );
    }
  }

  async renewSystemQueueKeys(queueName: string): Promise<void> {
    if (!queueName) return;
    await this.renewKeysByPattern(
      `${this.nodeName}:${queueName}:*`,
      this.keyTtlMs,
    );
  }

  async renewCurrentNamespaceKeys(): Promise<void> {
    if (this.renewing) return;
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
