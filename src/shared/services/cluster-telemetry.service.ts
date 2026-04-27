import { Redis } from 'ioredis';
import { InstanceService } from './instance.service';
import { EnvService } from './env.service';
import type { ClusterTelemetryRecord } from '../types';

export class ClusterTelemetryService {
  private readonly redis: Redis;
  private readonly instanceService: InstanceService;
  private readonly nodeName: string;

  constructor(deps: {
    redis: Redis;
    instanceService: InstanceService;
    envService: EnvService;
  }) {
    this.redis = deps.redis;
    this.instanceService = deps.instanceService;
    this.nodeName = deps.envService.get('NODE_NAME') || 'enfyra';
  }

  private keyPrefix() {
    return `${this.nodeName}:cluster-telemetry`;
  }

  private payloadKey(namespace: string, instanceId: string) {
    return `${this.keyPrefix()}:${namespace}:${instanceId}:payload`;
  }

  private instancesKey(namespace: string) {
    return `${this.keyPrefix()}:${namespace}:instances`;
  }

  async publish<T>(
    namespace: string,
    payload: T,
    options: { ttlMs: number; sampledAt?: string; instanceId?: string },
  ): Promise<void> {
    const instanceId =
      options.instanceId ?? this.instanceService.getInstanceId();
    const sampledAt = options.sampledAt ?? new Date().toISOString();
    const now = Date.now();
    const instancesKey = this.instancesKey(namespace);
    const pipeline = this.redis.pipeline();
    pipeline.set(
      this.payloadKey(namespace, instanceId),
      JSON.stringify({ instanceId, sampledAt, payload }),
      'PX',
      options.ttlMs,
    );
    pipeline.zadd(instancesKey, now, instanceId);
    pipeline.zremrangebyscore(instancesKey, 0, now - options.ttlMs);
    pipeline.pexpire(instancesKey, options.ttlMs);
    await pipeline.exec();
  }

  async readCluster<T>(
    namespace: string,
    options: { ttlMs: number },
  ): Promise<{ ttlMs: number; instances: Array<ClusterTelemetryRecord<T>> }> {
    const now = Date.now();
    const instancesKey = this.instancesKey(namespace);
    await this.redis.zremrangebyscore(instancesKey, 0, now - options.ttlMs);
    const instanceIds = await this.redis.zrangebyscore(
      instancesKey,
      now - options.ttlMs,
      '+inf',
    );
    if (instanceIds.length === 0) {
      return { ttlMs: options.ttlMs, instances: [] };
    }

    const values = await this.redis.mget(
      instanceIds.map((id) => this.payloadKey(namespace, id)),
    );
    return {
      ttlMs: options.ttlMs,
      instances: values
        .map((value) => {
          if (!value) return null;
          try {
            return JSON.parse(value) as ClusterTelemetryRecord<T>;
          } catch {
            return null;
          }
        })
        .filter(Boolean) as Array<ClusterTelemetryRecord<T>>,
    };
  }
}
