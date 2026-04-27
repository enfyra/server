import { afterEach, describe, expect, it, vi } from 'vitest';
import { ClusterTelemetryService } from 'src/shared/services';

class FakeRedis {
  values = new Map<string, string>();
  expiries = new Map<string, number>();
  zsets = new Map<string, Map<string, number>>();

  pipeline() {
    const ops: Array<() => void> = [];
    const api = {
      set: (key: string, value: string, _px: 'PX', ttlMs: number) => {
        ops.push(() => {
          this.values.set(key, value);
          this.expiries.set(key, Date.now() + ttlMs);
        });
        return api;
      },
      zadd: (key: string, score: number, member: string) => {
        ops.push(() => {
          const zset = this.zsets.get(key) ?? new Map<string, number>();
          zset.set(member, score);
          this.zsets.set(key, zset);
        });
        return api;
      },
      zremrangebyscore: (key: string, min: number, max: number) => {
        ops.push(() => this.zremrangebyscore(key, min, max));
        return api;
      },
      pexpire: (key: string, ttlMs: number) => {
        ops.push(() => this.expiries.set(key, Date.now() + ttlMs));
        return api;
      },
      exec: async () => {
        for (const op of ops) op();
        return [];
      },
    };
    return api;
  }

  async zremrangebyscore(key: string, min: number, max: number) {
    const zset = this.zsets.get(key);
    if (!zset) return 0;
    let removed = 0;
    for (const [member, score] of [...zset.entries()]) {
      if (score >= min && score <= max) {
        zset.delete(member);
        removed++;
      }
    }
    return removed;
  }

  async zrangebyscore(key: string, min: number, max: number | '+inf') {
    const upper = max === '+inf' ? Infinity : max;
    return [...(this.zsets.get(key)?.entries() ?? [])]
      .filter(([, score]) => score >= min && score <= upper)
      .sort((a, b) => a[1] - b[1])
      .map(([member]) => member);
  }

  async mget(keys: string[]) {
    return keys.map((key) => this.values.get(key) ?? null);
  }
}

describe('ClusterTelemetryService', () => {
  afterEach(() => {
    vi.restoreAllMocks();
  });

  it('publishes and reads active instance telemetry by namespace', async () => {
    vi.spyOn(Date, 'now').mockReturnValue(1000);
    const redis = new FakeRedis();
    const serviceA = new ClusterTelemetryService({
      redis: redis as any,
      instanceService: { getInstanceId: () => 'a' } as any,
    });
    const serviceB = new ClusterTelemetryService({
      redis: redis as any,
      instanceService: { getInstanceId: () => 'b' } as any,
    });

    await serviceA.publish('runtime', { requests: 1 }, { ttlMs: 5000, sampledAt: 't1' });
    await serviceB.publish('runtime', { requests: 2 }, { ttlMs: 5000, sampledAt: 't2' });

    const cluster = await serviceA.readCluster<{ requests: number }>('runtime', {
      ttlMs: 5000,
    });
    expect(cluster.instances).toEqual([
      { instanceId: 'a', sampledAt: 't1', payload: { requests: 1 } },
      { instanceId: 'b', sampledAt: 't2', payload: { requests: 2 } },
    ]);
  });

  it('prunes stale instances on read', async () => {
    const redis = new FakeRedis();
    const service = new ClusterTelemetryService({
      redis: redis as any,
      instanceService: { getInstanceId: () => 'a' } as any,
    });

    vi.spyOn(Date, 'now').mockReturnValue(1000);
    await service.publish('runtime', { ok: true }, { ttlMs: 1000, sampledAt: 't1' });

    vi.spyOn(Date, 'now').mockReturnValue(2501);
    const cluster = await service.readCluster('runtime', { ttlMs: 1000 });

    expect(cluster.instances).toEqual([]);
  });
});
