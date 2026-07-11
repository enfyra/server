import { describe, expect, it } from 'vitest';
import { SqlPoolClusterCoordinatorService } from '../../src/engines/knex';

class FakeRedis {
  zsets = new Map<string, Map<string, number>>();
  expiries = new Map<string, number>();

  pipeline() {
    const ops: Array<() => void> = [];
    return {
      zadd: (key: string, score: number, member: string) => {
        ops.push(() => {
          const zset = this.zsets.get(key) ?? new Map<string, number>();
          zset.set(member, score);
          this.zsets.set(key, zset);
        });
        return this.pipelineProxy(ops);
      },
      pexpire: (key: string, ttlMs: number) => {
        ops.push(() => this.expiries.set(key, ttlMs));
        return this.pipelineProxy(ops);
      },
      exec: async () => {
        for (const op of ops) op();
        return [];
      },
    };
  }

  private pipelineProxy(ops: Array<() => void>): any {
    return {
      zadd: (key: string, score: number, member: string) => {
        ops.push(() => {
          const zset = this.zsets.get(key) ?? new Map<string, number>();
          zset.set(member, score);
          this.zsets.set(key, zset);
        });
        return this.pipelineProxy(ops);
      },
      pexpire: (key: string, ttlMs: number) => {
        ops.push(() => this.expiries.set(key, ttlMs));
        return this.pipelineProxy(ops);
      },
      exec: async () => {
        for (const op of ops) op();
        return [];
      },
    };
  }

  async zadd(key: string, score: number, member: string) {
    const zset = this.zsets.get(key) ?? new Map<string, number>();
    zset.set(member, score);
    this.zsets.set(key, zset);
  }

  async zrem(key: string, member: string) {
    this.zsets.get(key)?.delete(member);
  }

  async zremrangebyscore(key: string, min: number | string, max: number) {
    const zset = this.zsets.get(key);
    if (!zset) return 0;
    const lower = min === '-inf' ? -Infinity : Number(min);
    let removed = 0;
    for (const [member, score] of [...zset.entries()]) {
      if (score >= lower && score <= max) {
        zset.delete(member);
        removed++;
      }
    }
    return removed;
  }

  async pexpire(key: string, ttlMs: number) {
    this.expiries.set(key, ttlMs);
    return 1;
  }

  async zcard(key: string) {
    return this.zsets.get(key)?.size ?? 0;
  }

  async zrange(key: string, _start: number, _end: number, withScores: string) {
    const rows = [...(this.zsets.get(key)?.entries() ?? [])];
    if (withScores !== 'WITHSCORES') return rows.map(([member]) => member);
    return rows.flatMap(([member, score]) => [member, String(score)]);
  }
}

function makeCoordinator(input: {
  redis: FakeRedis;
  nodeName: string;
  instanceId: string;
}) {
  return new SqlPoolClusterCoordinatorService({
    redis: input.redis as any,
    envService: {
      get: (key: string) => {
        if (key === 'NODE_NAME') return input.nodeName;
        if (key === 'DB_URI') return 'mysql://root:pass@db-host:3306/enfyra';
        return undefined;
      },
    } as any,
    databaseConfigService: {
      isMongoDb: () => false,
      isPostgres: () => false,
      isMySql: () => true,
    } as any,
    instanceService: { getInstanceId: () => input.instanceId } as any,
    knexService: {} as any,
    eventEmitter: { once: () => undefined } as any,
  });
}

describe('SqlPoolClusterCoordinatorService', () => {
  it('shares SQL pool heartbeats across NODE_NAME values when apps use the same DB server', async () => {
    const redis = new FakeRedis();
    const appA = makeCoordinator({
      redis,
      nodeName: 'app-a',
      instanceId: 'same-instance',
    });
    const appB = makeCoordinator({
      redis,
      nodeName: 'app-b',
      instanceId: 'same-instance',
    });

    await appA.init();
    await appB.init();

    await expect(appA.getClusterStats()).resolves.toEqual(
      expect.objectContaining({
        currentAppActiveCount: 1,
      }),
    );
    const appAStats = await appA.getClusterStats();
    expect(appAStats).not.toHaveProperty('instances');
    expect(appAStats).not.toHaveProperty('key');

    await expect(appB.getClusterStats()).resolves.toEqual(
      expect.objectContaining({
        currentAppActiveCount: 1,
      }),
    );
    const appBStats = await appB.getClusterStats();
    expect(appBStats).not.toHaveProperty('instances');
    expect(appBStats).not.toHaveProperty('key');

    await appA.onDestroy();
    await appB.onDestroy();
  });
});
