import { describe, expect, it } from 'vitest';
import { SqlPoolClusterCoordinatorService } from '../../src/engines/knex';

class FakeRedis {
  zsets = new Map<string, Map<string, number>>();

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
  it('isolates SQL pool heartbeats by NODE_NAME when apps share Redis and DB', async () => {
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
        activeCount: 1,
        instances: [expect.objectContaining({ id: 'same-instance' })],
        key: expect.stringMatching(/^app-a:coord:sql:pool:/),
      }),
    );
    await expect(appB.getClusterStats()).resolves.toEqual(
      expect.objectContaining({
        activeCount: 1,
        instances: [expect.objectContaining({ id: 'same-instance' })],
        key: expect.stringMatching(/^app-b:coord:sql:pool:/),
      }),
    );

    await appA.onDestroy();
    await appB.onDestroy();
  });
});
