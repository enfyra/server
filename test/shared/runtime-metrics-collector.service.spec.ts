import { describe, expect, it } from 'vitest';
import { RuntimeMetricsCollectorService } from '../../src/shared/services';

function createRedisMock() {
  const lists = new Map<string, string[]>();
  const hashes = new Map<string, Record<string, string>>();
  const ttls: Array<{ key: string; ttlMs: number }> = [];
  const redis = {
    pipeline: () => {
      const operations: Array<() => void> = [];
      const pipeline = {
        lpush: (key: string, value: string) => {
          operations.push(() => {
            const list = lists.get(key) ?? [];
            list.unshift(value);
            lists.set(key, list);
          });
          return pipeline;
        },
        ltrim: (key: string, _start: number, stop: number) => {
          operations.push(() => {
            const list = lists.get(key) ?? [];
            lists.set(key, list.slice(0, stop + 1));
          });
          return pipeline;
        },
        hset: (key: string, field: string, value: string) => {
          operations.push(() => {
            const hash = hashes.get(key) ?? {};
            hash[field] = value;
            hashes.set(key, hash);
          });
          return pipeline;
        },
        pexpire: (key: string, ttlMs: number) => {
          operations.push(() => {
            ttls.push({ key, ttlMs });
          });
          return pipeline;
        },
        exec: async () => {
          operations.forEach((operation) => operation());
          return [];
        },
      };
      return pipeline;
    },
    hget: async (key: string, field: string) => hashes.get(key)?.[field] ?? null,
    hgetall: async (key: string) => hashes.get(key) ?? {},
    lrange: async (key: string) => lists.get(key) ?? [],
  };
  return { redis, lists, hashes, ttls };
}

describe('RuntimeMetricsCollectorService', () => {
  it('groups query metrics by execution context', async () => {
    const collector = new RuntimeMetricsCollectorService();

    await collector.runWithQueryContext('cache', () =>
      collector.trackQuery({ op: 'find', table: 'route_definition' }, async () => true),
    );
    await collector.runWithQueryContext('flow', () =>
      collector.trackQuery({ op: 'find', table: 'route_definition' }, async () => true),
    );

    expect(collector.snapshot().database.queries).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          context: 'cache',
          op: 'find',
          table: 'route_definition',
          count: 1,
        }),
        expect.objectContaining({
          context: 'flow',
          op: 'find',
          table: 'route_definition',
          count: 1,
        }),
      ]),
    );
  });

  it('keeps cache reload step health in recent snapshots', () => {
    const collector = new RuntimeMetricsCollectorService();

    collector.recordCacheReload({
      flow: 'route',
      table: 'route_definition',
      scope: 'full',
      status: 'failed',
      durationMs: 12,
      steps: [
        { name: 'route', durationMs: 12, status: 'failed', error: 'boom' },
      ],
      startedAt: '2026-04-28T00:00:00.000Z',
      completedAt: '2026-04-28T00:00:00.012Z',
      error: 'boom',
    });

    expect(collector.snapshot().cache.recent[0]).toEqual(
      expect.objectContaining({
        flow: 'route',
        table: 'route_definition',
        status: 'failed',
        steps: [
          { name: 'route', durationMs: 12, status: 'failed', error: 'boom' },
        ],
      }),
    );
  });

  it('stores cache reload history in Redis when available', async () => {
    const { redis, ttls } = createRedisMock();
    const collector = new RuntimeMetricsCollectorService({
      redis: redis as any,
      envService: { get: () => 'test-node' } as any,
      instanceService: { getInstanceId: () => 'inst-a' } as any,
    });

    collector.recordCacheReload({
      flow: 'metadata',
      table: 'table_definition',
      scope: 'full',
      status: 'success',
      durationMs: 5,
      steps: [{ name: 'metadata', durationMs: 5, status: 'success' }],
      startedAt: '2026-04-28T00:00:00.000Z',
      completedAt: '2026-04-28T00:00:00.005Z',
    });

    await new Promise((resolve) => setTimeout(resolve, 0));

    expect((await collector.snapshotAsync()).cache.recent[0]).toEqual(
      expect.objectContaining({
        instanceId: 'inst-a',
        flow: 'metadata',
        table: 'table_definition',
      }),
    );
    expect(ttls).toContainEqual(
      expect.objectContaining({
        key: 'test-node:runtime-monitor:cache-reloads',
        ttlMs: 60 * 60 * 1000,
      }),
    );
  });

  it('stores request, query, and flow monitor metrics in Redis with TTL', async () => {
    const { redis, ttls } = createRedisMock();
    const collector = new RuntimeMetricsCollectorService({
      redis: redis as any,
      envService: { get: () => 'test-node' } as any,
      instanceService: { getInstanceId: () => 'inst-a' } as any,
    });

    collector.recordRequest({
      method: 'GET',
      route: '/orders',
      statusCode: 200,
      durationMs: 10,
    });
    await collector.trackQuery(
      { context: 'runtime', op: 'find', table: 'order_definition' },
      async () => true,
    );
    collector.startFlow(1, 'sync');
    collector.recordFlowStep({
      flowId: 1,
      flowName: 'sync',
      stepKey: 'load',
      durationMs: 7,
    });
    collector.completeFlow({
      flowId: 1,
      flowName: 'sync',
      durationMs: 20,
      status: 'completed',
    });

    const snapshot = await collector.snapshotAsync();

    expect(snapshot.requests.routes[0]).toEqual(
      expect.objectContaining({
        method: 'GET',
        route: '/orders',
        count: 1,
      }),
    );
    expect(snapshot.database.queries[0]).toEqual(
      expect.objectContaining({
        context: 'runtime',
        op: 'find',
        table: 'order_definition',
        count: 1,
      }),
    );
    expect(snapshot.flows.rows[0]).toEqual(
      expect.objectContaining({
        flowId: 1,
        flowName: 'sync',
        completed: 1,
      }),
    );
    expect(ttls).toEqual(
      expect.arrayContaining([
        { key: 'test-node:runtime-monitor:inst-a:requests', ttlMs: 10_000 },
        { key: 'test-node:runtime-monitor:inst-a:queries', ttlMs: 10_000 },
        { key: 'test-node:runtime-monitor:inst-a:flows', ttlMs: 10_000 },
      ]),
    );
  });
});
