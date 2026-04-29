import { describe, expect, it } from 'vitest';
import { RuntimeMetricsCollectorService } from '../../src/shared/services';

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
    const stored: string[] = [];
    const redis = {
      pipeline: () => ({
        lpush: (_key: string, value: string) => {
          stored.unshift(value);
          return redis.pipeline();
        },
        ltrim: (_key: string, start: number, stop: number) => {
          stored.splice(stop + 1);
          return redis.pipeline();
        },
        exec: async () => [],
      }),
      lrange: async () => stored,
    };
    const collector = new RuntimeMetricsCollectorService({
      redis: redis as any,
      envService: { get: () => 'test-node' } as any,
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
        flow: 'metadata',
        table: 'table_definition',
      }),
    );
  });
});
