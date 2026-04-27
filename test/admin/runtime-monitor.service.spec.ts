import { describe, expect, it, vi } from 'vitest';
import { RuntimeMonitorService } from 'src/modules/admin';

function makeService(deps: Partial<any> = {}) {
  return new RuntimeMonitorService({
    dynamicWebSocketGateway: {},
    isolatedExecutorService: {},
    runtimeProcessMetricsService: {},
    runtimeQueueMetricsService: {},
    runtimeDbMetricsService: {},
    runtimeMetricsCollectorService: {
      snapshot: vi.fn().mockReturnValue({ database: { queries: [] } }),
    },
    clusterTelemetryService: {
      publish: vi.fn().mockResolvedValue(undefined),
      readCluster: vi.fn().mockResolvedValue({
        ttlMs: 1000,
        instances: [
          {
            instanceId: 'a',
            sampledAt: '2026-04-28T00:00:00.000Z',
            payload: { database: { queries: [] } },
          },
        ],
      }),
    },
    ...deps,
  } as any);
}

describe('RuntimeMonitorService', () => {
  it('captures app telemetry through ClusterTelemetryService', async () => {
    const clusterTelemetryService = {
      publish: vi.fn().mockResolvedValue(undefined),
      readCluster: vi.fn().mockResolvedValue({
        ttlMs: 10000,
        instances: [
          {
            instanceId: 'a',
            sampledAt: '2026-04-28T00:00:00.000Z',
            payload: { requests: { total: 1 } },
          },
        ],
      }),
    };
    const runtimeMetricsCollectorService = {
      snapshot: vi.fn().mockReturnValue({ requests: { total: 1 } }),
    };
    const service = makeService({
      clusterTelemetryService,
      runtimeMetricsCollectorService,
    });

    const result = await service.captureAppTelemetry(
      '2026-04-28T00:00:00.000Z',
    );

    expect(runtimeMetricsCollectorService.snapshot).toHaveBeenCalled();
    expect(clusterTelemetryService.publish).toHaveBeenCalledWith(
      'runtime-monitor:app',
      { requests: { total: 1 } },
      {
        sampledAt: '2026-04-28T00:00:00.000Z',
        ttlMs: 10000,
      },
    );
    expect(result).toEqual({
      app: { requests: { total: 1 } },
      appCluster: {
        ttlMs: 10000,
        instances: [
          {
            instanceId: 'a',
            sampledAt: '2026-04-28T00:00:00.000Z',
            app: { requests: { total: 1 } },
          },
        ],
      },
    });
  });
});
