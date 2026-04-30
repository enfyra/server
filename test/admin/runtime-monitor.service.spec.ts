import { describe, expect, it, vi } from 'vitest';
import { RuntimeMonitorService } from '../../src/modules/admin';

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

  it('returns a multi-instance runtime payload contract', async () => {
    const service = makeService({
      isolatedExecutorService: {
        getMetrics: vi.fn().mockReturnValue({
          pool: { activeTasks: 1, waitingTasks: 2 },
          p95TaskMs: 30,
          p99TaskMs: 40,
          maxHeapRatio: 0.2,
        }),
      },
      runtimeProcessMetricsService: {
        getProcessSample: vi.fn().mockReturnValue({
          instance: {
            id: 'a',
            rssMb: 100,
            heapUsedMb: 50,
            heapTotalMb: 80,
            externalMb: 5,
            eventLoopLagMs: 1,
            cpuRatio: 0.1,
          },
          hardware: { effectiveCpuCount: 2, effectiveMemoryMb: 1024 },
        }),
        pushAverageSample: vi.fn().mockResolvedValue(undefined),
        getAverages: vi.fn().mockResolvedValue({ samples: 1, rssMb: 100 }),
      },
      runtimeQueueMetricsService: {
        getQueues: vi.fn().mockResolvedValue({
          flow: {
            waiting: 1,
            active: 0,
            delayed: 0,
            failed: 0,
            failedJobs: [],
          },
        }),
        getQueueTotals: vi.fn().mockReturnValue({ depth: 1, failed: 0 }),
      },
      runtimeDbMetricsService: {
        getDbStats: vi.fn().mockReturnValue({
          type: 'mysql',
          pool: { used: 1, available: 2, idle: 1, pending: 0 },
        }),
        getDbPoolTotals: vi.fn().mockReturnValue({
          used: 1,
          available: 2,
          idle: 1,
          pending: 0,
        }),
        getClusterStats: vi.fn().mockResolvedValue({
          instances: [{ instanceId: 'a' }, { instanceId: 'b' }],
        }),
      },
      dynamicWebSocketGateway: {
        getConnectionStats: vi.fn().mockReturnValue({ total: 1 }),
      },
    });

    const snapshot = await service.getSnapshot();

    expect(snapshot).toEqual(
      expect.objectContaining({
        kind: 'runtime-metrics',
        intervalMs: 2000,
        hardware: expect.any(Object),
        instance: expect.any(Object),
        executor: expect.any(Object),
        queues: expect.any(Object),
        websocket: expect.any(Object),
        db: expect.any(Object),
        cluster: { instances: [{ instanceId: 'a' }, { instanceId: 'b' }] },
        app: expect.any(Object),
        appCluster: expect.objectContaining({
          instances: expect.any(Array),
        }),
      }),
    );
  });
});
