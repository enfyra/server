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
  it('reports actual errors without flagging slow queries or other performance thresholds', () => {
    const service = makeService();
    const metrics = {
      instance: { eventLoopLagMs: 10000, heapUsedMb: 99, heapLimitMb: 100 },
      executor: {
        pool: { waitingTasks: 200, workers: [] }, maxHeapRatio: 0.99,
        p99TaskMs: 10000, p99QueueWaitMs: 10000, rotationsTotal: 20,
      },
      queues: { flow: { waiting: 2000, failed: 0 }, websocket: { waiting: 2000, failed: 0 } },
      db: { pool: { pending: 200 } },
      app: { database: { totalSlow: 200 }, flows: { rows: [] } },
    };
    expect(Object.values((service as any).evaluateHealth(metrics))).toEqual(
      Array.from({ length: 5 }, () => ({ severity: 'ok', messages: [] })),
    );
    metrics.app.database = { totalSlow: 200, totalErrors: 1, totalPoolAcquireTimeouts: 2 } as any;
    const health = (service as any).evaluateHealth(metrics);
    expect(health.database).toEqual({
      severity: 'error', messages: ['2 DB pool acquire timeouts.', '1 DB query error.'],
    });
  });

  it('samples each instance every two seconds without duplicating its timer', async () => {
    vi.useFakeTimers();
    vi.setSystemTime(0);
    const receivedAt: number[] = [];
    const service = makeService({
      dynamicWebSocketGateway: { emitToNamespaceRoom: vi.fn() },
      runtimeProcessMetricsService: {
        enable: vi.fn(),
        resetAverages: vi.fn().mockResolvedValue(undefined),
        resetEventLoop: vi.fn(),
        onDestroy: vi.fn().mockResolvedValue(undefined),
      },
      runtimeMetricsCollectorService: {
        onDestroy: vi.fn().mockResolvedValue(undefined),
      },
      clusterTelemetryService: {
        publish: vi.fn(() => { receivedAt.push(Date.now()); }),
        clearCurrentInstance: vi.fn().mockResolvedValue(undefined),
      },
    });
    vi.spyOn(service, 'getSnapshot').mockResolvedValue({ sampledAt: 'now' });
    vi.spyOn(service, 'emitRedisOverview').mockResolvedValue();

    try {
      service.start();
      service.start();
      await vi.advanceTimersByTimeAsync(6000);

      expect(receivedAt).toEqual([0, 2000, 4000, 6000]);
    } finally {
      await service.onDestroy();
      vi.useRealTimers();
    }
  });

  it('retains independent instance samples without broadcasting them to clients', async () => {
    vi.useFakeTimers();
    vi.setSystemTime(0);
    const receivedAt: number[] = [];
    const services = ['a', 'b'].map((id) => {
      const service = makeService({
        dynamicWebSocketGateway: {
          emitToNamespaceRoom: vi.fn(),
        },
        runtimeProcessMetricsService: {
          enable: vi.fn(),
          resetAverages: vi.fn().mockResolvedValue(undefined),
          resetEventLoop: vi.fn(),
          onDestroy: vi.fn().mockResolvedValue(undefined),
        },
        runtimeMetricsCollectorService: {
          onDestroy: vi.fn().mockResolvedValue(undefined),
        },
        clusterTelemetryService: {
          publish: vi.fn(() => { receivedAt.push(Date.now()); }),
          clearCurrentInstance: vi.fn().mockResolvedValue(undefined),
        },
      });
      vi.spyOn(service, 'getSnapshot').mockResolvedValue({
        sampledAt: 'now',
        instance: { id },
      } as any);
      vi.spyOn(service, 'emitRedisOverview').mockResolvedValue();
      return service;
    });

    try {
      services[0].start();
      await vi.advanceTimersByTimeAsync(1000);
      services[1].start();
      await vi.advanceTimersByTimeAsync(5000);

      expect(receivedAt).toEqual([0, 1000, 2000, 3000, 4000, 5000, 6000]);
    } finally {
      await Promise.all(services.map((service) => service.onDestroy()));
      vi.useRealTimers();
    }
  });

  it('publishes runtime snapshots to expiring cluster telemetry instead of sockets', async () => {
    const dynamicWebSocketGateway = {
      emitToNamespaceRoom: vi.fn(),
    };
    const publish = vi.fn().mockResolvedValue(undefined);
    const service = makeService({
      dynamicWebSocketGateway,
      clusterTelemetryService: { publish },
      runtimeProcessMetricsService: { resetEventLoop: vi.fn() },
    });
    vi.spyOn(service, 'getSnapshot').mockResolvedValue({ sampledAt: 'now' });
    vi.spyOn(service, 'emitRedisOverview').mockResolvedValue();

    await (service as any).emitSample();

    expect(dynamicWebSocketGateway.emitToNamespaceRoom).not.toHaveBeenCalled();
    expect(publish).toHaveBeenCalledWith(
      'runtime-monitor:metrics',
      { sampledAt: 'now' },
      { sampledAt: 'now', ttlMs: 10000 },
    );
  });

  it('returns every active instance in one cluster snapshot without collecting another sample', async () => {
    const instances = [
      { instanceId: 'a', payload: { instance: { id: 'a' } } },
      { instanceId: 'b', payload: { instance: { id: 'b' } } },
    ];
    const readCluster = vi.fn().mockResolvedValue({ ttlMs: 10000, instances });
    const service = makeService({ clusterTelemetryService: { readCluster } });
    const getSnapshot = vi.spyOn(service, 'getSnapshot');
    expect(await service.getClusterSnapshot()).toEqual({
      intervalMs: 2000, instances: instances.map((item) => item.payload),
    });
    expect(readCluster).toHaveBeenCalledWith('runtime-monitor:metrics', { ttlMs: 10000 });
    expect(getSnapshot).not.toHaveBeenCalled();
  });

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
        activeCount: 1,
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
    const pushAverageSample = vi.fn().mockResolvedValue(undefined);
    const service = makeService({
      isolatedExecutorService: {
        getMetrics: vi.fn().mockReturnValue({
          pool: { activeTasks: 1, waitingTasks: 2 },
          p95TaskMs: 30,
          p99TaskMs: 40,
          p95QueueWaitMs: 50,
          p99QueueWaitMs: 60,
          runnerRssBytes: 32 * 1024 * 1024,
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
        pushAverageSample,
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
          activeCount: expect.any(Number),
          instances: expect.any(Array),
        }),
      }),
    );
    expect(pushAverageSample).toHaveBeenCalledWith(
      expect.objectContaining({
        executorP95QueueWaitMs: 50,
        executorP99QueueWaitMs: 60,
        executorRunnerRssMb: 32,
      }),
    );
  });
});
