import { describe, expect, it } from 'vitest';
import { RuntimeProcessMetricsService } from '../../src/modules/admin/services/runtime-process-metrics.service';

class FakeRedis {
  hashes = new Map<string, Record<string, string>>();

  pipeline() {
    const ops: Array<() => void> = [];
    const api = {
      hincrby: (key: string, field: string, value: number) => {
        ops.push(() => {
          const hash = this.hashes.get(key) ?? {};
          hash[field] = String(Number(hash[field] ?? 0) + value);
          this.hashes.set(key, hash);
        });
        return api;
      },
      hincrbyfloat: (key: string, field: string, value: number) => {
        ops.push(() => {
          const hash = this.hashes.get(key) ?? {};
          hash[field] = String(Number(hash[field] ?? 0) + value);
          this.hashes.set(key, hash);
        });
        return api;
      },
      pexpire: () => api,
      exec: async () => {
        for (const op of ops) op();
        return [];
      },
    };
    return api;
  }

  async del(key: string) {
    this.hashes.delete(key);
  }

  async hgetall(key: string) {
    return this.hashes.get(key) ?? {};
  }
}

const sample = {
  rssMb: 10,
  heapUsedMb: 1,
  heapTotalMb: 2,
  externalMb: 3,
  eventLoopLagMs: 4,
  cpuRatio: 0.5,
  executorActiveTasks: 1,
  executorWaitingTasks: 2,
  executorP95TaskMs: 3,
  executorP99TaskMs: 4,
  executorMaxHeapRatio: 0.2,
  websocketConnections: 5,
  queueDepth: 6,
  queueFailed: 7,
  dbUsed: 8,
  dbAvailable: 9,
  dbIdle: 2,
  dbPending: 10,
};

describe('RuntimeProcessMetricsService', () => {
  it('isolates average samples by NODE_NAME when apps share Redis', async () => {
    const redis = new FakeRedis();
    const appA = new RuntimeProcessMetricsService({
      redis: redis as any,
      instanceService: { getInstanceId: () => 'same-instance' } as any,
      envService: { get: () => 'app-a' } as any,
    });
    const appB = new RuntimeProcessMetricsService({
      redis: redis as any,
      instanceService: { getInstanceId: () => 'same-instance' } as any,
      envService: { get: () => 'app-b' } as any,
    });

    await appA.pushAverageSample(sample);
    await appB.pushAverageSample({ ...sample, rssMb: 30 });

    await expect(appA.getAverages()).resolves.toEqual(
      expect.objectContaining({
        samples: 1,
        rssMb: 10,
        dbAvailable: 9,
        dbIdle: 2,
      }),
    );
    await expect(appB.getAverages()).resolves.toEqual(
      expect.objectContaining({ samples: 1, rssMb: 30 }),
    );
  });
});
