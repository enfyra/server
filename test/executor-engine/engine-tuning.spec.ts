import { computeEngineTuning } from '../../src/infrastructure/executor-engine/utils/engine-tuning.util';

describe('computeEngineTuning', () => {
  it('scales isolate mb from ram with 128mb ceiling and derives concurrency', () => {
    const small = computeEngineTuning({
      logicalCpuCount: 2,
      totalMemoryBytes: 2 * 1024 * 1024 * 1024,
    });
    expect(small.isolateMemoryLimitMb).toBe(51);
    expect(small.maxConcurrentWorkers).toBe(2);
    // 2048 / 51 / 2 = 20
    expect(small.tasksPerWorkerCap).toBe(20);

    const mid = computeEngineTuning({
      logicalCpuCount: 2,
      totalMemoryBytes: 8 * 1024 * 1024 * 1024,
    });
    expect(mid.isolateMemoryLimitMb).toBe(128);
    expect(mid.maxConcurrentWorkers).toBe(2);
    // 8192 / 128 / 2 = 32
    expect(mid.tasksPerWorkerCap).toBe(32);
  });

  it('caps workers at 2 even on huge machines (scale out via instances)', () => {
    const t = computeEngineTuning({
      logicalCpuCount: 32,
      totalMemoryBytes: 64 * 1024 * 1024 * 1024,
    });
    expect(t.isolateMemoryLimitMb).toBe(128);
    expect(t.maxConcurrentWorkers).toBe(2);
    // 65536 / 128 / 2 = 256 (hits ceiling)
    expect(t.tasksPerWorkerCap).toBe(256);
  });

  it('handles single cpu — floor at 1 worker', () => {
    const t = computeEngineTuning({
      logicalCpuCount: 1,
      totalMemoryBytes: 4 * 1024 * 1024 * 1024,
    });
    expect(t.maxConcurrentWorkers).toBe(1);
    // 4096 / 102 / 1 = 40
    expect(t.tasksPerWorkerCap).toBe(40);
  });

  it('handles missing cpu count — clamps to 1', () => {
    const t = computeEngineTuning({
      logicalCpuCount: 0,
      totalMemoryBytes: 4 * 1024 * 1024 * 1024,
    });
    expect(t.maxConcurrentWorkers).toBe(1);
  });

  it('clamps isolate mb between 16 and 128', () => {
    const tiny = computeEngineTuning({
      logicalCpuCount: 2,
      totalMemoryBytes: 512 * 1024 * 1024,
    });
    expect(tiny.isolateMemoryLimitMb).toBeGreaterThanOrEqual(16);
    expect(tiny.isolateMemoryLimitMb).toBeLessThanOrEqual(32);
    // 512 / 13 / 2 = 19 (floor) → clamped to 19
    expect(tiny.tasksPerWorkerCap).toBeGreaterThanOrEqual(16);

    const huge = computeEngineTuning({
      logicalCpuCount: 4,
      totalMemoryBytes: 128 * 1024 * 1024 * 1024,
    });
    expect(huge.isolateMemoryLimitMb).toBe(128);
    // 131072 / 128 / 2 = 512 → clamped to 256
    expect(huge.tasksPerWorkerCap).toBe(256);
  });

  it('clamps tasksPerWorkerCap between 16 and 256', () => {
    const verySmall = computeEngineTuning({
      logicalCpuCount: 2,
      totalMemoryBytes: 256 * 1024 * 1024,
    });
    expect(verySmall.tasksPerWorkerCap).toBe(16);

    const veryLarge = computeEngineTuning({
      logicalCpuCount: 2,
      totalMemoryBytes: 256 * 1024 * 1024 * 1024,
    });
    expect(veryLarge.tasksPerWorkerCap).toBe(256);
  });
});
