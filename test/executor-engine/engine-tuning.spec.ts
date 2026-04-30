import { computeEngineTuning } from '@enfyra/kernel';

describe('computeEngineTuning', () => {
  it('scales isolate mb from ram with 128mb ceiling and derives concurrency', () => {
    const small = computeEngineTuning({
      logicalCpuCount: 2,
      totalMemoryBytes: 2 * 1024 * 1024 * 1024,
    });
    expect(small.isolateMemoryLimitMb).toBe(64);
    expect(small.maxConcurrentWorkers).toBe(2);
    expect(small.tasksPerWorkerCap).toBe(16);

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
    expect(t.tasksPerWorkerCap).toBe(32);
  });

  it('handles missing cpu count — clamps to 1', () => {
    const t = computeEngineTuning({
      logicalCpuCount: 0,
      totalMemoryBytes: 4 * 1024 * 1024 * 1024,
    });
    expect(t.maxConcurrentWorkers).toBe(1);
  });

  it('clamps isolate mb between 40 and 128', () => {
    const tiny = computeEngineTuning({
      logicalCpuCount: 2,
      totalMemoryBytes: 512 * 1024 * 1024,
    });
    expect(tiny.isolateMemoryLimitMb).toBe(40);
    expect(tiny.tasksPerWorkerCap).toBe(16);

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

  it('derives isolatePoolSize from 25% ram budget, clamped to the warm pool size', () => {
    const tiny = computeEngineTuning({
      logicalCpuCount: 2,
      totalMemoryBytes: 256 * 1024 * 1024,
    });
    expect(tiny.isolatePoolSize).toBe(1);

    const starter = computeEngineTuning({
      logicalCpuCount: 1,
      totalMemoryBytes: 1 * 1024 * 1024 * 1024,
    });
    expect(starter.maxConcurrentWorkers).toBe(1);
    expect(starter.isolateMemoryLimitMb).toBe(40);
    expect(starter.tasksPerWorkerCap).toBe(25);
    expect(starter.isolatePoolSize).toBe(1);

    const small = computeEngineTuning({
      logicalCpuCount: 2,
      totalMemoryBytes: 2 * 1024 * 1024 * 1024,
    });
    expect(small.isolatePoolSize).toBe(2);

    const mid = computeEngineTuning({
      logicalCpuCount: 2,
      totalMemoryBytes: 8 * 1024 * 1024 * 1024,
    });
    // budget = 2048mb, memLimit=128, workers=2 -> floor(2048/256) = 8, capped to 2
    expect(mid.isolatePoolSize).toBe(2);

    const huge = computeEngineTuning({
      logicalCpuCount: 32,
      totalMemoryBytes: 64 * 1024 * 1024 * 1024,
    });
    // would be 64 but capped to 2 to reserve memory for the main runtime.
    expect(huge.isolatePoolSize).toBe(2);
  });

  it('isolatePoolSize total cap never exceeds 25% effective memory', () => {
    const cases = [
      256 * 1024 * 1024,
      1 * 1024 * 1024 * 1024,
      4 * 1024 * 1024 * 1024,
      16 * 1024 * 1024 * 1024,
      64 * 1024 * 1024 * 1024,
    ];
    for (const bytes of cases) {
      const t = computeEngineTuning({
        logicalCpuCount: 4,
        totalMemoryBytes: bytes,
      });
      const totalCapMb =
        t.isolatePoolSize * t.maxConcurrentWorkers * t.isolateMemoryLimitMb;
      const totalMb = bytes / (1024 * 1024);
      expect(totalCapMb).toBeLessThanOrEqual(totalMb * 0.25);
    }
  });
});
