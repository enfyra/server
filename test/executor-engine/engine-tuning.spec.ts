import { computeEngineTuning } from '@enfyra/kernel';

const MB = 1024 * 1024;
const GB = 1024 * MB;

function tuning(cpus: number, ramMb: number) {
  return computeEngineTuning({
    logicalCpuCount: cpus,
    totalMemoryBytes: ramMb * MB,
  });
}

describe('computeEngineTuning', () => {
  it('keeps worker concurrency CPU-bounded at two', () => {
    expect(tuning(1, 4096).maxConcurrentWorkers).toBe(1);
    expect(tuning(2, 2048).maxConcurrentWorkers).toBe(2);
    expect(tuning(32, 64 * 1024).maxConcurrentWorkers).toBe(2);
    expect(tuning(0, 4096).maxConcurrentWorkers).toBe(1);
  });

  it('clamps isolate memory between 40MB and 128MB', () => {
    expect(tuning(2, 256).isolateMemoryLimitMb).toBe(40);
    expect(tuning(2, 2048).isolateMemoryLimitMb).toBe(64);
    expect(tuning(4, 128 * 1024).isolateMemoryLimitMb).toBe(128);
  });

  it('uses one exclusive isolate lane for each worker task slot', () => {
    for (const [cpus, ramMb] of [
      [1, 256],
      [1, 1024],
      [2, 2048],
      [4, 8192],
      [32, 65536],
    ]) {
      const result = tuning(cpus, ramMb);
      expect(result.tasksPerWorkerCap).toBe(result.isolatePoolSize);
      expect(result.tasksPerWorkerCap).toBeGreaterThanOrEqual(1);
      expect(result.tasksPerWorkerCap).toBeLessThanOrEqual(16);
    }
  });

  it('derives isolate lanes from the 25% memory budget', () => {
    expect(tuning(2, 256).isolatePoolSize).toBe(1);
    expect(tuning(1, 1024).isolatePoolSize).toBe(1);
    expect(tuning(2, 2048).isolatePoolSize).toBe(4);
    expect(tuning(2, 8192).isolatePoolSize).toBe(8);
    expect(tuning(32, 64 * 1024).isolatePoolSize).toBe(16);
  });

  it('keeps total isolate lane capacity within 25% effective memory', () => {
    const cases = [256 * MB, 512 * MB, 1 * GB, 4 * GB, 16 * GB, 64 * GB];
    for (const bytes of cases) {
      const result = computeEngineTuning({
        logicalCpuCount: 32,
        totalMemoryBytes: bytes,
      });
      const totalCapMb =
        result.isolatePoolSize *
        result.maxConcurrentWorkers *
        result.isolateMemoryLimitMb;
      const allowedBudgetMb = Math.max(
        bytes / MB * 0.25,
        result.isolateMemoryLimitMb * result.maxConcurrentWorkers,
      );
      expect(totalCapMb).toBeLessThanOrEqual(allowedBudgetMb);
    }
  });
});
