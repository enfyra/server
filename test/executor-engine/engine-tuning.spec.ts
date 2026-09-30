import { computeEngineTuning } from '@enfyra/kernel';

const MB = 1024 * 1024;
function tuning(cpus: number, ramMb: number) {
  return computeEngineTuning({ logicalCpuCount: cpus, totalMemoryBytes: ramMb * MB });
}

describe('computeEngineTuning', () => {
  it('keeps worker concurrency CPU-bounded at two', () => {
    expect(tuning(1, 4096).maxConcurrentWorkers).toBe(1);
    expect(tuning(2, 2048).maxConcurrentWorkers).toBe(2);
    expect(tuning(32, 65536).maxConcurrentWorkers).toBe(2);
  });

  it('scales each task isolate memory with effective RAM without a fixed MB ceiling', () => {
    expect(tuning(2, 256).isolateMemoryLimitMb).toBe(40);
    expect(tuning(2, 2048).isolateMemoryLimitMb).toBe(64);
    expect(tuning(4, 16384).isolateMemoryLimitMb).toBe(512);
    expect(tuning(4, 131072).isolateMemoryLimitMb).toBe(4096);
  });

  it('admits independent task isolates directly in each worker', () => {
    const result = tuning(4, 16384);
    expect(result.isolatesPerWorker).toBe(96);
    expect(result).not.toHaveProperty('isolatePoolSize');
    expect(result).not.toHaveProperty('tasksPerIsolate');
    expect(result).not.toHaveProperty('tasksPerWorkerCap');
  });

  it('keeps admission capacity independent of the sum of isolate heap limits', () => {
    for (const [cpus, ramMb, workers, isolates] of [
      [1, 256, 1, 6], [2, 2048, 2, 24], [4, 8192, 2, 48],
      [4, 16384, 2, 96], [8, 32768, 2, 192], [14, 36864, 2, 216],
      [4, 131072, 2, 96], [64, 262144, 2, 768],
    ]) {
      const result = tuning(cpus, ramMb);
      expect(result.maxConcurrentWorkers).toBe(workers);
      expect(result.isolatesPerWorker).toBe(isolates);
    }
    const result = tuning(14, 36864);
    expect(result.maxConcurrentWorkers * result.isolatesPerWorker * result.isolateMemoryLimitMb).toBeGreaterThan(36864);
  });

  it('honors an explicit isolate capacity below the hardware admission cap', () => {
    expect(computeEngineTuning({ logicalCpuCount: 8, totalMemoryBytes: 16384 * MB, maxIsolatesPerWorker: 2 }).isolatesPerWorker).toBe(2);
  });
});
