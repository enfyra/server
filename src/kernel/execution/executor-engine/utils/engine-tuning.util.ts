import * as os from 'os';
import * as fs from 'fs';

export function getEffectiveMemoryBytes(): number {
  try {
    const v2 = fs.readFileSync('/sys/fs/cgroup/memory.max', 'utf8').trim();
    if (v2 !== 'max') {
      const n = parseInt(v2, 10);
      if (Number.isFinite(n) && n > 0 && n < os.totalmem()) return n;
    }
  } catch {}
  try {
    const v1 = fs
      .readFileSync('/sys/fs/cgroup/memory/memory.limit_in_bytes', 'utf8')
      .trim();
    const n = parseInt(v1, 10);
    if (Number.isFinite(n) && n > 0 && n < os.totalmem()) return n;
  } catch {}
  return os.totalmem();
}

export function getEffectiveCpuCount(): number {
  const hostCpus = os.availableParallelism?.() || os.cpus()?.length || 1;
  try {
    const raw = fs.readFileSync('/sys/fs/cgroup/cpu.max', 'utf8').trim();
    const [quotaStr, periodStr] = raw.split(/\s+/);
    if (quotaStr !== 'max') {
      const quota = parseInt(quotaStr, 10);
      const period = parseInt(periodStr, 10);
      if (quota > 0 && period > 0) {
        return Math.max(1, Math.ceil(quota / period));
      }
    }
  } catch {}
  try {
    const quota = parseInt(
      fs.readFileSync('/sys/fs/cgroup/cpu/cpu.cfs_quota_us', 'utf8').trim(),
      10,
    );
    const period = parseInt(
      fs.readFileSync('/sys/fs/cgroup/cpu/cpu.cfs_period_us', 'utf8').trim(),
      10,
    );
    if (quota > 0 && period > 0) {
      return Math.max(1, Math.ceil(quota / period));
    }
  } catch {}
  return hostCpus;
}

const ISOLATE_POOL_BUDGET_FRACTION = 0.25;
const ISOLATE_POOL_MAX = 2;
const ISOLATE_POOL_SINGLE_WARM_THRESHOLD_MB = 2048;
const ISOLATE_MEMORY_MIN_MB = 40;
const ISOLATE_MEMORY_MAX_MB = 128;
const ISOLATE_MEMORY_RAM_DIVISOR = 32;

export function computeEngineTuning(spec: {
  logicalCpuCount: number;
  totalMemoryBytes: number;
}): {
  maxConcurrentWorkers: number;
  isolateMemoryLimitMb: number;
  tasksPerWorkerCap: number;
  isolatePoolSize: number;
} {
  const cpus = Math.max(1, Math.trunc(spec.logicalCpuCount) || 1);
  const totalMb = Math.max(1, spec.totalMemoryBytes / (1024 * 1024));

  const isolateMemoryLimitMb = Math.min(
    ISOLATE_MEMORY_MAX_MB,
    Math.max(
      ISOLATE_MEMORY_MIN_MB,
      Math.round(totalMb / ISOLATE_MEMORY_RAM_DIVISOR),
    ),
  );

  const isolateBudgetMb = totalMb * ISOLATE_POOL_BUDGET_FRACTION;
  const memoryBoundWorkers = Math.max(
    1,
    Math.floor(isolateBudgetMb / isolateMemoryLimitMb),
  );
  const maxConcurrentWorkers = Math.max(
    1,
    Math.min(cpus, 2, memoryBoundWorkers),
  );

  const tasksPerWorkerCap = Math.min(
    256,
    Math.max(
      16,
      Math.floor(totalMb / isolateMemoryLimitMb / maxConcurrentWorkers),
    ),
  );

  const isolatePoolMax =
    totalMb < ISOLATE_POOL_SINGLE_WARM_THRESHOLD_MB ? 1 : ISOLATE_POOL_MAX;
  const isolatePoolSize = Math.max(
    1,
    Math.min(
      isolatePoolMax,
      Math.floor(
        isolateBudgetMb / (maxConcurrentWorkers * isolateMemoryLimitMb),
      ),
    ),
  );

  return {
    maxConcurrentWorkers,
    isolateMemoryLimitMb,
    tasksPerWorkerCap,
    isolatePoolSize,
  };
}

export function getEngineTuning(): {
  maxConcurrentWorkers: number;
  isolateMemoryLimitMb: number;
  tasksPerWorkerCap: number;
  isolatePoolSize: number;
} {
  return computeEngineTuning({
    logicalCpuCount: getEffectiveCpuCount(),
    totalMemoryBytes: getEffectiveMemoryBytes(),
  });
}
