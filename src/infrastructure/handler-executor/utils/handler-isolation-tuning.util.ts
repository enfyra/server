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
  const hostCpus = os.cpus()?.length || 1;
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

export function computeHandlerIsolationTuning(spec: {
  logicalCpuCount: number;
  totalMemoryBytes: number;
}): { maxConcurrentWorkers: number; isolateMemoryLimitMb: number } {
  const cpus = Math.max(1, Math.trunc(spec.logicalCpuCount) || 1);
  const totalMb = Math.max(1, spec.totalMemoryBytes / (1024 * 1024));

  const isolateMemoryLimitMb = Math.min(
    128,
    Math.max(32, Math.round(totalMb / 40)),
  );

  const perWorkerMb = isolateMemoryLimitMb + 100;
  const byMem = Math.max(2, Math.floor((totalMb * 0.2) / perWorkerMb));
  const byCpu = Math.max(2, Math.min(cpus * 2, 64));
  const maxConcurrentWorkers = Math.max(2, Math.min(byCpu, byMem, 96));

  return { maxConcurrentWorkers, isolateMemoryLimitMb };
}

export function getHandlerIsolationTuning(): {
  maxConcurrentWorkers: number;
  isolateMemoryLimitMb: number;
} {
  return computeHandlerIsolationTuning({
    logicalCpuCount: getEffectiveCpuCount(),
    totalMemoryBytes: getEffectiveMemoryBytes(),
  });
}

export class AsyncSemaphore {
  private active = 0;
  private readonly waiting: Array<() => void> = [];
  private max: number;

  constructor(max: number) {
    if (max < 1) {
      throw new Error('AsyncSemaphore max must be >= 1');
    }
    this.max = max;
  }

  acquire(): Promise<void> {
    return new Promise((resolve) => {
      const tryTake = () => {
        if (this.active < this.max) {
          this.active++;
          resolve();
        } else {
          this.waiting.push(tryTake);
        }
      };
      tryTake();
    });
  }

  release(): void {
    this.active--;
    const next = this.waiting.shift();
    if (next) {
      next();
    }
  }

  resize(newMax: number): void {
    this.max = Math.max(1, newMax);
    while (this.active < this.max && this.waiting.length > 0) {
      const next = this.waiting.shift();
      if (next) next();
    }
  }

  getMax(): number {
    return this.max;
  }

  getWaitingCount(): number {
    return this.waiting.length;
  }
}
