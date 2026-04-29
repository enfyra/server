import { monitorEventLoopDelay } from 'perf_hooks';
import { getHeapStatistics } from 'v8';
import { Redis } from 'ioredis';
import { EnvService, InstanceService } from '../../../shared/services';
import {
  getEffectiveCpuCount,
  getEffectiveMemoryBytes,
} from '../../../kernel/execution/executor-engine/utils/engine-tuning.util';
import type { RuntimeAverageSample } from '../../../shared/types';

const AVERAGE_FIELDS: Array<keyof RuntimeAverageSample> = [
  'rssMb',
  'heapUsedMb',
  'heapTotalMb',
  'externalMb',
  'eventLoopLagMs',
  'cpuRatio',
  'executorActiveTasks',
  'executorWaitingTasks',
  'executorP95TaskMs',
  'executorP99TaskMs',
  'executorMaxHeapRatio',
  'websocketConnections',
  'queueDepth',
  'queueFailed',
  'dbUsed',
  'dbAvailable',
  'dbIdle',
  'dbPending',
];
const DEFAULT_AVERAGE_TTL_MS = 20_000;

export class RuntimeProcessMetricsService {
  private readonly redis: Redis;
  private readonly instanceService: InstanceService;
  private readonly nodeName: string;
  private readonly eventLoopDelay = monitorEventLoopDelay({ resolution: 20 });
  private prevCpuUsage = process.cpuUsage();
  private prevCpuTime = process.hrtime.bigint();
  private lastCpuRatio = 0;

  constructor(deps: {
    redis: Redis;
    instanceService: InstanceService;
    envService: EnvService;
  }) {
    this.redis = deps.redis;
    this.instanceService = deps.instanceService;
    this.nodeName = deps.envService.get('NODE_NAME') || 'enfyra';
  }

  enable() {
    this.eventLoopDelay.enable();
  }

  disable() {
    this.eventLoopDelay.disable();
  }

  resetEventLoop() {
    this.eventLoopDelay.reset();
  }

  private averageKey() {
    return `${this.nodeName}:runtime-monitor:${this.instanceService.getInstanceId()}:averages`;
  }

  private emptyAverages(samples = 0) {
    return {
      onlineMs: process.uptime() * 1000,
      samples,
      rssMb: 0,
      heapUsedMb: 0,
      heapTotalMb: 0,
      externalMb: 0,
      eventLoopLagMs: 0,
      cpuRatio: 0,
      executorActiveTasks: 0,
      executorWaitingTasks: 0,
      executorP95TaskMs: 0,
      executorP99TaskMs: 0,
      executorMaxHeapRatio: 0,
      websocketConnections: 0,
      queueDepth: 0,
      queueFailed: 0,
      dbUsed: 0,
      dbAvailable: 0,
      dbIdle: 0,
      dbPending: 0,
    };
  }

  async resetAverages() {
    await this.redis.del(this.averageKey());
  }

  async onDestroy() {
    this.disable();
    await this.redis.del(this.averageKey());
  }

  private getCpuRatio(): number {
    const now = process.hrtime.bigint();
    const cpu = process.cpuUsage(this.prevCpuUsage);
    const elapsedUs = Number(now - this.prevCpuTime) / 1000;
    this.prevCpuUsage = process.cpuUsage();
    this.prevCpuTime = now;
    return elapsedUs > 0 ? (cpu.user + cpu.system) / elapsedUs : 0;
  }

  getProcessSample() {
    const memory = process.memoryUsage();
    const heapStats = getHeapStatistics();
    const eventLoopLagMs = this.eventLoopDelay.mean / 1e6;
    this.lastCpuRatio = this.getCpuRatio();
    return {
      instance: {
        id: this.instanceService.getInstanceId(),
        pid: process.pid,
        uptimeSec: process.uptime(),
        rssMb: memory.rss / 1024 / 1024,
        heapUsedMb: memory.heapUsed / 1024 / 1024,
        heapTotalMb: memory.heapTotal / 1024 / 1024,
        heapLimitMb: heapStats.heap_size_limit / 1024 / 1024,
        externalMb: memory.external / 1024 / 1024,
        eventLoopLagMs: Number.isFinite(eventLoopLagMs) ? eventLoopLagMs : 0,
        cpuRatio: this.lastCpuRatio,
      },
      hardware: this.getHardware(),
    };
  }

  getHardware() {
    const effectiveMemoryBytes = getEffectiveMemoryBytes();
    const effectiveCpuCount = getEffectiveCpuCount();
    return {
      effectiveMemoryMb: effectiveMemoryBytes / 1024 / 1024,
      effectiveCpuCount,
    };
  }

  async pushAverageSample(sample: RuntimeAverageSample) {
    const key = this.averageKey();
    const pipeline = this.redis.pipeline();
    pipeline.hincrby(key, 'samples', 1);
    for (const field of AVERAGE_FIELDS) {
      pipeline.hincrbyfloat(key, `total:${field}`, sample[field]);
    }
    pipeline.pexpire(key, DEFAULT_AVERAGE_TTL_MS);
    await pipeline.exec();
  }

  async getAverages() {
    const data = await this.redis.hgetall(this.averageKey());
    const count = Number(data.samples ?? 0);
    if (!count) return this.emptyAverages(0);

    const averages: Record<string, number> = this.emptyAverages(count);
    for (const field of AVERAGE_FIELDS) {
      averages[field] = Number(data[`total:${field}`] ?? 0) / count;
    }
    averages.onlineMs = process.uptime() * 1000;
    averages.samples = count;
    return averages;
  }
}
