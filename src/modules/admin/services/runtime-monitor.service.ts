import { monitorEventLoopDelay } from 'perf_hooks';
import { getHeapStatistics } from 'v8';
import * as os from 'os';
import { Queue } from 'bullmq';
import { Redis } from 'ioredis';
import { Logger } from '../../../shared/logger';
import { ENFYRA_ADMIN_WEBSOCKET_NAMESPACE } from '../../../shared/utils/constant';
import { DynamicWebSocketGateway } from '../../websocket';
import {
  InstanceService,
  DatabaseConfigService,
  RuntimeMetricsCollectorService,
} from '../../../shared/services';
import { IsolatedExecutorService } from '../../../kernel/execution';
import {
  KnexService,
  ReplicationManager,
  SqlPoolClusterCoordinatorService,
} from '../../../engine/knex';
import {
  getEffectiveCpuCount,
  getEffectiveMemoryBytes,
} from '../../../kernel/execution/executor-engine/utils/engine-tuning.util';

const SAMPLE_INTERVAL_MS = 2000;
const AVERAGE_TTL_MS = SAMPLE_INTERVAL_MS * 10;

type QueueLike = Queue | undefined | null;
type QueueFailedJob = {
  id: string;
  name: string;
  flowId?: string | number;
  flowName?: string;
  failedReason?: string;
  attemptsMade: number;
  timestamp?: number;
  finishedOn?: number;
};
type AverageSample = {
  rssMb: number;
  heapUsedMb: number;
  heapTotalMb: number;
  externalMb: number;
  eventLoopLagMs: number;
  cpuRatio: number;
  executorActiveTasks: number;
  executorWaitingTasks: number;
  executorP95TaskMs: number;
  executorP99TaskMs: number;
  executorMaxHeapRatio: number;
  websocketConnections: number;
  queueDepth: number;
  queueFailed: number;
  dbUsed: number;
  dbFree: number;
  dbPending: number;
};
const AVERAGE_FIELDS: Array<keyof AverageSample> = [
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
  'dbFree',
  'dbPending',
];

export class RuntimeMonitorService {
  private readonly logger = new Logger(RuntimeMonitorService.name);
  private readonly dynamicWebSocketGateway: DynamicWebSocketGateway;
  private readonly instanceService: InstanceService;
  private readonly isolatedExecutorService: IsolatedExecutorService;
  private readonly databaseConfigService: DatabaseConfigService;
  private readonly knexService: KnexService;
  private readonly replicationManager?: ReplicationManager;
  private readonly sqlPoolClusterCoordinatorService?: SqlPoolClusterCoordinatorService;
  private readonly runtimeMetricsCollectorService: RuntimeMetricsCollectorService;
  private readonly redis: Redis;
  private readonly flowQueue: QueueLike;
  private readonly wsConnectionQueue: QueueLike;
  private readonly wsEventQueue: QueueLike;
  private readonly eventLoopDelay = monitorEventLoopDelay({ resolution: 20 });
  private timer?: ReturnType<typeof setInterval>;
  private prevCpuUsage = process.cpuUsage();
  private prevCpuTime = process.hrtime.bigint();
  private sampling = false;
  private averageReset?: Promise<void>;
  private lastCpuRatio = 0;

  constructor(deps: {
    dynamicWebSocketGateway: DynamicWebSocketGateway;
    instanceService: InstanceService;
    isolatedExecutorService: IsolatedExecutorService;
    databaseConfigService: DatabaseConfigService;
    knexService: KnexService;
    redis: Redis;
    replicationManager?: ReplicationManager;
    sqlPoolClusterCoordinatorService?: SqlPoolClusterCoordinatorService;
    runtimeMetricsCollectorService: RuntimeMetricsCollectorService;
    flowQueue?: Queue;
    wsConnectionQueue?: Queue;
    wsEventQueue?: Queue;
  }) {
    this.dynamicWebSocketGateway = deps.dynamicWebSocketGateway;
    this.instanceService = deps.instanceService;
    this.isolatedExecutorService = deps.isolatedExecutorService;
    this.databaseConfigService = deps.databaseConfigService;
    this.knexService = deps.knexService;
    this.redis = deps.redis;
    this.replicationManager = deps.replicationManager;
    this.sqlPoolClusterCoordinatorService =
      deps.sqlPoolClusterCoordinatorService;
    this.runtimeMetricsCollectorService = deps.runtimeMetricsCollectorService;
    this.flowQueue = deps.flowQueue;
    this.wsConnectionQueue = deps.wsConnectionQueue;
    this.wsEventQueue = deps.wsEventQueue;
  }

  start(): void {
    if (this.timer) return;
    this.eventLoopDelay.enable();
    this.averageReset = this.resetAverages().catch((error) => {
      this.logger.warn(`Runtime metrics average reset failed: ${error.message}`);
    });
    this.timer = setInterval(() => {
      this.emitSample().catch((error) => {
        this.logger.warn(`Runtime metrics sample failed: ${error.message}`);
      });
    }, SAMPLE_INTERVAL_MS);
    this.timer.unref?.();
    this.emitSample().catch(() => {});
  }

  onDestroy(): void {
    if (this.timer) {
      clearInterval(this.timer);
      this.timer = undefined;
    }
    this.eventLoopDelay.disable();
  }

  private getCpuRatio(): number {
    const now = process.hrtime.bigint();
    const cpu = process.cpuUsage(this.prevCpuUsage);
    const elapsedUs = Number(now - this.prevCpuTime) / 1000;
    this.prevCpuUsage = process.cpuUsage();
    this.prevCpuTime = now;
    return elapsedUs > 0 ? (cpu.user + cpu.system) / elapsedUs : 0;
  }

  private async getQueueStats(queue: QueueLike, options?: { includeFailedJobs?: boolean }) {
    if (!queue) return null;
    try {
      const counts = await queue.getJobCounts(
        'waiting',
        'active',
        'delayed',
        'failed',
      );
      const failedJobs: QueueFailedJob[] = [];
      if (options?.includeFailedJobs && (counts.failed ?? 0) > 0) {
        const jobs = await queue.getFailed(0, 14);
        for (const job of jobs) {
          const data = job.data as any;
          failedJobs.push({
            id: String(job.id ?? ''),
            name: job.name,
            flowId: data?.flowId,
            flowName: data?.flowName,
            failedReason: job.failedReason,
            attemptsMade: job.attemptsMade,
            timestamp: job.timestamp,
            finishedOn: job.finishedOn,
          });
        }
      }
      return {
        waiting: counts.waiting ?? 0,
        active: counts.active ?? 0,
        delayed: counts.delayed ?? 0,
        failed: counts.failed ?? 0,
        failedJobs,
      };
    } catch {
      return null;
    }
  }

  private getDbStats() {
    if (this.databaseConfigService.isMongoDb()) {
      return { type: 'mongodb', pool: null };
    }
    return {
      type: this.databaseConfigService.getDbType(),
      pool:
        this.replicationManager?.getPoolStats?.() ??
        this.knexService?.getPoolStats?.() ??
        null,
    };
  }

  private async getClusterStats() {
    return this.sqlPoolClusterCoordinatorService?.getClusterStats?.() ?? null;
  }

  private getDbPoolTotals(db: any) {
    const pool = db?.pool;
    if (!pool) return { used: 0, free: 0, pending: 0 };
    const rows =
      pool.master || Array.isArray(pool.replicas)
        ? [
            pool.master,
            ...(pool.replicas ?? []).map((replica: any) => replica.pool),
          ]
        : [pool];

    return rows.reduce(
      (sum: { used: number; free: number; pending: number }, row: any) => ({
        used: sum.used + (row?.used ?? 0),
        free: sum.free + (row?.free ?? 0),
        pending: sum.pending + (row?.pending ?? 0),
      }),
      { used: 0, free: 0, pending: 0 },
    );
  }

  private getQueueTotals(queues: Record<string, Awaited<ReturnType<typeof this.getQueueStats>>>) {
    return Object.values(queues).reduce(
      (sum, queue) => ({
        depth:
          sum.depth +
          (queue
            ? queue.waiting + queue.active + queue.delayed + queue.failed
            : 0),
        failed: sum.failed + (queue?.failed ?? 0),
      }),
      { depth: 0, failed: 0 },
    );
  }

  private averageKey() {
    return `runtime-monitor:${this.instanceService.getInstanceId()}:averages`;
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
      dbFree: 0,
      dbPending: 0,
    };
  }

  private async resetAverages() {
    await this.redis.del(this.averageKey());
  }

  private async pushAverageSample(sample: AverageSample) {
    const key = this.averageKey();
    const pipeline = this.redis.pipeline();
    pipeline.hincrby(key, 'samples', 1);
    for (const field of AVERAGE_FIELDS) {
      pipeline.hincrbyfloat(key, `total:${field}`, sample[field]);
    }
    pipeline.pexpire(key, AVERAGE_TTL_MS);
    await pipeline.exec();
  }

  private async getAverages() {
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

  async getSnapshot() {
    await this.averageReset;
    const memory = process.memoryUsage();
    const heapStats = getHeapStatistics();
    const executor = this.isolatedExecutorService.getMetrics();
    const eventLoopLagMs = this.eventLoopDelay.mean / 1e6;
    const effectiveMemoryBytes = getEffectiveMemoryBytes();
    const hostMemoryBytes = os.totalmem();
    const effectiveCpuCount = getEffectiveCpuCount();
    const hostCpuCount = os.cpus()?.length || 1;
    const queues = {
      flow: await this.getQueueStats(this.flowQueue, { includeFailedJobs: true }),
      websocketConnection: await this.getQueueStats(this.wsConnectionQueue),
      websocketEvent: await this.getQueueStats(this.wsEventQueue),
    };
    const websocket = this.dynamicWebSocketGateway.getConnectionStats();
    const db = this.getDbStats();
    const queueTotals = this.getQueueTotals(queues);
    const dbTotals = this.getDbPoolTotals(db);

    this.lastCpuRatio = this.getCpuRatio();
    await this.pushAverageSample({
      rssMb: memory.rss / 1024 / 1024,
      heapUsedMb: memory.heapUsed / 1024 / 1024,
      heapTotalMb: memory.heapTotal / 1024 / 1024,
      externalMb: memory.external / 1024 / 1024,
      eventLoopLagMs: Number.isFinite(eventLoopLagMs) ? eventLoopLagMs : 0,
      cpuRatio: this.lastCpuRatio,
      executorActiveTasks: executor.pool.activeTasks,
      executorWaitingTasks: executor.pool.waitingTasks,
      executorP95TaskMs: executor.p95TaskMs,
      executorP99TaskMs: executor.p99TaskMs,
      executorMaxHeapRatio: executor.maxHeapRatio,
      websocketConnections: websocket.total,
      queueDepth: queueTotals.depth,
      queueFailed: queueTotals.failed,
      dbUsed: dbTotals.used,
      dbFree: dbTotals.free,
      dbPending: dbTotals.pending,
    });

    return {
      kind: 'runtime-metrics',
      sampledAt: new Date().toISOString(),
      intervalMs: SAMPLE_INTERVAL_MS,
      averages: await this.getAverages(),
      hardware: {
        effectiveMemoryMb: effectiveMemoryBytes / 1024 / 1024,
        hostMemoryMb: hostMemoryBytes / 1024 / 1024,
        effectiveCpuCount,
        hostCpuCount,
        constrained:
          effectiveMemoryBytes < hostMemoryBytes ||
          effectiveCpuCount < hostCpuCount,
      },
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
      executor,
      queues,
      websocket,
      db,
      cluster: await this.getClusterStats(),
      app: this.runtimeMetricsCollectorService.snapshot(),
    };
  }

  private async emitSample(): Promise<void> {
    if (this.sampling) return;
    this.sampling = true;
    try {
      const snapshot = await this.getSnapshot();
      this.dynamicWebSocketGateway.emitToNamespace(
        ENFYRA_ADMIN_WEBSOCKET_NAMESPACE,
        '$system:runtime:metrics',
        snapshot,
      );
    } finally {
      this.eventLoopDelay.reset();
      this.sampling = false;
    }
  }
}
