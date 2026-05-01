import { Logger } from '../../../shared/logger';
import {
  ENFYRA_ADMIN_ROOT_WEBSOCKET_ROOM,
  ENFYRA_ADMIN_WEBSOCKET_NAMESPACE,
} from '../../../shared/utils/constant';
import { DynamicWebSocketGateway } from '../../websocket';
import {
  RuntimeMetricsCollectorService,
  ClusterTelemetryService,
} from '../../../shared/services';
import { IsolatedExecutorService } from '@enfyra/kernel';
import { RuntimeProcessMetricsService } from './runtime-process-metrics.service';
import { RuntimeQueueMetricsService } from './runtime-queue-metrics.service';
import { RuntimeDbMetricsService } from './runtime-db-metrics.service';
import { RedisAdminService } from './redis-admin.service';

const SAMPLE_INTERVAL_MS = 2000;
const REDIS_SAMPLE_INTERVAL_MS = 5000;
const CLUSTER_APP_TTL_MS = SAMPLE_INTERVAL_MS * 5;
const APP_TELEMETRY_NAMESPACE = 'runtime-monitor:app';

export class RuntimeMonitorService {
  private readonly logger = new Logger(RuntimeMonitorService.name);
  private readonly dynamicWebSocketGateway: DynamicWebSocketGateway;
  private readonly isolatedExecutorService: IsolatedExecutorService;
  private readonly runtimeMetricsCollectorService: RuntimeMetricsCollectorService;
  private readonly clusterTelemetryService: ClusterTelemetryService;
  private readonly runtimeProcessMetricsService: RuntimeProcessMetricsService;
  private readonly runtimeQueueMetricsService: RuntimeQueueMetricsService;
  private readonly runtimeDbMetricsService: RuntimeDbMetricsService;
  private readonly redisAdminService: RedisAdminService;
  private timer?: ReturnType<typeof setInterval>;
  private sampling = false;
  private averageReset?: Promise<void>;
  private lastRedisSampleAt = 0;

  constructor(deps: {
    dynamicWebSocketGateway: DynamicWebSocketGateway;
    isolatedExecutorService: IsolatedExecutorService;
    runtimeMetricsCollectorService: RuntimeMetricsCollectorService;
    clusterTelemetryService: ClusterTelemetryService;
    runtimeProcessMetricsService: RuntimeProcessMetricsService;
    runtimeQueueMetricsService: RuntimeQueueMetricsService;
    runtimeDbMetricsService: RuntimeDbMetricsService;
    redisAdminService: RedisAdminService;
  }) {
    this.dynamicWebSocketGateway = deps.dynamicWebSocketGateway;
    this.isolatedExecutorService = deps.isolatedExecutorService;
    this.runtimeMetricsCollectorService = deps.runtimeMetricsCollectorService;
    this.clusterTelemetryService = deps.clusterTelemetryService;
    this.runtimeProcessMetricsService = deps.runtimeProcessMetricsService;
    this.runtimeQueueMetricsService = deps.runtimeQueueMetricsService;
    this.runtimeDbMetricsService = deps.runtimeDbMetricsService;
    this.redisAdminService = deps.redisAdminService;
  }

  start(): void {
    if (this.timer) return;
    this.runtimeProcessMetricsService.enable();
    this.averageReset = this.runtimeProcessMetricsService
      .resetAverages()
      .catch((error) => {
        this.logger.warn(
          `Runtime metrics average reset failed: ${error.message}`,
        );
      });
    this.timer = setInterval(() => {
      this.emitSample().catch((error) => {
        this.logger.warn(`Runtime metrics sample failed: ${error.message}`);
      });
    }, SAMPLE_INTERVAL_MS);
    this.timer.unref?.();
    this.emitSample().catch(() => {});
  }

  async onDestroy(): Promise<void> {
    if (this.timer) {
      clearInterval(this.timer);
      this.timer = undefined;
    }
    await Promise.all([
      this.runtimeProcessMetricsService.onDestroy(),
      this.runtimeMetricsCollectorService.onDestroy(),
      this.clusterTelemetryService.clearCurrentInstance(APP_TELEMETRY_NAMESPACE),
    ]);
  }

  async publishAppTelemetrySnapshot(app: any, sampledAt: string) {
    await this.clusterTelemetryService.publish(APP_TELEMETRY_NAMESPACE, app, {
      sampledAt,
      ttlMs: CLUSTER_APP_TTL_MS,
    });
  }

  async getAppTelemetryClusterSnapshot() {
    const cluster = await this.clusterTelemetryService.readCluster<any>(
      APP_TELEMETRY_NAMESPACE,
      { ttlMs: CLUSTER_APP_TTL_MS },
    );
    return {
      ttlMs: cluster.ttlMs,
      instances: cluster.instances.map((item) => ({
        instanceId: item.instanceId,
        sampledAt: item.sampledAt,
        app: item.payload,
      })),
    };
  }

  async captureAppTelemetry(sampledAt: string) {
    const app =
      typeof this.runtimeMetricsCollectorService.snapshotAsync === 'function'
        ? await this.runtimeMetricsCollectorService.snapshotAsync()
        : this.runtimeMetricsCollectorService.snapshot();
    await this.publishAppTelemetrySnapshot(app, sampledAt);
    return {
      app,
      appCluster: await this.getAppTelemetryClusterSnapshot(),
    };
  }

  async getSnapshot() {
    await this.averageReset;
    const executor = this.isolatedExecutorService.getMetrics();
    const processSample = this.runtimeProcessMetricsService.getProcessSample();
    const queues = await this.runtimeQueueMetricsService.getQueues();
    const websocket = this.dynamicWebSocketGateway.getConnectionStats();
    const db = this.runtimeDbMetricsService.getDbStats();
    const queueTotals = this.runtimeQueueMetricsService.getQueueTotals(queues);
    const dbTotals = this.runtimeDbMetricsService.getDbPoolTotals(db);

    await this.runtimeProcessMetricsService.pushAverageSample({
      rssMb: processSample.instance.rssMb,
      heapUsedMb: processSample.instance.heapUsedMb,
      heapTotalMb: processSample.instance.heapTotalMb,
      externalMb: processSample.instance.externalMb,
      eventLoopLagMs: processSample.instance.eventLoopLagMs,
      cpuRatio: processSample.instance.cpuRatio,
      executorActiveTasks: executor.pool.activeTasks,
      executorWaitingTasks: executor.pool.waitingTasks,
      executorP95TaskMs: executor.p95TaskMs,
      executorP99TaskMs: executor.p99TaskMs,
      executorMaxHeapRatio: executor.maxHeapRatio,
      websocketConnections: websocket.total,
      queueDepth: queueTotals.depth,
      queueFailed: queueTotals.failed,
      dbUsed: dbTotals.used,
      dbAvailable: dbTotals.available,
      dbIdle: dbTotals.idle,
      dbPending: dbTotals.pending,
    });

    const sampledAt = new Date().toISOString();
    const appTelemetry = await this.captureAppTelemetry(sampledAt);

    return {
      kind: 'runtime-metrics',
      sampledAt,
      intervalMs: SAMPLE_INTERVAL_MS,
      averages: await this.runtimeProcessMetricsService.getAverages(),
      hardware: processSample.hardware,
      instance: processSample.instance,
      executor,
      queues,
      websocket,
      db,
      cluster: await this.runtimeDbMetricsService.getClusterStats(),
      health: this.evaluateHealth({
        instance: processSample.instance,
        executor,
        queues,
        db,
        app: appTelemetry.app,
      }),
      ...appTelemetry,
    };
  }

  async emitRedisOverview(): Promise<void> {
    const listeners = await this.dynamicWebSocketGateway.namespaceRoomSize(
      ENFYRA_ADMIN_WEBSOCKET_NAMESPACE,
      ENFYRA_ADMIN_ROOT_WEBSOCKET_ROOM,
    );
    if (listeners === 0) return;
    const overview = await this.redisAdminService.getOverview();
    this.dynamicWebSocketGateway.emitToNamespaceRoom(
      ENFYRA_ADMIN_WEBSOCKET_NAMESPACE,
      ENFYRA_ADMIN_ROOT_WEBSOCKET_ROOM,
      '$system:redis:overview',
      overview,
    );
  }

  emitRedisKeyChanged(payload: any): void {
    this.dynamicWebSocketGateway.emitToNamespaceRoom(
      ENFYRA_ADMIN_WEBSOCKET_NAMESPACE,
      ENFYRA_ADMIN_ROOT_WEBSOCKET_ROOM,
      '$system:redis:key:changed',
      payload,
    );
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
      const now = Date.now();
      if (now - this.lastRedisSampleAt >= REDIS_SAMPLE_INTERVAL_MS) {
        this.lastRedisSampleAt = now;
        await this.emitRedisOverview();
      }
    } finally {
      this.runtimeProcessMetricsService.resetEventLoop();
      this.sampling = false;
    }
  }

  private evaluateHealth(metrics: {
    instance: any;
    executor: any;
    queues: Record<string, any>;
    db: any;
    app?: any;
  }) {
    const overview = this.buildOverviewWarnings(metrics);
    const workers = this.buildWorkerWarnings(metrics);
    const flows = this.buildFlowWarnings(metrics);
    const database = this.buildDatabaseWarnings(metrics);
    const connections = this.buildConnectionWarnings(metrics);
    return {
      overview,
      workers,
      flows,
      database,
      connections,
    };
  }

  private buildOverviewWarnings(metrics: { instance: any }) {
    const messages: string[] = [];
    const eventLoopLagMs = metrics.instance.eventLoopLagMs ?? 0;
    const heapLimitMb = metrics.instance.heapLimitMb ?? metrics.instance.heapTotalMb;
    const heapRatio = heapLimitMb > 0 ? metrics.instance.heapUsedMb / heapLimitMb : 0;
    if (eventLoopLagMs >= 50) {
      messages.push(`Event loop delay is ${this.formatMs(eventLoopLagMs)}.`);
    }
    if (heapRatio >= 0.75) {
      messages.push(`Main heap usage is ${this.formatPercent(heapRatio)} of V8 limit.`);
    }
    return {
      severity: this.maxSeverity(
        eventLoopLagMs >= 200 ? 'error' : eventLoopLagMs >= 50 ? 'warning' : 'ok',
        heapRatio >= 0.9 ? 'error' : heapRatio >= 0.75 ? 'warning' : 'ok',
      ),
      messages,
    };
  }

  private buildWorkerWarnings(metrics: { executor: any }) {
    const messages: string[] = [];
    const waitingTasks = metrics.executor.pool?.waitingTasks ?? 0;
    const maxHeapRatio = metrics.executor.maxHeapRatio ?? 0;
    const p99TaskMs = metrics.executor.p99TaskMs ?? 0;
    const taskErrorTotal = metrics.executor.taskErrorTotal ?? 0;
    const taskTimeoutTotal = metrics.executor.taskTimeoutTotal ?? 0;
    const crashesTotal = metrics.executor.crashesTotal ?? 0;
    const rotationsTotal = metrics.executor.rotationsTotal ?? 0;
    const scrubFailed = (metrics.executor.pool?.workers ?? []).some(
      (worker: any) => (worker.contextStats?.scrubFailed ?? 0) > 0,
    );
    if (waitingTasks > 0) {
      messages.push(
        `${waitingTasks} executor task${waitingTasks > 1 ? 's are' : ' is'} waiting.`,
      );
    }
    if (maxHeapRatio >= 0.65) {
      messages.push(`Isolate heap pressure is ${this.formatPercent(maxHeapRatio)}.`);
    }
    if (p99TaskMs >= 1000) {
      messages.push(`Executor p99 latency is ${this.formatMs(p99TaskMs)}.`);
    }
    if (taskErrorTotal > 0) {
      messages.push(`${taskErrorTotal} executor task error${taskErrorTotal > 1 ? 's' : ''}.`);
    }
    if (taskTimeoutTotal > 0) {
      messages.push(`${taskTimeoutTotal} executor timeout${taskTimeoutTotal > 1 ? 's' : ''}.`);
    }
    if (crashesTotal > 0) {
      messages.push(`${crashesTotal} executor worker crash${crashesTotal > 1 ? 'es' : ''}.`);
    }
    if (scrubFailed) {
      messages.push('Context scrub failure detected.');
    }
    if (rotationsTotal >= 10) {
      messages.push(`${rotationsTotal} executor rotations.`);
    }
    return {
      severity: this.maxSeverity(
        waitingTasks >= 100 ? 'error' : waitingTasks > 0 ? 'warning' : 'ok',
        maxHeapRatio >= 0.85 ? 'error' : maxHeapRatio >= 0.65 ? 'warning' : 'ok',
        p99TaskMs >= 5000 ? 'error' : p99TaskMs >= 1000 ? 'warning' : 'ok',
        taskTimeoutTotal > 0 || crashesTotal > 0 ? 'error' : taskErrorTotal > 0 ? 'warning' : 'ok',
        scrubFailed ? 'error' : 'ok',
        rotationsTotal >= 10 ? 'warning' : 'ok',
      ),
      messages,
    };
  }

  private buildFlowWarnings(metrics: { queues: Record<string, any>; app?: any }) {
    const messages: string[] = [];
    const queue = metrics.queues.flow;
    const failedQueueJobs = queue?.failed ?? 0;
    const total = this.queueTotal(queue);
    const failedFlows = (metrics.app?.flows?.rows ?? []).reduce(
      (sum: number, row: any) => sum + (row.failed ?? 0),
      0,
    );
    if (failedQueueJobs > 0) {
      messages.push(`Flow queue has ${failedQueueJobs} retained failed job${failedQueueJobs > 1 ? 's' : ''}.`);
    }
    if (total >= 100) {
      messages.push(`Flow queue depth is ${total}.`);
    }
    if (failedFlows > 0) {
      messages.push(`${failedFlows} flow execution${failedFlows > 1 ? 's have' : ' has'} failed.`);
    }
    return {
      severity: this.maxSeverity(
        this.queueSeverity(queue),
        failedFlows > 0 ? 'error' : 'ok',
      ),
      messages,
    };
  }

  private buildDatabaseWarnings(metrics: { db: any; app?: any }) {
    const messages: string[] = [];
    const pendingDb = this.dbPoolRows(metrics.db).reduce(
      (sum, row) => sum + (row.pending ?? 0),
      0,
    );
    const totalPoolAcquireTimeouts = metrics.app?.database?.totalPoolAcquireTimeouts ?? 0;
    const totalErrors = metrics.app?.database?.totalErrors ?? 0;
    const totalSlow = metrics.app?.database?.totalSlow ?? 0;
    if (pendingDb > 0) {
      messages.push(`DB pool has ${pendingDb} pending request${pendingDb > 1 ? 's' : ''}.`);
    }
    if (totalPoolAcquireTimeouts > 0) {
      messages.push(`${totalPoolAcquireTimeouts} DB pool acquire timeout${totalPoolAcquireTimeouts > 1 ? 's' : ''}.`);
    }
    if (totalErrors > 0) {
      messages.push(`${totalErrors} DB query error${totalErrors > 1 ? 's' : ''}.`);
    }
    if (totalSlow > 0) {
      messages.push(`${totalSlow} slow DB quer${totalSlow > 1 ? 'ies' : 'y'}.`);
    }
    return {
      severity: this.maxSeverity(
        pendingDb >= 100 ? 'error' : pendingDb > 0 ? 'warning' : 'ok',
        totalPoolAcquireTimeouts > 0 || totalErrors > 0 ? 'error' : 'ok',
        totalSlow > 0 ? 'warning' : 'ok',
      ),
      messages,
    };
  }

  private buildConnectionWarnings(metrics: { queues: Record<string, any> }) {
    const messages: string[] = [];
    const severities: Array<'ok' | 'warning' | 'error'> = [];
    for (const [name, queue] of this.connectionQueueEntries(metrics.queues)) {
      if (!queue) continue;
      const total = this.queueTotal(queue);
      const severity = this.queueSeverity(queue);
      severities.push(severity);
      if ((queue.failed ?? 0) > 0) {
        messages.push(`${name} queue has ${queue.failed} retained failed job${queue.failed > 1 ? 's' : ''}.`);
      }
      if (total >= 100) {
        messages.push(`${name} queue depth is ${total}.`);
      }
      if (severity === 'error' && total < 100) {
        messages.push(`${name} queue has critical failed job retention.`);
      }
    }
    return {
      severity: this.maxSeverity(...severities),
      messages,
    };
  }

  private queueTotal(queue: any): number {
    if (!queue) return 0;
    return (
      (queue.waiting ?? 0) +
      (queue.active ?? 0) +
      (queue.delayed ?? 0) +
      (queue.failed ?? 0)
    );
  }

  private queueSeverity(queue: any): 'ok' | 'warning' | 'error' {
    if (!queue) return 'ok';
    const total = this.queueTotal(queue);
    if (total >= 1000 || (queue.failed ?? 0) >= 100) return 'error';
    if (total >= 100 || (queue.failed ?? 0) > 0) return 'warning';
    return 'ok';
  }

  private connectionQueueEntries(queues: Record<string, any>) {
    return Object.entries(queues).filter(([name]) =>
      name === 'websocket' || name.startsWith('websocket:'),
    );
  }

  private dbPoolRows(db: any): Array<{ pending?: number }> {
    const pool = db?.pool;
    if (!pool) return [];
    if (Array.isArray(pool.replicas)) return pool.replicas.map((replica: any) => replica.pool ?? {});
    return [pool];
  }

  private maxSeverity(...values: Array<'ok' | 'warning' | 'error'>): 'ok' | 'warning' | 'error' {
    if (values.includes('error')) return 'error';
    if (values.includes('warning')) return 'warning';
    return 'ok';
  }

  private formatMs(value: number): string {
    if (value >= 1000) return `${(value / 1000).toFixed(1)}s`;
    return `${Math.round(value)}ms`;
  }

  private formatPercent(value: number): string {
    return `${Math.round(value * 100)}%`;
  }
}
