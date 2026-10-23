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
import type { RuntimeMetricsSnapshot } from '../types/runtime-monitor.types';

const SAMPLE_INTERVAL_MS = 2000;
const REDIS_SAMPLE_INTERVAL_MS = 5000;
const CLUSTER_APP_TTL_MS = SAMPLE_INTERVAL_MS * 5;
const APP_TELEMETRY_NAMESPACE = 'runtime-monitor:app';
const METRICS_TELEMETRY_NAMESPACE = 'runtime-monitor:metrics';

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
  private timer: ReturnType<typeof setInterval> | undefined;
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
      this.clusterTelemetryService.clearCurrentInstance(METRICS_TELEMETRY_NAMESPACE),
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
      activeCount: cluster.instances.length,
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

  async getClusterSnapshot() {
    const cluster = await this.clusterTelemetryService.readCluster<RuntimeMetricsSnapshot>(
      METRICS_TELEMETRY_NAMESPACE,
      { ttlMs: CLUSTER_APP_TTL_MS },
    );
    return {
      intervalMs: SAMPLE_INTERVAL_MS,
      instances: cluster.instances.map((item) => item.payload),
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
      executorP95QueueWaitMs: executor.p95QueueWaitMs ?? 0,
      executorP99QueueWaitMs: executor.p99QueueWaitMs ?? 0,
      executorRunnerRssMb: (executor.runnerRssBytes ?? 0) / 1024 / 1024,
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
      await this.clusterTelemetryService.publish(
        METRICS_TELEMETRY_NAMESPACE,
        snapshot,
        { sampledAt: snapshot.sampledAt, ttlMs: CLUSTER_APP_TTL_MS },
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

  private buildOverviewWarnings(_metrics: { instance: any }) {
    return {
      severity: 'ok' as const,
      messages: [],
    };
  }

  private buildWorkerWarnings(metrics: { executor: any }) {
    const messages: string[] = [];
    const taskErrorTotal = metrics.executor.taskErrorTotal ?? 0;
    const taskTimeoutTotal = metrics.executor.taskTimeoutTotal ?? 0;
    const crashesTotal = metrics.executor.crashesTotal ?? 0;
    const scrubFailed = (metrics.executor.pool?.workers ?? []).some(
      (worker: any) => (worker.contextStats?.scrubFailed ?? 0) > 0,
    );
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
    return {
      severity: this.maxSeverity(
        taskTimeoutTotal > 0 || crashesTotal > 0 ? 'error' : taskErrorTotal > 0 ? 'warning' : 'ok',
        scrubFailed ? 'error' : 'ok',
      ),
      messages,
    };
  }

  private buildFlowWarnings(metrics: { queues: Record<string, any>; app?: any }) {
    const messages: string[] = [];
    const queue = metrics.queues.flow;
    const failedQueueJobs = queue?.failed ?? 0;
    const failedFlows = (metrics.app?.flows?.rows ?? []).reduce(
      (sum: number, row: any) => sum + (row.failed ?? 0),
      0,
    );
    if (failedQueueJobs > 0) {
      messages.push(`Flow queue has ${failedQueueJobs} retained failed job${failedQueueJobs > 1 ? 's' : ''}.`);
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
    const totalPoolAcquireTimeouts = metrics.app?.database?.totalPoolAcquireTimeouts ?? 0;
    const totalErrors = metrics.app?.database?.totalErrors ?? 0;
    if (totalPoolAcquireTimeouts > 0) {
      messages.push(`${totalPoolAcquireTimeouts} DB pool acquire timeout${totalPoolAcquireTimeouts > 1 ? 's' : ''}.`);
    }
    if (totalErrors > 0) {
      messages.push(`${totalErrors} DB query error${totalErrors > 1 ? 's' : ''}.`);
    }
    return {
      severity: this.maxSeverity(
        totalPoolAcquireTimeouts > 0 || totalErrors > 0 ? 'error' : 'ok',
      ),
      messages,
    };
  }

  private buildConnectionWarnings(metrics: { queues: Record<string, any> }) {
    const messages: string[] = [];
    const severities: Array<'ok' | 'warning' | 'error'> = [];
    for (const [name, queue] of this.connectionQueueEntries(metrics.queues)) {
      if (!queue) continue;
      const severity = this.queueSeverity(queue);
      severities.push(severity);
      if ((queue.failed ?? 0) > 0) {
        messages.push(`${name} queue has ${queue.failed} retained failed job${queue.failed > 1 ? 's' : ''}.`);
      }
    }
    return {
      severity: this.maxSeverity(...severities),
      messages,
    };
  }

  private queueSeverity(queue: any): 'ok' | 'warning' | 'error' {
    if (!queue) return 'ok';
    if ((queue.failed ?? 0) >= 100) return 'error';
    if ((queue.failed ?? 0) > 0) return 'warning';
    return 'ok';
  }

  private connectionQueueEntries(queues: Record<string, any>) {
    return Object.entries(queues).filter(([name]) =>
      name === 'websocket' || name.startsWith('websocket:'),
    );
  }

  private maxSeverity(...values: Array<'ok' | 'warning' | 'error'>): 'ok' | 'warning' | 'error' {
    if (values.includes('error')) return 'error';
    if (values.includes('warning')) return 'warning';
    return 'ok';
  }

}
