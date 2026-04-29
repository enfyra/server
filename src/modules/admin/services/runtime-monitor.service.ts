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
import { IsolatedExecutorService } from '../../../kernel/execution';
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
}
