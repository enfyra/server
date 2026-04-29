import { AsyncLocalStorage } from 'async_hooks';
import type { Redis } from 'ioredis';
import type {
  QueryMetricContext,
  RuntimeCacheReloadMetric,
  RuntimeFlowMetric,
  RuntimeQueryMetric,
  RuntimeRouteMetric,
} from '../types/runtime-metrics.types';
import { EnvService } from './env.service';
import { InstanceService } from './instance.service';

const MAX_LATENCIES = 256;
const MAX_RECENT = 20;
const SLOW_QUERY_MS = 500;
const LIVE_METRIC_TTL_MS = 10_000;
const CACHE_RELOAD_TTL_MS = 60 * 60 * 1000;
type RuntimeMetricsHashBucket = 'requests' | 'queries' | 'flows';

function pushLatency(target: number[], value: number) {
  target.push(value);
  if (target.length > MAX_LATENCIES) {
    target.splice(0, target.length - MAX_LATENCIES);
  }
}

function percentile(values: number[], p: number) {
  if (values.length === 0) return 0;
  const sorted = [...values].sort((a, b) => a - b);
  const index = Math.min(sorted.length - 1, Math.ceil((p / 100) * sorted.length) - 1);
  return sorted[Math.max(0, index)];
}

function isPoolAcquireTimeout(error: unknown) {
  if (!error) return false;
  const message = error instanceof Error ? error.message : String(error);
  return /timeout acquiring a connection|acquire.*timeout|pool.*timeout/i.test(
    message,
  );
}

class RuntimeMetricsRedisStore {
  private writeChain: Promise<void> = Promise.resolve();

  constructor(
    private readonly redis: Redis,
    private readonly keys: {
      requests: string;
      queries: string;
      flows: string;
      cacheReloads: string;
    },
  ) {}

  updateHash<T>(
    bucket: RuntimeMetricsHashBucket,
    field: string,
    create: () => T,
    update: (metric: T) => void,
  ): void {
    this.enqueue(async () => {
      const key = this.keys[bucket];
      const raw = await this.redis.hget(key, field);
      const metric = raw ? (JSON.parse(raw) as T) : create();
      update(metric);
      await this.redis
        .pipeline()
        .hset(key, field, JSON.stringify(metric))
        .pexpire(key, LIVE_METRIC_TTL_MS)
        .exec();
    });
  }

  pushCacheReload(record: RuntimeCacheReloadMetric): void {
    this.enqueue(() =>
      this.redis
        .pipeline()
        .lpush(this.keys.cacheReloads, JSON.stringify(record))
        .ltrim(this.keys.cacheReloads, 0, MAX_RECENT - 1)
        .pexpire(this.keys.cacheReloads, CACHE_RELOAD_TTL_MS)
        .exec()
        .then(() => undefined),
    );
  }

  async readSnapshot(): Promise<{
    requests: RuntimeRouteMetric[];
    queries: RuntimeQueryMetric[];
    flows: RuntimeFlowMetric[];
    cacheReloads: RuntimeCacheReloadMetric[];
  }> {
    await this.writeChain;
    const [requests, queries, flows, cacheReloads] = await Promise.all([
      this.readHash<RuntimeRouteMetric>('requests'),
      this.readHash<RuntimeQueryMetric>('queries'),
      this.readHash<RuntimeFlowMetric>('flows'),
      this.readCacheReloads(),
    ]);
    await this.redis
      .pipeline()
      .pexpire(this.keys.requests, LIVE_METRIC_TTL_MS)
      .pexpire(this.keys.queries, LIVE_METRIC_TTL_MS)
      .pexpire(this.keys.flows, LIVE_METRIC_TTL_MS)
      .exec();
    return { requests, queries, flows, cacheReloads };
  }

  async readCacheReloads(): Promise<RuntimeCacheReloadMetric[]> {
    await this.writeChain;
    return this.parseRows(
      await this.redis.lrange(this.keys.cacheReloads, 0, MAX_RECENT - 1),
    );
  }

  private enqueue(operation: () => Promise<void>): void {
    this.writeChain = this.writeChain.then(operation).catch(() => {});
  }

  private async readHash<T>(bucket: RuntimeMetricsHashBucket): Promise<T[]> {
    return this.parseRows(Object.values(await this.redis.hgetall(this.keys[bucket])));
  }

  private parseRows<T>(values: string[]): T[] {
    return values
      .map((value) => {
        try {
          return JSON.parse(value) as T;
        } catch {
          return null;
        }
      })
      .filter(Boolean) as T[];
  }
}

export class RuntimeMetricsCollectorService {
  private readonly queryContext = new AsyncLocalStorage<QueryMetricContext>();
  private readonly redisStore?: RuntimeMetricsRedisStore;
  private readonly instanceId?: string;
  private readonly requests = new Map<string, RuntimeRouteMetric>();
  private readonly queries = new Map<string, RuntimeQueryMetric>();
  private readonly flows = new Map<string, RuntimeFlowMetric>();
  private recentCacheReloads: RuntimeCacheReloadMetric[] = [];
  private startedAt = Date.now();

  constructor(
    deps: {
      redis?: Redis;
      envService?: EnvService;
      instanceService?: InstanceService;
    } = {},
  ) {
    this.instanceId = deps.instanceService?.getInstanceId();
    const nodeName = deps.envService?.get('NODE_NAME') || 'enfyra';
    if (deps.redis) {
      const instanceId = this.instanceId || 'local';
      const baseKey = `${nodeName}:runtime-monitor:${instanceId}`;
      this.redisStore = new RuntimeMetricsRedisStore(deps.redis, {
        requests: `${baseKey}:requests`,
        queries: `${baseKey}:queries`,
        flows: `${baseKey}:flows`,
        cacheReloads: `${nodeName}:runtime-monitor:cache-reloads`,
      });
    }
  }

  recordRequest(input: {
    method: string;
    route: string;
    statusCode: number;
    durationMs: number;
  }) {
    if (this.redisStore) {
      const field = `${input.method}:${input.route}`;
      this.redisStore.updateHash<RuntimeRouteMetric>(
        'requests',
        field,
        () => ({
          method: input.method,
          route: input.route,
          count: 0,
          status2xx: 0,
          status3xx: 0,
          status4xx: 0,
          status5xx: 0,
          totalMs: 0,
          latencies: [],
        }),
        (current) => this.applyRequestMetric(current, input),
      );
      return;
    }

    const key = `${input.method}:${input.route}`;
    const current =
      this.requests.get(key) ??
      {
        method: input.method,
        route: input.route,
        count: 0,
        status2xx: 0,
        status3xx: 0,
        status4xx: 0,
        status5xx: 0,
        totalMs: 0,
        latencies: [],
      };
    this.applyRequestMetric(current, input);
    this.requests.set(key, current);
  }

  recordQuery(input: {
    context?: QueryMetricContext;
    op: string;
    table?: string;
    durationMs: number;
    error?: unknown;
  }) {
    const context = input.context ?? this.getQueryContext();
    const table = input.table || 'unknown';
    if (this.redisStore) {
      const field = `${context}:${input.op}:${table}`;
      this.redisStore.updateHash<RuntimeQueryMetric>(
        'queries',
        field,
        () => ({
          context,
          op: input.op,
          table,
          count: 0,
          errors: 0,
          poolAcquireTimeouts: 0,
          slow: 0,
          totalMs: 0,
          latencies: [],
        }),
        (current) => this.applyQueryMetric(current, input),
      );
      return;
    }

    const key = `${context}:${input.op}:${table}`;
    const current =
      this.queries.get(key) ??
      {
        context,
        op: input.op,
        table,
        count: 0,
        errors: 0,
        poolAcquireTimeouts: 0,
        slow: 0,
        totalMs: 0,
        latencies: [],
      };
    this.applyQueryMetric(current, input);
    this.queries.set(key, current);
  }

  async trackQuery<T>(
    input: { op: string; table?: string; context?: QueryMetricContext },
    callback: () => Promise<T>,
  ): Promise<T> {
    const startedAt = performance.now();
    try {
      const result = await callback();
      this.recordQuery({
        ...input,
        durationMs: performance.now() - startedAt,
      });
      return result;
    } catch (error) {
      this.recordQuery({
        ...input,
        durationMs: performance.now() - startedAt,
        error,
      });
      throw error;
    }
  }

  runWithQueryContext<T>(
    context: QueryMetricContext,
    callback: () => Promise<T>,
  ): Promise<T> {
    return this.queryContext.run(context, callback);
  }

  getQueryContext(): QueryMetricContext {
    return this.queryContext.getStore() ?? 'runtime';
  }

  recordCacheReload(metric: RuntimeCacheReloadMetric) {
    const record = {
      ...metric,
      instanceId: metric.instanceId ?? this.instanceId,
    };
    if (this.redisStore) {
      this.redisStore.pushCacheReload(record);
      return;
    }

    this.recentCacheReloads.unshift(record);
    if (this.recentCacheReloads.length > MAX_RECENT) {
      this.recentCacheReloads = this.recentCacheReloads.slice(0, MAX_RECENT);
    }
  }

  async getRecentCacheReloads(): Promise<RuntimeCacheReloadMetric[]> {
    return this.redisStore
      ? await this.redisStore.readCacheReloads()
      : this.recentCacheReloads;
  }

  startFlow(flowId: string | number, flowName: string) {
    if (this.redisStore) {
      this.redisStore.updateHash<RuntimeFlowMetric>(
        'flows',
        String(flowId),
        () => this.createFlowMetric(flowId, flowName),
        (metric) => {
          metric.running++;
        },
      );
      return;
    }
    const metric = this.getFlowMetric(flowId, flowName);
    metric.running++;
  }

  completeFlow(input: {
    flowId: string | number;
    flowName: string;
    durationMs: number;
    status: 'completed' | 'failed';
  }) {
    if (this.redisStore) {
      this.redisStore.updateHash<RuntimeFlowMetric>(
        'flows',
        String(input.flowId),
        () => this.createFlowMetric(input.flowId, input.flowName),
        (metric) => this.applyFlowCompletion(metric, input),
      );
      return;
    }
    const metric = this.getFlowMetric(input.flowId, input.flowName);
    this.applyFlowCompletion(metric, input);
  }

  recordFlowStep(input: {
    flowId: string | number;
    flowName: string;
    stepKey: string;
    durationMs: number;
    failed?: boolean;
  }) {
    if (this.redisStore) {
      this.redisStore.updateHash<RuntimeFlowMetric>(
        'flows',
        String(input.flowId),
        () => this.createFlowMetric(input.flowId, input.flowName),
        (metric) => this.applyFlowStep(metric, input),
      );
      return;
    }
    const metric = this.getFlowMetric(input.flowId, input.flowName);
    this.applyFlowStep(metric, input);
  }

  snapshot() {
    return this.buildSnapshot(
      [...this.requests.values()],
      [...this.queries.values()],
      [...this.flows.values()],
      this.recentCacheReloads,
    );
  }

  async snapshotAsync() {
    if (this.redisStore) {
      const snapshot = await this.redisStore.readSnapshot();
      return this.buildSnapshot(
        snapshot.requests,
        snapshot.queries,
        snapshot.flows,
        snapshot.cacheReloads,
      );
    }

    return this.snapshot();
  }

  private buildSnapshot(
    requests: RuntimeRouteMetric[],
    queries: RuntimeQueryMetric[],
    flows: RuntimeFlowMetric[],
    recentCacheReloads: RuntimeCacheReloadMetric[],
  ) {
    const uptimeSec = Math.max(1, (Date.now() - this.startedAt) / 1000);
    const requestRoutes = requests
      .map((item) => ({
        method: item.method,
        route: item.route,
        count: item.count,
        rps: item.count / uptimeSec,
        avgMs: item.totalMs / item.count,
        p50Ms: percentile(item.latencies, 50),
        p95Ms: percentile(item.latencies, 95),
        p99Ms: percentile(item.latencies, 99),
        status2xx: item.status2xx,
        status3xx: item.status3xx,
        status4xx: item.status4xx,
        status5xx: item.status5xx,
      }))
      .sort((a, b) => b.count - a.count)
      .slice(0, 20);

    const queryRows = queries
      .map((item) => ({
        op: item.op,
        table: item.table,
        context: item.context,
        count: item.count,
        errors: item.errors,
        poolAcquireTimeouts: item.poolAcquireTimeouts,
        slow: item.slow,
        avgMs: item.totalMs / item.count,
        p95Ms: percentile(item.latencies, 95),
        p99Ms: percentile(item.latencies, 99),
      }))
      .sort((a, b) => b.p95Ms - a.p95Ms)
      .slice(0, 20);

    const flowRows = flows
      .map((item) => ({
        flowId: item.flowId,
        flowName: item.flowName,
        running: item.running,
        completed: item.completed,
        failed: item.failed,
        avgMs: item.completed + item.failed > 0 ? item.totalMs / (item.completed + item.failed) : 0,
        p95Ms: percentile(item.latencies, 95),
        failedSteps: Object.entries(item.failedSteps)
          .map(([step, count]) => ({ step, count }))
          .sort((a, b) => b.count - a.count)
          .slice(0, 5),
        slowSteps: Object.entries(item.stepLatencies)
          .map(([step, latencies]) => ({ step, p95Ms: percentile(latencies, 95) }))
          .sort((a, b) => b.p95Ms - a.p95Ms)
          .slice(0, 5),
      }))
      .sort((a, b) => b.running - a.running || b.failed - a.failed || b.p95Ms - a.p95Ms)
      .slice(0, 20);

    return {
      requests: {
        total: requestRoutes.reduce((sum, row) => sum + row.count, 0),
        rps: requestRoutes.reduce((sum, row) => sum + row.rps, 0),
        routes: requestRoutes,
      },
      cache: {
        recent: recentCacheReloads,
      },
      database: {
        slowQueryThresholdMs: SLOW_QUERY_MS,
        queries: queryRows,
        totalErrors: queryRows.reduce((sum, row) => sum + row.errors, 0),
        totalPoolAcquireTimeouts: queryRows.reduce(
          (sum, row) => sum + row.poolAcquireTimeouts,
          0,
        ),
        totalSlow: queryRows.reduce((sum, row) => sum + row.slow, 0),
      },
      flows: {
        rows: flowRows,
        running: flowRows.reduce((sum, row) => sum + row.running, 0),
        completed: flowRows.reduce((sum, row) => sum + row.completed, 0),
        failed: flowRows.reduce((sum, row) => sum + row.failed, 0),
      },
    };
  }

  private getFlowMetric(
    flowId: string | number,
    flowName: string,
  ): RuntimeFlowMetric {
    const key = String(flowId);
    const current =
      this.flows.get(key) ??
      this.createFlowMetric(flowId, flowName);
    this.flows.set(key, current);
    return current;
  }

  private createFlowMetric(
    flowId: string | number,
    flowName: string,
  ): RuntimeFlowMetric {
    return {
      flowId,
      flowName,
      running: 0,
      completed: 0,
      failed: 0,
      totalMs: 0,
      latencies: [],
      failedSteps: {},
      stepLatencies: {},
    };
  }

  private applyRequestMetric(
    current: RuntimeRouteMetric,
    input: {
      method: string;
      route: string;
      statusCode: number;
      durationMs: number;
    },
  ): void {
    current.count++;
    current.totalMs += input.durationMs;
    pushLatency(current.latencies, input.durationMs);
    if (input.statusCode >= 500) current.status5xx++;
    else if (input.statusCode >= 400) current.status4xx++;
    else if (input.statusCode >= 300) current.status3xx++;
    else current.status2xx++;
  }

  private applyQueryMetric(
    current: RuntimeQueryMetric,
    input: {
      context?: QueryMetricContext;
      op: string;
      table?: string;
      durationMs: number;
      error?: unknown;
    },
  ): void {
    current.count++;
    current.totalMs += input.durationMs;
    if (input.error) current.errors++;
    if (isPoolAcquireTimeout(input.error)) current.poolAcquireTimeouts++;
    if (input.durationMs >= SLOW_QUERY_MS) current.slow++;
    pushLatency(current.latencies, input.durationMs);
  }

  private applyFlowCompletion(
    metric: RuntimeFlowMetric,
    input: {
      flowId: string | number;
      flowName: string;
      durationMs: number;
      status: 'completed' | 'failed';
    },
  ): void {
    metric.running = Math.max(0, metric.running - 1);
    if (input.status === 'completed') metric.completed++;
    else metric.failed++;
    metric.totalMs += input.durationMs;
    pushLatency(metric.latencies, input.durationMs);
  }

  private applyFlowStep(
    metric: RuntimeFlowMetric,
    input: {
      flowId: string | number;
      flowName: string;
      stepKey: string;
      durationMs: number;
      failed?: boolean;
    },
  ): void {
    metric.stepLatencies[input.stepKey] ??= [];
    pushLatency(metric.stepLatencies[input.stepKey], input.durationMs);
    if (input.failed) {
      metric.failedSteps[input.stepKey] =
        (metric.failedSteps[input.stepKey] ?? 0) + 1;
    }
  }

}
