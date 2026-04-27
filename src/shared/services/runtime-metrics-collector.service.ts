type RouteMetric = {
  method: string;
  route: string;
  count: number;
  status2xx: number;
  status3xx: number;
  status4xx: number;
  status5xx: number;
  totalMs: number;
  latencies: number[];
};

type QueryMetric = {
  op: string;
  table: string;
  count: number;
  errors: number;
  poolAcquireTimeouts: number;
  slow: number;
  totalMs: number;
  latencies: number[];
};

type CacheReloadMetric = {
  flow: string;
  table: string;
  scope?: string;
  status: 'success' | 'failed';
  durationMs: number;
  steps: Array<{ name: string; durationMs: number; status: 'success' | 'failed'; error?: string }>;
  startedAt: string;
  completedAt: string;
  error?: string;
};

type FlowMetric = {
  flowId: string | number;
  flowName: string;
  running: number;
  completed: number;
  failed: number;
  totalMs: number;
  latencies: number[];
  failedSteps: Record<string, number>;
  stepLatencies: Record<string, number[]>;
};

const MAX_LATENCIES = 256;
const MAX_RECENT = 20;
const SLOW_QUERY_MS = 500;

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

export class RuntimeMetricsCollectorService {
  private readonly requests = new Map<string, RouteMetric>();
  private readonly queries = new Map<string, QueryMetric>();
  private readonly flows = new Map<string, FlowMetric>();
  private recentCacheReloads: CacheReloadMetric[] = [];
  private startedAt = Date.now();

  recordRequest(input: {
    method: string;
    route: string;
    statusCode: number;
    durationMs: number;
  }) {
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
    current.count++;
    current.totalMs += input.durationMs;
    pushLatency(current.latencies, input.durationMs);
    if (input.statusCode >= 500) current.status5xx++;
    else if (input.statusCode >= 400) current.status4xx++;
    else if (input.statusCode >= 300) current.status3xx++;
    else current.status2xx++;
    this.requests.set(key, current);
  }

  recordQuery(input: {
    op: string;
    table?: string;
    durationMs: number;
    error?: unknown;
  }) {
    const table = input.table || 'unknown';
    const key = `${input.op}:${table}`;
    const current =
      this.queries.get(key) ??
      {
        op: input.op,
        table,
        count: 0,
        errors: 0,
        poolAcquireTimeouts: 0,
        slow: 0,
        totalMs: 0,
        latencies: [],
      };
    current.count++;
    current.totalMs += input.durationMs;
    if (input.error) current.errors++;
    if (isPoolAcquireTimeout(input.error)) current.poolAcquireTimeouts++;
    if (input.durationMs >= SLOW_QUERY_MS) current.slow++;
    pushLatency(current.latencies, input.durationMs);
    this.queries.set(key, current);
  }

  async trackQuery<T>(
    input: { op: string; table?: string },
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

  recordCacheReload(metric: CacheReloadMetric) {
    this.recentCacheReloads.unshift(metric);
    if (this.recentCacheReloads.length > MAX_RECENT) {
      this.recentCacheReloads = this.recentCacheReloads.slice(0, MAX_RECENT);
    }
  }

  startFlow(flowId: string | number, flowName: string) {
    const metric = this.getFlowMetric(flowId, flowName);
    metric.running++;
  }

  completeFlow(input: {
    flowId: string | number;
    flowName: string;
    durationMs: number;
    status: 'completed' | 'failed';
  }) {
    const metric = this.getFlowMetric(input.flowId, input.flowName);
    metric.running = Math.max(0, metric.running - 1);
    if (input.status === 'completed') metric.completed++;
    else metric.failed++;
    metric.totalMs += input.durationMs;
    pushLatency(metric.latencies, input.durationMs);
  }

  recordFlowStep(input: {
    flowId: string | number;
    flowName: string;
    stepKey: string;
    durationMs: number;
    failed?: boolean;
  }) {
    const metric = this.getFlowMetric(input.flowId, input.flowName);
    metric.stepLatencies[input.stepKey] ??= [];
    pushLatency(metric.stepLatencies[input.stepKey], input.durationMs);
    if (input.failed) {
      metric.failedSteps[input.stepKey] = (metric.failedSteps[input.stepKey] ?? 0) + 1;
    }
  }

  snapshot() {
    const uptimeSec = Math.max(1, (Date.now() - this.startedAt) / 1000);
    const requestRoutes = [...this.requests.values()]
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

    const queryRows = [...this.queries.values()]
      .map((item) => ({
        op: item.op,
        table: item.table,
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

    const flowRows = [...this.flows.values()]
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
        recent: this.recentCacheReloads,
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

  private getFlowMetric(flowId: string | number, flowName: string): FlowMetric {
    const key = String(flowId);
    const current =
      this.flows.get(key) ??
      {
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
    this.flows.set(key, current);
    return current;
  }
}
