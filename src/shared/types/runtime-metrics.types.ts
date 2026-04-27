export type QueryMetricContext =
  | 'runtime'
  | 'cache'
  | 'boot'
  | 'migration'
  | 'flow'
  | 'system';

export type RuntimeRouteMetric = {
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

export type RuntimeQueryMetric = {
  context: QueryMetricContext;
  op: string;
  table: string;
  count: number;
  errors: number;
  poolAcquireTimeouts: number;
  slow: number;
  totalMs: number;
  latencies: number[];
};

export type RuntimeCacheReloadMetric = {
  flow: string;
  table: string;
  scope?: string;
  status: 'success' | 'failed';
  durationMs: number;
  steps: Array<{
    name: string;
    durationMs: number;
    status: 'success' | 'failed';
    error?: string;
  }>;
  startedAt: string;
  completedAt: string;
  error?: string;
};

export type RuntimeFlowMetric = {
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

export type RuntimeAverageSample = {
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

export type RuntimeQueueStats = {
  waiting: number;
  active: number;
  delayed: number;
  failed: number;
  failedJobs: Array<{
    id: string;
    name: string;
    flowId?: string | number;
    flowName?: string;
    failedStepKey?: string;
    sourceFlowId?: string | number;
    sourceFlowName?: string;
    sourceStepKey?: string;
    failedReason?: string;
    attemptsMade: number;
    timestamp?: number;
    finishedOn?: number;
  }>;
} | null;

export type ClusterTelemetryRecord<T> = {
  instanceId: string;
  sampledAt: string;
  payload: T;
};
