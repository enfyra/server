export enum HealthStatus {
  HEALTHY = 'HEALTHY',
  DEGRADED = 'DEGRADED',
  UNHEALTHY = 'UNHEALTHY',
}

export interface ChildProcessMetadata {
  pid: number;
  createdAt: number;
  executionCount: number;
  errorCount: number;
  totalErrors: number;
  lastExecutionAt: number;
  lastExecutionTimeMs: number;
  avgExecutionTimeMs: number;
  lastError: string | null;
  lastErrorAt: number | null;
}

export interface HealthCheckResult {
  status: HealthStatus;
  shouldRecycle: boolean;
  reasons: string[];
  metadata: ChildProcessMetadata | null;
  memoryUsageMB: number;
  ageMs: number;
}

export interface HealthCheckStats {
  totalProcesses: number;
  healthyCount: number;
  degradedCount: number;
  unhealthyCount: number;
  totalExecutions: number;
  totalErrors: number;
}

export interface ProcessDetails {
  pid: number;
  status: HealthStatus;
  memoryUsageMB: number;
  ageMs: number;
  executionCount: number;
  errorCount: number;
  avgExecutionTimeMs: number;
  reasons: string[];
}

export const HEALTH_CHECK_CONFIG = {
  maxMemoryMB: 400,
  maxExecutionCount: 100,
  maxConsecutiveErrors: 5,
  maxAgeMs: 3600000,
  degradedMemoryMB: 320,
  degradedExecutionCount: 70,
} as const;