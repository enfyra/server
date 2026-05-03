/**
 * All tuning constants for the auto-scaling subsystem.
 * Adjust values here — no need to hunt across modules.
 */

// ─── SQL Pool Cluster Coordination ───────────────────────────────

export const SQL_MYSQL_POOL_MIN_DEFAULT = 1;
export const SQL_MYSQL_POOL_MAX_DEFAULT = 8;
export const SQL_POSTGRES_POOL_MIN_DEFAULT = 0;
export const SQL_POSTGRES_POOL_MAX_DEFAULT = 4;
export const SQL_BOOTSTRAP_POOL_MIN = SQL_MYSQL_POOL_MIN_DEFAULT;
export const SQL_BOOTSTRAP_POOL_MAX_TOTAL = SQL_MYSQL_POOL_MAX_DEFAULT;
export const SQL_MASTER_RATIO = 0.6;
export const SQL_ACQUIRE_TIMEOUT_MS = 60_000;

export const SQL_COORD_HEARTBEAT_MS = 12_000;
export const SQL_COORD_STALE_MS = 40_000;
export const SQL_COORD_RECONCILE_INTERVAL_MS = 90_000;
export const SQL_COORD_RESERVE_MIN = 10;
export const SQL_COORD_RESERVE_RATIO = 0.05;

// ─── Handler Worker Feedback Loop ────────────────────────────────

export const WORKER_TUNE_INTERVAL_MS = 30_000;
export const WORKER_RSS_HIGH = 0.85;
export const WORKER_RSS_LOW = 0.7;
export const WORKER_CPU_HIGH = 0.7;
export const WORKER_CPU_LOW = 0.5;
export const WORKER_FLOOR = 1;
export const WORKER_HYSTERESIS_TICKS = 3;
export const WORKER_DISPATCH_RSS_CEILING = 0.9;

export const WORKER_HEAP_ROTATE_THRESHOLD = 0.8;
export const WORKER_HEAP_SAMPLE_INTERVAL_MS = 5_000;
export const WORKER_DRAIN_TIMEOUT_MS = 60_000;
