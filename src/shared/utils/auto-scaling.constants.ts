/**
 * All tuning constants for the auto-scaling subsystem.
 * Adjust values here — no need to hunt across modules.
 */

// ─── SQL Pool Cluster Coordination ───────────────────────────────

export const SQL_BOOTSTRAP_POOL_MIN = 1;
export const SQL_BOOTSTRAP_POOL_MAX_TOTAL = 8;
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
