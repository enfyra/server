/**
 * All tuning constants for the auto-scaling subsystem.
 * Adjust values here — no need to hunt across modules.
 */

// ─── SQL Pool Cluster Coordination ───────────────────────────────

export const SQL_MYSQL_POOL_MIN_DEFAULT = 1;
export const SQL_MYSQL_POOL_MAX_DEFAULT = 8;
export const SQL_POSTGRES_POOL_MIN_DEFAULT = 0;
export const SQL_POSTGRES_POOL_MAX_DEFAULT = 4;
export const SQL_MASTER_RATIO = 0.6;
export const SQL_ACQUIRE_TIMEOUT_MS = 60_000;

export const SQL_COORD_HEARTBEAT_MS = 12_000;
export const SQL_COORD_STALE_MS = 40_000;
export const SQL_COORD_RECONCILE_INTERVAL_MS = 90_000;
export const SQL_COORD_RESERVE_MIN = 10;
export const SQL_COORD_RESERVE_RATIO = 0.05;
