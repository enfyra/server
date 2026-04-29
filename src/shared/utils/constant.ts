export const BOOTSTRAP_SCRIPT_EXECUTION_LOCK_KEY = 'bootstrap-script-execution';
export const ENFYRA_ADMIN_WEBSOCKET_NAMESPACE = '/enfyra-admin';
export const ENFYRA_ADMIN_ROOT_WEBSOCKET_ROOM = '$system:root-admin';
export const DEFAULT_MAX_QUERY_DEPTH = 7;
export const DEFAULT_MAX_UPLOAD_FILE_SIZE_MB = 10;
export const DEFAULT_MAX_REQUEST_BODY_SIZE_MB = 1;

export const SYSTEM_QUEUES = {
  SESSION_CLEANUP: 'sys_session-cleanup',
  WS_CONNECTION: 'sys_ws-connection',
  WS_EVENT: 'sys_ws-event',
  FLOW_EXECUTION: 'sys_flow-execution',
} as const;

export const SAGA_ORPHAN_RECOVERY_LOCK_KEY = 'enfyra:saga-orphan-recovery';
export const MONGO_MIGRATION_SAGA_RECOVERY_LOCK_KEY =
  'enfyra:mongo-migration-saga-recovery';

export const REDIS_TTL = {
  BOOTSTRAP_LOCK_TTL: 30000,
  RELOAD_LOCK_TTL: 30000,
  PROVISION_LOCK_TTL: 120000,
  SAGA_ORPHAN_RECOVERY_LOCK_TTL: 300000,
  MONGO_MIGRATION_SAGA_RECOVERY_LOCK_TTL: 300000,
} as const;

export const PROVISION_LOCK_KEY = 'sys:provision_init_lock';
