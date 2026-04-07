export const HIDDEN_FIELD_KEY = 'hidden_field';
export const GLOBAL_SETTINGS_KEY = 'global-settings';
export const METADATA_CACHE_KEY = 'metadata:all';

export const BOOTSTRAP_SCRIPT_RELOAD_EVENT_KEY = 'enfyra:bootstrap-script-reload';
export const BOOTSTRAP_SCRIPT_EXECUTION_LOCK_KEY = 'bootstrap-script-execution';
export const ROUTE_CACHE_SYNC_EVENT_KEY = 'enfyra:route-cache-sync';
export const PACKAGE_CACHE_SYNC_EVENT_KEY = 'enfyra:package-cache-sync';
export const METADATA_CACHE_SYNC_EVENT_KEY = 'enfyra:metadata-cache-sync';
export const STORAGE_CONFIG_CACHE_SYNC_EVENT_KEY = 'enfyra:storage-config-cache-sync';
export const WEBSOCKET_CACHE_SYNC_EVENT_KEY = 'enfyra:websocket-cache-sync';
export const ENFYRA_ADMIN_WEBSOCKET_NAMESPACE = '/enfyra-admin';
export const OAUTH_CONFIG_CACHE_SYNC_EVENT_KEY = 'enfyra:oauth-config-cache-sync';
export const FLOW_CACHE_SYNC_EVENT_KEY = 'enfyra:flow-cache-sync';
export const FOLDER_TREE_CACHE_SYNC_EVENT_KEY = 'enfyra:folder-tree-cache-sync';

export const SYSTEM_QUEUES = {
  SESSION_CLEANUP: 'sys_session-cleanup',
  WS_CONNECTION: 'sys_ws-connection',
  WS_EVENT: 'sys_ws-event',
  FLOW_EXECUTION: 'sys_flow-execution',
} as const;

export const REDIS_TTL = {
  BOOTSTRAP_LOCK_TTL: 30000,
  RELOAD_LOCK_TTL: 30000,
  PROVISION_LOCK_TTL: 120000,
} as const;

export const PROVISION_LOCK_KEY = 'sys:provision_init_lock';
