export const HIDDEN_FIELD_KEY = 'hidden_field';
export const GLOBAL_SETTINGS_KEY = 'global-settings';
export const METADATA_CACHE_KEY = 'metadata:all';
export const IS_PUBLIC_KEY = 'isPublic';

export const BOOTSTRAP_SCRIPT_RELOAD_EVENT_KEY = 'enfyra:bootstrap-script-reload';
export const BOOTSTRAP_SCRIPT_EXECUTION_LOCK_KEY = 'bootstrap-script-execution';
export const ROUTE_CACHE_SYNC_EVENT_KEY = 'enfyra:route-cache-sync';
export const PACKAGE_CACHE_SYNC_EVENT_KEY = 'enfyra:package-cache-sync';
export const METADATA_CACHE_SYNC_EVENT_KEY = 'enfyra:metadata-cache-sync';
export const STORAGE_CONFIG_CACHE_SYNC_EVENT_KEY = 'enfyra:storage-config-cache-sync';
export const AI_CONFIG_CACHE_SYNC_EVENT_KEY = 'enfyra:ai-config-cache-sync';
export const AI_AGENT_CANCEL_CHANNEL = 'enfyra:ai-agent-cancel';

export const METADATA_RELOAD_LOCK_KEY = 'metadata:reload:lock';
export const ROUTE_RELOAD_LOCK_KEY = 'routes:reload:lock';
export const PACKAGE_RELOAD_LOCK_KEY = 'packages:reload:lock';
export const STORAGE_CONFIG_RELOAD_LOCK_KEY = 'storage-config:reload:lock';
export const AI_CONFIG_RELOAD_LOCK_KEY = 'ai-config:reload:lock';
export const SESSION_CLEANUP_LOCK_KEY = 'session:cleanup:lock';

export const REDIS_TTL = {
  BOOTSTRAP_LOCK_TTL: 30000,
  RELOAD_LOCK_TTL: 30000,
  SESSION_CLEANUP_LOCK_TTL: 3600000,
} as const;
