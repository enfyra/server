export const HIDDEN_FIELD_KEY = 'hidden_field';
export const GLOBAL_ROUTES_KEY = 'global-routes';
export const GLOBAL_SETTINGS_KEY = 'global-settings';

export const RELOADING_DATASOURCE_KEY = 'reloading-data-source';
export const IS_PUBLIC_KEY = 'isPublic';
export const SCHEMA_UPDATED_EVENT_KEY = 'enfyra:schema-updated';
export const SCHEMA_LOCK_EVENT_KEY = 'enfyra:schema-lock';
export const SCHEMA_PULLING_EVENT_KEY = 'enfyra:pulling';

// Schema sync constants
export const SCHEMA_SYNC_LATEST_KEY = 'schema:sync:latest';
export const SCHEMA_SYNC_PROCESSING_LOCK_KEY = 'schema:sync:processing_lock';
export const SCHEMA_SYNC_MAX_RETRIES = 30; // 30 seconds max wait
export const SCHEMA_SYNC_RETRY_DELAY = 1000; // 1 second between retries

// Bootstrap script constants
export const BOOTSTRAP_SCRIPT_RELOAD_EVENT_KEY = 'enfyra:bootstrap-script-reload';
export const BOOTSTRAP_SCRIPT_EXECUTION_LOCK_KEY = 'bootstrap-script-execution';

// Redis TTL constants (all values in milliseconds)
export const REDIS_TTL = {
  // Cache TTL values
  ROUTES_CACHE_TTL: 60000,              // 60s - Routes cache
  PACKAGES_CACHE_TTL: 300000,           // 5m - Packages cache
  
  // Lock TTL values  
  REVALIDATION_LOCK_TTL: 30000,         // 30s - Cache revalidation lock
  BOOTSTRAP_LOCK_TTL: 30000,            // 30s - Bootstrap script execution lock
  SCHEMA_SYNC_LOCK_TTL: 60000,          // 60s - Schema sync processing lock
  
  // Session TTL values
  STALE_CACHE_TTL: 0,                   // 0s - Stale cache (no expiry)
  SCHEMA_SYNC_LATEST_TTL: 60 * 1000,    // 60s - Schema sync latest info TTL
  
  // File cache TTL values  
  FILE_CACHE_TTL: {
    SMALL: 3600 * 1000,   // 1h - Small files
    MEDIUM: 1800 * 1000,  // 30m - Medium files  
    LARGE: 600 * 1000,    // 10m - Large files
    XLARGE: 300 * 1000,   // 5m - Extra large files
  },
  
  // Background task intervals
  CACHE_STATS_INTERVAL: 600000,         // 10m - Cache stats logging
  CACHE_CLEANUP_INTERVAL: 300000,       // 5m - Cache cleanup task
} as const;
