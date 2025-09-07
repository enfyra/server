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
export const SCHEMA_SYNC_LATEST_TTL = 60; // 60 seconds TTL for cleanup
export const SCHEMA_SYNC_LOCK_TTL = 60000; // 60 seconds Redis lock TTL
