export type { TCacheInvalidationPayload } from '../types/cache.types';
import { SYSTEM_TABLES } from './system-tables.constants';

export const CACHE_EVENTS = {
  INVALIDATE: 'cache:invalidate',
  METADATA_LOADED: 'cache:metadata:loaded',
  METADATA_CHANGED: 'cache:metadata:changed',
  ROUTE_LOADED: 'cache:route:loaded',
  STORAGE_LOADED: 'cache:storage:loaded',
  OAUTH_CONFIG_LOADED: 'cache:oauth-config:loaded',
  AUTH_HEADER_LOADED: 'cache:auth-header:loaded',
  WEBSOCKET_LOADED: 'cache:websocket:loaded',
  PACKAGE_LOADED: 'cache:package:loaded',
  FLOW_LOADED: 'cache:flow:loaded',
  GUARD_LOADED: 'cache:guard:loaded',
  SETTING_LOADED: 'cache:setting:loaded',
  GRAPHQL_LOADED: 'cache:graphql:loaded',
  RUNTIME_CACHE_ACTIVATED: 'runtime:cache:activated',
  SYSTEM_READY: 'system:ready',
} as const;

export const DATA_EVENTS = {
  TABLE_MUTATION: 'data:table:mutation',
  ROUTE_EXECUTED: 'data:route:executed',
} as const;

export interface TableMutationPayload {
  table: string;
  action: 'create' | 'update' | 'delete';
  ids?: (string | number)[];
  data?: any;
  userId?: string | number | null;
}

export interface RouteExecutedPayload {
  routePath: string;
  method: string;
  userId?: string | number | null;
  result?: any;
}

export const CACHE_IDENTIFIERS = {
  METADATA: 'metadata',
  ROUTE: 'route',
  GRAPHQL: 'graphql',
  STORAGE: 'storage',
  WEBSOCKET: 'websocket',
  PACKAGE: 'package',
  BOOTSTRAP: 'bootstrap',
  OAUTH_CONFIG: 'oauth-config',
  AUTH_HEADER: 'auth-header',
  FOLDER_TREE: 'folder-tree',
  FLOW: 'flow',
  GUARD: 'guard',
  SETTING: 'setting',
  MENU: 'menu',
  EXTENSION: 'extension',
  FIELD_PERMISSION: 'field-permission',
  COLUMN_RULE: 'column-rule',
} as const;

const METADATA_TABLES = new Set<string>([
  SYSTEM_TABLES.table,
  SYSTEM_TABLES.column,
  SYSTEM_TABLES.relation,
]);

export function isMetadataTable(tableName: string): boolean {
  return METADATA_TABLES.has(tableName);
}
