export type { TCacheInvalidationPayload } from '../types/cache.types';
import { SYSTEM_TABLES } from './system-tables.constants';

export const CACHE_EVENTS = {
  INVALIDATE: 'cache:invalidate',
  METADATA_LOADED: 'cache:metadata:loaded',
  METADATA_CHANGED: 'cache:metadata:changed',
  ROUTE_LOADED: 'cache:route:loaded',
  STORAGE_LOADED: 'cache:storage:loaded',
  OAUTH_CONFIG_LOADED: 'cache:oauth-config:loaded',
  WEBSOCKET_LOADED: 'cache:websocket:loaded',
  PACKAGE_LOADED: 'cache:package:loaded',
  FLOW_LOADED: 'cache:flow:loaded',
  GUARD_LOADED: 'cache:guard:loaded',
  SETTING_LOADED: 'cache:setting:loaded',
  GRAPHQL_LOADED: 'cache:graphql:loaded',
  RUNTIME_CACHE_ACTIVATED: 'runtime:cache:activated',
  SYSTEM_READY: 'system:ready',
} as const;

export const CACHE_IDENTIFIERS = {
  METADATA: 'metadata',
  ROUTE: 'route',
  GRAPHQL: 'graphql',
  STORAGE: 'storage',
  WEBSOCKET: 'websocket',
  PACKAGE: 'package',
  BOOTSTRAP: 'bootstrap',
  OAUTH_CONFIG: 'oauth-config',
  FOLDER_TREE: 'folder-tree',
  FLOW: 'flow',
  GUARD: 'guard',
  SETTING: 'setting',
  MENU: 'menu',
  EXTENSION: 'extension',
  FIELD_PERMISSION: 'field-permission',
  COLUMN_RULE: 'column-rule',
} as const;

type CacheIdentifier =
  (typeof CACHE_IDENTIFIERS)[keyof typeof CACHE_IDENTIFIERS];

const ROUTE_GROUP: CacheIdentifier[] = [
  CACHE_IDENTIFIERS.ROUTE,
  CACHE_IDENTIFIERS.GRAPHQL,
];

const METADATA_GROUP: CacheIdentifier[] = [
  CACHE_IDENTIFIERS.METADATA,
  ...ROUTE_GROUP,
];

export const CACHE_INVALIDATION_MAP: Record<string, CacheIdentifier[]> = {
  [SYSTEM_TABLES.table]: METADATA_GROUP,
  [SYSTEM_TABLES.column]: METADATA_GROUP,
  [SYSTEM_TABLES.relation]: METADATA_GROUP,
  [SYSTEM_TABLES.columnRule]: [CACHE_IDENTIFIERS.COLUMN_RULE],

  [SYSTEM_TABLES.route]: ROUTE_GROUP,
  [SYSTEM_TABLES.preHook]: ROUTE_GROUP,
  [SYSTEM_TABLES.postHook]: ROUTE_GROUP,
  [SYSTEM_TABLES.routeHandler]: ROUTE_GROUP,
  [SYSTEM_TABLES.routePermission]: ROUTE_GROUP,
  [SYSTEM_TABLES.role]: ROUTE_GROUP,
  [SYSTEM_TABLES.method]: ROUTE_GROUP,

  [SYSTEM_TABLES.fieldPermission]: [
    CACHE_IDENTIFIERS.FIELD_PERMISSION,
    CACHE_IDENTIFIERS.GRAPHQL,
  ],

  [SYSTEM_TABLES.storageConfig]: [CACHE_IDENTIFIERS.STORAGE],
  [SYSTEM_TABLES.oauthConfig]: [CACHE_IDENTIFIERS.OAUTH_CONFIG],
  [SYSTEM_TABLES.websocket]: [CACHE_IDENTIFIERS.WEBSOCKET],
  [SYSTEM_TABLES.websocketEvent]: [CACHE_IDENTIFIERS.WEBSOCKET],
  [SYSTEM_TABLES.package]: [CACHE_IDENTIFIERS.PACKAGE],
  [SYSTEM_TABLES.bootstrapScript]: [CACHE_IDENTIFIERS.BOOTSTRAP],
  [SYSTEM_TABLES.folder]: [CACHE_IDENTIFIERS.FOLDER_TREE],
  [SYSTEM_TABLES.flow]: [CACHE_IDENTIFIERS.FLOW],
  [SYSTEM_TABLES.flowStep]: [CACHE_IDENTIFIERS.FLOW],
  [SYSTEM_TABLES.guard]: [CACHE_IDENTIFIERS.GUARD],
  [SYSTEM_TABLES.guardRule]: [CACHE_IDENTIFIERS.GUARD],
  [SYSTEM_TABLES.setting]: [CACHE_IDENTIFIERS.SETTING],
  [SYSTEM_TABLES.menu]: [CACHE_IDENTIFIERS.MENU, CACHE_IDENTIFIERS.EXTENSION],
  [SYSTEM_TABLES.extension]: [CACHE_IDENTIFIERS.EXTENSION],
  [SYSTEM_TABLES.graphql]: [CACHE_IDENTIFIERS.GRAPHQL],
};

export function shouldReloadCache(
  tableName: string,
  cacheIdentifier: CacheIdentifier,
): boolean {
  const cachesToReload = CACHE_INVALIDATION_MAP[tableName];
  return cachesToReload?.includes(cacheIdentifier) ?? false;
}

const METADATA_TABLES = new Set<string>([
  SYSTEM_TABLES.table,
  SYSTEM_TABLES.column,
  SYSTEM_TABLES.relation,
]);

export function isMetadataTable(tableName: string): boolean {
  return METADATA_TABLES.has(tableName);
}
