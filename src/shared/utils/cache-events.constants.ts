export const CACHE_EVENTS = {
  INVALIDATE: 'cache:invalidate',
  METADATA_LOADED: 'cache:metadata:loaded',
  ROUTE_LOADED: 'cache:route:loaded',
  STORAGE_LOADED: 'cache:storage:loaded',
  OAUTH_CONFIG_LOADED: 'cache:oauth-config:loaded',
  WEBSOCKET_LOADED: 'cache:websocket:loaded',
  PACKAGE_LOADED: 'cache:package:loaded',
  FLOW_LOADED: 'cache:flow:loaded',
  GRAPHQL_LOADED: 'cache:graphql:loaded',
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
} as const;

type CacheIdentifier = (typeof CACHE_IDENTIFIERS)[keyof typeof CACHE_IDENTIFIERS];

const ROUTE_GROUP: CacheIdentifier[] = [
  CACHE_IDENTIFIERS.ROUTE,
  CACHE_IDENTIFIERS.GRAPHQL,
];

const METADATA_GROUP: CacheIdentifier[] = [
  CACHE_IDENTIFIERS.METADATA,
  ...ROUTE_GROUP,
];

export const CACHE_INVALIDATION_MAP: Record<string, CacheIdentifier[]> = {
  table_definition: METADATA_GROUP,
  column_definition: METADATA_GROUP,
  relation_definition: METADATA_GROUP,

  route_definition: ROUTE_GROUP,
  pre_hook_definition: ROUTE_GROUP,
  post_hook_definition: ROUTE_GROUP,
  route_handler_definition: ROUTE_GROUP,
  route_permission_definition: ROUTE_GROUP,
  role_definition: ROUTE_GROUP,
  method_definition: ROUTE_GROUP,

  storage_config_definition: [CACHE_IDENTIFIERS.STORAGE],
  oauth_config_definition: [CACHE_IDENTIFIERS.OAUTH_CONFIG],
  websocket_definition: [CACHE_IDENTIFIERS.WEBSOCKET],
  websocket_event_definition: [CACHE_IDENTIFIERS.WEBSOCKET],
  package_definition: [CACHE_IDENTIFIERS.PACKAGE],
  bootstrap_script_definition: [CACHE_IDENTIFIERS.BOOTSTRAP],
  folder_definition: [CACHE_IDENTIFIERS.FOLDER_TREE],
  flow_definition: [CACHE_IDENTIFIERS.FLOW],
  flow_step_definition: [CACHE_IDENTIFIERS.FLOW],
};

export function shouldReloadCache(tableName: string, cacheIdentifier: CacheIdentifier): boolean {
  const cachesToReload = CACHE_INVALIDATION_MAP[tableName];
  return cachesToReload?.includes(cacheIdentifier) ?? false;
}
