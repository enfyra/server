export const SYSTEM_TABLES = {
  table: 'enfyra_table',
  column: 'enfyra_column',
  relation: 'enfyra_relation',
  columnRule: 'enfyra_column_rule',
  user: 'enfyra_user',
  oauthConfig: 'enfyra_oauth_config',
  oauthAccount: 'enfyra_oauth_account',
  setting: 'enfyra_setting',
  corsOrigin: 'enfyra_cors_origin',
  route: 'enfyra_route',
  role: 'enfyra_role',
  routePermission: 'enfyra_route_permission',
  fieldPermission: 'enfyra_field_permission',
  routeHandler: 'enfyra_route_handler',
  preHook: 'enfyra_pre_hook',
  postHook: 'enfyra_post_hook',
  session: 'enfyra_session',
  apiToken: 'enfyra_api_token',
  schemaMigration: 'enfyra_schema_migration',
  method: 'enfyra_method',
  menu: 'enfyra_menu',
  extension: 'enfyra_extension',
  folder: 'enfyra_folder',
  file: 'enfyra_file',
  filePermission: 'enfyra_file_permission',
  package: 'enfyra_package',
  bootstrapScript: 'enfyra_bootstrap_script',
  storageConfig: 'enfyra_storage_config',
  websocket: 'enfyra_websocket',
  websocketEvent: 'enfyra_websocket_event',
  flow: 'enfyra_flow',
  flowStep: 'enfyra_flow_step',
  flowExecution: 'enfyra_flow_execution',
  guard: 'enfyra_guard',
  guardRule: 'enfyra_guard_rule',
  graphql: 'enfyra_graphql',
  runtimeReloadLog: 'enfyra_runtime_reload_log',
} as const;

export const LEGACY_CORE_SYSTEM_TABLES = {
  table: 'table_definition',
  column: 'column_definition',
  relation: 'relation_definition',
} as const;

export const CORE_SYSTEM_TABLES = {
  table: SYSTEM_TABLES.table,
  column: SYSTEM_TABLES.column,
  relation: SYSTEM_TABLES.relation,
} as const;

export const METADATA_SYSTEM_TABLE_NAMES = [
  SYSTEM_TABLES.table,
  SYSTEM_TABLES.column,
  SYSTEM_TABLES.relation,
] as const;
