const GENERATED_SCRIPT_FIELDS = new Set(['compiledCode']);
const SCRIPT_TABLES = new Set([
  'oauth_config_definition',
  'route_handler_definition',
  'pre_hook_definition',
  'post_hook_definition',
  'bootstrap_script_definition',
  'websocket_definition',
  'websocket_event_definition',
  'flow_step_definition',
]);

export function isGeneratedScriptPersistenceField(
  tableName: string,
  fieldName: string,
): boolean {
  return SCRIPT_TABLES.has(tableName) && GENERATED_SCRIPT_FIELDS.has(fieldName);
}
