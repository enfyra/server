import { SYSTEM_TABLES } from './system-tables.constants';

export const GENERATED_SCRIPT_FIELDS = ['compiledCode'] as const;

export const SCRIPT_TABLE_LEGACY_FIELDS: Record<string, string> = {
  [SYSTEM_TABLES.oauthConfig]: '',
  [SYSTEM_TABLES.routeHandler]: 'logic',
  [SYSTEM_TABLES.preHook]: 'code',
  [SYSTEM_TABLES.postHook]: 'code',
  [SYSTEM_TABLES.bootstrapScript]: 'logic',
  [SYSTEM_TABLES.websocket]: 'connectionHandlerScript',
  [SYSTEM_TABLES.websocketEvent]: 'handlerScript',
  [SYSTEM_TABLES.flowStep]: '',
};

export const SCRIPT_TABLE_NAMES = Object.keys(SCRIPT_TABLE_LEGACY_FIELDS);

export const SCRIPT_TABLE_NAME_SET = new Set(SCRIPT_TABLE_NAMES);

export const GENERATED_SCRIPT_FIELD_SET = new Set(GENERATED_SCRIPT_FIELDS);
