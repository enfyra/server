import { SYSTEM_TABLES } from './system-tables.constants';

export const GENERATED_SCRIPT_FIELDS = ['compiledCode'] as const;

export const SCRIPT_TABLE_NAMES: readonly string[] = [
  SYSTEM_TABLES.oauthConfig,
  SYSTEM_TABLES.routeHandler,
  SYSTEM_TABLES.preHook,
  SYSTEM_TABLES.postHook,
  SYSTEM_TABLES.helper,
  SYSTEM_TABLES.websocket,
  SYSTEM_TABLES.websocketEvent,
  SYSTEM_TABLES.flowStep,
];

export const SCRIPT_TABLE_NAME_SET = new Set<string>(SCRIPT_TABLE_NAMES);

export const GENERATED_SCRIPT_FIELD_SET = new Set(GENERATED_SCRIPT_FIELDS);
