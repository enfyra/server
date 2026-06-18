import type {
  CORE_SYSTEM_TABLES,
  SYSTEM_TABLES,
} from '../utils/system-tables.constants';

export type SystemTableName =
  (typeof SYSTEM_TABLES)[keyof typeof SYSTEM_TABLES];

export type CoreSystemTableKey = keyof typeof CORE_SYSTEM_TABLES;

export type CoreSystemTableNames = Record<CoreSystemTableKey, string>;
