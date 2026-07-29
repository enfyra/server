import type { CORE_SYSTEM_TABLES } from '../utils/system-tables.constants';

export type CoreSystemTableKey = keyof typeof CORE_SYSTEM_TABLES;

export type CoreSystemTableNames = Record<CoreSystemTableKey, string>;
