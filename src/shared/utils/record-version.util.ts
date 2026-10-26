import { BadRequestException } from '../../domain/exceptions';
import { SYSTEM_TABLES } from './system-tables.constants';
import {
  isEnfyraVersion,
  isEnfyraVersionNewer,
  normalizeEnfyraVersion,
} from './enfyra-version.util';

export const VERSIONED_TABLE_NAMES = [
  SYSTEM_TABLES.routeHandler,
  SYSTEM_TABLES.preHook,
  SYSTEM_TABLES.postHook,
  SYSTEM_TABLES.helper,
  SYSTEM_TABLES.websocket,
  SYSTEM_TABLES.websocketEvent,
  SYSTEM_TABLES.oauthConfig,
  SYSTEM_TABLES.flowStep,
  SYSTEM_TABLES.extension,
] as const;

export const VERSIONED_TABLE_NAME_SET = new Set<string>(VERSIONED_TABLE_NAMES);

export const DEFAULT_RECORD_VERSION = '1.0.0';

export function isVersionedTable(tableName: string): boolean {
  return VERSIONED_TABLE_NAME_SET.has(tableName);
}

function assertValidVersionValue(value: unknown): string {
  const normalized =
    typeof value === 'string' ? normalizeEnfyraVersion(value) : '';
  if (!isEnfyraVersion(normalized) || !/^\d+\.\d+\.\d+/.test(normalized)) {
    throw new BadRequestException(
      'version must be a valid version string such as 1.0.0',
    );
  }
  return normalized;
}

export function normalizeCreateVersion(value: unknown): string | undefined {
  if (value === undefined || value === null || value === '') return undefined;
  return assertValidVersionValue(value);
}

export function normalizeUpdateVersion(
  value: unknown,
  current: unknown,
): string {
  if (value === undefined || value === null || value === '') {
    throw new BadRequestException(
      'version is required when updating this record; send a version greater than the current one',
    );
  }
  const next = assertValidVersionValue(value);
  const currentVersion = normalizeEnfyraVersion(current);
  if (!isEnfyraVersionNewer(next, currentVersion)) {
    throw new BadRequestException(
      `version must be greater than the current version ${currentVersion}`,
    );
  }
  return next;
}
