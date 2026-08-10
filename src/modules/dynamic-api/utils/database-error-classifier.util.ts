import type {
  DynamicDatabaseBackend,
  DynamicDatabaseDriver,
  DynamicDatabaseErrorClassification,
} from '../types/database-error-classifier.types';

function resolveBackend(value: string | null | undefined): DynamicDatabaseBackend {
  if (value === 'postgres' || value === 'mysql' || value === 'mongodb') {
    return value;
  }
  return 'unknown';
}

function resolveDriver(backend: DynamicDatabaseBackend): DynamicDatabaseDriver {
  if (backend === 'postgres') return 'pg';
  if (backend === 'mysql') return 'mysql2';
  if (backend === 'mongodb') return 'mongodb';
  return 'unknown';
}

function getErrorCode(error: unknown): string | null {
  if (!error || typeof error !== 'object' || !('code' in error)) return null;
  const code = error.code;
  return typeof code === 'string' || typeof code === 'number'
    ? String(code)
    : null;
}

export function classifyDynamicDatabaseError(
  error: unknown,
  backendValue: string | null | undefined,
): DynamicDatabaseErrorClassification {
  const backend = resolveBackend(backendValue);
  const driver = resolveDriver(backend);
  const code = getErrorCode(error);
  const kind =
    backend === 'postgres' && code === '42883'
      ? 'postgres_incompatible_operator'
      : 'unknown';
  return { backend, driver, code, kind };
}
