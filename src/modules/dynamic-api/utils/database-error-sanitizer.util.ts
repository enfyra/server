import type {
  TMutationErrorCategory,
  TMutationErrorSummary,
} from '../types/database-error-sanitizer.types';

const CATEGORY_MESSAGES: Record<TMutationErrorCategory, string> = {
  unique_violation: 'A record with the same unique value already exists',
  foreign_key_violation: 'Operation violates a related record constraint',
  not_null_violation: 'A required field is missing',
  check_violation: 'A field value violates a table constraint',
  invalid_value_type: 'A field value has an invalid type',
  validation_failed: 'Document failed validation',
  unknown: 'Database operation failed',
};

const POSTGRES_CODE_CATEGORIES: Record<string, TMutationErrorCategory> = {
  '23505': 'unique_violation',
  '23503': 'foreign_key_violation',
  '23502': 'not_null_violation',
  '23514': 'check_violation',
  '22P02': 'invalid_value_type',
  '22007': 'invalid_value_type',
  '22003': 'invalid_value_type',
};

const MYSQL_CODE_CATEGORIES: Record<string, TMutationErrorCategory> = {
  ER_DUP_ENTRY: 'unique_violation',
  '1062': 'unique_violation',
  ER_NO_REFERENCED_ROW_2: 'foreign_key_violation',
  ER_ROW_IS_REFERENCED_2: 'foreign_key_violation',
  '1452': 'foreign_key_violation',
  '1451': 'foreign_key_violation',
  ER_BAD_NULL_ERROR: 'not_null_violation',
  '1048': 'not_null_violation',
  ER_CHECK_CONSTRAINT_VIOLATED: 'check_violation',
  '3819': 'check_violation',
  ER_TRUNCATED_WRONG_VALUE: 'invalid_value_type',
  ER_TRUNCATED_WRONG_VALUE_FOR_FIELD: 'invalid_value_type',
  '1366': 'invalid_value_type',
  '1264': 'invalid_value_type',
};

const MONGO_CODE_CATEGORIES: Record<string, TMutationErrorCategory> = {
  '11000': 'unique_violation',
  '121': 'validation_failed',
  '2': 'invalid_value_type',
};

function readErrorCode(error: unknown): string | null {
  if (!error || typeof error !== 'object') return null;
  const record = error as Record<string, unknown>;
  for (const key of ['code', 'errno', 'statusCode']) {
    const value = record[key];
    if (typeof value === 'string' || typeof value === 'number') {
      return String(value);
    }
  }
  return null;
}

function resolveCategory(
  error: unknown,
  backend: string,
): TMutationErrorCategory {
  const code = readErrorCode(error);
  if (!code) return 'unknown';
  const table =
    backend === 'postgres'
      ? POSTGRES_CODE_CATEGORIES
      : backend === 'mysql'
        ? MYSQL_CODE_CATEGORIES
        : MONGO_CODE_CATEGORIES;
  return table[code] ?? 'unknown';
}

export function summarizeMutationError(
  error: unknown,
  backend: string | null | undefined,
): TMutationErrorSummary {
  const category = resolveCategory(error, backend ?? 'unknown');
  return { category, message: CATEGORY_MESSAGES[category] };
}

/**
 * Driver messages embed table, column, index and statement text. Anything that
 * crosses the script boundary has already lost its typed error code, so the
 * message is classified by shape instead: a known constraint phrase maps to the
 * matching category, and anything else falls back to the generic text.
 */
const MESSAGE_SHAPES: Array<[RegExp, TMutationErrorCategory]> = [
  [/duplicate key value violates unique constraint|duplicate entry|E11000 duplicate key/i, 'unique_violation'],
  [/violates foreign key constraint|cannot add or update a child row|a foreign key constraint fails/i, 'foreign_key_violation'],
  [/null value in column .* violates not-null constraint|cannot be null/i, 'not_null_violation'],
  [/violates check constraint|check constraint .* is violated/i, 'check_violation'],
  [/invalid input syntax for type|incorrect .* value|out of range value/i, 'invalid_value_type'],
  [/document failed validation|failed validation/i, 'validation_failed'],
];

export function sanitizeScriptErrorMessage(message: unknown): string {
  if (typeof message !== 'string' || message.length === 0) {
    return CATEGORY_MESSAGES.unknown;
  }
  for (const [pattern, category] of MESSAGE_SHAPES) {
    if (pattern.test(message)) return CATEGORY_MESSAGES[category];
  }
  return message;
}
