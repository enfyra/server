import { Long } from 'mongodb';

export const MONGO_LONG_COLUMN_TYPES = new Set(['long', 'bigint']);

export function isMongoLongColumnType(type: unknown): boolean {
  return MONGO_LONG_COLUMN_TYPES.has(String(type ?? '').toLowerCase());
}

const INTEGER_STRING_PATTERN = /^-?\d+$/;

/**
 * JSON carries no 64-bit integer, so a client sends a `long` column as a number
 * or as a decimal string. MongoDB's `$jsonSchema` validator requires a BSON Long
 * rather than either, so the write path converts before the driver sees it.
 *
 * Only an exact integer converts. `Long.fromNumber` truncates silently, so a
 * fractional value left alone reaches the validator and is rejected instead of
 * being stored as a different number than the client sent.
 */
export function coerceMongoLongWriteValue(value: unknown): unknown {
  if (typeof value === 'number') {
    return Number.isSafeInteger(value) ? Long.fromNumber(value) : value;
  }
  if (typeof value === 'string' && INTEGER_STRING_PATTERN.test(value)) {
    return Long.fromString(value);
  }
  return value;
}
