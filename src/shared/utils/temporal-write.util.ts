export const TEMPORAL_COLUMN_TYPES = new Set(['date', 'datetime', 'timestamp']);

export function isTemporalColumnType(type: unknown): boolean {
  return TEMPORAL_COLUMN_TYPES.has(String(type ?? '').toLowerCase());
}

/**
 * Clients send temporal values as ISO-8601 strings because JSON has no date
 * type, and reads hand back the same ISO-8601 form. The write path has to
 * convert that string to a Date before it reaches the driver: MySQL DATETIME
 * rejects the `T`/`Z` form outright, and MongoDB's `$jsonSchema` validator
 * requires a BSON date rather than a string. Leaving the value as a string
 * makes a temporal column impossible to round-trip on either backend.
 */
export function coerceTemporalWriteValue(value: unknown): unknown {
  if (typeof value !== 'string') return value;
  const trimmed = value.trim();
  if (trimmed === '') return value;
  const parsed = new Date(trimmed);
  return Number.isNaN(parsed.getTime()) ? value : parsed;
}
