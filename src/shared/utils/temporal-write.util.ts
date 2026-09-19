import { types as pgTypes } from 'pg';

export const TEMPORAL_COLUMN_TYPES = new Set(['date', 'datetime', 'timestamp']);

export function isTemporalColumnType(type: unknown): boolean {
  return TEMPORAL_COLUMN_TYPES.has(String(type ?? '').toLowerCase());
}

let pgDateParserRegistered = false;

/**
 * A `date` column carries no time zone, but the pg driver returns it as a Date at
 * local midnight, which serializes to the previous calendar day on any host behind
 * UTC (and to the next day ahead of UTC). The mysql2 side already reads DATE
 * columns as strings through its `typeCast`; this registers the equivalent for pg.
 *
 * Registering once is enough: every knex instance the server builds (direct and
 * replication-manager) resolves the same shared `pg` module.
 */
export function registerPgDateTypeParser(): void {
  if (pgDateParserRegistered) return;
  pgDateParserRegistered = true;
  pgTypes.setTypeParser(pgTypes.builtins.DATE, (value) => value);
}

const DATE_ONLY_PATTERN = /^\d{4}-\d{2}-\d{2}$/;

/**
 * Clients send temporal values as ISO-8601 strings because JSON has no date
 * type, and reads hand back the same ISO-8601 form. The write path has to
 * convert that string to a Date before it reaches the driver: MySQL DATETIME
 * rejects the `T`/`Z` form outright, and MongoDB's `$jsonSchema` validator
 * requires a BSON date rather than a string. Leaving the value as a string
 * makes a temporal column impossible to round-trip on either backend.
 *
 * `dateOnlyAsString` is set by the SQL engines only. A date-only string
 * (`2024-05-20`) parses as UTC midnight and mysql2 serializes a Date from its
 * *local* components, so on a host behind UTC the stored day would shift back
 * one (2024-05-19); the SQL drivers accept the date-only form verbatim, which
 * keeps the calendar day intact. Mongo has no such form — its validator demands
 * a BSON date — so it always receives a Date.
 */
export function coerceTemporalWriteValue(
  value: unknown,
  options: { dateOnlyAsString?: boolean } = {},
): unknown {
  if (typeof value !== 'string') return value;
  const trimmed = value.trim();
  if (trimmed === '') return value;
  if (options.dateOnlyAsString && DATE_ONLY_PATTERN.test(trimmed)) return value;
  const parsed = new Date(trimmed);
  return Number.isNaN(parsed.getTime()) ? value : parsed;
}
