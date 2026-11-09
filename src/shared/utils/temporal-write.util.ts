import { types as pgTypes } from 'pg';
import { BadRequestException } from '../../domain/exceptions';

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
const UTC_DATETIME_PATTERN = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,3})?Z$/;

export function isCalendarDate(value: unknown): boolean {
  if (typeof value !== 'string' || !DATE_ONLY_PATTERN.test(value) || value.startsWith('0000-')) return false;
  const date = new Date(`${value}T00:00:00.000Z`);
  return Number.isFinite(date.getTime()) && date.toISOString().slice(0, 10) === value;
}

export function isUtcDateTime(value: unknown): boolean {
  if (value instanceof Date) return Number.isFinite(value.getTime()) && value.getUTCFullYear() >= 1 && value.getUTCFullYear() <= 9999;
  if (typeof value !== 'string' || !UTC_DATETIME_PATTERN.test(value) || value.startsWith('0000-')) return false;
  const date = new Date(value);
  return Number.isFinite(date.getTime()) && date.toISOString().slice(0, 19) === value.slice(0, 19);
}

export function coerceTemporalContractValue(value: unknown, type: string): unknown {
  if (value === null || value === undefined) return value;
  if (type === 'date') {
    if (!isCalendarDate(value)) throw new BadRequestException('Date must be a valid calendar date in YYYY-MM-DD format');
    return value;
  }
  if (!isUtcDateTime(value)) throw new BadRequestException('Datetime must be a valid UTC timestamp ending in Z');
  return value instanceof Date ? value : new Date(value as string);
}

/** Permissive conversion for legacy temporal values during healing. */
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
