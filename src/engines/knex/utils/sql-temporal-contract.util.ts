/**
 * The temporal storage contract for a SQL backend.
 *
 * Enfyra stores a logical `datetime`/`timestamp` as an instant, so both SQL
 * backends must read and write it the same way. PostgreSQL gets that from
 * `TIMESTAMPTZ`. MySQL has no zone-bearing column type, so it is made equivalent
 * at the connection boundary instead: the driver serializes every `Date` from its
 * UTC components and the session reads in UTC, which keeps the stored wall clock
 * zone-independent and lets a client re-render it in any zone.
 *
 * Without both halves the stored value follows the host process zone (`TZ`), so
 * the same instant lands on a different wall clock on a developer machine than in
 * the UTC production container.
 */

/** The session zone every MySQL connection is pinned to. */
export const MYSQL_UTC_SESSION_TIME_ZONE = '+00:00';

/** mysql2 `timezone` option value that makes it serialize and parse dates as UTC. */
export const MYSQL_DRIVER_TIMEZONE = 'Z';

/**
 * Applied to every pooled MySQL connection. The driver option alone is not enough:
 * MySQL converts `TIMESTAMP` values between UTC and the session zone on read, so a
 * non-UTC session would hand back a different wall clock than was written.
 */
export function applyMySqlSessionTimeZone(
  connection: any,
  done: (error: unknown, connection?: any) => void,
): void {
  connection.query(
    `SET time_zone = '${MYSQL_UTC_SESSION_TIME_ZONE}'`,
    (error: unknown) => {
      done(error, connection);
    },
  );
}

const MYSQL_TEMPORAL_PATTERN =
  /^(\d{4})-(\d{2})-(\d{2})[ T](\d{2}):(\d{2}):(\d{2})(?:\.(\d{1,6}))?$/;

/**
 * Reads a MySQL temporal value back as the instant it stores.
 *
 * With the session and the driver both pinned to UTC, the value MySQL hands back is
 * a UTC wall clock with no offset in the text. `new Date(value)` would parse that
 * text in the host's zone, so every read would shift by the server offset — which is
 * what made an unexpired schema-fence lease look expired on a host behind UTC. The
 * components are read as UTC explicitly instead, matching what PostgreSQL returns
 * for `TIMESTAMPTZ`.
 *
 * A value that is not in that shape is returned unchanged so an unexpected format
 * surfaces rather than being silently coerced.
 */
export function parseMySqlUtcTemporal(value: unknown): unknown {
  if (typeof value !== 'string') return value;
  const match = MYSQL_TEMPORAL_PATTERN.exec(value.trim());
  if (!match) return value;
  const [, year, month, day, hour, minute, second, fraction] = match;
  const milliseconds = fraction
    ? Number(fraction.padEnd(3, '0').slice(0, 3))
    : 0;
  return new Date(
    Date.UTC(
      Number(year),
      Number(month) - 1,
      Number(day),
      Number(hour),
      Number(minute),
      Number(second),
      milliseconds,
    ),
  );
}
