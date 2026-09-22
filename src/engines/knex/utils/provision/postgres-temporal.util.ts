/**
 * The physical PostgreSQL target for Enfyra's logical temporal column types.
 *
 * Every logical temporal type — `datetime`, `timestamp` and `date` — holds an
 * instant. The `date` name is historical: the columns that use it are expiry and
 * lifecycle stamps (`expiredAt`, `expiresAt`, `lastUsedAt`, `startedAt`,
 * `completedAt`), and `enfyra_session.expiredAt` even defaults to `now`, which a
 * calendar-day type would truncate to midnight. So all three resolve to the same
 * zone-bearing physical type; a zone-less `timestamp` or a truncated `DATE` stores
 * something that compares differently depending on how a client spells its offset.
 */
export const POSTGRES_TEMPORAL_PHYSICAL_TYPE = 'TIMESTAMPTZ';

const LOGICAL_TEMPORAL_TYPES = new Set(['datetime', 'timestamp', 'date']);

const AWARE_PHYSICAL_TYPES = new Set([
  'timestamptz',
  'timestamp with time zone',
]);

const NAIVE_PHYSICAL_TYPES = new Set([
  'timestamp',
  'timestamp without time zone',
  'datetime',
]);

export function normalizeTemporalType(type: unknown): string {
  return String(type ?? '')
    .trim()
    .toLowerCase()
    .replace(/\(.*\)$/, '')
    .trim();
}

/**
 * True when the column already carries the zone-aware contract, so a caller can
 * treat it as a no-op instead of rewriting the table on every pass.
 */
export function isPostgresAwareTemporalType(type: unknown): boolean {
  return AWARE_PHYSICAL_TYPES.has(normalizeTemporalType(type));
}

/**
 * Resolves a logical Enfyra type or an already-physical PostgreSQL type name to the
 * physical target, or `null` when the column is not temporal at all. Both spellings
 * are accepted because the DDL paths differ: the metadata path carries logical types
 * while the diff path carries generated SQL.
 */
export function postgresTemporalPhysicalType(type: unknown): string | null {
  const normalized = normalizeTemporalType(type);
  if (
    LOGICAL_TEMPORAL_TYPES.has(normalized) ||
    AWARE_PHYSICAL_TYPES.has(normalized) ||
    NAIVE_PHYSICAL_TYPES.has(normalized) ||
    normalized === 'date'
  ) {
    return POSTGRES_TEMPORAL_PHYSICAL_TYPE;
  }
  return null;
}

/**
 * The `USING` expression that carries a temporal column onto its physical target.
 *
 * A zone-less column stores wall clock that the write path anchored to UTC, so the
 * conversion names UTC explicitly rather than inheriting the session zone, which
 * would shift every stored instant by the server's offset. A `date` column holds a
 * calendar day at midnight, which the same anchor places on that day in UTC.
 *
 * `columnRef` is a ready SQL identifier reference (`"name"` or the knex `??`
 * placeholder), not a raw name, so callers keep control of identifier escaping.
 */
export function postgresTemporalUsingExpression(
  columnRef: string,
  currentPhysicalType: unknown,
): string {
  const current = normalizeTemporalType(currentPhysicalType);

  // Returning the bare reference means the column already satisfies the contract,
  // which is what lets a caller treat an empty conversion as a no-op.
  if (AWARE_PHYSICAL_TYPES.has(current)) return columnRef;
  if (NAIVE_PHYSICAL_TYPES.has(current)) {
    return `(${columnRef} AT TIME ZONE 'UTC')`;
  }
  if (current === 'date') return `(${columnRef}::timestamp AT TIME ZONE 'UTC')`;
  return `${columnRef}::text::${POSTGRES_TEMPORAL_PHYSICAL_TYPE}`;
}
