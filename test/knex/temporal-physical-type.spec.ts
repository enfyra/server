import { describe, it, expect } from 'vitest';
import { generateColumnDefinition } from '../../src/engines/knex/utils/migration/sql-generator';
import { generateModifyColumnSQL } from '../../src/engines/knex/utils/migration/sql-dialect';
import { getKnexColumnType } from '../../src/engines/knex/utils/provision/schema-parser';
import { isTypeCompatible } from '../../src/engines/knex/utils/provision/schema-comparison';
import {
  POSTGRES_TEMPORAL_PHYSICAL_TYPE,
  postgresTemporalPhysicalType,
  postgresTemporalUsingExpression,
} from '../../src/engines/knex/utils/provision/postgres-temporal.util';
import {
  MYSQL_DRIVER_TIMEZONE,
  MYSQL_UTC_SESSION_TIME_ZONE,
  applyMySqlSessionTimeZone,
  parseMySqlUtcTemporal,
} from '../../src/engines/knex/utils/sql-temporal-contract.util';

describe('every logical temporal type resolves to an instant-bearing column', () => {
  // `date` is a historical name, not a calendar day: its columns are expiry and
  // lifecycle stamps, and `enfyra_session.expiredAt` defaults to `now`. Storing one
  // in a physical DATE would truncate it to midnight.
  it('maps datetime, timestamp and date to TIMESTAMPTZ on postgres', () => {
    for (const type of ['datetime', 'timestamp', 'date']) {
      expect(generateColumnDefinition({ name: 'c', type }, 'postgres')).toBe(
        'TIMESTAMPTZ',
      );
    }
  });

  it('maps every logical temporal type to DATETIME on mysql', () => {
    // MySQL has no zone-bearing column type, so the aware contract comes from the
    // connection instead. DATETIME stores the UTC wall clock verbatim and has no
    // 2038 ceiling.
    for (const type of ['datetime', 'timestamp', 'date']) {
      expect(generateColumnDefinition({ name: 'c', type }, 'mysql')).toBe(
        'DATETIME',
      );
    }
  });

  it('normalizes every logical temporal type to a knex timestamp', () => {
    expect(getKnexColumnType({ type: 'date' } as any)).toBe('timestamp');
    expect(getKnexColumnType({ type: 'datetime' } as any)).toBe('datetime');
    expect(getKnexColumnType({ type: 'timestamp' } as any)).toBe('timestamp');
  });
});

describe('postgres temporal drift detection', () => {
  // Collapsing a zone-less column into the aware label made the drift look like a
  // match, so it was never repaired.
  it('reports a zone-less column as drift from the aware target', () => {
    expect(
      isTypeCompatible('timestamp', 'timestamp without time zone', 'pg'),
    ).toBe(false);
    expect(isTypeCompatible('timestamp', 'timestamptz', 'pg')).toBe(true);
  });

  it('reports a date column as drift from the aware target', () => {
    expect(isTypeCompatible('timestamp', 'date', 'pg')).toBe(false);
  });

  it('treats aware spellings as equivalent to each other', () => {
    expect(
      isTypeCompatible('timestamptz', 'timestamp with time zone', 'pg'),
    ).toBe(true);
    expect(isTypeCompatible('datetime', 'timestamptz', 'pg')).toBe(true);
  });

  it('leaves MySQL temporal compatibility untouched', () => {
    expect(isTypeCompatible('timestamp', 'datetime', 'mysql2')).toBe(true);
  });
});

describe('temporal conversion expressions anchor the stored value to UTC', () => {
  it('names UTC when carrying a zone-less column to the aware target', () => {
    expect(
      postgresTemporalUsingExpression(
        '"occurredAt"',
        'timestamp without time zone',
      ),
    ).toBe(`("occurredAt" AT TIME ZONE 'UTC')`);
  });

  // A date column holds midnight, which the same UTC anchor places on that day.
  it('anchors a date column instead of truncating through a zone', () => {
    expect(postgresTemporalUsingExpression('"d"', 'date')).toBe(
      `("d"::timestamp AT TIME ZONE 'UTC')`,
    );
  });

  it('resolves the physical target for each logical and physical spelling', () => {
    expect(postgresTemporalPhysicalType('datetime')).toBe(
      POSTGRES_TEMPORAL_PHYSICAL_TYPE,
    );
    expect(postgresTemporalPhysicalType('timestamp')).toBe(
      POSTGRES_TEMPORAL_PHYSICAL_TYPE,
    );
    expect(postgresTemporalPhysicalType('date')).toBe(
      POSTGRES_TEMPORAL_PHYSICAL_TYPE,
    );
    expect(postgresTemporalPhysicalType('timestamptz')).toBe(
      POSTGRES_TEMPORAL_PHYSICAL_TYPE,
    );
    expect(postgresTemporalPhysicalType('varchar')).toBeNull();
  });

  // The bare reference is the caller's signal that no rewrite is needed, so a
  // repeated healing pass stays a no-op instead of rewriting the table.
  it('returns the bare reference when the column is already aware', () => {
    for (const aware of ['timestamptz', 'timestamp with time zone']) {
      expect(postgresTemporalUsingExpression('"c"', aware)).toBe('"c"');
    }
  });
});

describe('MODIFY COLUMN carries the zone when the physical type changes', () => {
  // Without a USING clause PostgreSQL reads the stored wall clock in the session
  // zone, so a naive column promoted to aware would shift by the server offset.
  it('emits a UTC-anchored USING for a naive source', () => {
    const sql = generateModifyColumnSQL(
      'ai_usage',
      'occurredAt',
      'TIMESTAMPTZ',
      'postgres',
      { type: 'datetime' },
      'timestamp',
    );
    expect(Array.isArray(sql) ? sql.join(' ') : sql).toContain(
      `USING ("occurredAt" AT TIME ZONE 'UTC')`,
    );
  });

  it('does not re-convert a column that is already aware', () => {
    const sql = generateModifyColumnSQL(
      'ai_usage',
      'occurredAt',
      'TIMESTAMPTZ',
      'postgres',
      { type: 'datetime' },
      'timestamptz',
    );
    const text = Array.isArray(sql) ? sql.join(' ') : sql;
    expect(text).toContain('TYPE TIMESTAMPTZ');
    expect(text).not.toContain('AT TIME ZONE');
  });

  it('stays a plain ALTER when the physical type is unknown', () => {
    const sql = generateModifyColumnSQL(
      'ai_usage',
      'occurredAt',
      'TIMESTAMPTZ',
      'postgres',
      { type: 'datetime' },
    );
    const text = Array.isArray(sql) ? sql.join(' ') : sql;
    expect(text).toContain('TYPE TIMESTAMPTZ');
    expect(text).not.toContain('AT TIME ZONE');
  });
});

describe('MySQL is made zone-aware at the connection boundary', () => {
  // MySQL has no zone-bearing column type, so equivalence with PostgreSQL's
  // TIMESTAMPTZ comes from pinning the driver and the session to UTC. Either half
  // alone leaves the stored wall clock following the host process zone.
  it('pins the driver and the session to UTC', () => {
    expect(MYSQL_DRIVER_TIMEZONE).toBe('Z');
    expect(MYSQL_UTC_SESSION_TIME_ZONE).toBe('+00:00');
  });

  it('issues the session time zone on every pooled connection', () => {
    const queries: string[] = [];
    const connection = {
      query: (sql: string, cb: (error: unknown) => void) => {
        queries.push(sql);
        cb(null);
      },
    };
    let called = 0;
    applyMySqlSessionTimeZone(connection, () => {
      called += 1;
    });
    expect(queries).toEqual([`SET time_zone = '+00:00'`]);
    expect(called).toBe(1);
  });

  it('surfaces a connection error instead of swallowing it', () => {
    const failure = new Error('session rejected');
    const connection = {
      query: (_sql: string, cb: (error: unknown) => void) => cb(failure),
    };
    let received: unknown = null;
    applyMySqlSessionTimeZone(connection, (error) => {
      received = error;
    });
    expect(received).toBe(failure);
  });
});

describe('MySQL temporal reads land on the stored instant', () => {
  // MySQL hands back a UTC wall clock with no offset once the session is pinned to
  // UTC. Parsing that text in the host zone shifted every read by the server offset,
  // which made an unexpired schema-fence lease look expired on a host behind UTC.
  it('reads a UTC wall clock as the instant it stores', () => {
    const parsed = parseMySqlUtcTemporal('2026-01-02 03:04:05');
    expect(parsed).toBeInstanceOf(Date);
    expect((parsed as Date).toISOString()).toBe('2026-01-02T03:04:05.000Z');
  });

  it('accepts the ISO separator and fractional seconds', () => {
    expect(
      (parseMySqlUtcTemporal('2026-01-02T03:04:05.250') as Date).toISOString(),
    ).toBe('2026-01-02T03:04:05.250Z');
  });

  it('leaves a non-temporal value untouched', () => {
    expect(parseMySqlUtcTemporal(null)).toBeNull();
    expect(parseMySqlUtcTemporal('not-a-date')).toBe('not-a-date');
  });

  it('is independent of the host zone', () => {
    const original = process.env.TZ;
    try {
      process.env.TZ = 'America/New_York';
      const west = (
        parseMySqlUtcTemporal('2026-01-02 03:04:05') as Date
      ).toISOString();
      process.env.TZ = 'Asia/Ho_Chi_Minh';
      const east = (
        parseMySqlUtcTemporal('2026-01-02 03:04:05') as Date
      ).toISOString();
      expect(west).toBe('2026-01-02T03:04:05.000Z');
      expect(east).toBe(west);
    } finally {
      if (original === undefined) delete process.env.TZ;
      else process.env.TZ = original;
    }
  });
});
