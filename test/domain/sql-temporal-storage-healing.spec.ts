import type { Knex } from 'knex';
import { describe, expect, it, vi } from 'vitest';
import { SqlSchemaHealingService } from '../../src/engines/bootstrap/services/schema-healing/sql-schema-healing.service';

function fixture(columnDefault: string | null, precision = 0, extra = '') {
  const db = Object.assign(
    vi.fn((table: string) => table === 'enfyra_table'
      ? { select: async () => [{ name: 'sessions' }] }
      : { join: () => ({ where: () => ({ select: async () => [{ tableName: 'sessions', columnName: 'expiresAt' }] }) }) }),
    {
      schema: { hasTable: async () => true },
      raw: vi
        .fn()
        .mockResolvedValueOnce([
          [
            {
              tableName: 'sessions',
              columnName: 'expiresAt',
              dataType: 'timestamp',
              isNullable: 'YES',
              columnDefault,
              extra,
              datetimePrecision: precision,
              columnComment: '',
            },
          ],
        ])
        .mockResolvedValue([]),
    },
  );
  const service = new SqlSchemaHealingService({
    queryBuilderService: {
      getKnex: () => db as unknown as Knex,
      getDatabaseType: () => 'mysql',
    } as never,
    metadataCacheService: {} as never,
    systemCoreTableResolver: {
      getNames: async () => ({
        table: 'enfyra_table',
        column: 'enfyra_column',
        relation: 'enfyra_relation',
      }),
    } as never,
    log: () => {},
    warn: () => {},
  });
  return { db, service };
}

describe('MySQL-compatible temporal default healing', () => {
  it.each([null, 'NULL'])(
    'preserves SQL NULL default %j without turning it into text',
    async (value) => {
      const { db, service } = fixture(value);
      await expect(service.repairSqlTemporalColumns({})).resolves.toBe(1);
      expect(db.raw).toHaveBeenLastCalledWith(
        'ALTER TABLE ?? MODIFY COLUMN ?? DATETIME(3) NULL',
        ['sessions', 'expiresAt'],
      );
    },
  );

  it.each([
    ['CURRENT_TIMESTAMP', 'CURRENT_TIMESTAMP(3)'],
    ['current_timestamp()', 'CURRENT_TIMESTAMP(3)'],
    ['2026-01-01 12:00:00', "'2026-01-01 12:00:00'"],
    ["'2026-01-01 12:00:00'", "'2026-01-01 12:00:00'"],
  ])('preserves temporal default %s', async (value, literal) => {
    const { db, service } = fixture(value);
    await expect(service.repairSqlTemporalColumns({})).resolves.toBe(1);
    expect(db.raw).toHaveBeenLastCalledWith(
      `ALTER TABLE ?? MODIFY COLUMN ?? DATETIME(3) NULL DEFAULT ${literal}`,
      ['sessions', 'expiresAt'],
    );
  });

  it('retains microsecond precision and matches the update expression', async () => {
    const { db, service } = fixture('CURRENT_TIMESTAMP(6)', 6, 'on update CURRENT_TIMESTAMP(6)');
    await expect(service.repairSqlTemporalColumns({})).resolves.toBe(1);
    expect(db.raw).toHaveBeenLastCalledWith(
      'ALTER TABLE ?? MODIFY COLUMN ?? DATETIME(6) NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6)',
      ['sessions', 'expiresAt'],
    );
  });
});
