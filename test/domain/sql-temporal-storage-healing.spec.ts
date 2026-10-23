import type { Knex } from 'knex';
import { describe, expect, it, vi } from 'vitest';
import { SqlSchemaHealingService } from '../../src/engines/bootstrap/services/schema-healing/sql-schema-healing.service';

function fixture(columnDefault: string | null) {
  const db = Object.assign(
    vi.fn(() => ({ select: async () => [{ name: 'sessions' }] })),
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
              extra: '',
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
        'ALTER TABLE `sessions` MODIFY COLUMN `expiresAt` DATETIME NULL',
      );
    },
  );

  it.each([
    ['CURRENT_TIMESTAMP', 'CURRENT_TIMESTAMP'],
    ['current_timestamp()', 'current_timestamp()'],
    ['2026-01-01 12:00:00', "'2026-01-01 12:00:00'"],
    ["'2026-01-01 12:00:00'", "'2026-01-01 12:00:00'"],
  ])('preserves temporal default %s', async (value, literal) => {
    const { db, service } = fixture(value);
    await expect(service.repairSqlTemporalColumns({})).resolves.toBe(1);
    expect(db.raw).toHaveBeenLastCalledWith(
      `ALTER TABLE \`sessions\` MODIFY COLUMN \`expiresAt\` DATETIME NULL DEFAULT ${literal}`,
    );
  });
});
