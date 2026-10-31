import { describe, expect, it, vi } from 'vitest';
import type { Knex } from 'knex';
import {
  ensureSqlEnumTypes,
  planPostgresEnumTypeCreation,
} from '../../src/engines/knex/utils/sql-enum.util';

function connection(rows: unknown[]) {
  const raw = vi.fn().mockResolvedValue({ rows });
  return { raw, db: { raw } as unknown as Knex };
}

describe('SQL enum type preparation', () => {
  it.each([undefined, null, [], ['raw', 1], ['raw', 'raw']])(
    'rejects invalid options %j before catalog access',
    async (options) => {
      const { db, raw } = connection([]);
      await expect(
        planPostgresEnumTypeCreation(db, 'auth', {
          name: 'scheme',
          type: 'enum',
          options,
        }),
      ).rejects.toThrow(/requires options|invalid options/);
      expect(raw).not.toHaveBeenCalled();
    },
  );

  it('reuses only identical ordered labels', async () => {
    const { db } = connection([{ values: ['raw', 'bearer'] }]);
    expect(
      await planPostgresEnumTypeCreation(db, 'auth', {
        name: 'scheme',
        options: ['raw', 'bearer'],
      }),
    ).toEqual({ typeName: 'auth_scheme_enum', statements: [] });
    await expect(
      planPostgresEnumTypeCreation(db, 'auth', {
        name: 'scheme',
        options: ['bearer', 'raw'],
      }),
    ).rejects.toThrow(/incompatible options/);
  });

  it('quotes identifiers and label literals while planning remains read-only', async () => {
    const { db, raw } = connection([]);
    const result = await planPostgresEnumTypeCreation(db, 'a"b', {
      name: 'scheme',
      options: ["it's", 'bearer'],
    });
    expect(result.statements).toEqual([
      "CREATE TYPE \"a\"\"b_scheme_enum\" AS ENUM ('it''s', 'bearer')",
    ]);
    expect(raw).toHaveBeenCalledTimes(1);
    expect(raw.mock.calls[0]?.[0]).toContain('SELECT');
  });

  it.each(['mysql', 'mysql2', 'mongodb'])(
    'does not query PostgreSQL catalogs for %s',
    async (dbType) => {
      const { db, raw } = connection([]);
      await ensureSqlEnumTypes(
        db,
        'auth',
        [{ name: 'scheme', type: 'enum', options: ['raw'] }],
        dbType,
      );
      expect(raw).not.toHaveBeenCalled();
    },
  );

  it('does not swallow catalog failures', async () => {
    const { db, raw } = connection([]);
    raw.mockRejectedValue(new Error('catalog unavailable'));
    await expect(
      ensureSqlEnumTypes(
        db,
        'auth',
        [{ name: 'scheme', type: 'enum', options: ['raw'] }],
        'pg',
      ),
    ).rejects.toThrow('catalog unavailable');
  });
});
