import { describe, expect, it, vi } from 'vitest';
import { addColumnToTable } from '../../src/engines/knex/utils/migration/column-operations';
import { getPostgresEnumTypeName } from '../../src/engines/knex/utils/sql-enum.util';

function createTableBuilder() {
  const column = {
    notNullable: vi.fn(),
    nullable: vi.fn(),
    defaultTo: vi.fn(),
  };
  return {
    column,
    table: {
      enu: vi.fn(() => column),
    } as any,
  };
}

describe('SQL enum column operations', () => {
  it('creates runtime PostgreSQL enum columns with a native physical type', () => {
    const { table } = createTableBuilder();

    addColumnToTable(
      table,
      {
        name: 'paymentProvider',
        type: 'enum',
        options: ['sepay', 'paypal', 'apipay'],
      },
      'postgres',
      'referral_credit_rewards',
    );

    expect(table.enu).toHaveBeenCalledWith(
      'paymentProvider',
      ['sepay', 'paypal', 'apipay'],
      {
        useNative: true,
        enumName: 'referral_credit_rewards_paymentProvider_enum',
      },
    );
  });

  it('creates runtime MySQL enum columns with a native physical type', () => {
    const { table } = createTableBuilder();

    addColumnToTable(
      table,
      {
        name: 'paymentProvider',
        type: 'enum',
        options: ['sepay', 'paypal', 'apipay'],
      },
      'mysql',
      'referral_credit_rewards',
    );

    expect(table.enu).toHaveBeenCalledWith('paymentProvider', [
      'sepay',
      'paypal',
      'apipay',
    ]);
  });

  it('rejects enum creation without physical table identity', () => {
    const { table } = createTableBuilder();

    expect(() =>
      addColumnToTable(
        table,
        { name: 'status', type: 'enum', options: ['active'] },
        'postgres',
      ),
    ).toThrow(/requires table identity and options/);
  });

  it('keeps generated PostgreSQL enum type names within the identifier limit', () => {
    const first = getPostgresEnumTypeName(
      'a_very_long_table_name_that_already_uses_most_identifier_space',
      'paymentProvider',
    );
    const second = getPostgresEnumTypeName(
      'a_very_long_table_name_that_already_uses_most_identifier_space',
      'settlementProvider',
    );

    expect(first.length).toBeLessThanOrEqual(63);
    expect(second.length).toBeLessThanOrEqual(63);
    expect(first).not.toBe(second);
  });
});
