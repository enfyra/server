import { describe, expect, it, vi } from 'vitest';
import { dropPostgresColumnCheckConstraints } from '../../src/engines/knex/utils/provision/postgres-column-check-constraints';
import { isTypeCompatible } from '../../src/engines/knex/utils/provision/schema-comparison';

describe('dropPostgresColumnCheckConstraints', () => {
  it('requires varchar-backed enum metadata to migrate to a physical enum', () => {
    expect(isTypeCompatible('enum', 'varchar', 'pg')).toBe(false);
    expect(isTypeCompatible('enum', 'character varying', 'pg')).toBe(false);
    expect(isTypeCompatible('enum', 'varchar', 'mysql2')).toBe(false);
    expect(isTypeCompatible('enum', 'enum', 'pg')).toBe(true);
  });

  it('drops every CHECK attached to the enum column, including suffixed duplicates', async () => {
    const raw = vi
      .fn()
      .mockResolvedValueOnce({
        rows: [
          { constraint_name: 'ai_payment_order_paymentProvider_check' },
          { constraint_name: 'ai_payment_order_paymentProvider_check1' },
        ],
      })
      .mockResolvedValue({ rows: [] });

    await dropPostgresColumnCheckConstraints(
      { raw } as any,
      'ai_payment_order',
      'paymentProvider',
    );

    expect(raw).toHaveBeenNthCalledWith(
      2,
      'ALTER TABLE ?? DROP CONSTRAINT ??',
      ['ai_payment_order', 'ai_payment_order_paymentProvider_check'],
    );
    expect(raw).toHaveBeenNthCalledWith(
      3,
      'ALTER TABLE ?? DROP CONSTRAINT ??',
      ['ai_payment_order', 'ai_payment_order_paymentProvider_check1'],
    );
  });

  it('does not issue a guessed DROP when the catalog has no matching CHECK', async () => {
    const raw = vi.fn(async () => ({ rows: [] }));

    await dropPostgresColumnCheckConstraints(
      { raw } as any,
      'ai_payment_order',
      'paymentProvider',
    );

    expect(raw).toHaveBeenCalledTimes(1);
  });
});
