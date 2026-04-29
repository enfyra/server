import { describe, expect, it } from 'vitest';
import { resolveSqlRelationOnDelete } from '../../src/engines/knex/services/sql-schema-migration.service';

describe('resolveSqlRelationOnDelete', () => {
  it('preserves explicit CASCADE for non-null relations', () => {
    expect(
      resolveSqlRelationOnDelete({
        onDelete: 'CASCADE',
        isNullable: false,
      }),
    ).toBe('CASCADE');
  });

  it('preserves explicit SET NULL and RESTRICT actions', () => {
    expect(
      resolveSqlRelationOnDelete({
        onDelete: 'SET NULL',
        isNullable: true,
      }),
    ).toBe('SET NULL');
    expect(
      resolveSqlRelationOnDelete({
        onDelete: 'RESTRICT',
        isNullable: true,
      }),
    ).toBe('RESTRICT');
  });

  it('falls back to nullability only when onDelete is missing', () => {
    expect(resolveSqlRelationOnDelete({ isNullable: false })).toBe('RESTRICT');
    expect(resolveSqlRelationOnDelete({ isNullable: 0 })).toBe('RESTRICT');
    expect(resolveSqlRelationOnDelete({ isNullable: true })).toBe('SET NULL');
    expect(resolveSqlRelationOnDelete({})).toBe('SET NULL');
  });
});
