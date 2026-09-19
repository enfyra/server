import { describe, expect, it } from 'vitest';
import { toExactDataMigrationWhere } from '../../src/domain/bootstrap/utils/data-migration-filter.util';

describe('toExactDataMigrationWhere', () => {
  it('accepts exact scalar filters and _eq selectors', () => {
    expect(toExactDataMigrationWhere({ path: '/logs' })).toEqual({
      path: '/logs',
    });
    expect(toExactDataMigrationWhere({ path: { _eq: '/logs' } })).toEqual({
      path: '/logs',
    });
    expect(toExactDataMigrationWhere({ id: 7, active: false, tag: null })).toEqual(
      { id: 7, active: false, tag: null },
    );
  });

  it('rejects operator selectors that are not exact matches', () => {
    expect(toExactDataMigrationWhere({ path: { _ne: '/logs' } })).toBeNull();
    expect(toExactDataMigrationWhere({ path: { _eq: '/logs', _ne: '/x' } })).toBeNull();
  });

  it('rejects mongo operator keys', () => {
    expect(toExactDataMigrationWhere({ $where: 'while(true){}' })).toBeNull();
    expect(toExactDataMigrationWhere({ $or: [{ path: '/logs' }] })).toBeNull();
    expect(toExactDataMigrationWhere({ path: '/logs', $comment: 'x' })).toBeNull();
  });

  it('rejects private and non-scalar values', () => {
    expect(toExactDataMigrationWhere({ _id: 1 })).toBeNull();
    expect(toExactDataMigrationWhere({ path: { $regex: '.*' } })).toBeNull();
    expect(toExactDataMigrationWhere({ path: ['/logs'] })).toBeNull();
    expect(toExactDataMigrationWhere({ path: Number.NaN })).toBeNull();
    expect(toExactDataMigrationWhere({})).toBeNull();
    expect(toExactDataMigrationWhere(null)).toBeNull();
  });
});
