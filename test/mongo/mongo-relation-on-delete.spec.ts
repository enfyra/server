import { normalizeRelationOnDelete } from '../../src/infrastructure/mongo/utils/mongo-relation-on-delete.util';

describe('normalizeRelationOnDelete', () => {
  it('defaults to SET NULL', () => {
    expect(normalizeRelationOnDelete({})).toBe('SET NULL');
    expect(normalizeRelationOnDelete(null)).toBe('SET NULL');
  });

  it('maps NO ACTION to RESTRICT', () => {
    expect(normalizeRelationOnDelete({ onDelete: 'NO ACTION' })).toBe(
      'RESTRICT',
    );
    expect(normalizeRelationOnDelete({ onDelete: 'NO_ACTION' })).toBe(
      'RESTRICT',
    );
  });

  it('preserves CASCADE, SET NULL, RESTRICT', () => {
    expect(normalizeRelationOnDelete({ onDelete: 'CASCADE' })).toBe('CASCADE');
    expect(normalizeRelationOnDelete({ onDelete: 'SET NULL' })).toBe(
      'SET NULL',
    );
    expect(normalizeRelationOnDelete({ onDelete: 'RESTRICT' })).toBe(
      'RESTRICT',
    );
  });
});
