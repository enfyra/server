import {
  getLookupKey,
  getManyToOneRelations,
  getScalarColumns,
  getUniqueFields,
} from '../../src/domain/bootstrap';

describe('snapshot-meta.util', () => {
  it('getLookupKey uses LOOKUP_KEY_MAP for known system tables', () => {
    expect(getLookupKey('enfyra_route')).toBe('path');
    expect(getLookupKey('enfyra_method')).toBe('name');
    expect(getLookupKey('enfyra_user')).toBe('email');
  });

  it('getManyToOneRelations returns propertyName, targetTable, lookupKey for enfyra_column', () => {
    const rels = getManyToOneRelations('enfyra_column');
    const tableRel = rels.find((r) => r.propertyName === 'table');
    expect(tableRel).toBeDefined();
    expect(tableRel!.targetTable).toBe('enfyra_table');
    expect(tableRel!.lookupKey).toBe('name');
  });

  it('getUniqueFields matches snapshot uniques when present', () => {
    const u = getUniqueFields('enfyra_route');
    expect(Array.isArray(u)).toBe(true);
  });

  it('getScalarColumns skips id and timestamp fields', () => {
    const cols = getScalarColumns('enfyra_table');
    expect(cols).not.toContain('id');
    expect(cols).not.toContain('createdAt');
    expect(cols).not.toContain('updatedAt');
  });
});
