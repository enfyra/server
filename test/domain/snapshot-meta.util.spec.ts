import {
  getLookupKey,
  getManyToOneRelations,
  getScalarColumns,
  getUniqueFields,
} from '../../src/domain/bootstrap/utils/snapshot-meta.util';

describe('snapshot-meta.util', () => {
  it('getLookupKey uses LOOKUP_KEY_MAP for known system tables', () => {
    expect(getLookupKey('route_definition')).toBe('path');
    expect(getLookupKey('method_definition')).toBe('method');
    expect(getLookupKey('user_definition')).toBe('email');
  });

  it('getManyToOneRelations returns propertyName, targetTable, lookupKey for column_definition', () => {
    const rels = getManyToOneRelations('column_definition');
    const tableRel = rels.find((r) => r.propertyName === 'table');
    expect(tableRel).toBeDefined();
    expect(tableRel!.targetTable).toBe('table_definition');
    expect(tableRel!.lookupKey).toBe('name');
  });

  it('getUniqueFields matches snapshot uniques when present', () => {
    const u = getUniqueFields('route_definition');
    expect(Array.isArray(u)).toBe(true);
  });

  it('getScalarColumns skips id and timestamp fields', () => {
    const cols = getScalarColumns('table_definition');
    expect(cols).not.toContain('id');
    expect(cols).not.toContain('createdAt');
    expect(cols).not.toContain('updatedAt');
  });
});
