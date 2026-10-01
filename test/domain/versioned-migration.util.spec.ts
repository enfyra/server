import { describe, expect, it } from 'vitest';
import { resolveSchemaMigration } from '../../src/shared/utils/versioned-migration.util';
import type { VersionedSchemaMigration } from '../../src/shared/types/schema-migration.types';

const versions = (first: VersionedSchemaMigration['schema'], second: VersionedSchemaMigration['schema']): VersionedSchemaMigration[] => [
  { fromVersion: '2.2.19-patch-1', toVersion: '2.3.0', schema: first },
  { fromVersion: '2.3.0', toVersion: '2.3.1', schema: second },
];

const table = (from: Record<string, unknown>, to: Record<string, unknown>) => ({
  _unique: { name: { _eq: 'enfyra_route_handler' } },
  tableToModify: { from, to },
});

describe('versioned schema migration merge', () => {
  it('keeps independent table changes from both applicable steps', () => {
    const resolved = resolveSchemaMigration(versions(
      { tables: [table({ description: 'old' }, { description: 'new' })] },
      { tables: [table({ uniques: [['route', 'method']] }, { uniques: [['route', 'method'], ['routeMethodConfig']] })] },
    ), '2.2.19-patch-1');

    expect(resolved.appliedSteps).toBe(2);
    expect(resolved.migration?.tables[0].tableToModify).toEqual({
      from: { description: 'old', uniques: [['route', 'method']] },
      to: { description: 'new', uniques: [['route', 'method'], ['routeMethodConfig']] },
    });
  });

  it('refuses a repeated table field rather than dropping the intermediate change', () => {
    const steps = versions(
      { tables: [table({ uniques: [['a']] }, { uniques: [['b']] })] },
      { tables: [table({ uniques: [['b']] }, { uniques: [['c']] })] },
    );
    expect(() => resolveSchemaMigration(steps, '2.2.19-patch-1')).toThrow('Upgrade through the intermediate version first');
    expect(resolveSchemaMigration(steps, '2.3.0').migration?.tables[0].tableToModify?.to.uniques).toEqual([['c']]);
  });
});
