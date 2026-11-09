import { describe, expect, it } from 'vitest';
import { resolveSchemaMigration } from '../../src/shared/utils/versioned-migration.util';
import type { VersionedSchemaMigration } from '../../src/shared/types/schema-migration.types';

const versions = (
  first: VersionedSchemaMigration['schema'],
  second: VersionedSchemaMigration['schema'],
): VersionedSchemaMigration[] => [
  { fromVersion: '2.2.19-patch-1', toVersion: '2.3.0', schema: first },
  { fromVersion: '2.3.0', toVersion: '2.3.1', schema: second },
];

const table = (from: Record<string, unknown>, to: Record<string, unknown>) => ({
  _unique: { name: { _eq: 'enfyra_route_handler' } },
  tableToModify: { from, to },
});

describe('versioned schema migration merge', () => {
  it('handles an empty migration chain without inventing a source version', () => {
    expect(resolveSchemaMigration([], '2.3.1')).toEqual({ migration: null, appliedSteps: 0, sourceVersion: '2.3.1', assumedSourceVersion: false });
  });

  it('rejects gapped migration chains before selecting declarations', () => {
    const steps = versions({ tables: [] }, { tables: [] });
    const [first] = steps;
    if (!first) throw new Error('Missing test migration');
    first.toVersion = '2.2.20';
    expect(() => resolveSchemaMigration(steps, '2.2.19-patch-1')).toThrow('Enfyra migration steps are not contiguous');
  });

  it('keeps historical execution steps but omits dropped tables from the merged final target', () => {
    const steps = versions(
      { tables: [table({ description: 'old' }, { description: 'new' })] },
      { tables: [], tablesToDrop: ['enfyra_route_handler'] },
    );
    const resolved = resolveSchemaMigration(steps, '2.2.19-patch-1');
    expect(resolved.migration?.tables).toEqual([]);
    expect(resolved.migration?.tablesToDrop).toEqual(['enfyra_route_handler']);
    expect(steps[0].schema.tables).toHaveLength(1);
  });
  it('keeps independent table changes from both applicable steps', () => {
    const resolved = resolveSchemaMigration(
      versions(
        { tables: [table({ description: 'old' }, { description: 'new' })] },
        {
          tables: [
            table(
              { uniques: [['route', 'method']] },
              { uniques: [['route', 'method'], ['routeMethodConfig']] },
            ),
          ],
        },
      ),
      '2.2.19-patch-1',
    );

    expect(resolved.appliedSteps).toBe(2);
    expect(resolved.migration?.tables[0].tableToModify).toEqual({
      from: { description: 'old', uniques: [['route', 'method']] },
      to: {
        description: 'new',
        uniques: [['route', 'method'], ['routeMethodConfig']],
      },
    });
  });

  it('composes historical type mappings without duplicate sources or identity changes', () => {
    const result = resolveSchemaMigration(versions(
      { tables: [], mongoColumnTypesToModify: [{ from: 'timestamp', to: 'date' }, { from: 'datetime', to: 'date' }] },
      { tables: [], mongoColumnTypesToModify: [{ from: 'date', to: 'datetime' }, { from: 'timestamp', to: 'datetime' }] },
    ), '2.2.19-patch-1');
    expect(result.migration?.mongoColumnTypesToModify).toEqual([
      { from: 'timestamp', to: 'datetime' },
      { from: 'date', to: 'datetime' },
    ]);
  });

  it('preserves independent column changes when composing later steps', () => {
    const column = (from: Record<string, unknown>, to: Record<string, unknown>) => ({
      _unique: { name: { _eq: 'example' } },
      columnsToModify: [{ from: { name: 'value', ...from }, to: { name: 'value', ...to } }],
    });
    const result = resolveSchemaMigration(versions(
      { tables: [column({ type: 'text' }, { type: 'varchar' })] },
      { tables: [column({ description: 'old' }, { description: 'new' })] },
    ), '2.2.19-patch-1');
    expect(result.migration?.tables[0].columnsToModify).toEqual([{
      from: { name: 'value', type: 'text', description: 'old' },
      to: { name: 'value', type: 'varchar', description: 'new' },
    }]);
  });

  it('refuses a repeated table field rather than dropping the intermediate change', () => {
    const steps = versions(
      { tables: [table({ uniques: [['a']] }, { uniques: [['b']] })] },
      { tables: [table({ uniques: [['b']] }, { uniques: [['c']] })] },
    );
    expect(() => resolveSchemaMigration(steps, '2.2.19-patch-1')).toThrow(
      'Upgrade through the intermediate version first',
    );
    expect(
      resolveSchemaMigration(steps, '2.3.0').migration?.tables[0].tableToModify
        ?.to.uniques,
    ).toEqual([['c']]);
  });
});
