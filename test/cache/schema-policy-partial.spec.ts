import { EventEmitter2 } from 'eventemitter2';
import { expect, it, vi } from 'vitest';
import { FieldPermissionCacheBuilder } from '../../src/engines/cache/services/field-permission-cache-builder.service';
import { ColumnRuleCacheBuilder } from '../../src/engines/cache/services/column-rule-cache-builder.service';

it('rebuilds only affected-table policy buckets for aggregate schema changes', async () => {
  const metadata = {
    tablesList: [
      {
        id: 1,
        name: 'one',
        columns: [{ id: 10, name: 'value' }],
        relations: [],
      },
      {
        id: 2,
        name: 'two',
        columns: [{ id: 20, name: 'value' }],
        relations: [],
      },
    ],
  };
  const rows = [
    {
      id: 1,
      column: { id: 10 },
      role: { id: 1 },
      action: 'read',
      effect: 'allow',
      isEnabled: true,
    },
    {
      id: 2,
      column: { id: 20 },
      role: { id: 1 },
      action: 'read',
      effect: 'allow',
      isEnabled: true,
    },
  ];
  const find = vi.fn(async (query: any) => ({
    data: query.filter?._or ? rows.slice(0, 1) : rows,
  }));
  const cache = new FieldPermissionCacheBuilder({
    eventEmitter: new EventEmitter2(),
    queryBuilderService: { find } as any,
    metadataCacheService: { getMetadata: async () => metadata } as any,
  });
  await cache.reload(false);
  const untouched = [...cache.getRawCache().values()].find(
    (bucket) => bucket.rules[0].id === 2,
  );
  find.mockClear();
  await cache.partialReload(
    {
      table: 'enfyra_table',
      ids: [1],
      scope: 'partial',
      action: 'reload',
      timestamp: 0,
    },
    false,
  );
  expect(
    [...cache.getRawCache().values()].find(
      (bucket) => bucket.rules[0].id === 2,
    ),
  ).toBe(untouched);
  expect(find.mock.calls[0][0].filter).toEqual({
    _or: [{ column: { id: { _in: ['10'] } } }],
  });
});

it('rebuilds only affected-table column rule buckets and clears removed columns', async () => {
  let metadata = {
    tablesList: [
      { id: 1, name: 'one', columns: [{ id: 10 }] },
      { id: 2, name: 'two', columns: [{ id: 20 }] },
    ],
  };
  const rows = [
    { id: 1, column: { id: 10 }, ruleType: 'required', isEnabled: true },
    { id: 2, column: { id: 20 }, ruleType: 'required', isEnabled: true },
  ];
  const find = vi.fn(async (query: any) => ({
    data: query.filter?.column ? [] : rows,
  }));
  const cache = new ColumnRuleCacheBuilder({
    eventEmitter: new EventEmitter2(),
    queryBuilderService: { find, isMongoDb: () => false } as any,
    metadataCacheService: { getMetadata: async () => metadata } as any,
  });
  await cache.reload(false);
  const untouched = cache.getRawCache().get('20');
  metadata.tablesList[0].columns = [];
  find.mockClear();
  await cache.partialReload(
    {
      table: 'enfyra_table',
      ids: [1],
      scope: 'partial',
      action: 'reload',
      timestamp: 0,
    },
    false,
  );
  expect(cache.getRawCache().has('10')).toBe(false);
  expect(cache.getRawCache().get('20')).toBe(untouched);
});
