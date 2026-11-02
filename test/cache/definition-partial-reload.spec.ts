import { EventEmitter2 } from 'eventemitter2';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { FlowCacheBuilder } from '../../src/engines/cache/services/flow-cache-builder.service';
import { SharedHelperCacheService } from '../../src/engines/cache/services/shared-helper-cache.service';
import { StorageConfigCacheBuilder } from '../../src/engines/cache/services/storage-config-cache-builder.service';
import { FolderTreeCacheService } from '../../src/engines/cache/services/folder-tree-cache.service';
import { GqlDefinitionCacheService } from '../../src/engines/cache/services/gql-definition-cache.service';
import { DatabaseConfigService } from '../../src/shared/services/database-config.service';

const payload = (table: string, ids: number[]) => ({
  table,
  ids,
  scope: 'partial' as const,
  action: 'reload' as const,
  timestamp: 0,
});
afterEach(() => DatabaseConfigService.resetForTesting());

describe('definition partial reload boundaries', () => {
  it.each(['postgres', 'mysql', 'mongodb'] as const)(
    'rebuilds only old/new owning flows for a moved step on %s',
    async (backend) => {
      DatabaseConfigService.overrideForTesting(backend);
      const pk = DatabaseConfigService.getPkField();
      let owner = 1;
      const rows = [1, 2, 3].map((id) => ({
        [pk]: id,
        name: `flow${id}`,
        isEnabled: true,
      }));
      const find = vi.fn(async (query: any) => {
        const ids = query.filter?._and?.find((entry: any) => entry[pk])?.[pk]
          ?._in;
        const flowIds = query.filter?._and?.find((entry: any) => entry.flow)
          ?.flow?.[pk]?._in;
        return {
          data:
            query.table === 'enfyra_flow'
              ? rows.filter(
                  (row) => !ids || ids.map(String).includes(String(row[pk])),
                )
              : query.table === 'enfyra_flow_step'
                ? !flowIds || flowIds.map(String).includes(String(owner))
                  ? [
                      {
                        [pk]: 10,
                        flow: { [pk]: owner },
                        type: 'script',
                        isEnabled: true,
                        sourceCode: 'return 1;',
                      },
                    ]
                  : []
                : [],
        };
      });
      const cache = new FlowCacheBuilder({
        eventEmitter: new EventEmitter2(),
        queryBuilderService: { find } as any,
      });
      await cache.reload(false);
      const untouched = (await cache.getCacheAsync())[2];
      owner = 2;
      find.mockClear();
      await cache.partialReload(payload('enfyra_flow_step', [10]), false);
      const data = await cache.getCacheAsync();
      expect(data.find((flow) => flow.id === 1)?.steps).toEqual([]);
      expect(
        data.find((flow) => flow.id === 2)?.steps.map((step) => step.id),
      ).toEqual([10]);
      expect(data.find((flow) => flow.id === 3)).toBe(untouched);
      expect(
        find.mock.calls.filter(([query]) => query.table === 'enfyra_flow'),
      ).toEqual([
        expect.arrayContaining([
          expect.objectContaining({
            filter: {
              _and: expect.arrayContaining([
                { [pk]: { _in: expect.arrayContaining([1, 2]) } },
              ]),
            },
          }),
        ]),
      ]);
    },
  );

  it('retains untouched helpers and removes a deleted helper', async () => {
    DatabaseConfigService.overrideForTesting('postgres');
    let rows = [
      { id: 1, name: 'first', isEnabled: true, sourceCode: 'return 1;' },
      { id: 2, name: 'second', isEnabled: true, sourceCode: 'return 2;' },
    ];
    const find = vi.fn(async (query: any) => ({
      data: rows.filter(
        (row) =>
          !query.filter?.id ||
          query.filter.id._in.map(String).includes(String(row.id)),
      ),
    }));
    const cache = new SharedHelperCacheService({
      eventEmitter: new EventEmitter2(),
      queryBuilderService: { find } as any,
    });
    await cache.reload(false);
    const second = (await cache.getCacheAsync())[1];
    rows = rows.slice(1);
    find.mockClear();
    await cache.partialReload(payload('enfyra_helper', [1]), false);
    expect(await cache.getCacheAsync()).toEqual([second]);
    expect((await cache.getCacheAsync())[0]).toBe(second);
    expect(find.mock.calls[0][0].filter).toEqual({ id: { _in: [1] } });
  });

  it('removes both SQL storage identity aliases when disabled', async () => {
    DatabaseConfigService.overrideForTesting('postgres');
    let rows = [
      { id: 1, isEnabled: true },
      { id: 2, isEnabled: true },
    ];
    const find = vi.fn(async (query: any) => ({
      data: rows.filter(
        (row) =>
          row.isEnabled &&
          (!query.filter?._and || query.filter._and[1].id._in.includes(row.id)),
      ),
    }));
    const cache = new StorageConfigCacheBuilder({
      queryBuilderService: { find, isMongoDb: () => false } as any,
    });
    await cache.reload(false);
    rows[0].isEnabled = false;
    await cache.partialReload(payload('enfyra_storage_config', [1]), false);
    expect((await cache.getCacheAsync()).has(1)).toBe(false);
    expect((await cache.getCacheAsync()).has('1')).toBe(false);
    expect((await cache.getCacheAsync()).has(2)).toBe(true);
    expect(find.mock.calls.at(-1)?.[0].filter).toEqual({
      _and: [{ isEnabled: { _eq: true } }, { id: { _in: [1] } }],
    });
  });

  it('refreshes a folder parent/order without querying unrelated folders', async () => {
    DatabaseConfigService.overrideForTesting('postgres');
    const rows = [
      { id: '1', name: 'root', parent: null },
      { id: '2', name: 'child', parent: { id: '1' } },
    ];
    const find = vi.fn(async (query: any) => ({
      data: rows.filter(
        (row) =>
          !query.filter?.id || query.filter.id._in.map(String).includes(row.id),
      ),
    }));
    const cache = new FolderTreeCacheService({
      queryBuilderService: { find } as any,
    });
    await cache.reload(false);
    rows[1].parent = null as any;
    find.mockClear();
    await cache.partialReload(payload('enfyra_folder', [2]), false);
    expect((await cache.getCacheAsync()).tree).toHaveLength(2);
    expect(find.mock.calls[0][0].filter).toEqual({ id: { _in: [2] } });
  });

  it('removes a deleted GraphQL permission from its old owner without querying every definition', async () => {
    DatabaseConfigService.overrideForTesting('postgres');
    let deleted = false;
    const find = vi.fn(async (query: any) => ({
      data:
        query.table === 'enfyra_graphql_permission'
          ? []
          : [
              {
                id: 1,
                table: { id: 10, name: 'one' },
                isEnabled: true,
                permissions: deleted
                  ? []
                  : [
                      {
                        id: 7,
                        role: { id: 2 },
                        operations: [{ name: 'QUERY' }],
                        allowedUsers: [],
                      },
                    ],
              },
            ],
    }));
    const cache = new GqlDefinitionCacheService({
      eventEmitter: new EventEmitter2(),
      queryBuilderService: { find } as any,
    });
    await cache.reload(false);
    deleted = true;
    find.mockClear();
    await cache.partialReload(payload('enfyra_graphql_permission', [7]), false);
    expect((await cache.getCacheAsync()).get('one')?.permissions).toEqual([]);
    expect(
      find.mock.calls.find(([query]) => query.table === 'enfyra_graphql')?.[0]
        .filter,
    ).toEqual({ id: { _in: ['1'] } });
  });

  it('replaces a changed GraphQL definition without rebuilding other definitions', async () => {
    DatabaseConfigService.overrideForTesting('postgres');
    const rows = [
      { id: 1, table: { id: 10, name: 'one' }, isEnabled: true },
      { id: 2, table: { id: 20, name: 'two' }, isEnabled: true },
    ];
    const find = vi.fn(async (query: any) => ({
      data: rows.filter(
        (row) =>
          !query.filter?.id ||
          query.filter.id._in.map(String).includes(String(row.id)),
      ),
    }));
    const cache = new GqlDefinitionCacheService({
      eventEmitter: new EventEmitter2(),
      queryBuilderService: { find } as any,
    });
    await cache.reload(false);
    const two = (await cache.getCacheAsync()).get('two');
    find.mockClear();
    await cache.partialReload(payload('enfyra_graphql', [1]), false);
    expect((await cache.getCacheAsync()).get('two')).toBe(two);
    expect(find.mock.calls[0][0].filter).toEqual({ id: { _in: [1] } });
  });
});
