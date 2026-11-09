import { EventEmitter2 } from 'eventemitter2';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { RouteCacheService } from '../../src/engines/cache/services/route-cache.service';
import { DatabaseConfigService } from '../../src/shared/services/database-config.service';

afterEach(() => DatabaseConfigService.resetForTesting());

describe('partial route parent ownership', () => {
  it.each([
    'enfyra_route_method_config',
    'enfyra_route_method_config_file_field',
    'enfyra_pre_hook',
    'enfyra_post_hook',
    'enfyra_route_permission',
  ])(
    'removes a deleted %s from its cached parent without full reload',
    async (table) => {
      DatabaseConfigService.overrideForTesting('postgres');
      const hook = {
        id: 8,
        isEnabled: true,
        isGlobal: false,
        methods: [{ id: 1 }],
        sourceCode: 'return 1;',
        scriptLanguage: 'javascript',
      };
      const routes = [
        {
          id: 1,
          path: '/one',
          preHooks: [hook],
          postHooks: [hook],
          routePermissions: [{ id: 8 }],
          methodConfigs: [
            {
              id: 8,
              method: { id: 1 },
              available: true,
              preHooks: [hook],
              postHooks: [hook],
              fileFields: [{ id: 8 }],
              routePermissions: [{ id: 8 }],
            },
          ],
        },
        {
          id: 2,
          path: '/two',
          preHooks: [],
          postHooks: [],
          methodConfigs: [
            {
              id: 9,
              available: true,
              method: { id: 1 },
              preHooks: [],
              postHooks: [],
            },
          ],
        },
      ];
      let deleted = false;
      const find = vi.fn(async (query: any) => ({
        data:
          query.table === 'enfyra_method'
            ? [{ id: 1, name: 'GET' }]
            : query.table === 'enfyra_route'
              ? structuredClone(
                  deleted
                    ? [
                        {
                          ...routes[0],
                          preHooks: [],
                          postHooks: [],
                          routePermissions: [],
                          methodConfigs:
                            table === 'enfyra_route_method_config'
                              ? []
                              : [
                                  {
                                    ...routes[0].methodConfigs[0],
                                    preHooks: [],
                                    postHooks: [],
                                    fileFields: [],
                                    routePermissions: [],
                                  },
                                ],
                        },
                      ]
                    : routes,
                )
              : [],
      }));
      const cache = new RouteCacheService({
        eventEmitter: new EventEmitter2(),
        queryBuilderService: { find, isMongoDb: () => false } as any,
        metadataCacheService: {
          getMetadata: async () => ({ tablesList: [] }),
        } as any,
      });
      await cache.reload(false);
      deleted = true;
      find.mockClear();
      const full = vi.spyOn(cache, 'reload');
      await cache.partialReload(
        { table, ids: [8], action: 'delete', scope: 'partial', timestamp: 0 },
        false,
      );
      expect(full).not.toHaveBeenCalled();
      expect(
        find.mock.calls.filter(([query]) => query.table === 'enfyra_route'),
      ).toHaveLength(1);
      expect((await cache.getRoutes()).map((route) => route.id)).toEqual([
        1, 2,
      ]);
      expect(
        find.mock.calls.find(([query]) => query.table === 'enfyra_route')?.[0]
          .filter._and[1].id._in,
      ).toEqual([1]);
    },
  );

  it('resolves a new hook parent through canonical method configurations', async () => {
    DatabaseConfigService.overrideForTesting('postgres');
    let partial = false;
    const find = vi.fn(async (query: any) => ({
      data:
        query.table === 'enfyra_method'
          ? [{ id: 1, name: 'GET' }]
          : query.table === 'enfyra_pre_hook' && query.filter.id
            ? [
                {
                  id: 10,
                  isGlobal: false,
                  route: null,
                  routeMethodConfigs: [{ route: { id: 2 } }],
                },
              ]
            : query.table === 'enfyra_route'
              ? [
                  {
                    id: 2,
                    path: '/canonical',
                    preHooks: [],
                    postHooks: [],
                    methodConfigs: [
                      {
                        id: 3,
                        method: { id: 1 },
                        available: true,
                        preHooks: partial
                          ? [
                              {
                                id: 10,
                                isGlobal: false,
                                isEnabled: true,
                                methods: [],
                                sourceCode: 'return 2;',
                                scriptLanguage: 'javascript',
                              },
                            ]
                          : [],
                        postHooks: [],
                      },
                    ],
                  },
                ]
              : [],
    }));
    const cache = new RouteCacheService({
      eventEmitter: new EventEmitter2(),
      queryBuilderService: { find, isMongoDb: () => false } as any,
      metadataCacheService: {
        getMetadata: async () => ({ tablesList: [] }),
      } as any,
    });
    await cache.reload(false);
    partial = true;
    await cache.partialReload(
      {
        table: 'enfyra_pre_hook',
        ids: [10],
        action: 'reload',
        scope: 'partial',
        timestamp: 0,
      },
      false,
    );
    expect(
      (await cache.getRoutes())[0].methodConfigs[0].preHooks.map(
        (hook: any) => hook.id,
      ),
    ).toEqual([10]);
  });
});
