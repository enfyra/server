import { EventEmitter2 } from 'eventemitter2';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { RouteCacheService } from '../../src/engines/cache/services/route-cache.service';
import { CacheOrchestratorService } from '../../src/engines/cache/services/cache-orchestrator.service';
import { DatabaseConfigService } from '../../src/shared/services/database-config.service';

function fixture() {
  DatabaseConfigService.overrideForTesting('postgres');
  const handler = (id: number) => ({
    id,
    sourceCode: `return ${id};`,
    scriptLanguage: 'javascript',
    method: { id: 1 },
  });
  const route = (id: number) => ({
    id,
    path: `/items/${id}`,
    preHooks: [
      {
        id: 100 + id,
        isGlobal: false,
        isEnabled: true,
        sourceCode: 'return 1;',
        scriptLanguage: 'javascript',
        methods: [{ id: 1 }],
      },
    ],
    postHooks: [],
    handlers: [handler(id)],
    methodConfigs: [
      {
        id,
        available: true,
        method: { id: 1 },
        handler: handler(id),
        preHooks: [{ id: 100 + id }],
        postHooks: [],
      },
    ],
  });
  const find = vi.fn(async ({ table, filter }: any) => ({
    data:
      table === 'enfyra_method'
        ? [{ id: 1, name: 'GET' }]
        : table === 'enfyra_route'
          ? filter?._and
            ? [route(1)]
            : [route(1), route(2), route(3)]
          : table === 'enfyra_pre_hook' && filter?.id
            ? [{ id: 101, isGlobal: false, route: { id: 1 } }]
            : [],
  }));
  const cache = new RouteCacheService({
    eventEmitter: new EventEmitter2(),
    metadataCacheService: {
      getMetadata: async () => ({ tablesList: [] }),
    } as any,
    queryBuilderService: { find, isMongoDb: () => false } as any,
  });
  return { cache, find };
}

afterEach(() => DatabaseConfigService.resetForTesting());

describe('reload work bounds', () => {
  it('compiles each canonical handler once during a full build', async () => {
    const { cache } = fixture();
    const compile = vi.spyOn(cache as any, 'resolveScriptCode');
    await cache.reload(false);
    expect(
      compile.mock.calls.filter(([table]) => table === 'enfyra_route_handler'),
    ).toHaveLength(3);
  });

  it('does not compile unrelated handlers or reload global hooks for a local hook', async () => {
    const { cache, find } = fixture();
    await cache.reload(false);
    find.mockClear();
    const compile = vi.spyOn(cache as any, 'resolveScriptCode');
    const build = vi.spyOn(cache as any, 'buildRouteEngine');
    await cache.partialReload(
      {
        table: 'enfyra_pre_hook',
        scope: 'partial',
        action: 'reload',
        ids: [101],
        timestamp: 0,
      },
      false,
    );
    expect(
      compile.mock.calls.filter(([table]) => table === 'enfyra_route_handler'),
    ).toHaveLength(1);
    expect(
      find.mock.calls.filter(
        ([query]) =>
          ['enfyra_pre_hook', 'enfyra_post_hook'].includes(query.table) &&
          !query.filter?.id,
      ),
    ).toHaveLength(0);
    expect(build).toHaveBeenCalledTimes(1);
    expect((await cache.getRoutes()).map((route) => route.id)).toEqual([
      1, 2, 3,
    ]);
  });

  it('runs the GraphQL definition builder once per mutation chain', async () => {
    const builder = () => ({
      reload: vi.fn(async () => {}),
      isLoaded: () => true,
    });
    const gql = builder();
    const orchestrator = new CacheOrchestratorService({
      eventEmitter: new EventEmitter2(),
      instanceService: { getInstanceId: () => 'perf-test' },
      redisPubSubService: { publish: async () => {} },
      metadataCacheService: builder(),
      routeCacheService: { ...builder(), supportsPartialReload: () => false },
      guardCacheBuilder: builder(),
      fieldPermissionCacheBuilder: builder(),
      columnRuleCacheBuilder: builder(),
      gqlDefinitionCacheService: gql,
      repoRegistryService: {
        stageFromMetadata: async () => new Map(),
        activateAliases: () => {},
      },
      graphqlService: { reloadSchema: async () => {} },
    } as any);
    try {
      await (orchestrator as any).executeChain(
        {
          table: 'enfyra_table',
          action: 'reload',
          scope: 'full',
          timestamp: 0,
        },
        true,
      );
      expect(gql.reload).toHaveBeenCalledTimes(1);
    } finally {
      orchestrator.onDestroy();
    }
  });
});
