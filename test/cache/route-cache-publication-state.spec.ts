import { EventEmitter2 } from 'eventemitter2';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { RouteCacheService } from '../../src/engines/cache/services/route-cache.service';
import { DatabaseConfigService } from '../../src/shared/services';

function fixture(shared = false) {
  DatabaseConfigService.overrideForTesting('postgres');
  let preHooks: any[] = [
    {
      id: 10,
      isGlobal: true,
      isEnabled: true,
      methods: [{ id: 1 }],
      sourceCode: 'return "old";',
      scriptLanguage: 'javascript',
    },
  ];
  let postHooks: any[] = [{ ...preHooks[0], id: 11 }];
  const route = () => ({
    id: 1,
    path: '/items',
    preHooks: [],
    postHooks: [],
    methodConfigs: [
      {
        id: 1,
        method: { id: 1 },
        available: true,
        preHooks: [],
        postHooks: [],
      },
    ],
  });
  let committed: any;
  let staged: any;
  let publishing = false;
  const store = {
    isEnabled: () => shared,
    runWithPublication: async (work: () => Promise<any>) => {
      if (publishing) return work();
      publishing = true;
      staged = undefined;
      try {
        const result = await work();
        if (staged) committed = structuredClone(staged);
        return result;
      } finally {
        publishing = false;
        staged = undefined;
      }
    },
    acquireRefreshLockWithWait: async () => 'lease',
    releaseRefreshLock: async () => {},
    setSnapshot: async (_id: string, data: any) => {
      staged = { data: structuredClone(data) };
    },
    getSnapshot: async () => structuredClone(staged ?? committed ?? null),
    waitForSnapshot: async () => structuredClone(committed),
    deleteAuxByPrefix: async () => {},
    setAux: async () => {},
  };
  const find = vi.fn(async ({ table }: any) => ({
    data: structuredClone(
      table === 'enfyra_method'
        ? [{ id: 1, name: 'GET' }]
        : table === 'enfyra_route'
          ? [route()]
          : table === 'enfyra_pre_hook'
            ? preHooks
            : table === 'enfyra_post_hook'
              ? postHooks
              : [],
    ),
  }));
  const build = () =>
    new RouteCacheService({
      eventEmitter: new EventEmitter2(),
      metadataCacheService: { getMetadata: async () => ({ tablesList: [] }) },
      queryBuilderService: { isMongoDb: () => false, find },
      redisRuntimeCacheStore: store,
    } as any);
  return {
    build,
    store,
    find,
    setHooks: (pre: any[], post: any[]) => {
      preHooks = pre;
      postHooks = post;
    },
  };
}

afterEach(() => DatabaseConfigService.resetForTesting());

describe('route candidate build inputs', () => {
  it.each([
    { shared: false, table: 'enfyra_pre_hook', field: 'preHooks', id: 10 },
    { shared: true, table: 'enfyra_pre_hook', field: 'preHooks', id: 10 },
    { shared: false, table: 'enfyra_post_hook', field: 'postHooks', id: 11 },
    { shared: true, table: 'enfyra_post_hook', field: 'postHooks', id: 11 },
  ])(
    'removes deleted global $field from method arrays (shared=$shared)',
    async ({ shared, table, field, id }) => {
      const { build, setHooks } = fixture(shared);
      const cache = build();
      await cache.reload(false);
      const initial = await cache.getCacheAsync();
      expect(initial.routes[0].methodConfigs[0][field]).toHaveLength(1);
      setHooks(
        table === 'enfyra_pre_hook' ? [] : initial.globalPreHooks,
        table === 'enfyra_post_hook' ? [] : initial.globalPostHooks,
      );
      await cache.partialReload(
        {
          table,
          scope: 'partial',
          action: 'reload',
          ids: [id],
          timestamp: Date.now(),
        },
        false,
      );
      const data = await cache.getCacheAsync();
      expect(data.routes[0][field]).toEqual([]);
      expect(data.routes[0].methodConfigs[0][field]).toEqual([]);
      const untouched = field === 'preHooks' ? 'postHooks' : 'preHooks';
      expect(data.routes[0].methodConfigs[0][untouched]).toHaveLength(1);
    },
  );

  it('uses the replayed global hooks when another replica partially rebuilds a route', async () => {
    const { build, setHooks } = fixture(true);
    const writer = build();
    const receiver = build();
    await writer.reload(false);
    await receiver.reload(false);
    setHooks(
      [
        {
          id: 20,
          isGlobal: true,
          isEnabled: true,
          methods: [{ id: 1 }],
          sourceCode: 'return "new";',
          scriptLanguage: 'javascript',
        },
      ],
      [],
    );
    await writer.reload(false);
    await receiver.syncFromSharedCache();
    await receiver.partialReload(
      {
        table: 'enfyra_route',
        scope: 'partial',
        action: 'reload',
        ids: [1],
        timestamp: Date.now(),
      },
      false,
    );
    expect(
      (await receiver.getRoutes())[0].methodConfigs[0].preHooks.map(
        (hook: any) => hook.id,
      ),
    ).toEqual([20]);
  });

  it('does not reuse global hooks from an aborted full candidate in a later partial rebuild', async () => {
    const { build, setHooks, store } = fixture(true);
    const cache = build();
    await cache.reload(false);
    setHooks(
      [
        {
          id: 20,
          isGlobal: true,
          isEnabled: true,
          methods: [{ id: 1 }],
          sourceCode: 'return "candidate";',
          scriptLanguage: 'javascript',
        },
      ],
      [],
    );
    await expect(
      store.runWithPublication(async () => {
        await cache.reload(false);
        throw new Error('sibling failed');
      }),
    ).rejects.toThrow('sibling failed');
    await cache.partialReload(
      {
        table: 'enfyra_route',
        scope: 'partial',
        action: 'reload',
        ids: [1],
        timestamp: Date.now(),
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
