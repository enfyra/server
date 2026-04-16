describe('RouteCacheService — partial reload for hooks/handlers/permissions', () => {
  const CHILD_ARRAY_KEY: Record<string, string> = {
    pre_hook_definition: 'preHooks',
    post_hook_definition: 'postHooks',
    route_handler_definition: 'handlers',
    route_permission_definition: 'routePermissions',
  };

  let cacheRoutes: any[];
  let dbRecords: Record<string, any[]>;
  let reloadGlobalHooksAndMergeCalls: number;
  let reloadSpecificRoutesCalls: Array<(string | number)[]>;

  const queryBuilder = {
    isMongoDb: () => false,
    find: jest.fn(async ({ table, filter }: any) => {
      const records = dbRecords[table] || [];
      const ids = filter?.id?._in || [];
      const matched = records.filter((r) =>
        ids.some((id: any) => String(id) === String(r.id)),
      );
      return { data: matched };
    }),
  };

  async function findRouteIdsForChildRecords(
    tableName: string,
    ids: (string | number)[],
  ): Promise<(string | number)[]> {
    const result = await queryBuilder.find({
      table: tableName,
      filter: { id: { _in: ids } },
      fields: ['route.*'],
    });
    const routeIds = new Set<string | number>();
    for (const record of result.data) {
      const rid = record.route?.id ?? record.routeId;
      if (rid != null) routeIds.add(rid);
    }
    return [...routeIds];
  }

  function findCachedRouteIdsForChildRecords(
    tableName: string,
    ids: (string | number)[],
  ): (string | number)[] {
    const arrayKey = CHILD_ARRAY_KEY[tableName];
    if (!arrayKey) return [];
    const idSet = new Set(ids.map(String));
    const out = new Map<string, string | number>();
    for (const route of cacheRoutes) {
      const children = route?.[arrayKey];
      if (!Array.isArray(children)) continue;
      for (const child of children) {
        if (child?.id == null) continue;
        if (idSet.has(String(child.id))) {
          out.set(String(route.id), route.id);
          break;
        }
      }
    }
    return [...out.values()];
  }

  async function resolveAffectedRouteIds(
    tableName: string,
    ids: (string | number)[],
  ): Promise<(string | number)[]> {
    const [fresh, cached] = await Promise.all([
      findRouteIdsForChildRecords(tableName, ids),
      Promise.resolve(findCachedRouteIdsForChildRecords(tableName, ids)),
    ]);
    const merged = new Map<string, string | number>();
    for (const rid of [...fresh, ...cached]) {
      if (rid != null) merged.set(String(rid), rid);
    }
    return [...merged.values()];
  }

  async function applyPartialUpdate(payload: any) {
    if (
      ['route_handler_definition', 'route_permission_definition'].includes(
        payload.table,
      ) &&
      payload.ids?.length
    ) {
      const routeIds = await resolveAffectedRouteIds(payload.table, payload.ids);
      if (routeIds.length > 0) {
        reloadSpecificRoutesCalls.push(routeIds);
        return;
      }
    }

    if (
      ['pre_hook_definition', 'post_hook_definition'].includes(payload.table)
    ) {
      reloadGlobalHooksAndMergeCalls++;
      if (payload.ids?.length) {
        const routeIds = await resolveAffectedRouteIds(
          payload.table,
          payload.ids,
        );
        if (routeIds.length > 0) {
          reloadSpecificRoutesCalls.push(routeIds);
        }
      }
      return;
    }
  }

  beforeEach(() => {
    cacheRoutes = [];
    dbRecords = {};
    reloadGlobalHooksAndMergeCalls = 0;
    reloadSpecificRoutesCalls = [];
    queryBuilder.find.mockClear();
  });

  describe('hooks', () => {
    it('add global hook (no route): only reloads global hooks, no specific route reload', async () => {
      dbRecords.pre_hook_definition = [{ id: 1, isGlobal: true, route: null }];

      await applyPartialUpdate({
        table: 'pre_hook_definition',
        scope: 'partial',
        ids: [1],
      });

      expect(reloadGlobalHooksAndMergeCalls).toBe(1);
      expect(reloadSpecificRoutesCalls).toEqual([]);
    });

    it('add route-specific pre-hook: reloads global hooks AND the affected route', async () => {
      dbRecords.pre_hook_definition = [
        { id: 42, isGlobal: false, route: { id: 7 } },
      ];

      await applyPartialUpdate({
        table: 'pre_hook_definition',
        scope: 'partial',
        ids: [42],
      });

      expect(reloadGlobalHooksAndMergeCalls).toBe(1);
      expect(reloadSpecificRoutesCalls).toEqual([[7]]);
    });

    it('delete route-specific hook: reloads the old route (scanned from cache)', async () => {
      cacheRoutes = [
        { id: 5, preHooks: [{ id: 42 }] },
        { id: 6, preHooks: [] },
      ];
      dbRecords.pre_hook_definition = [];

      await applyPartialUpdate({
        table: 'pre_hook_definition',
        scope: 'partial',
        ids: [42],
      });

      expect(reloadGlobalHooksAndMergeCalls).toBe(1);
      expect(reloadSpecificRoutesCalls).toEqual([[5]]);
    });

    it('reparent hook (A → B): reloads BOTH old route A and new route B', async () => {
      cacheRoutes = [
        { id: 10, preHooks: [{ id: 99 }] },
        { id: 20, preHooks: [] },
      ];
      dbRecords.pre_hook_definition = [
        { id: 99, isGlobal: false, route: { id: 20 } },
      ];

      await applyPartialUpdate({
        table: 'pre_hook_definition',
        scope: 'partial',
        ids: [99],
      });

      expect(reloadGlobalHooksAndMergeCalls).toBe(1);
      expect(reloadSpecificRoutesCalls).toHaveLength(1);
      expect(
        [...reloadSpecificRoutesCalls[0]].sort((a, b) => Number(a) - Number(b)),
      ).toEqual([10, 20]);
    });

    it('update in place (same route): dedupes to single route ID', async () => {
      cacheRoutes = [{ id: 7, preHooks: [{ id: 42 }] }];
      dbRecords.pre_hook_definition = [
        { id: 42, isGlobal: false, route: { id: 7 } },
      ];

      await applyPartialUpdate({
        table: 'pre_hook_definition',
        scope: 'partial',
        ids: [42],
      });

      expect(reloadSpecificRoutesCalls).toEqual([[7]]);
    });

    it('post-hook flow mirrors pre-hook', async () => {
      cacheRoutes = [{ id: 3, postHooks: [{ id: 11 }] }];
      dbRecords.post_hook_definition = [];

      await applyPartialUpdate({
        table: 'post_hook_definition',
        scope: 'partial',
        ids: [11],
      });

      expect(reloadGlobalHooksAndMergeCalls).toBe(1);
      expect(reloadSpecificRoutesCalls).toEqual([[3]]);
    });

    it('full-scope (no ids): only global hooks refreshed, no route lookup', async () => {
      await applyPartialUpdate({ table: 'pre_hook_definition', scope: 'full' });

      expect(reloadGlobalHooksAndMergeCalls).toBe(1);
      expect(reloadSpecificRoutesCalls).toEqual([]);
      expect(queryBuilder.find).not.toHaveBeenCalled();
    });
  });

  describe('route_handler_definition', () => {
    it('delete handler: reloads old route from cache scan', async () => {
      cacheRoutes = [{ id: 1, handlers: [{ id: 77 }] }];
      dbRecords.route_handler_definition = [];

      await applyPartialUpdate({
        table: 'route_handler_definition',
        scope: 'partial',
        ids: [77],
      });

      expect(reloadSpecificRoutesCalls).toEqual([[1]]);
    });

    it('reparent handler (A → B): reloads both', async () => {
      cacheRoutes = [
        { id: 1, handlers: [{ id: 77 }] },
        { id: 2, handlers: [] },
      ];
      dbRecords.route_handler_definition = [{ id: 77, route: { id: 2 } }];

      await applyPartialUpdate({
        table: 'route_handler_definition',
        scope: 'partial',
        ids: [77],
      });

      expect(reloadSpecificRoutesCalls).toHaveLength(1);
      expect(
        [...reloadSpecificRoutesCalls[0]].sort((a, b) => Number(a) - Number(b)),
      ).toEqual([1, 2]);
    });
  });

  describe('route_permission_definition', () => {
    it('delete permission: reloads old route from cache scan', async () => {
      cacheRoutes = [{ id: 9, routePermissions: [{ id: 500 }] }];
      dbRecords.route_permission_definition = [];

      await applyPartialUpdate({
        table: 'route_permission_definition',
        scope: 'partial',
        ids: [500],
      });

      expect(reloadSpecificRoutesCalls).toEqual([[9]]);
    });

    it('reparent permission (A → B): reloads both', async () => {
      cacheRoutes = [
        { id: 9, routePermissions: [{ id: 500 }] },
        { id: 10, routePermissions: [] },
      ];
      dbRecords.route_permission_definition = [
        { id: 500, route: { id: 10 } },
      ];

      await applyPartialUpdate({
        table: 'route_permission_definition',
        scope: 'partial',
        ids: [500],
      });

      expect(reloadSpecificRoutesCalls).toHaveLength(1);
      expect(
        [...reloadSpecificRoutesCalls[0]].sort((a, b) => Number(a) - Number(b)),
      ).toEqual([9, 10]);
    });
  });
});
