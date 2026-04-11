describe('RouteCacheService — partial reload for table_definition', () => {
  let cache: { routes: any[] };
  let dbRoutes: any[];
  let reloadSpecificRoutesCalled: { ids: any[] } | null;
  let fullReloadCalled: boolean;
  let engineRebuilt: boolean;

  const queryBuilder = {
    isMongoDb: () => false,
    select: jest.fn(async ({ tableName, filter }: any) => {
      if (tableName === 'route_definition' && filter?.mainTableId) {
        const ids = filter.mainTableId._in || [];
        const matched = dbRoutes.filter((r) =>
          ids.some((id: any) => String(id) === String(r.mainTableId)),
        );
        return { data: matched };
      }
      return { data: [] };
    }),
  };

  async function applyPartialUpdate(payload: any) {
    const affectedTableNames = new Set<string>(payload.affectedTables || []);

    if (payload.tableName === 'table_definition' && payload.ids?.length) {
      const result = await queryBuilder.select({
        tableName: 'route_definition',
        filter: { mainTableId: { _in: payload.ids } },
        fields: ['id'],
      });
      const routeIds = result.data.map((r: any) => r.id).filter(Boolean);
      if (routeIds.length > 0) {
        reloadSpecificRoutesCalled = { ids: routeIds };
        return;
      }
      for (const route of cache.routes) {
        if (payload.ids.some((id: any) => String(id) === String(route.mainTable?.id))) {
          affectedTableNames.add(route.mainTable?.name);
        }
      }
      if (affectedTableNames.size > 0) {
        cache.routes = cache.routes.filter(
          (r: any) => !affectedTableNames.has(r.mainTable?.name),
        );
        engineRebuilt = true;
        return;
      }
      return;
    }

    fullReloadCalled = true;
  }

  beforeEach(() => {
    cache = { routes: [] };
    dbRoutes = [];
    reloadSpecificRoutesCalled = null;
    fullReloadCalled = false;
    engineRebuilt = false;
    queryBuilder.select.mockClear();
  });

  it('create table: query DB for new route → reloadSpecificRoutes (no full reload)', async () => {
    dbRoutes = [{ id: 99, mainTableId: 50 }];

    await applyPartialUpdate({
      tableName: 'table_definition',
      scope: 'partial',
      ids: [50],
    });

    expect(reloadSpecificRoutesCalled).toEqual({ ids: [99] });
    expect(fullReloadCalled).toBe(false);
  });

  it('delete table: no routes in DB → remove stale from cache', async () => {
    cache.routes = [
      { id: 10, mainTable: { id: 50, name: 'deleted_table' }, path: '/deleted_table' },
      { id: 11, mainTable: { id: 60, name: 'other_table' }, path: '/other_table' },
    ];
    dbRoutes = [];

    await applyPartialUpdate({
      tableName: 'table_definition',
      scope: 'partial',
      ids: [50],
    });

    expect(reloadSpecificRoutesCalled).toBeNull();
    expect(cache.routes).toHaveLength(1);
    expect(cache.routes[0].path).toBe('/other_table');
    expect(engineRebuilt).toBe(true);
    expect(fullReloadCalled).toBe(false);
  });

  it('update table: existing route found in DB → reloadSpecificRoutes', async () => {
    cache.routes = [
      { id: 10, mainTable: { id: 50, name: 'my_table' }, path: '/my_table' },
    ];
    dbRoutes = [{ id: 10, mainTableId: 50 }];

    await applyPartialUpdate({
      tableName: 'table_definition',
      scope: 'partial',
      ids: [50],
    });

    expect(reloadSpecificRoutesCalled).toEqual({ ids: [10] });
    expect(fullReloadCalled).toBe(false);
  });

  it('table with no route at all: no fallback to full reload', async () => {
    cache.routes = [];
    dbRoutes = [];

    await applyPartialUpdate({
      tableName: 'table_definition',
      scope: 'partial',
      ids: [999],
    });

    expect(reloadSpecificRoutesCalled).toBeNull();
    expect(fullReloadCalled).toBe(false);
    expect(engineRebuilt).toBe(false);
  });

  it('non-table_definition payload: falls through to other handlers', async () => {
    await applyPartialUpdate({
      tableName: 'setting_definition',
      scope: 'partial',
      ids: [1],
    });

    expect(fullReloadCalled).toBe(true);
    expect(reloadSpecificRoutesCalled).toBeNull();
  });
});
