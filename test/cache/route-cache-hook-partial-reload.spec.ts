describe('RouteCacheService — partial reload for hooks', () => {
  let hookRecords: Record<string, any[]>;
  let reloadGlobalHooksAndMergeCalls: number;
  let reloadSpecificRoutesCalls: Array<(string | number)[]>;

  const queryBuilder = {
    isMongoDb: () => false,
    find: jest.fn(async ({ table, filter }: any) => {
      const records = hookRecords[table] || [];
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
      const rid = record.route?.id || record.routeId;
      if (rid) routeIds.add(rid);
    }
    return [...routeIds];
  }

  async function applyPartialUpdate(payload: any) {
    if (
      ['pre_hook_definition', 'post_hook_definition'].includes(payload.table)
    ) {
      reloadGlobalHooksAndMergeCalls++;
      if (payload.ids?.length) {
        const routeIds = await findRouteIdsForChildRecords(
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
    hookRecords = {};
    reloadGlobalHooksAndMergeCalls = 0;
    reloadSpecificRoutesCalls = [];
    queryBuilder.find.mockClear();
  });

  it('add global hook (no route relation): only reloads global hooks, no specific route reload', async () => {
    hookRecords.pre_hook_definition = [
      { id: 1, isGlobal: true, route: null },
    ];

    await applyPartialUpdate({
      table: 'pre_hook_definition',
      scope: 'partial',
      ids: [1],
    });

    expect(reloadGlobalHooksAndMergeCalls).toBe(1);
    expect(reloadSpecificRoutesCalls).toEqual([]);
  });

  it('add route-specific pre-hook: reloads global hooks AND the affected route', async () => {
    hookRecords.pre_hook_definition = [
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

  it('add route-specific post-hook: same flow as pre-hook', async () => {
    hookRecords.post_hook_definition = [
      { id: 99, isGlobal: false, route: { id: 3 } },
    ];

    await applyPartialUpdate({
      table: 'post_hook_definition',
      scope: 'partial',
      ids: [99],
    });

    expect(reloadGlobalHooksAndMergeCalls).toBe(1);
    expect(reloadSpecificRoutesCalls).toEqual([[3]]);
  });

  it('multiple hooks on different routes: dedupes to distinct route IDs', async () => {
    hookRecords.pre_hook_definition = [
      { id: 1, isGlobal: false, route: { id: 10 } },
      { id: 2, isGlobal: false, route: { id: 20 } },
      { id: 3, isGlobal: false, route: { id: 10 } },
    ];

    await applyPartialUpdate({
      table: 'pre_hook_definition',
      scope: 'partial',
      ids: [1, 2, 3],
    });

    expect(reloadGlobalHooksAndMergeCalls).toBe(1);
    expect(reloadSpecificRoutesCalls).toHaveLength(1);
    const [rid] = reloadSpecificRoutesCalls;
    expect([...rid].sort()).toEqual([10, 20]);
  });

  it('deleted hook (record no longer in DB): no specific route reload, global-only refresh', async () => {
    hookRecords.pre_hook_definition = [];

    await applyPartialUpdate({
      table: 'pre_hook_definition',
      scope: 'partial',
      ids: [500],
    });

    expect(reloadGlobalHooksAndMergeCalls).toBe(1);
    expect(reloadSpecificRoutesCalls).toEqual([]);
  });

  it('mix of global + route-specific hooks in same payload: both reloads happen, only route-scoped IDs returned', async () => {
    hookRecords.post_hook_definition = [
      { id: 1, isGlobal: true, route: null },
      { id: 2, isGlobal: false, route: { id: 55 } },
    ];

    await applyPartialUpdate({
      table: 'post_hook_definition',
      scope: 'partial',
      ids: [1, 2],
    });

    expect(reloadGlobalHooksAndMergeCalls).toBe(1);
    expect(reloadSpecificRoutesCalls).toEqual([[55]]);
  });

  it('full-scope reload (no ids): only global hooks refreshed, no route lookup', async () => {
    await applyPartialUpdate({
      table: 'pre_hook_definition',
      scope: 'full',
    });

    expect(reloadGlobalHooksAndMergeCalls).toBe(1);
    expect(reloadSpecificRoutesCalls).toEqual([]);
    expect(queryBuilder.find).not.toHaveBeenCalled();
  });
});
