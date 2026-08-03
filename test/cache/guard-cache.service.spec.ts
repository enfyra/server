import { EventEmitter2 } from 'eventemitter2';
import { Logger } from '../../src/shared/logger';
import { GuardCacheBuilder } from '../../src/engines/cache';
import { RuntimeRegistryService } from '../../src/engines/cache/services/runtime-registry.service';
import { CACHE_IDENTIFIERS } from '../../src/shared/utils/cache-events.constants';

async function loadGuardCache(
  guards: any[],
  rules: any[],
): Promise<{ svc: GuardCacheBuilder; registry: RuntimeRegistryService }> {
  const find = jest.fn(async (params: any) => {
    if (params.table === 'enfyra_guard') return { data: guards };
    if (params.table === 'enfyra_guard_rule') return { data: rules };
    return { data: [] };
  });
  const qb = { find, isMongoDb: () => false };
  const ee = new EventEmitter2();
  const registry = new RuntimeRegistryService();
  const svc = new GuardCacheBuilder({
    queryBuilderService: qb as any,
    eventEmitter: ee,
  });
  await svc.reload(false);
  await registry.publishFromCache(CACHE_IDENTIFIERS.GUARD, svc);
  return { svc, registry };
}

describe('GuardCacheBuilder — tree building', () => {
  it('should build flat guard with rules', async () => {
    const { svc } = await loadGuardCache(
      [
        {
          id: 1,
          name: 'ip-guard',
          position: 'pre_auth',
          combinator: 'and',
          isEnabled: true,
          isGlobal: true,
          priority: 0,
          parent: null,
          route: null,
          methods: [],
        },
      ],
      [
        {
          id: 10,
          type: 'rate_limit_by_ip',
          config: { maxRequests: 100, perSeconds: 60 },
          priority: 0,
          isEnabled: true,
          guard: { id: 1 },
          users: [],
        },
      ],
    );
    const cache = svc.getRawCache();
    expect(cache.preAuthGlobal).toHaveLength(1);
    expect(cache.preAuthGlobal[0].rules).toHaveLength(1);
    expect(cache.preAuthGlobal[0].rules[0].type).toBe('rate_limit_by_ip');
  });

  it('should build nested guard tree', async () => {
    const { svc } = await loadGuardCache(
      [
        {
          id: 1,
          name: 'root',
          position: 'post_auth',
          combinator: 'or',
          isEnabled: true,
          isGlobal: true,
          priority: 0,
          parent: null,
          route: null,
          methods: [],
        },
        {
          id: 2,
          name: 'child-a',
          combinator: 'and',
          isEnabled: true,
          priority: 0,
          parent: { id: 1 },
          route: null,
          methods: [],
        },
        {
          id: 3,
          name: 'child-b',
          combinator: 'and',
          isEnabled: true,
          priority: 1,
          parent: { id: 1 },
          route: null,
          methods: [],
        },
      ],
      [
        {
          id: 10,
          type: 'rate_limit_by_ip',
          config: { maxRequests: 100, perSeconds: 60 },
          priority: 0,
          isEnabled: true,
          guard: { id: 2 },
          users: [],
        },
        {
          id: 20,
          type: 'ip_whitelist',
          config: { ips: ['10.0.0.0/8'] },
          priority: 0,
          isEnabled: true,
          guard: { id: 3 },
          users: [],
        },
      ],
    );
    const cache = svc.getRawCache();
    expect(cache.postAuthGlobal).toHaveLength(1);
    const root = cache.postAuthGlobal[0];
    expect(root.children).toHaveLength(2);
    expect(root.children[0].name).toBe('child-a');
    expect(root.children[0].rules[0].type).toBe('rate_limit_by_ip');
    expect(root.children[1].name).toBe('child-b');
    expect(root.children[1].rules[0].type).toBe('ip_whitelist');
  });

  it('should group by route path', async () => {
    const { svc } = await loadGuardCache(
      [
        {
          id: 1,
          name: 'posts-guard',
          position: 'pre_auth',
          combinator: 'and',
          isEnabled: true,
          isGlobal: false,
          priority: 0,
          parent: null,
          route: { id: 10, path: '/posts' },
          methods: [],
        },
        {
          id: 2,
          name: 'users-guard',
          position: 'pre_auth',
          combinator: 'and',
          isEnabled: true,
          isGlobal: false,
          priority: 0,
          parent: null,
          route: { id: 20, path: '/users' },
          methods: [],
        },
      ],
      [],
    );
    const cache = svc.getRawCache();
    expect(cache.preAuthGlobal).toHaveLength(0);
    expect(cache.preAuthByRoute.get('/posts')).toHaveLength(1);
    expect(cache.preAuthByRoute.get('/users')).toHaveLength(1);
    expect(cache.preAuthByRoute.has('/comments')).toBe(false);
  });

  it('should treat a guard as global when isGlobal is true even if route scope is present (global wins)', async () => {
    const { svc, registry } = await loadGuardCache(
      [
        {
          id: 1,
          name: 'both-set',
          position: 'pre_auth',
          combinator: 'and',
          isEnabled: true,
          isGlobal: true, // global wins over the route scope
          priority: 0,
          parent: null,
          route: { id: 10, path: '/posts' },
          methods: [],
        },
      ],
      [],
    );
    const cache = svc.getRawCache();
    expect(cache.preAuthGlobal).toHaveLength(1);
    expect(cache.preAuthByRoute.get('/posts')).toBeUndefined();
    expect(
      registry.getGuardsForRoute('pre_auth', '/posts', 'GET'),
    ).toHaveLength(1);
    expect(
      registry.getGuardsForRoute('pre_auth', '/other', 'GET'),
    ).toHaveLength(1);
  });

  it('should drop root guards with neither route scope nor isGlobal (no silent half-applied state)', async () => {
    const { svc } = await loadGuardCache(
      [
        {
          id: 1,
          name: 'nowhere',
          position: 'pre_auth',
          combinator: 'and',
          isEnabled: true,
          isGlobal: false,
          priority: 0,
          parent: null,
          route: null,
          methods: [],
        },
      ],
      [],
    );
    const cache = svc.getRawCache();
    expect(cache.preAuthGlobal).toHaveLength(0);
    expect(cache.preAuthByRoute.size).toBe(0);
  });

  it('should merge global + route guards in getGuardsForRoute', async () => {
    const { registry } = await loadGuardCache(
      [
        {
          id: 1,
          name: 'global',
          position: 'pre_auth',
          combinator: 'and',
          isEnabled: true,
          isGlobal: true,
          priority: 0,
          parent: null,
          route: null,
          methods: [],
        },
        {
          id: 2,
          name: 'route-specific',
          position: 'pre_auth',
          combinator: 'and',
          isEnabled: true,
          isGlobal: false,
          priority: 0,
          parent: null,
          route: { id: 10, path: '/posts' },
          methods: [],
        },
      ],
      [],
    );
    const guards = registry.getGuardsForRoute('pre_auth', '/posts', 'GET');
    expect(guards).toHaveLength(2);
  });

  it('should filter by method', async () => {
    const { registry } = await loadGuardCache(
      [
        {
          id: 1,
          name: 'post-only',
          position: 'pre_auth',
          combinator: 'and',
          isEnabled: true,
          isGlobal: true,
          priority: 0,
          parent: null,
          route: null,
          methods: [{ name: 'POST' }],
        },
      ],
      [],
    );
    expect(
      registry.getGuardsForRoute('pre_auth', '/test', 'POST'),
    ).toHaveLength(1);
    expect(registry.getGuardsForRoute('pre_auth', '/test', 'GET')).toHaveLength(
      0,
    );
  });

  it('should apply to all methods when methods is empty', async () => {
    const { registry } = await loadGuardCache(
      [
        {
          id: 1,
          name: 'all-methods',
          position: 'pre_auth',
          combinator: 'and',
          isEnabled: true,
          isGlobal: true,
          priority: 0,
          parent: null,
          route: null,
          methods: [],
        },
      ],
      [],
    );
    expect(registry.getGuardsForRoute('pre_auth', '/test', 'GET')).toHaveLength(
      1,
    );
    expect(
      registry.getGuardsForRoute('pre_auth', '/test', 'DELETE'),
    ).toHaveLength(1);
  });

  it('should sort children by priority', async () => {
    const { svc } = await loadGuardCache(
      [
        {
          id: 1,
          name: 'root',
          position: 'pre_auth',
          combinator: 'and',
          isEnabled: true,
          isGlobal: true,
          priority: 0,
          parent: null,
          route: null,
          methods: [],
        },
        {
          id: 2,
          name: 'second',
          combinator: 'and',
          isEnabled: true,
          priority: 10,
          parent: { id: 1 },
          route: null,
          methods: [],
        },
        {
          id: 3,
          name: 'first',
          combinator: 'and',
          isEnabled: true,
          priority: 0,
          parent: { id: 1 },
          route: null,
          methods: [],
        },
      ],
      [],
    );
    const root = svc.getRawCache().preAuthGlobal[0];
    expect(root.children[0].name).toBe('first');
    expect(root.children[1].name).toBe('second');
  });

  it('should load userIds from rule.users', async () => {
    const { svc } = await loadGuardCache(
      [
        {
          id: 1,
          name: 'g',
          position: 'post_auth',
          combinator: 'and',
          isEnabled: true,
          isGlobal: true,
          priority: 0,
          parent: null,
          route: null,
          methods: [],
        },
      ],
      [
        {
          id: 10,
          type: 'rate_limit_by_user',
          config: { maxRequests: 10, perSeconds: 60 },
          priority: 0,
          isEnabled: true,
          guard: { id: 1 },
          users: [{ id: 'u1' }, { id: 'u2' }],
        },
      ],
    );
    expect(svc.getRawCache().postAuthGlobal[0].rules[0].userIds).toEqual([
      'u1',
      'u2',
    ]);
  });
});

describe('GuardCacheBuilder — validation', () => {
  let warnSpy: jest.SpyInstance;

  beforeEach(() => {
    warnSpy = jest.spyOn(Logger.prototype, 'warn').mockImplementation(() => {});
  });

  afterEach(() => {
    warnSpy.mockRestore();
  });

  it('should skip rate_limit_by_user in pre_auth guard', async () => {
    const { svc } = await loadGuardCache(
      [
        {
          id: 1,
          name: 'g',
          position: 'pre_auth',
          combinator: 'and',
          isEnabled: true,
          isGlobal: true,
          priority: 0,
          parent: null,
          route: null,
          methods: [],
        },
      ],
      [
        {
          id: 10,
          type: 'rate_limit_by_user',
          config: { maxRequests: 10, perSeconds: 60 },
          priority: 0,
          isEnabled: true,
          guard: { id: 1 },
          users: [],
        },
      ],
    );
    expect(svc.getRawCache().preAuthGlobal[0].rules).toHaveLength(0);
    expect(
      warnSpy.mock.calls.some((c) =>
        c.some((x) => String(x).includes('rate_limit_by_user')),
      ),
    ).toBe(true);
  });

  it('should clear userIds on pre_auth rules', async () => {
    const { svc } = await loadGuardCache(
      [
        {
          id: 1,
          name: 'g',
          position: 'pre_auth',
          combinator: 'and',
          isEnabled: true,
          isGlobal: true,
          priority: 0,
          parent: null,
          route: null,
          methods: [],
        },
      ],
      [
        {
          id: 10,
          type: 'rate_limit_by_ip',
          config: { maxRequests: 100, perSeconds: 60 },
          priority: 0,
          isEnabled: true,
          guard: { id: 1 },
          users: [{ id: 'u1' }],
        },
      ],
    );
    expect(svc.getRawCache().preAuthGlobal[0].rules[0].userIds).toEqual([]);
    expect(
      warnSpy.mock.calls.some((c) =>
        c.some((x) => String(x).includes('pre_auth')),
      ),
    ).toBe(true);
  });

  it('should allow rate_limit_by_user in post_auth guard', async () => {
    const { svc } = await loadGuardCache(
      [
        {
          id: 1,
          name: 'g',
          position: 'post_auth',
          combinator: 'and',
          isEnabled: true,
          isGlobal: true,
          priority: 0,
          parent: null,
          route: null,
          methods: [],
        },
      ],
      [
        {
          id: 10,
          type: 'rate_limit_by_user',
          config: { maxRequests: 10, perSeconds: 60 },
          priority: 0,
          isEnabled: true,
          guard: { id: 1 },
          users: [],
        },
      ],
    );
    expect(svc.getRawCache().postAuthGlobal[0].rules).toHaveLength(1);
    expect(warnSpy).not.toHaveBeenCalled();
  });

  it('should validate nested child rules against root position', async () => {
    const { svc } = await loadGuardCache(
      [
        {
          id: 1,
          name: 'root',
          position: 'pre_auth',
          combinator: 'and',
          isEnabled: true,
          isGlobal: true,
          priority: 0,
          parent: null,
          route: null,
          methods: [],
        },
        {
          id: 2,
          name: 'child',
          combinator: 'and',
          isEnabled: true,
          priority: 0,
          parent: { id: 1 },
          route: null,
          methods: [],
        },
      ],
      [
        {
          id: 10,
          type: 'rate_limit_by_user',
          config: { maxRequests: 10, perSeconds: 60 },
          priority: 0,
          isEnabled: true,
          guard: { id: 2 },
          users: [],
        },
      ],
    );
    expect(svc.getRawCache().preAuthGlobal[0].children[0].rules).toHaveLength(
      0,
    );
    expect(
      warnSpy.mock.calls.some((c) =>
        c.some((x) => String(x).includes('rate_limit_by_user')),
      ),
    ).toBe(true);
  });

  it('rejects enabled root guards with no position', async () => {
    await expect(
      loadGuardCache(
        [
          {
            id: 1,
            name: 'no-pos',
            position: null,
            combinator: 'and',
            isEnabled: true,
            isGlobal: true,
            priority: 0,
            parent: null,
            route: null,
            methods: [],
          },
        ],
        [],
      ),
    ).rejects.toThrow(/requires position/);
  });

  it('rejects missing parents and cyclic persisted guard trees', async () => {
    await expect(
      loadGuardCache(
        [
          {
            id: 1,
            name: 'orphan',
            position: null,
            combinator: 'and',
            isEnabled: true,
            parent: { id: 99 },
            route: null,
            methods: [],
          },
        ],
        [],
      ),
    ).rejects.toThrow(/parent guard 99 does not exist/);

    await expect(
      loadGuardCache(
        [
          {
            id: 1,
            name: 'cycle-a',
            position: null,
            combinator: 'and',
            isEnabled: true,
            parent: { id: 2 },
            route: null,
            methods: [],
          },
          {
            id: 2,
            name: 'cycle-b',
            position: null,
            combinator: 'and',
            isEnabled: true,
            parent: { id: 1 },
            route: null,
            methods: [],
          },
        ],
        [],
      ),
    ).rejects.toThrow(/cycle/);
  });

  it('keeps disabled ancestors for topology but excludes their subtrees', async () => {
    const disabledRoot = await loadGuardCache(
      [
        {
          id: 1,
          name: 'disabled-root',
          position: null,
          combinator: 'and',
          isEnabled: false,
          isGlobal: true,
          priority: 0,
          parent: null,
          route: null,
          methods: [],
        },
        {
          id: 2,
          name: 'enabled-child',
          position: null,
          combinator: 'and',
          isEnabled: true,
          isGlobal: false,
          priority: 0,
          parent: { id: 1 },
          route: null,
          methods: [],
        },
      ],
      [],
    );
    expect(disabledRoot.svc.getRawCache().preAuthGlobal).toHaveLength(0);
    expect(disabledRoot.svc.getRawCache().postAuthGlobal).toHaveLength(0);

    const disabledIntermediate = await loadGuardCache(
      [
        {
          id: 10,
          name: 'enabled-root',
          position: 'pre_auth',
          combinator: 'and',
          isEnabled: true,
          isGlobal: true,
          priority: 0,
          parent: null,
          route: null,
          methods: [],
        },
        {
          id: 11,
          name: 'disabled-intermediate',
          position: null,
          combinator: 'and',
          isEnabled: false,
          isGlobal: false,
          priority: 0,
          parent: { id: 10 },
          route: null,
          methods: [],
        },
        {
          id: 12,
          name: 'enabled-grandchild',
          position: null,
          combinator: 'and',
          isEnabled: true,
          isGlobal: false,
          priority: 0,
          parent: { id: 11 },
          route: null,
          methods: [],
        },
      ],
      [],
    );
    const root = disabledIntermediate.svc.getRawCache().preAuthGlobal[0];
    expect(root.children[0].isEnabled).toBe(false);
    expect(root.children[0].children[0].isEnabled).toBe(true);
  });
});

describe('GuardCacheBuilder — GraphQL matrix buckets', () => {
  function gqlGuard(
    id: number,
    name: string,
    position: string,
    opts: {
      tableName?: string | null;
      op?: string | null;
      priority?: number;
    } = {},
  ) {
    return {
      id,
      name,
      position,
      combinator: 'and',
      isEnabled: true,
      isGlobal: false,
      type: 'graphql',
      gqlOperation: opts.op ?? null,
      table:
        opts.tableName != null ? { id: 100 + id, name: opts.tableName } : null,
      priority: opts.priority ?? 0,
      parent: null,
      route: null,
      methods: [],
    };
  }

  it('classifies (null,null) into global bucket', async () => {
    const { svc } = await loadGuardCache(
      [gqlGuard(1, 'gql-global', 'pre_auth')],
      [],
    );
    const cache = svc.getRawCache();
    expect(cache.gqlPreAuthGlobal).toHaveLength(1);
    expect(cache.gqlPreAuthByTable.size).toBe(0);
    expect(cache.gqlPreAuthByOperation.size).toBe(0);
    expect(cache.gqlPreAuthExact.size).toBe(0);
    expect(cache.preAuthGlobal).toHaveLength(0);
  });

  it('classifies (table,null) into byTable bucket', async () => {
    const { svc } = await loadGuardCache(
      [gqlGuard(1, 'gql-table', 'post_auth', { tableName: 'orders' })],
      [],
    );
    const cache = svc.getRawCache();
    expect(cache.gqlPostAuthByTable.get('orders')).toHaveLength(1);
    expect(cache.gqlPostAuthGlobal).toHaveLength(0);
  });

  it('classifies (null,op) into byOperation bucket', async () => {
    const { svc } = await loadGuardCache(
      [gqlGuard(1, 'gql-op', 'pre_auth', { op: 'CREATE' })],
      [],
    );
    const cache = svc.getRawCache();
    expect(cache.gqlPreAuthByOperation.get('CREATE')).toHaveLength(1);
  });

  it('classifies (table,op) into exact bucket', async () => {
    const { svc } = await loadGuardCache(
      [
        gqlGuard(1, 'gql-exact', 'pre_auth', {
          tableName: 'orders',
          op: 'CREATE',
        }),
      ],
      [],
    );
    const cache = svc.getRawCache();
    expect(cache.gqlPreAuthExact.get('orders:CREATE')).toHaveLength(1);
  });

  it('keeps route guards out of GQL buckets and GQL guards out of route buckets', async () => {
    const { svc } = await loadGuardCache(
      [
        {
          id: 1,
          name: 'route-guard',
          position: 'pre_auth',
          combinator: 'and',
          isEnabled: true,
          isGlobal: true,
          priority: 0,
          parent: null,
          route: null,
          methods: [],
        },
        gqlGuard(2, 'gql-guard', 'pre_auth'),
      ],
      [],
    );
    const cache = svc.getRawCache();
    expect(cache.preAuthGlobal).toHaveLength(1);
    expect(cache.gqlPreAuthGlobal).toHaveLength(1);
  });

  it('getGuardsForGraphql merges exact + byTable + byOperation + global, sorted by priority', async () => {
    const { registry } = await loadGuardCache(
      [
        gqlGuard(1, 'global', 'pre_auth', { priority: 30 }),
        gqlGuard(2, 'by-op', 'pre_auth', { op: 'CREATE', priority: 10 }),
        gqlGuard(3, 'by-table', 'pre_auth', {
          tableName: 'orders',
          priority: 20,
        }),
        gqlGuard(4, 'exact', 'pre_auth', {
          tableName: 'orders',
          op: 'CREATE',
          priority: 5,
        }),
      ],
      [],
    );
    const guards = registry.getGuardsForGraphql('pre_auth', 'orders', 'CREATE');
    expect(guards.map((g) => g.name)).toEqual([
      'exact',
      'by-op',
      'by-table',
      'global',
    ]);
  });

  it('route guards are not returned by getGuardsForGraphql', async () => {
    const { registry } = await loadGuardCache(
      [
        {
          id: 1,
          name: 'route-guard',
          position: 'pre_auth',
          combinator: 'and',
          isEnabled: true,
          isGlobal: true,
          priority: 0,
          parent: null,
          route: null,
          methods: [],
        },
        gqlGuard(2, 'gql-global', 'pre_auth'),
      ],
      [],
    );
    expect(
      registry.getGuardsForGraphql('pre_auth', 'orders', 'QUERY'),
    ).toHaveLength(1);
  });
});
