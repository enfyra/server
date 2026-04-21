import { EventEmitter2 } from 'eventemitter2';
import { Logger } from '../../src/shared/logger';
import { GuardCacheService } from '../../src/infrastructure/cache/services/guard-cache.service';

async function loadGuardCache(
  guards: any[],
  rules: any[],
): Promise<GuardCacheService> {
  const find = jest.fn(async (params: any) => {
    if (params.table === 'guard_definition') return { data: guards };
    if (params.table === 'guard_rule_definition') return { data: rules };
    return { data: [] };
  });
  const qb = { find, isMongoDb: () => false };
  const ee = new EventEmitter2();
  const svc = new GuardCacheService({
    queryBuilderService: qb as any,
    eventEmitter: ee,
  });
  await svc.reload(false);
  return svc;
}

describe('GuardCacheService — tree building', () => {
  it('should build flat guard with rules', async () => {
    const svc = await loadGuardCache(
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
    const svc = await loadGuardCache(
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
    const svc = await loadGuardCache(
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

  it('should merge global + route guards in getGuardsForRoute', async () => {
    const svc = await loadGuardCache(
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
    const guards = svc.getGuardsForRoute('pre_auth', '/posts', 'GET');
    expect(guards).toHaveLength(2);
  });

  it('should filter by method', async () => {
    const svc = await loadGuardCache(
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
          methods: [{ method: 'POST' }],
        },
      ],
      [],
    );
    expect(svc.getGuardsForRoute('pre_auth', '/test', 'POST')).toHaveLength(1);
    expect(svc.getGuardsForRoute('pre_auth', '/test', 'GET')).toHaveLength(0);
  });

  it('should apply to all methods when methods is empty', async () => {
    const svc = await loadGuardCache(
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
    expect(svc.getGuardsForRoute('pre_auth', '/test', 'GET')).toHaveLength(1);
    expect(svc.getGuardsForRoute('pre_auth', '/test', 'DELETE')).toHaveLength(
      1,
    );
  });

  it('should sort children by priority', async () => {
    const svc = await loadGuardCache(
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
    const svc = await loadGuardCache(
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

describe('GuardCacheService — validation', () => {
  let warnSpy: jest.SpyInstance;

  beforeEach(() => {
    warnSpy = jest.spyOn(Logger.prototype, 'warn').mockImplementation(() => {});
  });

  afterEach(() => {
    warnSpy.mockRestore();
  });

  it('should skip rate_limit_by_user in pre_auth guard', async () => {
    const svc = await loadGuardCache(
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
    const svc = await loadGuardCache(
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
    const svc = await loadGuardCache(
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
    const svc = await loadGuardCache(
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

  it('should skip guards with no position', async () => {
    const svc = await loadGuardCache(
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
    );
    const cache = svc.getRawCache();
    expect(cache.preAuthGlobal).toHaveLength(0);
    expect(cache.postAuthGlobal).toHaveLength(0);
  });
});
