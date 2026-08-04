import { describe, expect, it, vi } from 'vitest';
import { BadRequestException } from '../../src/domain/exceptions';
import { GuardValidationService } from '../../src/modules/dynamic-api';

function makeService(
  overrides: {
    findOne?: (args: any) => Promise<any>;
    find?: (args: any) => Promise<any>;
  } = {},
) {
  const queryBuilderService = {
    getPkField: vi.fn(() => 'id'),
    find: vi.fn(
      overrides.find ?? (() => Promise.resolve({ data: [], count: 0 })),
    ),
    findOne: vi.fn(
      overrides.findOne ??
        (() => Promise.resolve({ id: 1, type: 'route', parent: null })),
    ),
  };
  return new GuardValidationService({
    queryBuilderService: queryBuilderService as any,
  });
}

describe('GuardValidationService.assertGuardBody', () => {
  it('rejects non-object bodies', () => {
    const svc = makeService();
    for (const bad of [null, undefined, 'x', 42, []]) {
      expect(() => svc.assertGuardBody(bad as any)).toThrow(
        BadRequestException,
      );
    }
  });

  it('rejects an invalid guard type', () => {
    const svc = makeService();
    expect(() => svc.assertGuardBody({ type: 'websocket' })).toThrow(
      /Invalid guard type "websocket"/,
    );
  });

  it('type=route (default) requires a route, isGlobal, or a parent', () => {
    const svc = makeService();
    expect(() => svc.assertGuardBody({})).toThrow(
      /requires a route or isGlobal=true/,
    );
    expect(() => svc.assertGuardBody({ name: 'x' })).toThrow(
      BadRequestException,
    );
    expect(() => svc.assertGuardBody({ route: {} })).toThrow(
      BadRequestException,
    ); // empty route object = absent
    // valid root guard forms
    expect(() => svc.assertGuardBody({ isGlobal: true })).not.toThrow();
    expect(() => svc.assertGuardBody({ route: { id: 1 } })).not.toThrow();
    expect(() => svc.assertGuardBody({ route: 1 })).not.toThrow();
  });

  it('child guards (parent set) skip the route/isGlobal requirement', () => {
    const svc = makeService();
    expect(() => svc.assertGuardBody({ parent: { id: 5 } })).not.toThrow();
    expect(() => svc.assertGuardBody({ parent: 5 })).not.toThrow();
  });

  it('type=route rejects gqlOperation and table targeting', () => {
    const svc = makeService();
    expect(() =>
      svc.assertGuardBody({ isGlobal: true, gqlOperation: 'QUERY' }),
    ).toThrow(/type=route cannot set gqlOperation/);
    expect(() =>
      svc.assertGuardBody({ isGlobal: true, table: { id: 2 } }),
    ).toThrow(/type=route cannot set table/);
  });

  it('type=graphql rejects route, isGlobal, and methods', () => {
    const svc = makeService();
    expect(() =>
      svc.assertGuardBody({ type: 'graphql', route: { id: 1 } }),
    ).toThrow(/cannot have a route/);
    expect(() =>
      svc.assertGuardBody({ type: 'graphql', isGlobal: true }),
    ).toThrow(/cannot set isGlobal/);
    expect(() =>
      svc.assertGuardBody({ type: 'graphql', methods: ['GET'] }),
    ).toThrow(/cannot set methods/);
    expect(() =>
      svc.assertGuardBody({ type: 'graphql', methods: 'GET' }),
    ).toThrow(/cannot set methods/); // scalar methods normalized to array
  });

  it('type=graphql rejects an invalid gqlOperation', () => {
    const svc = makeService();
    expect(() =>
      svc.assertGuardBody({ type: 'graphql', gqlOperation: 'SUBSCRIBE' }),
    ).toThrow(/Invalid gqlOperation "SUBSCRIBE"/);
  });

  it('type=graphql accepts valid (table, gqlOperation) matrix targeting', () => {
    const svc = makeService();
    expect(() => svc.assertGuardBody({ type: 'graphql' })).not.toThrow(); // null = all
    expect(() =>
      svc.assertGuardBody({ type: 'graphql', gqlOperation: 'QUERY' }),
    ).not.toThrow();
    expect(() =>
      svc.assertGuardBody({
        type: 'graphql',
        table: { id: 3 },
        gqlOperation: 'DELETE',
      }),
    ).not.toThrow();
  });

  it('requires a position when an enabled guard is a root', () => {
    const svc = makeService();
    expect(() =>
      svc.assertGuardBody({ isGlobal: true, isEnabled: true }),
    ).toThrow(/requires position/);
    expect(() =>
      svc.assertGuardBody({
        isGlobal: true,
        isEnabled: true,
        position: 'pre_auth',
      }),
    ).not.toThrow();
    expect(() =>
      svc.assertGuardBody({
        type: 'graphql',
        isEnabled: true,
        position: 'post_auth',
      }),
    ).not.toThrow();
  });

  it('PATCH merges existing values before asserting', () => {
    const svc = makeService();
    const existing = { type: 'graphql', gqlOperation: 'CREATE' };
    // patching table onto an existing graphql guard is valid
    expect(() =>
      svc.assertGuardBody({ table: { id: 3 } }, existing),
    ).not.toThrow();
    // patching a route onto an existing graphql guard is invalid (merged)
    expect(() => svc.assertGuardBody({ route: { id: 1 } }, existing)).toThrow(
      /cannot have a route/,
    );
    // untouched fields of an existing route guard are not re-asserted
    expect(() =>
      svc.assertGuardBody({ description: 'x' }, { isGlobal: true }),
    ).not.toThrow();
  });

  it('PATCH can clear gqlOperation back to null (= all)', () => {
    const svc = makeService();
    const existing = { type: 'graphql', gqlOperation: 'QUERY' };
    expect(() =>
      svc.assertGuardBody({ gqlOperation: null }, existing),
    ).not.toThrow();
  });

  it.each(['rules', 'children'])(
    'rejects nested inverse writes through %s',
    (field) => {
      const svc = makeService();
      expect(() =>
        svc.assertGuardBody({
          isGlobal: true,
          [field]: [{ type: 'rate_limit_by_route' }],
        }),
      ).toThrow(/Nested guard writes/);
    },
  );
});

describe('GuardValidationService.assertGuardRuleBody', () => {
  it('rejects non-object bodies', async () => {
    const svc = makeService();
    for (const bad of [null, 'x', 42, []]) {
      await expect(svc.assertGuardRuleBody(bad as any)).rejects.toThrow(
        BadRequestException,
      );
    }
  });

  it('rejects an unknown rule type', async () => {
    const svc = makeService();
    await expect(
      svc.assertGuardRuleBody({ type: 'rate_limit_by_session' }),
    ).rejects.toThrow(/Invalid rule type "rate_limit_by_session"/);
  });

  it('binds rate_limit_by_route to route guards only', async () => {
    const svc = makeService({
      findOne: async (args) => ({
        id: args.where.id,
        type: args.where.id === 2 ? 'graphql' : 'route',
        parent: null,
      }),
    });
    await expect(
      svc.assertGuardRuleBody({
        type: 'rate_limit_by_route',
        guard: 2,
      }),
    ).rejects.toThrow(/only valid on guards with type=route/);
    await expect(
      svc.assertGuardRuleBody({
        type: 'rate_limit_by_route',
        guard: 1,
      }),
    ).resolves.toBeUndefined();
  });

  it('binds rate_limit_by_operation to graphql guards only', async () => {
    const svc = makeService({
      findOne: async (args) => ({
        id: args.where.id,
        type: args.where.id === 2 ? 'graphql' : 'route',
        parent: null,
      }),
    });
    await expect(
      svc.assertGuardRuleBody({
        type: 'rate_limit_by_operation',
        guard: 1,
      }),
    ).rejects.toThrow(/only valid on guards with type=graphql/);
    await expect(
      svc.assertGuardRuleBody({
        type: 'rate_limit_by_operation',
        guard: 2,
      }),
    ).resolves.toBeUndefined();
  });

  it('resolves scalar guard ids through the query builder', async () => {
    const findOne = vi.fn().mockResolvedValue({
      id: 42,
      type: 'graphql',
      parent: null,
    });
    const svc = makeService({ findOne: findOne as any });
    await expect(
      svc.assertGuardRuleBody({ type: 'rate_limit_by_operation', guard: 42 }),
    ).resolves.toBeUndefined();
    expect(findOne).toHaveBeenCalledWith(
      expect.objectContaining({ table: 'enfyra_guard', where: { id: 42 } }),
    );
  });

  it('resolves guard objects with an id through the query builder', async () => {
    const findOne = vi.fn().mockResolvedValue({
      id: 7,
      type: 'route',
      parent: null,
    });
    const svc = makeService({ findOne: findOne as any });
    await expect(
      svc.assertGuardRuleBody({
        type: 'rate_limit_by_route',
        guard: { id: 7 },
      }),
    ).resolves.toBeUndefined();
    expect(findOne).toHaveBeenCalledWith(
      expect.objectContaining({ table: 'enfyra_guard', where: { id: 7 } }),
    );
  });

  it('rejects validation when the owning guard cannot be resolved', async () => {
    const svc = makeService();
    await expect(
      svc.assertGuardRuleBody({ type: 'rate_limit_by_operation', guard: null }),
    ).rejects.toThrow(/requires an owning guard/);
  });

  it('rejects ID-less owning guard objects', async () => {
    const svc = makeService();
    await expect(
      svc.assertGuardRuleBody({
        type: 'rate_limit_by_operation',
        guard: { type: 'graphql' },
      }),
    ).rejects.toThrow(/persisted guard id/);
  });

  it('fails closed when the guard lookup fails', async () => {
    const svc = makeService({
      findOne: () => Promise.reject(new Error('db down')),
    });
    await expect(
      svc.assertGuardRuleBody({ type: 'rate_limit_by_operation', guard: 9 }),
    ).rejects.toThrow('db down');
  });

  it('PATCH merges existing rule values before asserting', async () => {
    const svc = makeService({
      findOne: async (args) => ({
        id: args.where.id,
        type: args.where.id === 2 ? 'graphql' : 'route',
        parent: null,
      }),
    });
    const existing = { type: 'rate_limit_by_operation' };
    // patching a route guard onto the existing operation rule is invalid
    await expect(
      svc.assertGuardRuleBody({ guard: 1 }, existing),
    ).rejects.toThrow(/only valid on guards with type=graphql/);
    // patching a graphql guard keeps it valid
    await expect(
      svc.assertGuardRuleBody({ guard: 2 }, existing),
    ).resolves.toBeUndefined();
  });

  it('validates guard updates against canonical stored targeting fields', async () => {
    const svc = makeService({
      findOne: async () => ({
        id: 1,
        type: 'route',
        route: { id: 10 },
        table: null,
        parent: null,
        methods: [],
        isGlobal: false,
        gqlOperation: null,
      }),
    });

    await expect(svc.assertGuardUpdate(1, { type: 'graphql' })).rejects.toThrow(
      /cannot have a route/,
    );
  });

  it('does not scan guard tables for non-structural PATCH fields', async () => {
    const find = vi.fn().mockResolvedValue({ data: [], count: 0 });
    const svc = makeService({
      find,
      findOne: async () => ({
        id: 1,
        type: 'route',
        route: { id: 10 },
        table: null,
        parent: null,
        methods: [],
        isGlobal: false,
        isEnabled: true,
        position: 'pre_auth',
        gqlOperation: null,
      }),
    });

    await svc.assertGuardUpdate(1, { description: 'Updated' });

    expect(find).not.toHaveBeenCalled();
  });

  it('resolves the effective root target type for child guard rules', async () => {
    const svc = makeService({
      findOne: async (args) => {
        const id = args.where.id;
        if (id === 2) {
          return { id: 2, type: 'route', parent: { id: 1 } };
        }
        if (id === 1) {
          return { id: 1, type: 'graphql', parent: null };
        }
        return null;
      },
    });

    await expect(
      svc.assertGuardRuleBody({
        type: 'rate_limit_by_operation',
        guard: 2,
      }),
    ).resolves.toBeUndefined();
    await expect(
      svc.assertGuardRuleBody({
        type: 'rate_limit_by_route',
        guard: 2,
      }),
    ).rejects.toThrow(/only valid on guards with type=route/);
  });

  it('rejects self-parent, cyclic, and missing proposed parent chains', async () => {
    const guards = [
      {
        id: 1,
        type: 'route',
        parent: null,
        route: { id: 10 },
        methods: [],
        isGlobal: false,
        isEnabled: true,
        position: 'pre_auth',
      },
      {
        id: 2,
        type: 'route',
        parent: { id: 1 },
        methods: [],
        isGlobal: false,
        isEnabled: true,
        position: null,
      },
    ];
    const svc = makeService({
      findOne: async (args) =>
        guards.find((guard) => guard.id === args.where.id) ?? null,
      find: async (args) => ({
        data: args.table === 'enfyra_guard' ? guards : [],
        count: args.table === 'enfyra_guard' ? guards.length : 0,
      }),
    });

    await expect(svc.assertGuardUpdate(1, { parent: 1 })).rejects.toThrow(
      /cycle/,
    );
    await expect(svc.assertGuardUpdate(1, { parent: 2 })).rejects.toThrow(
      /cycle/,
    );
    await expect(svc.assertGuardUpdate(1, { parent: 999 })).rejects.toThrow(
      /does not exist/,
    );
  });

  it('rejects root type changes that invalidate descendant rules', async () => {
    const guards = [
      {
        id: 1,
        type: 'route',
        parent: null,
        route: { id: 10 },
        table: null,
        methods: [],
        isGlobal: false,
        isEnabled: true,
        position: 'pre_auth',
        gqlOperation: null,
      },
      {
        id: 2,
        type: 'route',
        parent: { id: 1 },
        methods: [],
        isGlobal: false,
        isEnabled: true,
        position: null,
      },
    ];
    const rules = [
      {
        id: 20,
        type: 'rate_limit_by_route',
        guard: { id: 2 },
      },
    ];
    const find = vi.fn(async (args) => ({
      data: args.table === 'enfyra_guard' ? guards : rules,
      count: args.table === 'enfyra_guard' ? guards.length : rules.length,
    }));
    const svc = makeService({
      findOne: async (args) =>
        guards.find((guard) => guard.id === args.where.id) ?? null,
      find,
    });

    await expect(
      svc.assertGuardUpdate(1, {
        type: 'graphql',
        route: null,
        table: null,
        methods: [],
        isGlobal: false,
      }),
    ).rejects.toThrow(/rate_limit_by_route/);
    expect(find).toHaveBeenCalledWith(
      expect.objectContaining({
        table: 'enfyra_guard_rule',
        filter: { guard: { _in: [1, 2] } },
      }),
    );
  });
});
