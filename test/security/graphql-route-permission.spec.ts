import jwt from 'jsonwebtoken';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { DynamicResolver } from '../../src/modules/graphql/resolvers/dynamic.resolver';

const mocks = vi.hoisted(() => ({
  loadCachedUserWithRole: vi.fn(),
}));

vi.mock('../../src/shared/utils/load-user-with-role.util', () => ({
  loadCachedUserWithRole: mocks.loadCachedUserWithRole,
}));

function makeResolver(overrides: Record<string, any> = {}) {
  const executorEngineService = {
    run: vi.fn().mockResolvedValue({ data: [{ id: '1', title: 'Updated' }] }),
  };
  const runtimeRegistryService = {
    requireRoutes: vi.fn().mockReturnValue([]),
  };
  const resolver = new DynamicResolver({
    queryBuilderService: {},
    executorEngineService,
    gqlDefinitionCacheService: {
      isEnabledForTable: vi.fn().mockResolvedValue(true),
    },
    repoRegistryService: {
      createReposProxy: vi.fn().mockReturnValue({
        main: {},
      }),
    },
    guardCacheService: {
      ensureGuardsLoaded: vi.fn().mockResolvedValue(undefined),
      getGuardsForRoute: vi.fn().mockResolvedValue([]),
    },
    guardEvaluatorService: {
      evaluateGuard: vi.fn(),
    },
    runtimeRegistryService,
    policyService: {
      checkRequestAccess: vi.fn().mockReturnValue({ allow: true }),
    },
    envService: {
      get: vi.fn().mockReturnValue('test-secret'),
    },
    dynamicContextFactory: {
      createGraphql: vi.fn().mockImplementation((input) => ({
        $user: input.user,
        $body: input.body,
        $params: input.params,
      })),
    },
    cacheService: {},
    ...overrides,
  } as any);

  return { resolver, executorEngineService, runtimeRegistryService };
}

function authContext() {
  const token = jwt.sign({ id: 'user-1' }, 'test-secret');
  return {
    request: {
      headers: new Map([['authorization', `Bearer ${token}`]]),
    },
  };
}

describe('DynamicResolver route permissions', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mocks.loadCachedUserWithRole.mockResolvedValue({
      id: 'user-1',
      role: { id: 'role-user' },
      isRootAdmin: false,
    });
  });

  it('checks PATCH route permission for GraphQL update mutations', async () => {
    const policyService = {
      checkRequestAccess: vi.fn().mockReturnValue({
        allow: false,
        statusCode: 403,
        message: 'Forbidden',
      }),
    };
    const { resolver, executorEngineService, runtimeRegistryService } =
      makeResolver({
        policyService,
      });
    runtimeRegistryService.requireRoutes.mockReturnValue([
      {
        path: '/posts',
        availableMethods: [{ name: 'PATCH' }],
        routePermissions: [],
        publicMethods: [],
        skipRoleGuardMethods: [],
      },
    ]);

    await expect(
      resolver.dynamicMutationResolver(
        'update_posts',
        { id: '1', input: { title: 'Blocked' } },
        authContext(),
        {},
      ),
    ).rejects.toMatchObject({
      extensions: { code: 'MUTATION_ERROR' },
    });

    expect(runtimeRegistryService.requireRoutes).toHaveBeenCalled();
    expect(policyService.checkRequestAccess).toHaveBeenCalledWith(
      expect.objectContaining({
        method: 'PATCH',
        user: expect.objectContaining({ id: 'user-1' }),
      }),
    );
    expect(executorEngineService.run).not.toHaveBeenCalled();
  });

  it('checks DELETE route permission for GraphQL delete mutations', async () => {
    const { resolver, runtimeRegistryService } = makeResolver();
    runtimeRegistryService.requireRoutes.mockReturnValue([
      {
        path: '/posts',
        availableMethods: [{ name: 'DELETE' }],
        routePermissions: [
          { methods: [{ name: 'DELETE' }], role: { id: 'role-user' } },
        ],
        publicMethods: [],
        skipRoleGuardMethods: [],
      },
    ]);

    await resolver.dynamicMutationResolver(
      'delete_posts',
      { id: '1' },
      authContext(),
      {},
    );

    expect(runtimeRegistryService.requireRoutes).toHaveBeenCalled();
  });
});
