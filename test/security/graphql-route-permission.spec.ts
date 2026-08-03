import jwt from 'jsonwebtoken';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { DynamicResolver } from '../../src/modules/graphql/resolvers/dynamic.resolver';

const mocks = vi.hoisted(() => ({
  loadCachedUserWithRole: vi.fn(),
}));

vi.mock('../../src/shared/utils/load-user-with-role.util', () => ({
  loadCachedUserWithRole: mocks.loadCachedUserWithRole,
}));

const baseDefinition = {
  id: 'graphql-1',
  isEnabled: true,
  isSystem: false,
  description: null,
  metadata: null,
  tableName: 'posts',
  publicOperations: [],
  permissions: [],
};

function makeResolver(definition: any = baseDefinition) {
  const executorEngineService = {
    run: vi.fn().mockResolvedValue({ data: [{ id: '1', title: 'Updated' }] }),
  };
  const runtimeRegistryService = {
    getGraphqlDefinitionForTable: vi.fn().mockReturnValue(definition),
    getGuardsForGraphql: vi.fn().mockReturnValue([]),
  };
  const resolver = new DynamicResolver({
    queryBuilderService: {},
    executorEngineService,
    repoRegistryService: {
      createReposProxy: vi.fn().mockReturnValue({ main: {} }),
    },
    runtimeRegistryService,
    envService: { get: vi.fn().mockReturnValue('test-secret') },
    dynamicContextFactory: {
      createGraphql: vi.fn().mockImplementation((input) => ({
        $user: input.user,
        $body: input.body,
        $params: input.params,
      })),
    },
    apiTokenService: { validateAccessPayload: vi.fn().mockResolvedValue(true) },
    guardCacheBuilder: {
      ensureGuardsLoaded: vi.fn().mockResolvedValue(undefined),
    },
    guardEvaluatorService: {},
    guardAlertService: { recordAlert: vi.fn() },
  } as any);

  return { resolver, executorEngineService, runtimeRegistryService };
}

function requestContext(token?: string) {
  return {
    request: {
      headers: new Map(token ? [['authorization', `Bearer ${token}`]] : []),
    },
  };
}

function sessionToken(id = 'user-1') {
  return jwt.sign({ id, tokenType: 'session' }, 'test-secret');
}

async function runUpdate(
  resolver: DynamicResolver,
  context = requestContext(),
) {
  return resolver.dynamicMutationResolver(
    'update_posts',
    { id: '1', input: { title: 'Changed' } },
    context,
    {},
  );
}

describe('DynamicResolver independent GraphQL operation access', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mocks.loadCachedUserWithRole.mockResolvedValue({
      id: 'user-1',
      role: { id: 'role-user' },
      isRootAdmin: false,
    });
  });

  it('returns 404 when GraphQL is disabled', async () => {
    const { resolver } = makeResolver({ ...baseDefinition, isEnabled: false });
    await expect(runUpdate(resolver)).rejects.toMatchObject({
      extensions: { code: '404' },
    });
  });

  it('allows anonymous access only for public operations', async () => {
    const { resolver, executorEngineService } = makeResolver({
      ...baseDefinition,
      publicOperations: ['UPDATE'],
    });

    await runUpdate(resolver);
    expect(executorEngineService.run).toHaveBeenCalledOnce();
    expect(mocks.loadCachedUserWithRole).not.toHaveBeenCalled();
  });

  it('returns 401 for a private operation without a token', async () => {
    const { resolver, executorEngineService } = makeResolver();
    await expect(runUpdate(resolver)).rejects.toMatchObject({
      extensions: { code: '401' },
    });
    expect(executorEngineService.run).not.toHaveBeenCalled();
  });

  it('returns 403 for an authenticated user without a matching permission', async () => {
    const { resolver, executorEngineService } = makeResolver();
    await expect(
      runUpdate(resolver, requestContext(sessionToken())),
    ).rejects.toMatchObject({ extensions: { code: '403' } });
    expect(executorEngineService.run).not.toHaveBeenCalled();
  });

  it('allows a matching role permission', async () => {
    const { resolver, executorEngineService } = makeResolver({
      ...baseDefinition,
      permissions: [
        {
          isEnabled: true,
          roleId: 'role-user',
          allowedUserIds: [],
          operations: ['UPDATE'],
        },
      ],
    });

    await runUpdate(resolver, requestContext(sessionToken()));
    expect(executorEngineService.run).toHaveBeenCalledOnce();
  });

  it('allows a matching explicit-user permission', async () => {
    const { resolver, executorEngineService } = makeResolver({
      ...baseDefinition,
      permissions: [
        {
          isEnabled: true,
          roleId: null,
          allowedUserIds: ['user-1', 'user-2'],
          operations: ['UPDATE'],
        },
      ],
    });

    await runUpdate(resolver, requestContext(sessionToken()));
    expect(executorEngineService.run).toHaveBeenCalledOnce();
  });

  it('allows root admin for private operations', async () => {
    mocks.loadCachedUserWithRole.mockResolvedValue({
      id: 'root-1',
      role: null,
      isRootAdmin: true,
    });
    const { resolver, executorEngineService } = makeResolver();

    await runUpdate(resolver, requestContext(sessionToken('root-1')));
    expect(executorEngineService.run).toHaveBeenCalledOnce();
  });

  it('rejects an invalid supplied token even when the operation is public', async () => {
    const { resolver, executorEngineService } = makeResolver({
      ...baseDefinition,
      publicOperations: ['UPDATE'],
    });
    const invalidToken = jwt.sign({ id: 'user-1' }, 'wrong-secret');

    await expect(
      runUpdate(resolver, requestContext(invalidToken)),
    ).rejects.toMatchObject({ extensions: { code: '401' } });
    expect(executorEngineService.run).not.toHaveBeenCalled();
  });
});
