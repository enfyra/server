import jwt from 'jsonwebtoken';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { DynamicResolver } from '../../src/modules/graphql/resolvers/dynamic.resolver';
import {
  AuthenticationService,
  JwtVerifierService,
} from '../../src/domain/auth';

const mocks = vi.hoisted(() => ({
  loadCachedUserWithRoles: vi.fn(),
}));

vi.mock('../../src/shared/utils/load-user-with-role.util', () => ({
  loadCachedUserWithRoles: mocks.loadCachedUserWithRoles,
  withUserRequestContext: (user: any) => user,
}));

const baseDefinition = {
  id: 'graphql-1',
  isEnabled: true,
  isSystem: false,
  description: null,
  metadata: null,
  tableName: 'users',
  publicOperations: [],
  permissions: [
    {
      isEnabled: true,
      roleId: 'role-user',
      allowedUserIds: [],
      operations: ['UPDATE'],
    },
  ],
};

function makeResolver(
  options: {
    definition?: any;
    validateAccessPayload?: ReturnType<typeof vi.fn>;
  } = {},
) {
  const validateAccessPayload =
    options.validateAccessPayload ?? vi.fn().mockResolvedValue(true);
  const authenticationService = new AuthenticationService({
    queryBuilderService: {},
    patVerifierService: { validateAccessPayload } as any,
    jwtVerifierService: new JwtVerifierService({
      envService: { get: () => 'test-secret' } as any,
    }),
  });
  const executorEngineService = {
    run: vi.fn().mockResolvedValue({ data: [{ id: 'user-1' }] }),
  };
  const resolver = new DynamicResolver({
    queryBuilderService: {},
    executorEngineService,
    repoRegistryService: {
      createReposProxy: vi.fn().mockReturnValue({ main: {} }),
    },
    runtimeRegistryService: {
      getGraphqlDefinitionForTable: vi
        .fn()
        .mockReturnValue(options.definition ?? baseDefinition),
      getGuardsForGraphql: vi.fn().mockReturnValue([]),
    },
    envService: { get: vi.fn().mockReturnValue('test-secret') },
    dynamicContextFactory: {
      createGraphql: vi.fn().mockImplementation((input) => ({
        $user: input.user,
        $body: input.body,
        $params: input.params,
      })),
    },
    authenticationService,
    guardCacheBuilder: {
      ensureGuardsLoaded: vi.fn().mockResolvedValue(undefined),
    },
    guardEvaluatorService: {},
    guardAlertService: { recordAlert: vi.fn() },
  } as any);

  return { resolver, executorEngineService, validateAccessPayload };
}

function requestContext(token: string) {
  return {
    request: {
      headers: new Map([['authorization', `Bearer ${token}`]]),
    },
  };
}

function apiToken() {
  return jwt.sign(
    { id: 'user-1', tokenType: 'api_token', tokenId: 'tok-1' },
    'test-secret',
  );
}

function sessionToken() {
  return jwt.sign({ id: 'user-1', tokenType: 'session' }, 'test-secret');
}

async function runUpdate(resolver: DynamicResolver, token: string) {
  return resolver.dynamicMutationResolver(
    'update_users',
    { id: 'user-1', input: { name: 'Updated' } },
    requestContext(token),
    {},
  );
}

describe('GraphQL API token revocation check', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mocks.loadCachedUserWithRoles.mockResolvedValue({
      id: 'user-1',
      roles: [{ id: 'role-user' }],
      isRootAdmin: false,
    });
  });

  it('rejects a revoked API token for a private operation with 401', async () => {
    const { resolver, executorEngineService, validateAccessPayload } =
      makeResolver({
        validateAccessPayload: vi.fn().mockResolvedValue(false),
      });

    await expect(runUpdate(resolver, apiToken())).rejects.toMatchObject({
      extensions: { code: '401' },
    });

    expect(validateAccessPayload).toHaveBeenCalledOnce();
    expect(mocks.loadCachedUserWithRoles).not.toHaveBeenCalled();
    expect(executorEngineService.run).not.toHaveBeenCalled();
  });

  it('rejects a revoked supplied API token even when the operation is public', async () => {
    const { resolver, executorEngineService } = makeResolver({
      definition: { ...baseDefinition, publicOperations: ['UPDATE'] },
      validateAccessPayload: vi.fn().mockResolvedValue(false),
    });

    await expect(runUpdate(resolver, apiToken())).rejects.toMatchObject({
      extensions: { code: '401' },
    });
    expect(executorEngineService.run).not.toHaveBeenCalled();
  });

  it('accepts a valid API token with a matching GraphQL permission', async () => {
    const { resolver, executorEngineService, validateAccessPayload } =
      makeResolver();

    await expect(runUpdate(resolver, apiToken())).resolves.toMatchObject({
      id: 'user-1',
    });

    expect(validateAccessPayload).toHaveBeenCalledOnce();
    expect(mocks.loadCachedUserWithRoles).toHaveBeenCalledOnce();
    expect(executorEngineService.run).toHaveBeenCalledOnce();
  });

  it('does not call validateAccessPayload for a normal session JWT', async () => {
    const { resolver, validateAccessPayload } = makeResolver();

    await expect(runUpdate(resolver, sessionToken())).resolves.toMatchObject({
      id: 'user-1',
    });

    expect(validateAccessPayload).not.toHaveBeenCalled();
    expect(mocks.loadCachedUserWithRoles).toHaveBeenCalledOnce();
  });

  it('rejects an invalid JWT signature with 401', async () => {
    const { resolver } = makeResolver();
    const token = jwt.sign({ id: 'user-1' }, 'wrong-secret');

    await expect(runUpdate(resolver, token)).rejects.toMatchObject({
      extensions: { code: '401' },
    });
  });
});
