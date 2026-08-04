import jwt from 'jsonwebtoken';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { GraphQLError } from 'graphql';
import { DynamicResolver } from '../../src/modules/graphql/resolvers/dynamic.resolver';
import {
  AuthenticationService,
  JwtVerifierService,
} from '../../src/domain/auth';

const mocks = vi.hoisted(() => ({
  loadCachedUserWithRole: vi.fn(),
}));

vi.mock('../../src/shared/utils/load-user-with-role.util', () => ({
  loadCachedUserWithRole: mocks.loadCachedUserWithRole,
  withUserRequestContext: (user: any) => user,
}));

const baseDefinition = {
  id: 'graphql-1',
  isEnabled: true,
  isSystem: false,
  description: null,
  metadata: null,
  tableName: 'posts',
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
  overrides: {
    getGuardsForGraphql?: (position: string) => any[];
    evaluateGuard?: (guard: any, ctx: any) => Promise<any>;
  } = {},
) {
  const authenticationService = new AuthenticationService({
    queryBuilderService: {},
    patVerifierService: {
      validateAccessPayload: vi.fn().mockResolvedValue(true),
    } as any,
    jwtVerifierService: new JwtVerifierService({
      envService: { get: () => 'test-secret' } as any,
    }),
  });
  const executorEngineService = {
    run: vi.fn().mockResolvedValue({ data: [{ id: '1', title: 'Updated' }] }),
  };
  const runtimeRegistryService = {
    getGraphqlDefinitionForTable: vi.fn().mockReturnValue(baseDefinition),
    getGuardsForGraphql: vi.fn(
      (position: string) => overrides.getGuardsForGraphql?.(position) ?? [],
    ),
  };
  const guardEvaluatorService = {
    evaluateGuard: vi.fn(
      overrides.evaluateGuard ??
        (async () => ({ reject: null, rateLimitSnapshots: [] })),
    ),
  };
  const guardAlertService = { recordAlert: vi.fn() };
  const guardCacheBuilder = {
    ensureGuardsLoaded: vi.fn().mockResolvedValue(undefined),
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
    authenticationService,
    guardCacheBuilder,
    guardEvaluatorService,
    guardAlertService,
  } as any);

  return {
    resolver,
    executorEngineService,
    runtimeRegistryService,
    guardEvaluatorService,
    guardAlertService,
    guardCacheBuilder,
  };
}

function requestContext(
  token?: string,
  options: { clientIp?: string; forwardedFor?: string } = {},
) {
  const headers = new Map<string, string>();
  if (token) headers.set('authorization', `Bearer ${token}`);
  if (options.forwardedFor) {
    headers.set('x-forwarded-for', options.forwardedFor);
  }
  return {
    clientIp: options.clientIp ?? '203.0.113.10',
    request: {
      headers,
    },
  };
}

function sessionToken(id = 'user-1') {
  return jwt.sign({ id, tokenType: 'session' }, 'test-secret');
}

function rateLimitReject() {
  return {
    guardName: 'gql-rate',
    ruleType: 'rate_limit_by_operation',
    statusCode: 429,
    errorCode: 'RATE_LIMIT_EXCEEDED',
    message: 'Too Many Requests',
    details: {
      reason: 'rate_limit',
      scope: 'operation',
      limit: 5,
      remaining: 0,
      windowSeconds: 60,
      retryAfterSeconds: 30,
      resetAt: Date.now() + 60000,
    },
    headers: { 'Retry-After': '30' },
  };
}

function ipBlockedReject() {
  return {
    guardName: 'gql-ip',
    ruleType: 'ip_blacklist',
    statusCode: 403,
    errorCode: 'IP_BLOCKED',
    message: 'Forbidden',
    details: { reason: 'ip_blocked' },
    headers: { 'X-Enfyra-Guard-Error-Code': 'IP_BLOCKED' },
  };
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

describe('DynamicResolver GraphQL guards', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mocks.loadCachedUserWithRole.mockResolvedValue({
      id: 'user-1',
      role: { id: 'role-user' },
      isRootAdmin: false,
    });
  });

  it('runs pre_auth guards before authenticate()', async () => {
    const { resolver, guardEvaluatorService, executorEngineService } =
      makeResolver({
        getGuardsForGraphql: (position) =>
          position === 'pre_auth' ? [{ id: 1, name: 'gql-pre' }] : [],
      });
    // pre_auth guard rejects → no auth needed, no permission check
    guardEvaluatorService.evaluateGuard.mockResolvedValue({
      reject: ipBlockedReject(),
      rateLimitSnapshots: [],
    });

    await expect(
      runUpdate(resolver, requestContext(sessionToken())),
    ).rejects.toMatchObject({
      extensions: { code: 'IP_BLOCKED', statusCode: 403 },
    });
    expect(mocks.loadCachedUserWithRole).not.toHaveBeenCalled();
    expect(executorEngineService.run).not.toHaveBeenCalled();
  });

  it('uses the trusted GraphQL context IP instead of forwarded headers', async () => {
    const { resolver, guardEvaluatorService } = makeResolver({
      getGuardsForGraphql: (position) =>
        position === 'pre_auth' ? [{ id: 1, name: 'gql-pre' }] : [],
    });

    await runUpdate(
      resolver,
      requestContext(sessionToken(), {
        clientIp: '203.0.113.10',
        forwardedFor: '198.51.100.25',
      }),
    );

    expect(guardEvaluatorService.evaluateGuard).toHaveBeenCalledWith(
      expect.anything(),
      expect.objectContaining({ clientIp: '203.0.113.10' }),
    );
  });

  it('reads only the activated guard registry on the request hot path', async () => {
    const { resolver, guardCacheBuilder } = makeResolver({
      getGuardsForGraphql: () => [{ id: 1, name: 'gql' }],
    });

    await runUpdate(resolver, requestContext(sessionToken()));

    expect(guardCacheBuilder.ensureGuardsLoaded).not.toHaveBeenCalled();
  });

  it('runs post_auth guards after authenticate() and before permission check', async () => {
    const { resolver, guardEvaluatorService, executorEngineService } =
      makeResolver({
        getGuardsForGraphql: (position) =>
          position === 'post_auth' ? [{ id: 2, name: 'gql-post' }] : [],
      });
    guardEvaluatorService.evaluateGuard.mockResolvedValue({
      reject: rateLimitReject(),
      rateLimitSnapshots: [],
    });

    await expect(
      runUpdate(resolver, requestContext(sessionToken())),
    ).rejects.toMatchObject({
      extensions: { code: 'RATE_LIMIT_EXCEEDED', statusCode: 429 },
    });
    expect(mocks.loadCachedUserWithRole).toHaveBeenCalled();
    expect(executorEngineService.run).not.toHaveBeenCalled();
  });

  it('records alert with routePath=/graphql and method=operation on reject', async () => {
    const { resolver, guardAlertService, guardEvaluatorService } = makeResolver(
      {
        getGuardsForGraphql: (position) =>
          position === 'post_auth' ? [{ id: 2, name: 'gql-post' }] : [],
      },
    );
    guardEvaluatorService.evaluateGuard.mockResolvedValue({
      reject: rateLimitReject(),
      rateLimitSnapshots: [],
    });

    await expect(
      runUpdate(resolver, requestContext(sessionToken())),
    ).rejects.toBeInstanceOf(GraphQLError);

    expect(guardAlertService.recordAlert).toHaveBeenCalledWith({
      scope: 'operation',
      scopeKey: 'posts:UPDATE',
      routePath: '/graphql',
      method: 'UPDATE',
      errorCode: 'RATE_LIMIT_EXCEEDED',
      guardName: 'gql-post',
    });
  });

  it('lets an anonymous public operation be blocked by pre_auth rate limit', async () => {
    const { resolver, guardEvaluatorService, executorEngineService } =
      makeResolver({
        getGuardsForGraphql: (position) =>
          position === 'pre_auth' ? [{ id: 1, name: 'gql-pre' }] : [],
      });
    guardEvaluatorService.evaluateGuard.mockResolvedValue({
      reject: rateLimitReject(),
      rateLimitSnapshots: [],
    });

    // anonymous public UPDATE
    await expect(runUpdate(resolver, requestContext())).rejects.toMatchObject({
      extensions: { code: 'RATE_LIMIT_EXCEEDED', statusCode: 429 },
    });
    expect(mocks.loadCachedUserWithRole).not.toHaveBeenCalled();
    expect(executorEngineService.run).not.toHaveBeenCalled();
  });

  it('proceeds when no guard rejects', async () => {
    const { resolver, executorEngineService, guardEvaluatorService } =
      makeResolver({
        getGuardsForGraphql: () => [{ id: 1, name: 'gql' }],
      });
    guardEvaluatorService.evaluateGuard.mockResolvedValue({
      reject: null,
      rateLimitSnapshots: [],
    });

    await runUpdate(resolver, requestContext(sessionToken()));
    expect(executorEngineService.run).toHaveBeenCalledOnce();
  });
});
