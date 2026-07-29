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
  const resolver = new DynamicResolver({
    queryBuilderService: {},
    executorEngineService: { run: vi.fn() },
    repoRegistryService: { createReposProxy: vi.fn().mockReturnValue({}) },
    guardCacheBuilder: { ensureGuardsLoaded: vi.fn().mockResolvedValue(undefined) },
    guardEvaluatorService: { evaluateGuard: vi.fn() },
    runtimeRegistryService: {
      requireRoutes: vi.fn().mockReturnValue([]),
      isGraphqlEnabledForTable: vi.fn().mockReturnValue(true),
      getGuardsForRoute: vi.fn().mockReturnValue([]),
    },
    policyService: { checkRequestAccess: vi.fn().mockReturnValue({ allow: true }) },
    envService: { get: vi.fn().mockReturnValue('test-secret') },
    dynamicContextFactory: { createGraphql: vi.fn() },
    apiTokenService: { validateAccessPayload: vi.fn().mockResolvedValue(true) },
    ...overrides,
  } as any);
  return resolver;
}

describe('GraphQL API token revocation check', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mocks.loadCachedUserWithRole.mockResolvedValue({ id: 'user-1', role: 'admin' });
  });

  it('rejects revoked API token', async () => {
    const validateAccessPayload = vi.fn().mockResolvedValue(false);
    const resolver = makeResolver({
      apiTokenService: { validateAccessPayload },
    });

    const token = jwt.sign({ id: 'user-1', tokenType: 'api_token', tokenId: 'tok-1' }, 'test-secret');

    await expect(
      (resolver as any).checkAccess('users', 'GET', token),
    ).rejects.toThrow();

    expect(validateAccessPayload).toHaveBeenCalledOnce();
    expect(mocks.loadCachedUserWithRole).not.toHaveBeenCalled();
  });

  it('accepts valid API token', async () => {
    const validateAccessPayload = vi.fn().mockResolvedValue(true);
    const resolver = makeResolver({
      apiTokenService: { validateAccessPayload },
    });

    const token = jwt.sign({ id: 'user-1', tokenType: 'api_token', tokenId: 'tok-1' }, 'test-secret');

    const user = await (resolver as any).checkAccess('users', 'GET', token);

    expect(validateAccessPayload).toHaveBeenCalledOnce();
    expect(mocks.loadCachedUserWithRole).toHaveBeenCalledOnce();
    expect(user).toMatchObject({ id: 'user-1' });
  });

  it('does not call validateAccessPayload for normal JWT', async () => {
    const validateAccessPayload = vi.fn();
    const resolver = makeResolver({
      apiTokenService: { validateAccessPayload },
    });

    const token = jwt.sign({ id: 'user-1', tokenType: 'session' }, 'test-secret');

    const user = await (resolver as any).checkAccess('users', 'GET', token);

    expect(validateAccessPayload).not.toHaveBeenCalled();
    expect(mocks.loadCachedUserWithRole).toHaveBeenCalledOnce();
    expect(user).toMatchObject({ id: 'user-1' });
  });

  it('rejects invalid JWT signature', async () => {
    const resolver = makeResolver();
    const token = jwt.sign({ id: 'user-1' }, 'wrong-secret');

    await expect(
      (resolver as any).checkAccess('users', 'GET', token),
    ).rejects.toThrow();
  });
});
