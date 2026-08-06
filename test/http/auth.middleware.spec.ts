import { afterEach, describe, expect, it, vi } from 'vitest';
import { SignJWT } from 'jose';
import { authMiddleware } from '../../src/http/middlewares/auth.middleware';
import {
  AuthenticationService,
  JwtVerifierService,
} from '../../src/domain/auth';
import { InvalidTokenException } from '../../src/domain/exceptions';
import {
  clearLocalUserCacheForTesting,
  primeCachedUserSnapshot,
} from '../../src/shared/utils/load-user-with-role.util';

const secret = 'test-secret';

function makeMiddleware(queryBuilder: any = {}, patVerifierService: any = {}) {
  const authenticationService = new AuthenticationService({
    queryBuilderService: queryBuilder,
    patVerifierService: {
      validateAccessPayload: vi.fn().mockResolvedValue(true),
      ...patVerifierService,
    },
    jwtVerifierService: new JwtVerifierService({
      envService: { get: () => secret } as any,
    }),
  });
  return authMiddleware(authenticationService);
}

function makeMiddlewareWithAuthHeaders(
  authHeaderConfigs: any[],
  queryBuilder: any = {},
  patVerifierService: any = {},
) {
  const authenticationService = new AuthenticationService({
    queryBuilderService: queryBuilder,
    patVerifierService: {
      validateAccessPayload: vi.fn().mockResolvedValue(true),
      ...patVerifierService,
    },
    jwtVerifierService: new JwtVerifierService({
      envService: { get: () => secret } as any,
    }),
    runtimeRegistryService: {
      getAuthHeaderConfigs: () => authHeaderConfigs,
    } as any,
  });
  return authMiddleware(authenticationService);
}

async function signToken(payload: Record<string, any>) {
  return new SignJWT(payload)
    .setProtectedHeader({ alg: 'HS256' })
    .sign(new TextEncoder().encode(secret));
}

describe('authMiddleware', () => {
  afterEach(() => {
    clearLocalUserCacheForTesting();
  });

  it('treats invalid Bearer tokens as anonymous on public methods', async () => {
    const req: any = {
      method: 'GET',
      headers: { authorization: 'Bearer malformed.jwt.token' },
      routeData: {
        publicMethods: [{ name: 'GET' }],
        context: { $user: 'existing' },
      },
    };
    const next = vi.fn();

    await makeMiddleware()(req, {} as any, next);

    expect(req.user).toBeNull();
    expect(req.routeData.context.$user).toBeNull();
    expect(next).toHaveBeenCalledWith();
  });

  it('rejects invalid Bearer tokens on non-public methods', async () => {
    const req: any = {
      method: 'GET',
      headers: { authorization: 'Bearer malformed.jwt.token' },
      routeData: {
        isPublished: false,
        context: { $user: null },
      },
    };
    const next = vi.fn();

    await makeMiddleware()(req, {} as any, next);

    expect(next).toHaveBeenCalledWith(expect.any(InvalidTokenException));
  });

  it('hydrates and caches users after a verified JWT', async () => {
    const user = { id: '1', email: 'root@example.com', roleId: '2' };
    const role = { id: '2', name: 'Admin' };
    const findOne = vi.fn(async ({ table }) => {
      if (table === 'enfyra_user') return user;
      if (table === 'enfyra_role') return role;
      return null;
    });
    const queryBuilder = {
      isMongoDb: () => false,
      findOne,
    };
    const token = await signToken({ id: '1' });
    const req: any = {
      method: 'GET',
      headers: { authorization: `Bearer ${token}` },
      routeData: { context: { $user: null } },
    };
    const next = vi.fn();

    await makeMiddleware(queryBuilder)(req, {} as any, next);
    const secondReq: any = {
      method: 'GET',
      headers: { authorization: `Bearer ${token}` },
      routeData: { context: { $user: null } },
    };
    await makeMiddleware(queryBuilder)(secondReq, {} as any, vi.fn());

    expect(findOne).toHaveBeenCalledTimes(2);
    expect(req.user).toEqual(
      expect.objectContaining({
        id: '1',
        role,
        loginProvider: null,
        tokenType: null,
        apiTokenId: null,
      }),
    );
    expect(secondReq.user).toEqual(expect.objectContaining({ id: '1', role }));
    expect(req.routeData.context.$user).toBe(req.user);
    expect(next).toHaveBeenCalledWith();
  });

  it('verifies a PAT directly without creating a Bearer JWT', async () => {
    primeCachedUserSnapshot('1', {
      id: '1',
      email: 'root@example.com',
      role: { id: '2', name: 'Admin' },
    });
    const verify = vi.fn().mockResolvedValue({
      payload: {
        id: '1',
        loginProvider: 'api_token',
        tokenType: 'api_token',
        tokenId: 'token-1',
      },
      expiresAt: null,
    });
    const req: any = {
      method: 'GET',
      headers: { 'x-enfyra-pat': 'efy_pat_test' },
      routeData: { context: { $user: null } },
    };
    const next = vi.fn();

    await makeMiddleware({ isMongoDb: () => false }, { verify })(
      req,
      {} as any,
      next,
    );

    expect(verify).toHaveBeenCalledWith('efy_pat_test');
    expect(req.headers.authorization).toBeUndefined();
    expect(req.user).toEqual(
      expect.objectContaining({
        id: '1',
        loginProvider: 'api_token',
        tokenType: 'api_token',
        apiTokenId: 'token-1',
      }),
    );
    expect(next).toHaveBeenCalledWith();
  });

  it('skips token verification when no configured auth header is present', async () => {
    const verify = vi.fn();
    const req: any = {
      method: 'GET',
      headers: {
        'content-type': 'application/json',
        'x-request-id': 'req-1',
      },
      routeData: { context: { $user: null } },
    };
    const next = vi.fn();

    await makeMiddlewareWithAuthHeaders(
      [
        {
          id: 1,
          headerKey: 'x-api-key',
          credentialType: 'pat',
          scheme: 'raw',
          priority: 0,
          isEnabled: true,
          isSystem: false,
        },
      ],
      { isMongoDb: () => false },
      { verify },
    )(req, {} as any, next);

    expect(verify).not.toHaveBeenCalled();
    expect(req.user).toBeNull();
    expect(next).toHaveBeenCalledWith();
  });

  it('skips token verification when a configured auth header is empty', async () => {
    const verify = vi.fn();
    const req: any = {
      method: 'GET',
      headers: { 'x-api-key': '   ' },
      routeData: { context: { $user: null } },
    };
    const next = vi.fn();

    await makeMiddlewareWithAuthHeaders(
      [
        {
          id: 1,
          headerKey: 'x-api-key',
          credentialType: 'pat',
          scheme: 'raw',
          priority: 0,
          isEnabled: true,
          isSystem: false,
        },
      ],
      { isMongoDb: () => false },
      { verify },
    )(req, {} as any, next);

    expect(verify).not.toHaveBeenCalled();
    expect(req.user).toBeNull();
    expect(next).toHaveBeenCalledWith();
  });

  it('verifies a PAT from a dynamically configured header', async () => {
    primeCachedUserSnapshot('1', {
      id: '1',
      email: 'root@example.com',
      role: { id: '2', name: 'Admin' },
    });
    const verify = vi.fn().mockResolvedValue({
      payload: {
        id: '1',
        loginProvider: 'api_token',
        tokenType: 'api_token',
        tokenId: 'token-1',
      },
      expiresAt: null,
    });
    const req: any = {
      method: 'GET',
      headers: { 'x-api-key': 'efy_pat_gateway' },
      routeData: { context: { $user: null } },
    };
    const next = vi.fn();

    await makeMiddlewareWithAuthHeaders(
      [
        {
          id: 1,
          headerKey: 'x-api-key',
          credentialType: 'pat',
          scheme: 'raw',
          priority: 120,
          isEnabled: true,
          isSystem: false,
        },
      ],
      { isMongoDb: () => false },
      { verify },
    )(req, {} as any, next);

    expect(verify).toHaveBeenCalledWith('efy_pat_gateway');
    expect(req.user).toEqual(expect.objectContaining({ id: '1' }));
    expect(next).toHaveBeenCalledWith();
  });

  it('uses configured priority when multiple authentication headers are present', async () => {
    primeCachedUserSnapshot('1', {
      id: '1',
      email: 'root@example.com',
      role: { id: '2', name: 'Admin' },
    });
    const verify = vi.fn().mockResolvedValue({
      payload: {
        id: '1',
        loginProvider: 'api_token',
        tokenType: 'api_token',
        tokenId: 'token-1',
      },
      expiresAt: null,
    });
    const req: any = {
      method: 'GET',
      headers: {
        'x-api-key': 'efy_pat_custom_first',
        'x-enfyra-pat': 'efy_pat_system_second',
      },
      routeData: { context: { $user: null } },
    };
    const next = vi.fn();

    await makeMiddlewareWithAuthHeaders(
      [
        {
          id: 1,
          headerKey: 'x-api-key',
          credentialType: 'pat',
          scheme: 'raw',
          priority: 0,
          isEnabled: true,
          isSystem: false,
        },
        {
          id: 2,
          headerKey: 'x-enfyra-pat',
          credentialType: 'pat',
          scheme: 'raw',
          priority: 1,
          isEnabled: true,
          isSystem: true,
        },
      ],
      { isMongoDb: () => false },
      { verify },
    )(req, {} as any, next);

    expect(verify).toHaveBeenCalledWith('efy_pat_custom_first');
    expect(verify).toHaveBeenCalledTimes(1);
    expect(next).toHaveBeenCalledWith();
  });

  it('does not write request token context into cached user snapshots', async () => {
    const cachedUser: any = {
      id: '1',
      email: 'root@example.com',
      role: { id: '2', name: 'Admin' },
    };
    primeCachedUserSnapshot('1', cachedUser);
    const token = await signToken({
      id: '1',
      loginProvider: 'api_token',
      tokenType: 'api_token',
      tokenId: 'token-1',
    });
    const req: any = {
      method: 'GET',
      headers: { authorization: `Bearer ${token}` },
      routeData: { context: { $user: null } },
    };
    const next = vi.fn();

    await makeMiddleware(
      { isMongoDb: () => false },
      {
        validateAccessPayload: vi.fn().mockResolvedValue(true),
      },
    )(req, {} as any, next);

    expect(req.user).toEqual(
      expect.objectContaining({
        id: '1',
        loginProvider: 'api_token',
        tokenType: 'api_token',
        apiTokenId: 'token-1',
      }),
    );
    expect(cachedUser).toEqual({
      id: '1',
      email: 'root@example.com',
      role: { id: '2', name: 'Admin' },
    });
  });
});
