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
const authRevisionCache = {
  get: vi.fn(async () => 'test'),
  set: vi.fn(async () => undefined),
};

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
    cacheService: authRevisionCache as any,
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
        routeMethodConfig: { isPublic: true },
        context: { $user: 'existing', $authSource: 'pat' },
      },
    };
    const next = vi.fn();

    await makeMiddleware()(req, {} as any, next);

    expect(req.user).toBeNull();
    expect(req.routeData.context.$user).toBeNull();
    expect(req.routeData.context.$authSource).toBeUndefined();
    expect(Object.hasOwn(req.routeData.context, '$authSource')).toBe(true);
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
    const user = { id: '1', email: 'root@example.com', roles: [{ id: '2' }] };
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
        roles: [role],
        loginProvider: null,
        tokenType: null,
        apiTokenId: null,
      }),
    );
    expect(secondReq.user).toEqual(expect.objectContaining({ id: '1', roles: [role] }));
    expect(req.routeData.context.$user).toBe(req.user);
    expect(next).toHaveBeenCalledWith();
  });

  it('verifies a PAT directly without creating a Bearer JWT', async () => {
    primeCachedUserSnapshot('1', {
      id: '1',
      email: 'root@example.com',
      roles: [{ id: '2', name: 'Admin' }],
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

  it.each(['efy_pat_test', 'sk-1e_fixture'])('rejects non-JWT Bearer credentials %s without native verification', async token => {
    const verify = vi.fn();
    const req: any = { method: 'GET', headers: { authorization: `Bearer ${token}` }, routeData: { context: { $user: null } } };
    const next = vi.fn();
    await makeMiddleware({}, { verify })(req, {} as any, next);
    expect(verify).not.toHaveBeenCalled();
    expect(next).toHaveBeenCalledWith(expect.any(InvalidTokenException));
  });

  it.each([{}, { 'x-api-key': 'efy_pat_gateway' }, { 'x-api-key': '   ' }])('ignores non-native credential headers %j', async headers => {
    const verify = vi.fn();
    const req: any = { method: 'GET', headers, routeData: { context: { $user: null } } };
    const next = vi.fn();
    await makeMiddleware({}, { verify })(req, {} as any, next);
    expect(verify).not.toHaveBeenCalled();
    expect(req.user).toBeNull();
    expect(next).toHaveBeenCalledWith();
  });

  it('uses only the fixed native PAT header when a custom header is also present', async () => {
    primeCachedUserSnapshot('1', { id: '1', roles: [{ id: '2', name: 'Admin' }] });
    const verify = vi.fn().mockResolvedValue({ payload: { id: '1', loginProvider: 'api_token', tokenType: 'api_token', tokenId: 'token-1' }, expiresAt: null });
    const req: any = { method: 'GET', headers: { 'x-api-key': 'efy_pat_custom', 'x-enfyra-pat': 'efy_pat_native' }, routeData: { context: { $user: null } } };
    const next = vi.fn();
    await makeMiddleware({ isMongoDb: () => false }, { verify })(req, {} as any, next);
    expect(verify).toHaveBeenCalledExactlyOnceWith('efy_pat_native');
    expect(next).toHaveBeenCalledWith();
  });

  it.each([{ authorization: ['Bearer aaa.bbb.ccc'] }, { 'x-enfyra-pat': ['efy_pat_one', 'efy_pat_two'] }])('rejects duplicate native headers %j', async headers => {
    const verify = vi.fn();
    const req: any = { method: 'GET', headers, routeData: { context: { $user: null } } };
    const next = vi.fn();
    await makeMiddleware({}, { verify })(req, {} as any, next);
    expect(verify).not.toHaveBeenCalled();
    expect(next).toHaveBeenCalledWith(expect.any(InvalidTokenException));
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
