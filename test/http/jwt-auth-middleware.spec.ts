import { afterEach, describe, expect, it, vi } from 'vitest';
import { SignJWT } from 'jose';
import { jwtAuthMiddleware } from '../../src/http/middlewares/jwt-auth.middleware';
import { InvalidTokenException } from '../../src/domain/exceptions';
import {
  clearLocalUserCacheForTesting,
  primeCachedUserSnapshot,
} from '../../src/shared/utils/load-user-with-role.util';

const secret = 'test-secret';

function makeMiddleware(
  queryBuilder: any = {},
  cacheService: any = {},
  apiTokenService?: any,
) {
  return jwtAuthMiddleware(queryBuilder, cacheService, secret, apiTokenService);
}

async function signToken(payload: Record<string, any>) {
  return new SignJWT(payload)
    .setProtectedHeader({ alg: 'HS256' })
    .sign(new TextEncoder().encode(secret));
}

describe('jwtAuthMiddleware', () => {
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

  it('caches hydrated users after a verified JWT', async () => {
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
      {},
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
