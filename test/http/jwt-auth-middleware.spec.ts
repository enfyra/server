import { describe, expect, it, vi } from 'vitest';
import { jwtAuthMiddleware } from '../../src/http/middlewares/jwt-auth.middleware';
import { InvalidTokenException } from '../../src/domain/exceptions';

function makeMiddleware() {
  return jwtAuthMiddleware({} as any, {} as any, 'test-secret');
}

describe('jwtAuthMiddleware', () => {
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
});
