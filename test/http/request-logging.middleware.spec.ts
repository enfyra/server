import { afterEach, describe, expect, it, vi } from 'vitest';
import { Logger } from '../../src/shared/logger';
import { requestLoggingEnd } from '../../src/http/middlewares/request-logging.middleware';
import { roleGuardMiddleware } from '../../src/http/middlewares/role-guard.middleware';

describe('request logging middleware', () => {
  afterEach(() => {
    vi.restoreAllMocks();
  });

  it('writes client error responses at warning level', () => {
    const warn = vi.spyOn(Logger.prototype, 'warn');
    const next = vi.fn();

    requestLoggingEnd(
      {
        method: 'POST',
        url: '/gateway/v1/messages',
        query: {},
        startTime: Date.now(),
        user: { id: 7 },
      } as any,
      { statusCode: 400 } as any,
      next,
    );

    expect(warn).toHaveBeenCalledWith(
      expect.objectContaining({
        method: 'POST',
        url: '/gateway/v1/messages',
        statusCode: 400,
        userId: 7,
      }),
    );
    expect(next).toHaveBeenCalledOnce();
  });

  it('logs route context for a role-guard 403 before request logging begins', async () => {
    const warn = vi.spyOn(Logger.prototype, 'warn');
    const response = {
      status: vi.fn().mockReturnThis(),
      json: vi.fn(),
    };
    const next = vi.fn();
    const middleware = roleGuardMiddleware({
      checkRequestAccess: vi.fn(() => ({
        allow: false,
        statusCode: 403,
        message: 'Forbidden',
      })),
    } as any);

    await middleware(
      {
        method: 'GET',
        url: '/gateway/v1/models',
        originalUrl: '/gateway/v1/models',
        routeData: { id: 238, path: '/gateway/v1/*' },
        user: {
          id: 'user-1',
          email: 'member@example.com',
          roles: [{ id: 3 }],
        },
      },
      response as any,
      next,
    );

    expect(warn).toHaveBeenCalledWith(
      expect.objectContaining({
        message: 'Route access denied',
        statusCode: 403,
        routeId: 238,
        routePath: '/gateway/v1/*',
        userId: 'user-1',
        userEmail: 'member@example.com',
        roleIds: [3],
      }),
    );
    expect(response.status).toHaveBeenCalledWith(403);
    expect(next).not.toHaveBeenCalled();
  });
});
