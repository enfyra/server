import { registerAuthRoutes } from '../../src/http/routes/auth.routes';
import { registerMeRoutes } from '../../src/http/routes/me.routes';
import { sanitizeExtensionBuildName } from '../../src/modules/extension-definition/utils/compiler.util';
import {
  assertValidVueSFC,
  isProbablyVueSFC,
} from '../../src/modules/extension-definition/utils/validation.util';

function createResponse() {
  return {
    body: undefined as unknown,
    json(payload: unknown) {
      this.body = payload;
      return this;
    },
  };
}

function createRateLimitService(allowed: boolean) {
  return {
    calls: [] as string[],
    async check(
      key: string,
      options: { maxRequests: number; perSeconds: number },
    ) {
      this.calls.push(key);
      return {
        allowed,
        remaining: allowed ? options.maxRequests - 1 : 0,
        resetAt: Date.now() + options.perSeconds * 1000,
        retryAfter: allowed ? 0 : 30,
        limit: options.maxRequests,
        window: options.perSeconds,
      };
    },
  };
}

function createAppRecorder() {
  const routes = new Map<string, (req: any, res: any) => Promise<void>>();
  return {
    post(path: string, handler: (req: any, res: any) => Promise<void>) {
      routes.set(`POST ${path}`, handler);
    },
    get(path: string, handler: (req: any, res: any) => Promise<void>) {
      routes.set(`GET ${path}`, handler);
    },
    patch(path: string, handler: (req: any, res: any) => Promise<void>) {
      routes.set(`PATCH ${path}`, handler);
    },
    delete(path: string, handler: (req: any, res: any) => Promise<void>) {
      routes.set(`DELETE ${path}`, handler);
    },
    routes,
  };
}

describe('GitHub Advanced Security hardening', () => {
  it('validates Vue SFC tags without case-sensitive HTML regex assumptions', () => {
    const sfc = '<SCRIPT setup>const ok = true</script >';

    expect(isProbablyVueSFC(sfc)).toBe(true);
    expect(() => assertValidVueSFC(sfc)).not.toThrow();
  });

  it('sanitizes extension build names before path or browser key usage', () => {
    expect(sanitizeExtensionBuildName('../bad/name";alert(1)//')).toBe(
      '___bad_name__alert_1___',
    );
    expect(sanitizeExtensionBuildName('')).toBe('extension');
  });

  it('rate-limits built-in auth login before calling the auth service', async () => {
    const app = createAppRecorder();
    const rateLimitService = createRateLimitService(false);
    const authService = {
      login: vi.fn(),
    };
    const container = {
      cradle: { rateLimitService, authService },
    };

    registerAuthRoutes(app as any, container as any);

    await expect(
      app.routes.get('POST /auth/login')!(
        {
          body: { email: 'user@example.com', password: 'secret' },
          headers: {},
          ip: '127.0.0.1',
          path: '/auth/login',
          route: { path: '/auth/login' },
        },
        createResponse(),
      ),
    ).rejects.toMatchObject({ statusCode: 429 });

    expect(authService.login).not.toHaveBeenCalled();
    expect(rateLimitService.calls[0]).toContain('builtin-auth:');
  });

  it('rate-limits built-in OAuth account reads before calling the account service', async () => {
    const app = createAppRecorder();
    const rateLimitService = createRateLimitService(false);
    const meService = {
      find: vi.fn(),
      update: vi.fn(),
      findOAuthAccounts: vi.fn(),
    };
    const container = {
      cradle: { rateLimitService, meService },
    };

    registerMeRoutes(app as any, container as any);

    await expect(
      app.routes.get('GET /me/oauth-accounts')!(
        {
          headers: {},
          ip: '127.0.0.1',
          path: '/me/oauth-accounts',
          route: { path: '/me/oauth-accounts' },
          user: { id: 123 },
        },
        createResponse(),
      ),
    ).rejects.toMatchObject({ statusCode: 429 });

    expect(meService.findOAuthAccounts).not.toHaveBeenCalled();
    expect(rateLimitService.calls[0]).toContain('builtin-me:123:');
  });
});
