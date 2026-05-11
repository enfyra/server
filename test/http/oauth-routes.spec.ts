import { describe, expect, it, vi } from 'vitest';
import { registerOAuthRoutes } from '../../src/http/routes/oauth.routes';

function createHarness() {
  const handlers = new Map<string, any>();
  const app = {
    get: (path: string, handler: any) => handlers.set(path, handler),
  };
  const oauthService = {
    getAuthorizationUrl: vi.fn(async (_provider: string, state: string) => {
      return `https://accounts.example/auth?state=${state}`;
    }),
    handleCallback: vi.fn(async () => ({
      accessToken: 'access-token',
      refreshToken: 'refresh-token',
      expTime: 123456,
      loginProvider: 'google',
    })),
  };
  const oauthConfigCacheService = {
    getAllProviders: vi.fn(async () => ['google']),
    getDirectConfigByProvider: vi.fn(async () => ({
      clientId: 'client-id',
      clientSecret: 'client-secret',
      redirectUri: 'https://demo.enfyra.io/api/auth/google/callback',
      autoSetCookies: true,
      isEnabled: true,
    })),
  };
  const configService = {
    get: vi.fn(() => 'test-secret'),
  };
  const cradle = { oauthService, oauthConfigCacheService, configService };

  registerOAuthRoutes(app as any, { cradle } as any);

  async function get(path: string, params: Record<string, string>, query: any) {
    const response = {
      location: '',
      redirect: vi.fn((location: string) => {
        response.location = location;
        return response;
      }),
    };
    await handlers.get(path)({ params, query, scope: { cradle } }, response);
    return response;
  }

  return { get };
}

function getStateFromLocation(location: string) {
  return new URL(location).searchParams.get('state') || '';
}

describe('OAuth routes', () => {
  it('defaults auto cookie callback to /api/auth/set-cookies on the redirect origin', async () => {
    const { get } = createHarness();
    const start = await get(
      '/auth/:provider',
      { provider: 'google' },
      { redirect: 'https://demo.enfyra.io/chat' },
    );
    const callback = await get(
      '/auth/:provider/callback',
      { provider: 'google' },
      { code: 'oauth-code', state: getStateFromLocation(start.location) },
    );
    const location = new URL(callback.location);

    expect(`${location.origin}${location.pathname}`).toBe(
      'https://demo.enfyra.io/api/auth/set-cookies',
    );
    expect(location.searchParams.get('redirect')).toBe(
      'https://demo.enfyra.io/chat',
    );
  });

  it.each(['enfyra', '/enfyra', '/enfyra/'])(
    'normalizes cookieBridgePrefix %s for third-app auto cookie callback',
    async (cookieBridgePrefix) => {
    const { get } = createHarness();
    const start = await get(
      '/auth/:provider',
      { provider: 'google' },
      {
        redirect: 'https://chat.example.com/chat',
        cookieBridgePrefix,
      },
    );
    const callback = await get(
      '/auth/:provider/callback',
      { provider: 'google' },
      { code: 'oauth-code', state: getStateFromLocation(start.location) },
    );
    const location = new URL(callback.location);

    expect(`${location.origin}${location.pathname}`).toBe(
      'https://chat.example.com/enfyra/auth/set-cookies',
    );
    expect(location.searchParams.get('redirect')).toBe(
      'https://chat.example.com/chat',
    );
    },
  );
});
