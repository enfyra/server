import { createHmac } from 'node:crypto';
import { describe, expect, it, vi } from 'vitest';
import { registerOAuthRoutes } from '../../src/http/routes/oauth.routes';
import { registerAuthRoutes } from '../../src/http/routes/auth.routes';

function createHarness() {
  const handlers = new Map<string, (req: any, res: any) => Promise<unknown>>();
  const config = {
    redirectUri: 'https://app.example.test/api/auth/google/callback',
    autoSetCookies: true,
    appCallbackUrl: null as string | null,
  };
  const find = vi.fn(async () => ({
    data: [] as Array<{ value: string; isEnabled?: boolean }>,
  }));
  const getAuthorizationUrl = vi.fn(
    async (_provider: string, state: string) =>
      `https://provider.example.test/auth?state=${state}`,
  );
  const handleCallback = vi.fn(async () => ({
    accessToken: 'access',
    refreshToken: 'refresh',
    expTime: 123,
    sessionId: 'session',
  }));
  const createCodeForTokens = vi.fn(async () => 'exchange-code');
  const exchange = vi.fn(async () => ({
    accessToken: 'access',
    refreshToken: 'refresh',
    expTime: 123,
  }));
  const cradle = {
    queryBuilderService: { find },
    runtimeRegistryService: {
      getOauthProviders: () => ['google'],
      getOauthConfigByProvider: () => config,
    },
    oauthService: { getAuthorizationUrl, handleCallback },
    oauthExchangeCodeService: { createCodeForTokens, exchange },
    configService: { get: () => 'test-secret' },
  };
  const app = {
    get: (path: string, handler: any) => handlers.set(path, handler),
    post: () => {},
    delete: () => {},
  };
  registerOAuthRoutes(app as any, { cradle } as any);
  registerAuthRoutes(app as any, { cradle } as any);
  async function get(
    path: string,
    query: unknown,
    headers: Record<string, string> = {},
  ) {
    const res = {
      location: '',
      headers: new Map(),
      redirect: (url: string) => {
        res.location = url;
      },
      setHeader: (key: string, value: unknown) => res.headers.set(key, value),
    };
    await handlers.get(path)!(
      {
        params: { provider: 'google' },
        query,
        headers,
        protocol: 'http',
        scope: { cradle },
      },
      res,
    );
    return res;
  }
  return {
    get,
    find,
    config,
    getAuthorizationUrl,
    handleCallback,
    createCodeForTokens,
    exchange,
  };
}

function signedState(redirect: string) {
  const p = JSON.stringify({ redirect, ts: Date.now() });
  const s = createHmac('sha256', 'test-secret').update(p).digest('hex');
  return Buffer.from(JSON.stringify({ p, s })).toString('base64url');
}

describe('ESV OAuth redirect origin policy', () => {
  it('rejects a third-party initiation before requesting provider authorization', async () => {
    const h = createHarness();
    await expect(
      h.get('/auth/:provider', {
        redirect: 'https://evil.example.test/return',
      }),
    ).rejects.toMatchObject({ statusCode: 403 });
    expect(h.getAuthorizationUrl).not.toHaveBeenCalled();
  });

  it('allows the configured app origin when the allowlist is empty', async () => {
    const h = createHarness();
    await expect(
      h.get('/auth/:provider', { redirect: 'https://app.example.test/chat' }),
    ).resolves.toMatchObject({
      location: expect.stringContaining('https://provider.example.test/'),
    });
    expect(h.find).toHaveBeenCalledWith(
      expect.objectContaining({
        table: 'enfyra_auth_origin',
        fields: ['value', 'isEnabled'],
      }),
    );
  });

  it('keeps the configured app origin usable for initiation, callback and cookie finalization with an external allowlist', async () => {
    const h = createHarness();
    h.find.mockResolvedValue({ data: [{ value: 'https://client.example.test' }] });
    const start = await h.get('/auth/:provider', { redirect: 'https://app.example.test/settings' });
    const callback = await h.get('/auth/:provider/callback', {
      code: 'provider-code', state: new URL(start.location).searchParams.get('state'),
    });
    expect(new URL(callback.location).origin).toBe('https://app.example.test');
    const finalized = await h.get('/auth/set-cookies', {
      code: 'exchange-code', redirect: 'https://app.example.test/settings',
    });
    expect(finalized.location).toBe('https://app.example.test/settings');
    expect(h.exchange).toHaveBeenCalledWith('exchange-code');
  });

  it('allows an exact enabled origin in a nonempty allowlist', async () => {
    const h = createHarness();
    h.find.mockResolvedValue({
      data: [{ value: ' https://client.example.test ', isEnabled: true }],
    });
    const start = await h.get('/auth/:provider', {
      redirect: 'https://client.example.test/chat',
      cookieBridgePrefix: '/enfyra',
    });
    const state = new URL(start.location).searchParams.get('state');
    const callback = await h.get('/auth/:provider/callback', {
      code: 'provider-code',
      state,
    });
    expect(new URL(callback.location).origin).toBe(
      'https://client.example.test',
    );
  });

  it.each([
    'https://client.example.test.evil.test/return',
    'http://client.example.test/return',
    'https://client.example.test:8443/return',
    'http://app.example.test/return',
    'https://app.example.test:8443/return',
    'https://app.example.test.evil.test/return',
  ])(
    'rejects a redirect outside the nonempty allowlist: %s',
    async (redirect) => {
      const h = createHarness();
      h.find.mockResolvedValue({
        data: [{ value: 'https://client.example.test' }],
      });
      await expect(
        h.get('/auth/:provider', { redirect }),
      ).rejects.toMatchObject({ statusCode: 403 });
    },
  );

  it('ignores disabled entries and does not trust caller host/forwarding headers', async () => {
    const h = createHarness();
    h.find.mockResolvedValue({
      data: [{ value: 'https://evil.example.test', isEnabled: false }],
    });
    await expect(
      h.get(
        '/auth/:provider',
        { redirect: 'https://evil.example.test/return' },
        {
          host: 'evil.example.test',
          'x-forwarded-host': 'evil.example.test',
          'x-forwarded-proto': 'https',
        },
      ),
    ).rejects.toMatchObject({ statusCode: 403 });
  });

  it('uses same-app-origin fallback when reading the allowlist fails', async () => {
    const h = createHarness();
    h.find.mockRejectedValue(new Error('unavailable'));
    await expect(
      h.get('/auth/:provider', { redirect: 'https://app.example.test/chat' }),
    ).resolves.toBeDefined();
    await expect(
      h.get('/auth/:provider', { redirect: 'https://evil.example.test/chat' }),
    ).rejects.toMatchObject({ statusCode: 403 });
  });

  it.each([
    'javascript:alert(1)',
    '//evil.example.test/path',
    ['https://app.example.test/chat'],
  ])('rejects malformed redirect input: %s', async (redirect) => {
    const h = createHarness();
    await expect(h.get('/auth/:provider', { redirect })).rejects.toMatchObject({
      statusCode: 400,
    });
  });

  it.each([{ code: 'provider-code' }, { error: 'access_denied' }])(
    'revalidates a signed third-party redirect before success or error callback: %s',
    async (query) => {
      const h = createHarness();
      await expect(
        h.get('/auth/:provider/callback', {
          ...query,
          state: signedState('https://evil.example.test/return'),
        }),
      ).rejects.toMatchObject({ statusCode: 403 });
      expect(h.handleCallback).not.toHaveBeenCalled();
      expect(h.createCodeForTokens).not.toHaveBeenCalled();
    },
  );

  it('rejects a callback when its origin has been removed from the allowlist', async () => {
    const h = createHarness();
    h.find.mockResolvedValue({
      data: [{ value: 'https://client.example.test' }],
    });
    const start = await h.get('/auth/:provider', {
      redirect: 'https://client.example.test/return',
    });
    h.find.mockResolvedValue({ data: [] });
    await expect(
      h.get('/auth/:provider/callback', {
        code: 'provider-code',
        state: new URL(start.location).searchParams.get('state'),
      }),
    ).rejects.toMatchObject({ statusCode: 403 });
    expect(h.handleCallback).not.toHaveBeenCalled();
  });

  it('validates a configured non-auto cookie callback before creating a session', async () => {
    const h = createHarness();
    h.config.autoSetCookies = false;
    h.config.appCallbackUrl = 'https://evil.example.test/complete';
    await expect(
      h.get('/auth/:provider/callback', {
        code: 'provider-code',
        state: signedState('https://app.example.test/return'),
      }),
    ).rejects.toMatchObject({ statusCode: 403 });
    expect(h.handleCallback).not.toHaveBeenCalled();
  });

  it('allows a non-auto cookie callback explicitly present in the allowlist', async () => {
    const h = createHarness();
    h.config.autoSetCookies = false;
    h.config.appCallbackUrl = 'https://client.example.test/complete';
    h.find.mockResolvedValue({
      data: [
        { value: 'https://app.example.test' },
        { value: 'https://client.example.test' },
      ],
    });
    const start = await h.get('/auth/:provider', {
      redirect: 'https://app.example.test/return',
    });
    const callback = await h.get('/auth/:provider/callback', {
      code: 'provider-code',
      state: new URL(start.location).searchParams.get('state'),
    });
    expect(new URL(callback.location).origin).toBe(
      'https://client.example.test',
    );
  });

  it('preserves the validated provider-error fallback when no app callback is configured', async () => {
    const h = createHarness();
    h.config.autoSetCookies = false;
    const result = await h.get('/auth/:provider/callback', {
      error: 'access_denied',
      state: signedState('https://app.example.test/return'),
    });
    expect(new URL(result.location).origin).toBe('https://app.example.test');
    expect(h.handleCallback).not.toHaveBeenCalled();
  });

  it('fails closed without a trusted configured app origin', async () => {
    const h = createHarness();
    h.config.redirectUri = '';
    await expect(
      h.get(
        '/auth/:provider',
        { redirect: 'https://evil.example.test/return' },
        { host: 'evil.example.test' },
      ),
    ).rejects.toMatchObject({ statusCode: 400 });
  });

  it.each([{ code: 'exchange-code' }, { error: 'access_denied' }])(
    'validates the direct cookie finalizer before exchanging or redirecting: %s',
    async (query) => {
      const h = createHarness();
      await expect(
        h.get('/auth/set-cookies', {
          ...query,
          redirect: 'https://evil.example.test/return',
        }),
      ).rejects.toMatchObject({ statusCode: 403 });
      expect(h.exchange).not.toHaveBeenCalled();
    },
  );

  it('allows same-app-origin cookie finalization', async () => {
    const h = createHarness();
    const res = await h.get('/auth/set-cookies', {
      code: 'exchange-code',
      redirect: 'https://app.example.test/return',
    });
    expect(res.location).toBe('https://app.example.test/return');
    expect(h.exchange).toHaveBeenCalledWith('exchange-code');
  });
});
