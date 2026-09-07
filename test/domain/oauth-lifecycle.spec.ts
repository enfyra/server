import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { OAuthService } from '../../src/domain/auth';
import { DatabaseConfigService } from '../../src/shared/services';

const originalFetch = globalThis.fetch;

type HarnessOptions = {
  existingAccount?: any;
  existingUser?: any;
  scriptError?: Error;
  userInsertError?: Error & { code?: string };
};

function createHarness(options: HarnessOptions = {}) {
  const events: string[] = [];
  let transactionActive = false;
  const ctx: any = {
    $data: undefined,
    $repos: {},
    $user: null,
  };
  ctx.$transaction = {
    run: vi.fn(async (callback: () => Promise<unknown>) => {
      events.push('transaction:begin');
      transactionActive = true;
      try {
        const result = await callback();
        events.push('transaction:commit');
        return result;
      } catch (error) {
        events.push('transaction:rollback');
        throw error;
      } finally {
        transactionActive = false;
      }
    }),
  };

  const findOne = vi.fn(async ({ table }: { table: string }) => {
    events.push(`find:${table}:${transactionActive}`);
    if (table === 'enfyra_oauth_account') {
      return options.existingAccount ?? null;
    }
    if (table === 'enfyra_user') {
      if (options.existingAccount) {
        return (
          options.existingUser ?? {
            id: 'user-existing',
            email: 'oauth@example.com',
          }
        );
      }
      return options.existingUser ?? null;
    }
    return null;
  });
  const insert = vi.fn(async (table: string, data: Record<string, unknown>) => {
    events.push(`insert:${table}:${transactionActive}`);
    if (table === 'enfyra_user') {
      if (options.userInsertError) throw options.userInsertError;
      return { ...data, id: data.id };
    }
    if (table === 'enfyra_oauth_account') return { ...data, id: 1 };
    if (table === 'enfyra_session') {
      return { ...data, id: 'session-1' };
    }
    return data;
  });
  const update = vi.fn(async (table: string) => {
    events.push(`update:${table}:${transactionActive}`);
    return {};
  });
  const run = vi.fn(async (_code: string, executionContext: any) => {
    events.push(
      `script:${executionContext.$data.oauth.event}:${transactionActive}`,
    );
    if (options.scriptError) throw options.scriptError;
    return undefined;
  });
  const queryBuilderService = {
    findOne,
    insert,
    update,
    isMongoDb: vi.fn().mockReturnValue(false),
  };
  const service = new OAuthService({
    queryBuilderService: queryBuilderService as any,
    runtimeRegistryService: {
      getOauthConfigByProvider: vi.fn().mockReturnValue({
        id: 1,
        provider: 'google',
        clientId: 'client-id',
        clientSecret: 'client-secret',
        redirectUri: 'https://api.example.com/auth/google/callback',
        autoSetCookies: true,
        sourceCode: 'return undefined;',
        compiledCode: 'return undefined;',
        scriptLanguage: 'typescript',
        isEnabled: true,
      }),
    } as any,
    envService: {
      get: vi.fn((key: string) => {
        if (key === 'SECRET_KEY') return 'test-secret';
        if (key === 'ACCESS_TOKEN_EXP') return '1h';
        if (key === 'REFRESH_TOKEN_REMEMBER_EXP') return '1d';
        return undefined;
      }),
    } as any,
    cacheService: undefined as any,
    executorEngineService: { run } as any,
    dynamicContextFactory: {
      createBase: vi.fn().mockReturnValue(ctx),
    } as any,
    repoRegistryService: {
      createReposProxy: vi.fn().mockReturnValue({}),
    } as any,
  });

  return {
    ctx,
    events,
    findOne,
    insert,
    run,
    service,
    transactionRun: ctx.$transaction.run,
    update,
  };
}

function mockGoogleOAuth() {
  const fetchMock = vi
    .fn()
    .mockResolvedValueOnce({
      ok: true,
      json: async () => ({
        access_token: 'provider-access-token',
        token_type: 'Bearer',
        scope: 'openid email profile',
        expires_in: 3600,
      }),
    })
    .mockResolvedValueOnce({
      ok: true,
      json: async () => ({
        sub: 'google-user-1',
        email: 'oauth@example.com',
        email_verified: true,
        name: 'OAuth User',
        given_name: 'OAuth',
        family_name: 'User',
        picture: 'https://example.com/avatar.png',
        locale: 'en',
        hd: 'example.com',
      }),
    });
  globalThis.fetch = fetchMock as any;
  return fetchMock;
}

describe('OAuth lifecycle transaction', () => {
  beforeEach(() => {
    DatabaseConfigService.overrideForTesting('postgres');
  });

  afterEach(() => {
    globalThis.fetch = originalFetch;
    DatabaseConfigService.resetForTesting();
  });

  it('creates a user, runs the lifecycle script, and creates auth records in one transaction', async () => {
    mockGoogleOAuth();
    const harness = createHarness();

    await expect(
      harness.service.handleCallback('google', 'oauth-code'),
    ).resolves.toEqual(
      expect.objectContaining({
        accessToken: expect.any(String),
        refreshToken: expect.any(String),
        sessionId: 'session-1',
      }),
    );

    expect(harness.transactionRun).toHaveBeenCalledTimes(1);
    expect(harness.ctx.$user).toEqual(
      expect.objectContaining({
        id: expect.any(String),
        email: 'oauth@example.com',
      }),
    );
    expect(harness.ctx.$data).toEqual({
      oauth: {
        event: 'user_created',
        provider: 'google',
        profile: {
          providerUserId: 'google-user-1',
          email: 'oauth@example.com',
          emailVerified: true,
          name: 'OAuth User',
          givenName: 'OAuth',
          familyName: 'User',
          username: null,
          avatarUrl: 'https://example.com/avatar.png',
          profileUrl: null,
          locale: 'en',
        },
        claims: { hd: 'example.com' },
        accessToken: 'provider-access-token',
        token: {
          type: 'Bearer',
          scopes: ['openid', 'email', 'profile'],
          expiresAt: expect.any(String),
        },
      },
    });
    expect(harness.events).toEqual([
      'transaction:begin',
      'find:enfyra_oauth_account:true',
      'find:enfyra_user:true',
      'insert:enfyra_user:true',
      'insert:enfyra_oauth_account:true',
      'script:user_created:true',
      'insert:enfyra_session:true',
      'update:enfyra_session:true',
      'transaction:commit',
    ]);
  });

  it('runs the same script with a login event for an existing OAuth account', async () => {
    mockGoogleOAuth();
    const harness = createHarness({
      existingAccount: {
        id: 1,
        provider: 'google',
        providerUserId: 'google-user-1',
        user: { id: 'user-existing' },
      },
      existingUser: {
        id: 'user-existing',
        email: 'oauth@example.com',
      },
    });

    await harness.service.handleCallback('google', 'oauth-code');

    expect(harness.ctx.$data.oauth.event).toBe('login');
    expect(harness.run).toHaveBeenCalledTimes(1);
    expect(
      harness.insert.mock.calls.some(([table]) => table === 'enfyra_user'),
    ).toBe(false);
    expect(
      harness.insert.mock.calls.some(
        ([table]) => table === 'enfyra_oauth_account',
      ),
    ).toBe(false);
  });

  it('rejects an unlinked OAuth login when the email already belongs to a user', async () => {
    mockGoogleOAuth();
    const harness = createHarness({
      existingUser: {
        id: 'user-existing',
        email: 'oauth@example.com',
      },
    });

    await expect(
      harness.service.handleCallback('google', 'oauth-code'),
    ).rejects.toMatchObject({ statusCode: 409, code: 'CONFLICT' });

    expect(harness.events.at(-1)).toBe('transaction:rollback');
    expect(harness.insert).not.toHaveBeenCalled();
    expect(harness.run).not.toHaveBeenCalled();
  });

  it('rolls back before session creation when the lifecycle script fails', async () => {
    mockGoogleOAuth();
    const harness = createHarness({
      scriptError: new Error('provisioning failed'),
    });

    await expect(
      harness.service.handleCallback('google', 'oauth-code'),
    ).rejects.toThrow('provisioning failed');

    expect(harness.events.at(-1)).toBe('transaction:rollback');
    expect(
      harness.insert.mock.calls.some(([table]) => table === 'enfyra_session'),
    ).toBe(false);
  });

  it('turns a concurrent unique-email insert into a conflict and rolls back', async () => {
    mockGoogleOAuth();
    const duplicateError = Object.assign(new Error('duplicate'), {
      code: '23505',
    });
    const harness = createHarness({ userInsertError: duplicateError });

    await expect(
      harness.service.handleCallback('google', 'oauth-code'),
    ).rejects.toMatchObject({ statusCode: 409, code: 'CONFLICT' });

    expect(harness.events.at(-1)).toBe('transaction:rollback');
    expect(harness.run).not.toHaveBeenCalled();
  });
});
