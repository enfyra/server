import { describe, it, expect, beforeEach, vi, afterEach } from 'vitest';
import { OAuthService } from '../../src/domain/auth';
import { DatabaseConfigService } from '../../src/shared/services';

function makeService(): OAuthService {
  return new OAuthService({
    queryBuilderService: {} as any,
    runtimeRegistryService: {} as any,
    envService: {} as any,
    cacheService: {} as any,
    executorEngineService: {} as any,
    dynamicContextFactory: {} as any,
    repoRegistryService: {} as any,
  });
}

const originalFetch = globalThis.fetch;

describe('OAuthService.fetchUserInfo — provider mapping', () => {
  let service: OAuthService;
  let fetchMock: ReturnType<typeof vi.fn>;

  beforeEach(() => {
    service = makeService();
    fetchMock = vi.fn();
    globalThis.fetch = fetchMock as any;
  });

  afterEach(() => {
    globalThis.fetch = originalFetch;
    DatabaseConfigService.resetForTesting();
  });

  function mockJsonResponse(body: any) {
    fetchMock.mockResolvedValueOnce({
      ok: true,
      json: async () => body,
    } as any);
  }

  it('google → maps sub→id, picture→avatar', async () => {
    mockJsonResponse({
      sub: 'g-123',
      email: 'a@b.com',
      name: 'Alice',
      picture: 'https://pic',
    });
    const result = await (service as any).fetchUserInfo(
      'https://userinfo',
      'tok',
      'google',
    );
    expect(result).toEqual({
      profile: {
        providerUserId: 'g-123',
        email: 'a@b.com',
        emailVerified: null,
        name: 'Alice',
        givenName: null,
        familyName: null,
        username: null,
        avatarUrl: 'https://pic',
        profileUrl: null,
        locale: null,
      },
      claims: {},
    });
  });

  it('facebook → maps id, picture.data.url→avatar', async () => {
    mockJsonResponse({
      id: 'fb-456',
      email: 'a@b.com',
      name: 'Alice',
      picture: { data: { url: 'https://fb-pic' } },
    });
    const result = await (service as any).fetchUserInfo(
      'https://userinfo',
      'tok',
      'facebook',
    );
    expect(result).toEqual({
      profile: {
        providerUserId: 'fb-456',
        email: 'a@b.com',
        emailVerified: null,
        name: 'Alice',
        givenName: null,
        familyName: null,
        username: null,
        avatarUrl: 'https://fb-pic',
        profileUrl: null,
        locale: null,
      },
      claims: {},
    });
  });

  it('facebook → handles missing picture (no avatar)', async () => {
    mockJsonResponse({ id: 'fb-1', email: 'x@y.com', name: 'X' });
    const result = await (service as any).fetchUserInfo(
      'https://u',
      'tok',
      'facebook',
    );
    expect(result.profile.avatarUrl).toBeNull();
  });

  it('github → coerces id to string, login fallback for name', async () => {
    mockJsonResponse({
      id: 12345,
      email: 'a@b.com',
      name: null,
      login: 'alice-dev',
      avatar_url: 'https://gh-pic',
    });
    const result = await (service as any).fetchUserInfo(
      'https://userinfo',
      'tok',
      'github',
    );
    expect(result).toEqual({
      profile: {
        providerUserId: '12345',
        email: 'a@b.com',
        emailVerified: null,
        name: 'alice-dev',
        givenName: null,
        familyName: null,
        username: 'alice-dev',
        avatarUrl: 'https://gh-pic',
        profileUrl: null,
        locale: null,
      },
      claims: {},
    });
  });

  it('github → uses name when present, ignores login fallback', async () => {
    mockJsonResponse({
      id: 1,
      email: 'a@b.com',
      name: 'Real Name',
      login: 'realname',
      avatar_url: 'https://x',
    });
    const result = await (service as any).fetchUserInfo(
      'https://u',
      'tok',
      'github',
    );
    expect(result.profile.name).toBe('Real Name');
  });

  it('throws BadRequestException when fetch returns non-ok', async () => {
    fetchMock.mockResolvedValueOnce({
      ok: false,
      text: async () => 'invalid token',
    } as any);
    await expect(
      (service as any).fetchUserInfo('https://u', 'bad-tok', 'google'),
    ).rejects.toThrow(/Failed to fetch user info/);
  });

  it('throws clear error when provider is outside the supported union (runtime safety)', async () => {
    mockJsonResponse({ id: 'x', email: 'a@b.com' });
    await expect(
      (service as any).fetchUserInfo('https://u', 'tok', 'apple' as any),
    ).rejects.toThrow(/Unsupported OAuth provider: apple/);
  });

  it('exhaustiveness throw mentions where to fix', async () => {
    mockJsonResponse({ id: 'x' });
    try {
      await (service as any).fetchUserInfo(
        'https://u',
        'tok',
        'discord' as any,
      );
      throw new Error('should have thrown');
    } catch (e: any) {
      expect(e.message).toContain('Add a case to fetchUserInfo()');
    }
  });

  it('throws even when API returns valid-looking data for unknown provider', async () => {
    mockJsonResponse({ id: 'x', email: 'fake@x.com', name: 'X' });
    await expect(
      (service as any).fetchUserInfo('https://u', 'tok', 'twitter' as any),
    ).rejects.toThrow(/Unsupported OAuth provider/);
  });

  it('ignores return values from OAuth lifecycle scripts', async () => {
    const ctx = {
      $user: { id: 'user-1', email: 'a@b.com' },
      $data: { oauth: { event: 'user_created' } },
    };
    const run = vi.fn().mockResolvedValue({ role: { id: 2 } });
    service = new OAuthService({
      queryBuilderService: {} as any,
      runtimeRegistryService: {} as any,
      envService: {} as any,
      cacheService: {} as any,
      executorEngineService: { run } as any,
      dynamicContextFactory: {
        createBase: vi.fn().mockReturnValue({}),
      } as any,
      repoRegistryService: {
        createReposProxy: vi.fn().mockReturnValue({}),
      } as any,
    });

    await expect(
      (service as any).runOAuthLifecycleScript(
        {
          sourceCode: 'return { role: { id: 2 } }',
          compiledCode: 'return { role: { id: 2 } }',
          scriptLanguage: 'typescript',
        },
        ctx,
      ),
    ).resolves.toBeUndefined();
    expect(run).toHaveBeenCalledWith(
      expect.any(String),
      ctx,
      30000,
      expect.objectContaining({ sourceKind: 'oauth' }),
    );
  });

  it('allows empty OAuth lifecycle scripts', async () => {
    const run = vi.fn();
    service = new OAuthService({
      queryBuilderService: {} as any,
      runtimeRegistryService: {} as any,
      envService: {} as any,
      cacheService: {} as any,
      executorEngineService: { run } as any,
      dynamicContextFactory: {
        createBase: vi.fn().mockReturnValue({}),
      } as any,
      repoRegistryService: {
        createReposProxy: vi.fn().mockReturnValue({}),
      } as any,
    });

    await expect(
      (service as any).runOAuthLifecycleScript(
        {
          sourceCode: null,
          compiledCode: null,
          scriptLanguage: 'typescript',
        },
        { $user: { id: 'user-1' } },
      ),
    ).resolves.toBeUndefined();
    expect(run).not.toHaveBeenCalled();
  });

  it('accepts scripts that return no value', async () => {
    service = new OAuthService({
      queryBuilderService: {} as any,
      runtimeRegistryService: {} as any,
      envService: {} as any,
      cacheService: {} as any,
      executorEngineService: {
        run: vi.fn().mockResolvedValue(undefined),
      } as any,
      dynamicContextFactory: {
        createBase: vi.fn().mockReturnValue({}),
      } as any,
      repoRegistryService: {
        createReposProxy: vi.fn().mockReturnValue({}),
      } as any,
    });

    await expect(
      (service as any).runOAuthLifecycleScript(
        {
          sourceCode:
            'await @REPOS.enfyra_user.update({ id: @USER.id, data: {} })',
          compiledCode:
            'await $ctx.$repos.enfyra_user.update({ id: $ctx.$user.id, data: {} })',
          scriptLanguage: 'typescript',
        },
        { $user: { id: 'user-1' }, $repos: {} },
      ),
    ).resolves.toBeUndefined();
  });

  it('uses the relation user id when an existing SQL OAuth account is loaded', async () => {
    DatabaseConfigService.overrideForTesting('postgres');
    const findOne = vi
      .fn()
      .mockResolvedValueOnce({
        id: 7,
        provider: 'google',
        providerUserId: 'google-user-1',
        user: { id: 'user-1', email: 'user@example.com' },
      })
      .mockResolvedValueOnce({ id: 'user-1', email: 'user@example.com' });
    service = new OAuthService({
      queryBuilderService: {
        isMongoDb: vi.fn().mockReturnValue(false),
        findOne,
      } as any,
      runtimeRegistryService: {} as any,
      envService: {} as any,
      cacheService: {} as any,
      executorEngineService: {} as any,
      dynamicContextFactory: {} as any,
      repoRegistryService: {} as any,
    });

    await expect(
      (service as any).findOrCreateUser('google', {
        profile: {
          providerUserId: 'google-user-1',
          email: 'user@example.com',
        },
        claims: {},
      }),
    ).resolves.toEqual({
      user: { id: 'user-1', email: 'user@example.com' },
      isNewUser: false,
    });

    expect(findOne).toHaveBeenNthCalledWith(2, {
      table: 'enfyra_user',
      where: { id: 'user-1' },
    });
  });

  it('fails clearly when an existing OAuth account has no linked user id', async () => {
    DatabaseConfigService.overrideForTesting('postgres');
    const findOne = vi.fn().mockResolvedValueOnce({
      id: 7,
      provider: 'google',
      providerUserId: 'google-user-1',
    });
    service = new OAuthService({
      queryBuilderService: {
        isMongoDb: vi.fn().mockReturnValue(false),
        findOne,
      } as any,
      runtimeRegistryService: {} as any,
      envService: {} as any,
      cacheService: {} as any,
      executorEngineService: {} as any,
      dynamicContextFactory: {} as any,
      repoRegistryService: {} as any,
    });

    await expect(
      (service as any).findOrCreateUser('google', {
        profile: {
          providerUserId: 'google-user-1',
          email: 'user@example.com',
        },
        claims: {},
      }),
    ).rejects.toThrow(/missing user relation/);

    expect(findOne).toHaveBeenCalledTimes(1);
  });
});
