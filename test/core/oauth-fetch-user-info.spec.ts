import { describe, it, expect, beforeEach, vi, afterEach } from 'vitest';
import { OAuthService } from '../../src/core/auth/services/oauth.service';

function makeService(): OAuthService {
  return new OAuthService({
    queryBuilderService: {} as any,
    oauthConfigCacheService: {} as any,
    envService: {} as any,
    cacheService: {} as any,
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
    const result = await (service as any).fetchUserInfo('https://userinfo', 'tok', 'google');
    expect(result).toEqual({
      id: 'g-123',
      email: 'a@b.com',
      name: 'Alice',
      avatar: 'https://pic',
    });
  });

  it('facebook → maps id, picture.data.url→avatar', async () => {
    mockJsonResponse({
      id: 'fb-456',
      email: 'a@b.com',
      name: 'Alice',
      picture: { data: { url: 'https://fb-pic' } },
    });
    const result = await (service as any).fetchUserInfo('https://userinfo', 'tok', 'facebook');
    expect(result).toEqual({
      id: 'fb-456',
      email: 'a@b.com',
      name: 'Alice',
      avatar: 'https://fb-pic',
    });
  });

  it('facebook → handles missing picture (no avatar)', async () => {
    mockJsonResponse({ id: 'fb-1', email: 'x@y.com', name: 'X' });
    const result = await (service as any).fetchUserInfo('https://u', 'tok', 'facebook');
    expect(result.avatar).toBeUndefined();
  });

  it('github → coerces id to string, login fallback for name', async () => {
    mockJsonResponse({
      id: 12345,
      email: 'a@b.com',
      name: null,
      login: 'alice-dev',
      avatar_url: 'https://gh-pic',
    });
    const result = await (service as any).fetchUserInfo('https://userinfo', 'tok', 'github');
    expect(result).toEqual({
      id: '12345',
      email: 'a@b.com',
      name: 'alice-dev',
      avatar: 'https://gh-pic',
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
    const result = await (service as any).fetchUserInfo('https://u', 'tok', 'github');
    expect(result.name).toBe('Real Name');
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
      await (service as any).fetchUserInfo('https://u', 'tok', 'discord' as any);
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
});
