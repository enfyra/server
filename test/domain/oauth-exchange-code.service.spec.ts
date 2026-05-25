import { describe, expect, it, vi } from 'vitest';
import { OAuthExchangeCodeService } from '../../src/domain/auth';

function createHarness() {
  const store = new Map<string, any>();
  const redis = {
    zadd: vi.fn(async () => 1),
    zrangebyscore: vi.fn(async () => [] as string[]),
    zrem: vi.fn(async () => 1),
  };
  const cacheService = {
    get: vi.fn(async (key: string) => store.get(key) ?? null),
    set: vi.fn(async (key: string, value: any) => {
      store.set(key, value);
    }),
    deleteKey: vi.fn(async (key: string) => {
      store.delete(key);
    }),
  };
  const queryBuilderService = {
    delete: vi.fn(async () => undefined),
  };
  const envService = {
    get: vi.fn(() => null),
  };
  const service = new OAuthExchangeCodeService({
    cacheService: cacheService as any,
    queryBuilderService: queryBuilderService as any,
    redis: redis as any,
    envService: envService as any,
  });

  return { service, redis, cacheService, queryBuilderService, store };
}

describe('OAuthExchangeCodeService', () => {
  it('stores exchange tokens behind a temporary code', async () => {
    const { service, redis, cacheService } = createHarness();
    const code = await service.createCodeForTokens({
      accessToken: 'access',
      refreshToken: 'refresh',
      expTime: 123,
      loginProvider: 'google',
      sessionId: 'session-1',
    });

    expect(code).toEqual(expect.any(String));
    expect(cacheService.set).toHaveBeenCalledWith(
      `auth:oauth-exchange:code:${code}`,
      expect.objectContaining({ accessToken: 'access', sessionId: 'session-1' }),
      600000,
    );
    expect(redis.zadd).toHaveBeenCalledWith(
      'auth:oauth-exchange:pending-index',
      expect.any(Number),
      code,
    );
  });

  it('deletes the temporary code when exchange succeeds', async () => {
    const { service, cacheService } = createHarness();
    const payload = {
      accessToken: 'access',
      refreshToken: 'refresh',
      expTime: 123,
      loginProvider: 'google',
      sessionId: 'session-1',
    };
    const code = await service.createCodeForTokens(payload);
    const exchanged = await service.exchange(code);

    expect(exchanged).toEqual(payload);
    expect(cacheService.deleteKey).toHaveBeenCalledWith(
      `auth:oauth-exchange:code:${code}`,
    );
    expect(cacheService.deleteKey).toHaveBeenCalledWith(
      `auth:oauth-exchange:pending:${code}`,
    );
  });

  it('cleans up sessions for expired unexchanged codes', async () => {
    const { service, redis, store, queryBuilderService } = createHarness();
    redis.zrangebyscore.mockResolvedValueOnce(['expired-code']);
    store.set('auth:oauth-exchange:pending:expired-code', {
      sessionId: 'session-1',
      expiresAt: Date.now() - 1,
    });

    const result = await service.cleanupExpired();

    expect(result).toEqual({ deleted: 1 });
    expect(queryBuilderService.delete).toHaveBeenCalledWith(
      'session_definition',
      'session-1',
    );
    expect(store.has('auth:oauth-exchange:pending:expired-code')).toBe(false);
  });
});
