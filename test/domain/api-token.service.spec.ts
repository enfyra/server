import { afterEach, describe, expect, it, vi } from 'vitest';
import * as jwt from 'jsonwebtoken';
import { ApiTokenService } from '../../src/domain/auth';

function createHarness() {
  const userId = '019e39d4-dca8-72d9-a33f-3488f7400c54';
  const users = new Map<string, any>([
    [userId, { id: userId, email: 'admin@example.com' }],
  ]);
  const tokens = new Map<string, any>();
  const cache = new Map<string, any>();
  const queryBuilder: any = {
    isMongoDb: () => false,
    getPkField: () => 'id',
    find: vi.fn(async ({ table, filter }: any) => {
      if (table !== 'enfyra_api_token') return { data: [] };
      const userId = filter?.user?._eq;
      return {
        data: [...tokens.values()].filter((token) => token.userId === userId),
      };
    }),
    findOne: vi.fn(async ({ table, where }: any) => {
      if (table === 'enfyra_user') return users.get(where.id) || null;
      if (table === 'enfyra_role') return null;
      if (table !== 'enfyra_api_token') return null;
      if (where.id) return tokens.get(where.id) || null;
      if (where.tokenHash) {
        return (
          [...tokens.values()].find(
            (token) => token.tokenHash === where.tokenHash,
          ) || null
        );
      }
      return null;
    }),
    insert: vi.fn(async (_table: string, data: any) => {
      const record = { ...data, userId: data.user };
      delete record.user;
      tokens.set(record.id, record);
      return record;
    }),
    update: vi.fn(async (_table: string, id: string, data: any) => {
      const current = tokens.get(id);
      const next = { ...current, ...data };
      tokens.set(id, next);
      return next;
    }),
    delete: vi.fn(async (_table: string, id: string) => {
      tokens.delete(id);
      return true;
    }),
  };
  const cacheService: any = {
    get: vi.fn(async (key: string) => cache.get(key) || null),
    set: vi.fn(async (key: string, value: any) => {
      cache.set(key, value);
    }),
    deleteKey: vi.fn(async (key: string) => {
      cache.delete(key);
    }),
  };
  const redisPubSubService: any = {
    publish: vi.fn(async () => undefined),
    subscribeWithHandler: vi.fn(),
  };
  const service = new ApiTokenService({
    queryBuilderService: queryBuilder,
    envService: { get: () => 'secret' } as any,
    cacheService,
    redisPubSubService,
  });

  return {
    service,
    tokens,
    cacheService,
    redisPubSubService,
    userId,
    req: { user: { id: userId } },
  };
}

describe('ApiTokenService', () => {
  const now = new Date('2026-01-01T00:00:00.000Z');

  afterEach(() => {
    vi.useRealTimers();
  });

  it('creates a token with the exact never-expiry contract', async () => {
    const { service, req } = createHarness();

    const created = await service.create(
      { name: 'MCP token', expiresAt: 'never' },
      req,
    );

    expect(created.token).toMatch(/^efy_pat_/);
    expect(created.expiresAt).toBe('never');
    expect(created.last4).toBe(created.token.slice(-4));
    expect(created).not.toHaveProperty('tokenHash');
  });

  it('rejects missing or past expiration values before returning a token', async () => {
    const { service, req } = createHarness();

    await expect(service.create({ name: 'bad' }, req)).rejects.toThrow(
      /expiresAt is required/,
    );
    await expect(
      service.create(
        { name: 'bad', expiresAt: new Date(Date.now() - 1000).toISOString() },
        req,
      ),
    ).rejects.toThrow(/expiresAt must be in the future/);
  });

  it('exchanges a valid API token into a JWT tied to the token record', async () => {
    vi.useFakeTimers();
    vi.setSystemTime(now);
    const { service, req, userId } = createHarness();
    const created = await service.create(
      {
        name: 'MCP token',
        expiresAt: new Date(Date.now() + 3_600_000).toISOString(),
      },
      req,
    );

    const exchanged = await service.exchange({ apiToken: created.token });
    const decoded = jwt.decode(exchanged.accessToken) as jwt.JwtPayload;

    expect(decoded.id).toBe(userId);
    expect(decoded.tokenType).toBe('api_token');
    expect(decoded.tokenId).toBe(created.id);
    expect(decoded.exp).toBe(Math.floor((now.getTime() + 60_000) / 1000));
    expect(exchanged.expTime).toBe(decoded.exp! * 1000);
    await expect(service.validateAccessPayload(decoded)).resolves.toBe(true);
  });

  it('caps exchanged JWT expiry to the API token expiry when sooner than the access TTL', async () => {
    vi.useFakeTimers();
    vi.setSystemTime(now);
    const { service, req } = createHarness();
    const expiresAt = new Date(now.getTime() + 30_000);
    const created = await service.create(
      {
        name: 'Short MCP token',
        expiresAt: expiresAt.toISOString(),
      },
      req,
    );

    const exchanged = await service.exchange({ apiToken: created.token });
    const decoded = jwt.decode(exchanged.accessToken) as jwt.JwtPayload;

    expect(decoded.exp).toBe(Math.floor(expiresAt.getTime() / 1000));
    expect(exchanged.expTime).toBe(decoded.exp! * 1000);
  });

  it('hard-deletes revoked tokens and invalidates their cached access state', async () => {
    const { service, req, tokens, cacheService, redisPubSubService } =
      createHarness();
    const created = await service.create(
      { name: 'MCP token', expiresAt: 'never' },
      req,
    );
    const exchanged = await service.exchange({ apiToken: created.token });
    const decoded = jwt.decode(exchanged.accessToken) as jwt.JwtPayload;

    await service.revoke(created.id, req);

    expect(tokens.has(created.id)).toBe(false);
    expect(cacheService.deleteKey).toHaveBeenCalledWith(
      `auth:api-token:${created.id}`,
    );
    expect(redisPubSubService.publish).toHaveBeenCalledWith(
      'api-token:revoked',
      { tokenId: created.id },
    );
    await expect(service.validateAccessPayload(decoded)).resolves.toBe(false);
  });
});
