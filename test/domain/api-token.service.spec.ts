import { afterEach, describe, expect, it, vi } from 'vitest';
import * as jwt from 'jsonwebtoken';
import { ApiTokenService, PatVerifierService } from '../../src/domain/auth';

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
    updateMany: vi.fn(async (_table: string, ids: string[], data: any) => {
      for (const id of ids) {
        const current = tokens.get(id);
        if (current) tokens.set(id, { ...current, ...data });
      }
      return ids.length;
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
    setManyIfKeyAbsent: vi.fn(async (_guardKey: string, entries: any[]) => {
      if (cache.has(_guardKey)) return false;
      for (const entry of entries) cache.set(entry.key, entry.value);
      return true;
    }),
    setManyAndDelete: vi.fn(async (entries: any[], keysToDelete: string[]) => {
      for (const entry of entries) cache.set(entry.key, entry.value);
      for (const key of keysToDelete) cache.delete(key);
    }),
    compareAndSet: vi.fn(
      async (key: string, expectedValue: any, value: any) => {
        if (JSON.stringify(cache.get(key)) !== JSON.stringify(expectedValue)) {
          return false;
        }
        cache.set(key, value);
        return true;
      },
    ),
    acquire: vi.fn(async () => true),
    release: vi.fn(async () => true),
    deleteKey: vi.fn(async (key: string) => {
      cache.delete(key);
    }),
  };
  const redisPubSubService: any = {
    publish: vi.fn(async () => undefined),
    subscribeWithHandler: vi.fn(),
  };
  const patVerifierService = new PatVerifierService({
    queryBuilderService: queryBuilder,
    cacheService,
    redisPubSubService,
  });
  const service = new ApiTokenService({
    queryBuilderService: queryBuilder,
    envService: { get: () => 'secret' } as any,
    patVerifierService,
  });

  return {
    service,
    patVerifierService,
    tokens,
    cache,
    cacheService,
    redisPubSubService,
    queryBuilder,
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

  it('issues and verifies a PAT for trusted dynamic scripts without exposing hashes', async () => {
    const { service, userId } = createHarness();

    const created = await service.createForUser({
      userId,
      name: 'Dynamic script token',
      expiresAt: 'never',
    });
    const verified = await service.verifyForScript(created.token);

    expect(created.token).toMatch(/^efy_pat_/);
    expect(created).not.toHaveProperty('tokenHash');
    expect(verified).toEqual({
      userId,
      tokenId: created.id,
      expiresAt: 'never',
    });
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

  it('verifies a PAT directly without issuing a JWT', async () => {
    const { service, patVerifierService, req, userId } = createHarness();
    const created = await service.create(
      { name: 'Reusable PAT', expiresAt: 'never' },
      req,
    );

    const verified = await patVerifierService.verify(created.token);

    expect(verified.payload).toEqual({
      id: userId,
      loginProvider: 'api_token',
      tokenType: 'api_token',
      tokenId: created.id,
    });
    expect(verified.expiresAt).toBeNull();
  });

  it('caches a verified raw PAT and avoids database reads until its short TTL expires', async () => {
    const { service, patVerifierService, req, queryBuilder } = createHarness();
    const created = await service.create(
      { name: 'Cached PAT', expiresAt: 'never' },
      req,
    );

    queryBuilder.findOne.mockClear();
    await patVerifierService.verify(created.token);
    expect(queryBuilder.findOne).toHaveBeenCalledTimes(1);
    const readsAfterFirstVerification = queryBuilder.findOne.mock.calls.length;

    await patVerifierService.verify(created.token);

    expect(queryBuilder.findOne).toHaveBeenCalledTimes(readsAfterFirstVerification);
  });

  it('coalesces concurrent raw PAT cache misses into one database lookup', async () => {
    const { service, patVerifierService, req, queryBuilder } = createHarness();
    const created = await service.create(
      { name: 'Concurrent PAT', expiresAt: 'never' },
      req,
    );
    const findOne = queryBuilder.findOne.getMockImplementation();
    let releaseLookup!: () => void;
    const lookupReleased = new Promise<void>((resolve) => {
      releaseLookup = resolve;
    });
    let markLookupStarted!: () => void;
    const lookupStarted = new Promise<void>((resolve) => {
      markLookupStarted = resolve;
    });

    queryBuilder.findOne.mockClear();
    queryBuilder.findOne.mockImplementationOnce(async (options: any) => {
      markLookupStarted();
      await lookupReleased;
      return findOne(options);
    });

    const verifications = Array.from({ length: 20 }, () =>
      patVerifierService.verify(created.token),
    );
    await lookupStarted;

    expect(queryBuilder.findOne).toHaveBeenCalledTimes(1);

    releaseLookup();
    await expect(Promise.all(verifications)).resolves.toHaveLength(20);
    expect(queryBuilder.findOne).toHaveBeenCalledTimes(1);
  });

  it('waits for another replica to populate a PAT state before falling back to the database', async () => {
    vi.useFakeTimers();
    const { service, patVerifierService, req, cache, cacheService, queryBuilder } =
      createHarness();
    const created = await service.create(
      { name: 'Distributed PAT', expiresAt: 'never' },
      req,
    );
    const tokenHash = patVerifierService.hashToken(created.token);

    await patVerifierService.verify(created.token);
    const peerState = cache.get(`auth:api-token:hash:${tokenHash}`);
    cache.clear();
    queryBuilder.findOne.mockClear();
    cacheService.acquire.mockResolvedValueOnce(false);

    const verification = patVerifierService.verify(created.token);
    await Promise.resolve();
    cache.set(`auth:api-token:hash:${tokenHash}`, peerState);
    await vi.advanceTimersByTimeAsync(25);

    await expect(verification).resolves.toMatchObject({
      payload: { tokenId: created.id },
    });
    expect(queryBuilder.findOne).not.toHaveBeenCalled();
  });

  it('does not repopulate a PAT cache when revocation wins the database-read race', async () => {
    const { service, patVerifierService, req, tokens, cache, queryBuilder } =
      createHarness();
    const created = await service.create(
      { name: 'Revocation race', expiresAt: 'never' },
      req,
    );
    const staleRecord = { ...tokens.get(created.id) };
    let releaseLookup!: () => void;
    const lookupReleased = new Promise<void>((resolve) => {
      releaseLookup = resolve;
    });
    let markLookupStarted!: () => void;
    const lookupStarted = new Promise<void>((resolve) => {
      markLookupStarted = resolve;
    });

    queryBuilder.findOne.mockImplementationOnce(async () => {
      markLookupStarted();
      await lookupReleased;
      return staleRecord;
    });

    const verification = patVerifierService.verify(created.token);
    await lookupStarted;
    await service.revoke(created.id, req);
    releaseLookup();

    await expect(verification).rejects.toThrow(/Invalid API token/);
    const tokenHash = patVerifierService.hashToken(created.token);
    expect(cache.get(`auth:api-token:${created.id}`)).toBeUndefined();
    expect(cache.get(`auth:api-token:hash:${tokenHash}`)).toBeUndefined();
    await expect(patVerifierService.verify(created.token)).rejects.toThrow(
      /Invalid API token/,
    );
  });

  it('refreshes a hot PAT cache before soft expiry and revalidates it after the hard deadline', async () => {
    vi.useFakeTimers();
    vi.setSystemTime(now);
    const { service, patVerifierService, req, queryBuilder, cacheService } =
      createHarness();
    const created = await service.create(
      {
        name: 'Hot PAT',
        expiresAt: new Date(now.getTime() + 3_600_000).toISOString(),
      },
      req,
    );

    await patVerifierService.verify(created.token);
    queryBuilder.findOne.mockClear();

    await vi.advanceTimersByTimeAsync(46_000);
    await patVerifierService.verify(created.token);
    await Promise.resolve();

    expect(cacheService.compareAndSet).toHaveBeenCalledOnce();
    expect(queryBuilder.findOne).not.toHaveBeenCalled();

    await vi.advanceTimersByTimeAsync(15 * 60_000);
    await patVerifierService.verify(created.token);

    expect(queryBuilder.findOne).toHaveBeenCalledOnce();
  });

  it('writes lastUsedAt asynchronously through the coalesced flush job', async () => {
    vi.useFakeTimers();
    vi.setSystemTime(now);
    const { service, patVerifierService, req, queryBuilder } = createHarness();
    await patVerifierService.init();
    const created = await service.create(
      {
        name: 'Observed PAT',
        expiresAt: new Date(now.getTime() + 3_600_000).toISOString(),
      },
      req,
    );

    queryBuilder.updateMany.mockClear();
    await patVerifierService.verify(created.token);
    await Promise.resolve();

    expect(queryBuilder.updateMany).not.toHaveBeenCalled();

    await vi.advanceTimersByTimeAsync(60_000);

    expect(queryBuilder.updateMany).toHaveBeenCalledWith(
      'enfyra_api_token',
      [created.id],
      expect.objectContaining({
        lastUsedAt: expect.any(Date),
        updatedAt: expect.any(Date),
      }),
      'id',
    );

    await patVerifierService.verify(created.token);
    await vi.advanceTimersByTimeAsync(60_000);

    expect(queryBuilder.updateMany).toHaveBeenCalledTimes(1);
    patVerifierService.onDestroy();
  });

  it('persists queued PAT activity in one batched update', async () => {
    vi.useFakeTimers();
    vi.setSystemTime(now);
    const { service, patVerifierService, req, queryBuilder } = createHarness();
    await patVerifierService.init();
    const first = await service.create(
      { name: 'First observed PAT', expiresAt: 'never' },
      req,
    );
    const second = await service.create(
      { name: 'Second observed PAT', expiresAt: 'never' },
      req,
    );

    queryBuilder.updateMany.mockClear();
    await Promise.all([
      patVerifierService.verify(first.token),
      patVerifierService.verify(second.token),
    ]);
    await Promise.resolve();
    await vi.advanceTimersByTimeAsync(60_000);

    expect(queryBuilder.updateMany).toHaveBeenCalledTimes(1);
    expect(queryBuilder.updateMany).toHaveBeenCalledWith(
      'enfyra_api_token',
      expect.arrayContaining([first.id, second.id]),
      expect.objectContaining({ lastUsedAt: expect.any(Date) }),
      'id',
    );
    patVerifierService.onDestroy();
  });

  it('exchanges a valid API token into a JWT tied to the token record', async () => {
    vi.useFakeTimers();
    vi.setSystemTime(now);
    const { service, patVerifierService, req, userId } = createHarness();
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
    await expect(
      patVerifierService.validateAccessPayload(decoded),
    ).resolves.toBe(true);
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
    const {
      service,
      patVerifierService,
      req,
      tokens,
      cacheService,
      redisPubSubService,
    } = createHarness();
    const created = await service.create(
      { name: 'MCP token', expiresAt: 'never' },
      req,
    );
    const exchanged = await service.exchange({ apiToken: created.token });
    const decoded = jwt.decode(exchanged.accessToken) as jwt.JwtPayload;

    await service.revoke(created.id, req);

    expect(tokens.has(created.id)).toBe(false);
    const tokenHash = patVerifierService.hashToken(created.token);
    expect(cacheService.setManyAndDelete).toHaveBeenCalledWith(
      expect.arrayContaining([
        expect.objectContaining({
          key: `auth:api-token:revoked:${created.id}`,
          value: true,
        }),
        expect.objectContaining({
          key: `auth:api-token:revoked:hash:${tokenHash}`,
          value: true,
        }),
      ]),
      [
        `auth:api-token:${created.id}`,
        `auth:api-token:hash:${tokenHash}`,
      ],
    );
    expect(redisPubSubService.publish).toHaveBeenCalledWith(
      'api-token:revoked',
      { tokenId: created.id, tokenHash },
    );
    await expect(
      patVerifierService.validateAccessPayload(decoded),
    ).resolves.toBe(false);
    await expect(patVerifierService.verify(created.token)).rejects.toThrow(
      /Invalid API token/,
    );
  });
});
