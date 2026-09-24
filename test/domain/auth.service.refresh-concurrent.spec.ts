import { createHash } from 'crypto';
import * as jwt from 'jsonwebtoken';
import { ObjectId } from 'mongodb';
import { AuthService, BcryptService } from '../../src/domain/auth';
import { QueryBuilderService } from '@enfyra/kernel';
import { EnvService } from '../../src/shared/services';
import type { ICache } from '../../src/domain/shared/interfaces/cache.interface';

describe('AuthService.refreshToken rotation (SQL session)', () => {
  let auth: AuthService;
  let createAuth: () => AuthService;
  const sessionStore: Record<string, any> = {
    id: 'sess-concurrent',
    userId: 'user-1',
    refreshTokenHash: '',
    expiredAt: new Date(Date.now() + 86400000 * 365),
    remember: false,
    loginProvider: null,
  };

  beforeEach(() => {
    sessionStore.id = 'sess-concurrent';
    sessionStore.userId = 'user-1';
    sessionStore.expiredAt = new Date(Date.now() + 86400000 * 365);
    sessionStore.remember = false;
    sessionStore.loginProvider = null;

    const queryBuilder = {
      isMongoDb: () => false,
      getPkField: () => 'id',
      findOne: jest.fn(async (opts: any) => {
        if (opts.where?.id === sessionStore.id) {
          return { ...sessionStore };
        }
        return null;
      }),
      getKnex: jest.fn(() => {
        const builder: any = {
          _hashOk: true,
          where: jest.fn().mockReturnThis(),
          andWhere: jest.fn().mockImplementation(function (fn: () => void) {
            const sub: any = {
              where: jest.fn((col: string, val: string) => {
                builder._hashOk =
                  sessionStore.refreshTokenHash === val ||
                  !sessionStore.refreshTokenHash;
                return sub;
              }),
              orWhereNull: jest.fn(() => {
                if (!sessionStore.refreshTokenHash) builder._hashOk = true;
                return sub;
              }),
            };
            fn.call(sub);
            return builder;
          }),
          update: jest.fn(async (patch: Record<string, unknown>) => {
            if (!builder._hashOk) return 0;
            Object.assign(sessionStore, patch);
            return 1;
          }),
        };
        return (_table: string) => builder;
      }),
    } as unknown as QueryBuilderService;

    const mockEnvService = {
      get: jest.fn((key: string) => {
        const envVars: Record<string, string> = {
          SECRET_KEY: 'test-secret-concurrent-auth',
          ACCESS_TOKEN_EXP: '15m',
          REFRESH_TOKEN_NO_REMEMBER_EXP: '7d',
          REFRESH_TOKEN_REMEMBER_EXP: '30d',
        };
        return envVars[key];
      }),
    } as unknown as EnvService;

    const mockBcryptService = {} as BcryptService;
    const cacheStore = new Map<string, unknown>();
    const cacheService = {
      get: jest.fn(async (key: string) => cacheStore.get(key) ?? null),
      set: jest.fn(async (key: string, value: unknown) => {
        cacheStore.set(key, value);
      }),
    } as unknown as ICache;

    createAuth = () =>
      new AuthService({
        bcryptService: mockBcryptService,
        queryBuilderService: queryBuilder,
        envService: mockEnvService,
        cacheService,
      });
    auth = createAuth();
  });

  it('rejects an unrecognized refresh token after rotation', async () => {
    const oldRt = jwt.sign(
      { sessionId: sessionStore.id },
      'test-secret-concurrent-auth',
      { expiresIn: '7d' },
    );
    sessionStore.refreshTokenHash = createHash('sha256')
      .update(oldRt)
      .digest('hex');

    const { refreshToken: newRt } = await auth.refreshToken({
      refreshToken: oldRt,
    });

    expect(newRt).not.toBe(oldRt);

    const revokedRt = jwt.sign(
      { sessionId: sessionStore.id, jti: 'revoked-token' },
      'test-secret-concurrent-auth',
      { expiresIn: '7d' },
    );

    await expect(
      auth.refreshToken({ refreshToken: revokedRt }),
    ).rejects.toThrow('revoked');

    const again = await auth.refreshToken({ refreshToken: newRt });
    expect(again.refreshToken).toBeDefined();
  }, 25000);

  it('replays the same rotation result when a late request reuses the previous refresh token', async () => {
    const oldRt = jwt.sign(
      { sessionId: sessionStore.id, jti: 'late-request' },
      'test-secret-concurrent-auth',
      { expiresIn: '7d' },
    );
    sessionStore.refreshTokenHash = createHash('sha256')
      .update(oldRt)
      .digest('hex');

    const first = await auth.refreshToken({ refreshToken: oldRt });

    await expect(auth.refreshToken({ refreshToken: oldRt })).resolves.toEqual(
      first,
    );
  });

  it('returns one rotation result across concurrent auth service instances', async () => {
    const oldRt = jwt.sign(
      { sessionId: sessionStore.id, jti: 'concurrent-instances' },
      'test-secret-concurrent-auth',
      { expiresIn: '7d' },
    );
    sessionStore.refreshTokenHash = createHash('sha256')
      .update(oldRt)
      .digest('hex');

    const [first, second] = await Promise.all([
      auth.refreshToken({ refreshToken: oldRt }),
      createAuth().refreshToken({ refreshToken: oldRt }),
    ]);

    expect(second).toEqual(first);
    expect(sessionStore.refreshTokenHash).toBe(
      createHash('sha256').update(first.refreshToken).digest('hex'),
    );
  });

  it('does not replay a rotation after the session is deleted', async () => {
    const oldRt = jwt.sign(
      { sessionId: sessionStore.id, jti: 'deleted-session' },
      'test-secret-concurrent-auth',
      { expiresIn: '7d' },
    );
    sessionStore.refreshTokenHash = createHash('sha256')
      .update(oldRt)
      .digest('hex');

    await auth.refreshToken({ refreshToken: oldRt });
    sessionStore.id = 'deleted-session-record';

    await expect(auth.refreshToken({ refreshToken: oldRt })).rejects.toThrow(
      'Session not found',
    );
  });

  it('does not roll the session back to an older replay after another rotation', async () => {
    const oldRt = jwt.sign(
      { sessionId: sessionStore.id, jti: 'first-generation' },
      'test-secret-concurrent-auth',
      { expiresIn: '7d' },
    );
    sessionStore.refreshTokenHash = createHash('sha256')
      .update(oldRt)
      .digest('hex');

    const first = await auth.refreshToken({ refreshToken: oldRt });
    await auth.refreshToken({ refreshToken: first.refreshToken });

    await expect(auth.refreshToken({ refreshToken: oldRt })).rejects.toThrow(
      'revoked',
    );
  });
});

describe('AuthService.refreshToken rotation (Mongo session)', () => {
  it('returns one rotation result across concurrent auth service instances', async () => {
    const session = {
      _id: new ObjectId(),
      user: 'user-1',
      refreshTokenHash: '',
      expiredAt: new Date(Date.now() + 86400000 * 365),
      remember: true,
      loginProvider: 'google',
    };
    const cacheStore = new Map<string, unknown>();
    const cacheService = {
      get: jest.fn(async (key: string) => cacheStore.get(key) ?? null),
      set: jest.fn(async (key: string, value: unknown) => {
        cacheStore.set(key, value);
      }),
    } as unknown as ICache;
    const queryBuilder = {
      isMongoDb: () => true,
      getPkField: () => '_id',
      findOne: jest.fn(async (opts: any) =>
        String(opts.where?._id) === String(session._id) ? { ...session } : null,
      ),
      getMongoDb: jest.fn(() => ({
        collection: () => ({
          findOneAndUpdate: async (filter: any, update: any) => {
            const expectedHash = filter.$or?.[0]?.refreshTokenHash;
            if (
              String(filter._id) !== String(session._id) ||
              session.refreshTokenHash !== expectedHash
            ) {
              return null;
            }
            Object.assign(session, update.$set);
            return { ...session };
          },
        }),
      })),
    } as unknown as QueryBuilderService;
    const envService = {
      get: jest.fn((key: string) => {
        const values: Record<string, string> = {
          SECRET_KEY: 'test-secret-mongo-auth',
          ACCESS_TOKEN_EXP: '15m',
          REFRESH_TOKEN_NO_REMEMBER_EXP: '7d',
          REFRESH_TOKEN_REMEMBER_EXP: '30d',
        };
        return values[key];
      }),
    } as unknown as EnvService;
    const createAuth = () =>
      new AuthService({
        bcryptService: {} as BcryptService,
        queryBuilderService: queryBuilder,
        envService,
        cacheService,
      });
    const oldRt = jwt.sign(
      { sessionId: session._id.toString(), jti: 'mongo-concurrent' },
      'test-secret-mongo-auth',
      { expiresIn: '7d' },
    );
    session.refreshTokenHash = createHash('sha256').update(oldRt).digest('hex');

    const [first, second] = await Promise.all([
      createAuth().refreshToken({ refreshToken: oldRt }),
      createAuth().refreshToken({ refreshToken: oldRt }),
    ]);

    expect(second).toEqual(first);
    expect(session.refreshTokenHash).toBe(
      createHash('sha256').update(first.refreshToken).digest('hex'),
    );
  });
});
