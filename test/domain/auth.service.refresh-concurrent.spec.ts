import { createHash } from 'crypto';
import * as jwt from 'jsonwebtoken';
import { AuthService, BcryptService } from '../../src/domain/auth';
import { QueryBuilderService } from '@enfyra/kernel';
import { EnvService } from '../../src/shared/services';

describe('AuthService.refreshToken rotation (SQL session)', () => {
  let auth: AuthService;
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

    auth = new AuthService({
      bcryptService: mockBcryptService,
      queryBuilderService: queryBuilder,
      envService: mockEnvService,
    });
  });

  it('after refresh, old refresh token is rejected (rotation)', async () => {
    const oldRt = jwt.sign(
      { sessionId: sessionStore.id },
      'test-secret-concurrent-auth',
      { expiresIn: '7d' },
    );
    sessionStore.refreshTokenHash = createHash('sha256')
      .update(oldRt)
      .digest('hex');

    await new Promise((r) => setTimeout(r, 1100));

    const { refreshToken: newRt } = await auth.refreshToken({
      refreshToken: oldRt,
    });

    expect(newRt).not.toBe(oldRt);

    await new Promise((r) => setTimeout(r, 1100));

    await expect(auth.refreshToken({ refreshToken: oldRt })).rejects.toThrow(
      'revoked',
    );

    const again = await auth.refreshToken({ refreshToken: newRt });
    expect(again.refreshToken).toBeDefined();
  }, 25000);
});
