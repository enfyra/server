import { createHash } from 'crypto';
import { Test, TestingModule } from '@nestjs/testing';
import { JwtModule, JwtService } from '@nestjs/jwt';
import { ConfigModule } from '@nestjs/config';
import { AuthService } from '../../src/core/auth/services/auth.service';
import { BcryptService } from '../../src/core/auth/services/bcrypt.service';
import { QueryBuilderService } from '../../src/infrastructure/query-builder/query-builder.service';

describe('AuthService.refreshToken rotation (SQL session)', () => {
  let auth: AuthService;
  let jwt: JwtService;
  const sessionStore = {
    id: 'sess-concurrent',
    userId: 'user-1',
    refreshTokenHash: '',
    expiredAt: new Date(Date.now() + 86400000 * 365),
    remember: false,
    loginProvider: null as string | null,
  };

  beforeEach(async () => {
    sessionStore.id = 'sess-concurrent';
    sessionStore.userId = 'user-1';
    sessionStore.expiredAt = new Date(Date.now() + 86400000 * 365);
    sessionStore.remember = false;
    sessionStore.loginProvider = null;

    const queryBuilder = {
      isMongoDb: () => false,
      findOneWhere: jest.fn(async () => ({ ...sessionStore })),
      updateById: jest.fn(
        async (_table: string, _id: string, patch: Record<string, unknown>) => {
          Object.assign(sessionStore, patch);
        },
      ),
    };

    const moduleRef: TestingModule = await Test.createTestingModule({
      imports: [
        ConfigModule.forRoot({
          isGlobal: true,
          load: [
            () => ({
              ACCESS_TOKEN_EXP: '15m',
              REFRESH_TOKEN_NO_REMEMBER_EXP: '7d',
              REFRESH_TOKEN_REMEMBER_EXP: '30d',
            }),
          ],
        }),
        JwtModule.register({
          secret: 'test-secret-concurrent-auth',
          signOptions: { expiresIn: '7d' },
        }),
      ],
      providers: [
        AuthService,
        {
          provide: BcryptService,
          useValue: {},
        },
        {
          provide: QueryBuilderService,
          useValue: queryBuilder,
        },
      ],
    }).compile();

    auth = moduleRef.get(AuthService);
    jwt = moduleRef.get(JwtService);
  });

  it(
    'after refresh, old refresh token is rejected (rotation)',
    async () => {
    const oldRt = jwt.sign(
      { sessionId: sessionStore.id },
      { secret: 'test-secret-concurrent-auth', expiresIn: '7d' },
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

    await expect(
      auth.refreshToken({ refreshToken: oldRt }),
    ).rejects.toThrow('revoked');

    const again = await auth.refreshToken({ refreshToken: newRt });
    expect(again.refreshToken).toBeDefined();
    },
    25000,
  );
});
