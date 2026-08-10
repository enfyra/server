import { createHash } from 'crypto';
import * as jwt from 'jsonwebtoken';
import { AuthService, BcryptService } from '../../src/domain/auth';
import { EnvService } from '../../src/shared/services';

const SECRET_KEY = 'test-secret-logout';

function makeAuth(session: Record<string, unknown> | null) {
  const queryBuilder = {
    isMongoDb: () => false,
    getPkField: () => 'id',
    findOne: jest.fn().mockResolvedValue(session),
    delete: jest.fn().mockResolvedValue(undefined),
  };
  const envService = {
    get: jest.fn((key: string) => (key === 'SECRET_KEY' ? SECRET_KEY : '')),
  } as unknown as EnvService;

  return {
    auth: new AuthService({
      bcryptService: {} as BcryptService,
      queryBuilderService: queryBuilder as any,
      envService,
      cacheService: {} as any,
    }),
    queryBuilder,
  };
}

describe('AuthService.logout', () => {
  it('revokes the session with a valid refresh token without an access-token principal', async () => {
    const refreshToken = jwt.sign({ sessionId: 'session-1' }, SECRET_KEY, {
      expiresIn: '1h',
    });
    const { auth, queryBuilder } = makeAuth({
      id: 'session-1',
      userId: 'user-1',
      refreshTokenHash: createHash('sha256').update(refreshToken).digest('hex'),
    });

    await expect(auth.logout({ refreshToken }, {})).resolves.toBe(
      'Logout successfully!',
    );
    expect(queryBuilder.delete).toHaveBeenCalledWith(
      'enfyra_session',
      'session-1',
    );
  });

  it('does not revoke a session with a superseded refresh token', async () => {
    const refreshToken = jwt.sign({ sessionId: 'session-1' }, SECRET_KEY, {
      expiresIn: '1h',
    });
    const { auth, queryBuilder } = makeAuth({
      id: 'session-1',
      userId: 'user-1',
      refreshTokenHash: createHash('sha256').update('newer-token').digest('hex'),
    });

    await expect(auth.logout({ refreshToken }, {})).rejects.toThrow('revoked');
    expect(queryBuilder.delete).not.toHaveBeenCalled();
  });
});
