import { jwtVerify } from 'jose';
import type { EnvService } from '../../../shared/services';
import { InvalidTokenException, TokenExpiredException } from '../../exceptions';
import type { AuthTokenPayload } from '../types/auth.types';

export class JwtVerifierService {
  private readonly key: Uint8Array;

  constructor(deps: { envService: EnvService }) {
    this.key = new TextEncoder().encode(deps.envService.get('SECRET_KEY'));
  }

  async verify(accessToken: string): Promise<AuthTokenPayload | null> {
    try {
      const { payload } = await jwtVerify(accessToken, this.key);
      if (payload.id === undefined || payload.id === null) return null;

      return {
        id: String(payload.id),
        loginProvider:
          typeof payload.loginProvider === 'string'
            ? payload.loginProvider
            : null,
        tokenType:
          typeof payload.tokenType === 'string' ? payload.tokenType : null,
        ...(typeof payload.tokenId === 'string'
          ? { tokenId: payload.tokenId }
          : {}),
        ...(typeof payload.exp === 'number' ? { exp: payload.exp } : {}),
      };
    } catch (error: any) {
      if (error?.code === 'ERR_JWT_EXPIRED') {
        throw new TokenExpiredException();
      }
      throw new InvalidTokenException();
    }
  }
}
