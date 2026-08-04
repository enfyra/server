import type { IQueryBuilder } from '../../shared/interfaces/query-builder.interface';
import { InvalidTokenException } from '../../exceptions';
import {
  loadCachedUserWithRole,
  withUserRequestContext,
} from '../../../shared/utils/load-user-with-role.util';
import type { PatVerifierService } from './pat-verifier.service';
import type { JwtVerifierService } from './jwt-verifier.service';
import type {
  AuthenticatedRequest,
  AuthenticationInput,
  AuthTokenPayload,
} from '../types/auth.types';

export class AuthenticationService {
  private readonly queryBuilder: IQueryBuilder;
  private readonly patVerifierService: PatVerifierService;
  private readonly jwtVerifierService: JwtVerifierService;

  constructor(deps: {
    queryBuilderService: IQueryBuilder;
    patVerifierService: PatVerifierService;
    jwtVerifierService: JwtVerifierService;
  }) {
    this.queryBuilder = deps.queryBuilderService;
    this.patVerifierService = deps.patVerifierService;
    this.jwtVerifierService = deps.jwtVerifierService;
  }

  async authenticate(
    input: AuthenticationInput,
  ): Promise<AuthenticatedRequest | null> {
    const patToken = normalizeToken(input.patToken);
    if (patToken) {
      const verified = await this.patVerifierService.verify(patToken);
      return this.hydrate('pat', verified.payload);
    }

    const accessToken = readBearerToken(input.authorization);
    if (!accessToken) return null;

    let payload: AuthTokenPayload | null;
    try {
      payload = await this.jwtVerifierService.verify(accessToken);
    } catch (error) {
      if (input.allowAnonymous) return null;
      throw error;
    }

    if (!payload) return null;
    if (
      payload.tokenType === 'api_token' &&
      !(await this.patVerifierService.validateAccessPayload(payload))
    ) {
      throw new InvalidTokenException();
    }

    return this.hydrate('jwt', payload);
  }

  private async hydrate(
    source: AuthenticatedRequest['source'],
    payload: AuthTokenPayload,
  ): Promise<AuthenticatedRequest | null> {
    const cachedUser = await loadCachedUserWithRole(
      this.queryBuilder,
      payload.id,
    );
    if (!cachedUser) return null;

    return {
      source,
      payload,
      user: withUserRequestContext(cachedUser, {
        loginProvider: payload.loginProvider,
        tokenType: payload.tokenType,
        tokenId: payload.tokenId,
      }),
    };
  }
}

function normalizeToken(value: unknown): string | null {
  if (typeof value !== 'string') return null;
  const token = value.trim();
  return token.length > 0 ? token : null;
}

function readBearerToken(value: unknown): string | null {
  if (typeof value !== 'string') return null;
  const match = value.match(/^Bearer\s+(.+)$/i);
  return normalizeToken(match?.[1]);
}
