import type { IQueryBuilder } from '../../shared/interfaces/query-builder.interface';
import type { ICache } from '../../shared/interfaces/cache.interface';
import { InvalidTokenException } from '../../exceptions';
import {
  loadCachedUserWithRoles,
  withUserRequestContext,
} from '../../../shared/utils/load-user-with-role.util';
import type { PatVerifierService } from './pat-verifier.service';
import type { JwtVerifierService } from './jwt-verifier.service';
import {
  ENFYRA_PAT_HEADER,
  type AuthenticatedRequest,
  type AuthenticationInput,
  type AuthTokenPayload,
} from '../types/auth.types';

export class AuthenticationService {
  private readonly queryBuilder: IQueryBuilder;
  private readonly patVerifierService: PatVerifierService;
  private readonly jwtVerifierService: JwtVerifierService;
  private readonly cacheService: ICache | undefined;

  constructor(deps: {
    queryBuilderService: IQueryBuilder;
    patVerifierService: PatVerifierService;
    jwtVerifierService: JwtVerifierService;
    cacheService?: ICache;
  }) {
    this.queryBuilder = deps.queryBuilderService;
    this.patVerifierService = deps.patVerifierService;
    this.jwtVerifierService = deps.jwtVerifierService;
    this.cacheService = deps.cacheService;
  }

  hasCredentials(headers: AuthenticationInput['headers']): boolean {
    return this.resolveHeaderCredentials(headers).hasCredential;
  }

  async authenticate(
    input: AuthenticationInput,
  ): Promise<AuthenticatedRequest | null> {
    if (input.headers) {
      const resolved = this.resolveHeaderCredentials(input.headers);
      if (!resolved.hasCredential) return null;
      if (!resolved.patToken && !resolved.authorization) {
        if (input.allowAnonymous) return null;
        throw new InvalidTokenException();
      }
      return this.authenticateTokens({
        ...input,
        patToken: resolved.patToken,
        authorization: resolved.authorization,
      });
    }

    return this.authenticateTokens(input);
  }

  private async authenticateTokens(
    input: AuthenticationInput,
  ): Promise<AuthenticatedRequest | null> {
    const patToken = normalizeToken(input.patToken);
    if (patToken) {
      const verified = await this.patVerifierService.verify(patToken);
      return this.hydrate('pat', verified.payload);
    }

    const accessToken = readBearerToken(input.authorization);
    if (!accessToken) return null;
    if (!isCompactJwt(accessToken)) {
      if (input.allowAnonymous) return null;
      throw new InvalidTokenException();
    }

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

  private resolveHeaderCredentials(
    headers: AuthenticationInput['headers'],
  ): {
    hasCredential: boolean;
    patToken: string | null;
    authorization: string | null;
  } {
    if (!headers) {
      return { hasCredential: false, patToken: null, authorization: null };
    }

    const rawPat = readHeaderValue(headers, ENFYRA_PAT_HEADER);
    const authorization = readHeaderValue(headers, 'authorization');
    if (rawPat === '' || authorization === '') {
      return { hasCredential: true, patToken: null, authorization: null };
    }
    if (rawPat?.startsWith('efy_pat_')) {
      return { hasCredential: true, patToken: rawPat, authorization: null };
    }

    const token = readBearerToken(authorization);
    return {
      hasCredential: rawPat !== null || authorization !== null,
      patToken: null,
      authorization: token && isCompactJwt(token) ? `Bearer ${token}` : null,
    };
  }

  private async hydrate(
    source: AuthenticatedRequest['source'],
    payload: AuthTokenPayload,
  ): Promise<AuthenticatedRequest | null> {
    const cachedUser = await loadCachedUserWithRoles(
      this.queryBuilder,
      this.cacheService,
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

function readHeaderValue(
  headers: NonNullable<AuthenticationInput['headers']>,
  name: string,
): string | null {
  if ('get' in headers && typeof headers.get === 'function') {
    return normalizeToken(headers.get(name));
  }

  const entries = Object.entries(headers).filter(
    ([key]) => key.toLowerCase() === name,
  );
  if (entries.length !== 1) return entries.length > 1 ? '' : null;
  const value = entries[0]?.[1];
  return Array.isArray(value) ? '' : normalizeToken(value);
}

function isCompactJwt(token: string): boolean {
  const parts = token.split('.');
  return (
    parts.length === 3 &&
    parts.every((part) => part.length > 0 && /^[A-Za-z0-9_-]+$/.test(part))
  );
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
