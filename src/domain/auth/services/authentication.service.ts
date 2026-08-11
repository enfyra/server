import type { IQueryBuilder } from '../../shared/interfaces/query-builder.interface';
import type { ICache } from '../../shared/interfaces/cache.interface';
import { InvalidTokenException } from '../../exceptions';
import {
  loadCachedUserWithRoles,
  withUserRequestContext,
} from '../../../shared/utils/load-user-with-role.util';
import type { PatVerifierService } from './pat-verifier.service';
import type { JwtVerifierService } from './jwt-verifier.service';
import type { RuntimeRegistryService } from '../../../engines/cache/services/runtime-registry.service';
import type {
  AuthenticatedRequest,
  AuthenticationInput,
  AuthTokenPayload,
  AuthHeaderConfig,
  AuthHeaderCredentialType,
} from '../types/auth.types';
import { SYSTEM_AUTH_HEADER_CONFIGS as DEFAULT_AUTH_HEADER_CONFIGS } from '../types/auth.types';

export class AuthenticationService {
  private readonly queryBuilder: IQueryBuilder;
  private readonly patVerifierService: PatVerifierService;
  private readonly jwtVerifierService: JwtVerifierService;
  private readonly runtimeRegistryService?: RuntimeRegistryService;
  private readonly cacheService?: ICache;
  private authHeaderConfigSource: readonly AuthHeaderConfig[] | null = null;
  private authHeaderConfigs: readonly AuthHeaderConfig[] = [];
  private configuredHeaderKeys = new Set<string>();

  constructor(deps: {
    queryBuilderService: IQueryBuilder;
    patVerifierService: PatVerifierService;
    jwtVerifierService: JwtVerifierService;
    runtimeRegistryService?: RuntimeRegistryService;
    cacheService?: ICache;
  }) {
    this.queryBuilder = deps.queryBuilderService;
    this.patVerifierService = deps.patVerifierService;
    this.jwtVerifierService = deps.jwtVerifierService;
    this.runtimeRegistryService = deps.runtimeRegistryService;
    this.cacheService = deps.cacheService;
  }

  hasCredentials(
    headers: AuthenticationInput['headers'],
  ): boolean {
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

    const configs = this.getAuthHeaderConfigs();
    if (configs.length === 0) {
      return { hasCredential: false, patToken: null, authorization: null };
    }

    if (isHeadersObject(headers)) {
      const values = collectConfiguredHeaderValues(
        headers,
        this.configuredHeaderKeys,
      );
      if (values.size === 0) {
        return { hasCredential: false, patToken: null, authorization: null };
      }

      let hasConfiguredCredential = false;
      for (const config of configs) {
        const rawValue = normalizeHeaderValue(values.get(config.headerKey));
        if (!rawValue) continue;

        const token = extractHeaderToken(rawValue, config);
        if (!token) continue;
        hasConfiguredCredential = true;
        const detectedCredentialType = detectCredentialType(token);
        if (detectedCredentialType !== config.credentialType) continue;

        if (config.credentialType === 'pat') {
          return { hasCredential: true, patToken: token, authorization: null };
        }

        return {
          hasCredential: true,
          patToken: null,
          authorization: `Bearer ${token}`,
        };
      }

      return { hasCredential: hasConfiguredCredential, patToken: null, authorization: null };
    }

    let hasConfiguredCredential = false;
    for (const config of configs) {
      const rawValue = readHeaderValue(headers, config.headerKey);
      if (!rawValue) continue;

      const token = extractHeaderToken(rawValue, config);
      if (!token) continue;
      hasConfiguredCredential = true;
      const detectedCredentialType = detectCredentialType(token);
      if (detectedCredentialType !== config.credentialType) continue;

      if (config.credentialType === 'pat') {
        return { hasCredential: true, patToken: token, authorization: null };
      }

      return {
        hasCredential: true,
        patToken: null,
        authorization: `Bearer ${token}`,
      };
    }

    return { hasCredential: hasConfiguredCredential, patToken: null, authorization: null };
  }

  private getAuthHeaderConfigs(): readonly AuthHeaderConfig[] {
    const configs =
      this.runtimeRegistryService?.getAuthHeaderConfigs() ??
      DEFAULT_AUTH_HEADER_CONFIGS;
    if (configs === this.authHeaderConfigSource) return this.authHeaderConfigs;

    this.authHeaderConfigSource = configs;
    this.authHeaderConfigs = configs;
    this.configuredHeaderKeys = new Set(
      configs.map((config) => config.headerKey.toLowerCase()),
    );
    return this.authHeaderConfigs;
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
    const value = headers.get(name);
    return typeof value === 'string' && value.trim() ? value.trim() : null;
  }

  const entries = headers as Record<string, unknown>;
  const matchedKey = Object.keys(entries).find(
    (key) => key.toLowerCase() === name.toLowerCase(),
  );
  if (!matchedKey) return null;

  const rawValue = entries[matchedKey];
  const value = Array.isArray(rawValue) ? rawValue[0] : rawValue;
  return typeof value === 'string' && value.trim() ? value.trim() : null;
}

function isHeadersObject(
  headers: NonNullable<AuthenticationInput['headers']>,
): headers is Record<string, unknown> {
  return !('get' in headers && typeof headers.get === 'function');
}

function collectConfiguredHeaderValues(
  headers: Record<string, unknown>,
  configuredHeaderKeys: ReadonlySet<string>,
): Map<string, unknown> {
  if (configuredHeaderKeys.size === 0) return new Map();

  const values = new Map<string, unknown>();
  for (const [key, value] of Object.entries(headers)) {
    const normalizedKey = key.toLowerCase();
    if (!configuredHeaderKeys.has(normalizedKey) || values.has(normalizedKey)) {
      continue;
    }
    values.set(normalizedKey, value);
  }
  return values;
}

function normalizeHeaderValue(rawValue: unknown): string | null {
  const value = Array.isArray(rawValue) ? rawValue[0] : rawValue;
  return typeof value === 'string' && value.trim() ? value.trim() : null;
}

function extractHeaderToken(value: string, config: AuthHeaderConfig): string | null {
  if (config.scheme === 'raw') return value.trim() || null;
  const match = value.match(/^Bearer\s+(.+)$/i);
  return match?.[1]?.trim() || null;
}

function detectCredentialType(token: string): AuthHeaderCredentialType | null {
  if (token.startsWith('efy_pat_')) return 'pat';
  return isCompactJwt(token) ? 'jwt' : null;
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
