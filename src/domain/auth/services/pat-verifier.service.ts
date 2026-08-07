import { createHash } from 'crypto';
import { ObjectId } from 'mongodb';
import { DatabaseConfigService } from '../../../shared/services';
import { ICache } from '../../shared/interfaces/cache.interface';
import { IQueryBuilder } from '../../shared/interfaces/query-builder.interface';
import { IRedisPubSub } from '../../shared/interfaces/redis-pubsub.interface';
import { UnauthorizedException } from '../../exceptions';
import { Logger } from '../../../shared/logger';
import {
  API_TOKEN_CACHE_PREFIX,
  API_TOKEN_REVOKED_CHANNEL,
  API_TOKEN_STATE_TTL_MS,
  API_TOKEN_TABLE,
} from '../auth.constants';
import type {
  AuthTokenPayload,
  PatVerificationResult,
} from '../types/auth.types';

type ApiTokenState = {
  id: string;
  userId: string;
  expiresAt: string | null;
  tokenHash?: string;
};

export class PatVerifierService {
  private readonly logger = new Logger(PatVerifierService.name);
  private readonly queryBuilder: IQueryBuilder;
  private readonly cacheService: ICache;
  private readonly redisPubSubService: IRedisPubSub;

  constructor(deps: {
    queryBuilderService: IQueryBuilder;
    cacheService: ICache;
    redisPubSubService: IRedisPubSub;
  }) {
    this.queryBuilder = deps.queryBuilderService;
    this.cacheService = deps.cacheService;
    this.redisPubSubService = deps.redisPubSubService;
  }

  async init(): Promise<void> {
    this.redisPubSubService.subscribeWithHandler(
      API_TOKEN_REVOKED_CHANNEL,
      (_channel, message) => {
        try {
          const payload = JSON.parse(message);
          if (payload?.tokenId !== undefined) {
            const tokenHash =
              typeof payload.tokenHash === 'string' ? payload.tokenHash : undefined;
            this.invalidateTokenCache(payload.tokenId, tokenHash).catch((err) => {
              this.logger.error('API token cache invalidation failed', err);
            });
          }
        } catch (err) {
          this.logger.error(
            'Invalid API token revocation message',
            err as Error,
          );
        }
      },
    );
  }

  async verify(apiToken: string): Promise<PatVerificationResult> {
    const normalizedToken = typeof apiToken === 'string' ? apiToken.trim() : '';
    if (!normalizedToken) {
      throw new UnauthorizedException('Invalid API token');
    }

    const tokenHash = this.hashToken(normalizedToken);
    const cachedState = await this.cacheService.get<ApiTokenState>(
      this.tokenHashCacheKey(tokenHash),
    );
    if (this.isCachedTokenState(cachedState)) {
      return this.toVerificationResult(cachedState, tokenHash);
    }

    const record = await this.queryBuilder.findOne({
      table: API_TOKEN_TABLE,
      where: { tokenHash },
    });

    if (!record) {
      throw new UnauthorizedException('Invalid API token');
    }

    const tokenId = String(this.recordId(record));
    const userId = String(this.tokenUserId(record));
    const expiresAt = this.recordExpiresAt(record);
    if (expiresAt && expiresAt.getTime() <= Date.now()) {
      await this.invalidateTokenCache(tokenId, tokenHash);
      throw new UnauthorizedException('API token has expired');
    }

    await this.queryBuilder.update(API_TOKEN_TABLE, this.recordId(record), {
      lastUsedAt: new Date(),
    });
    const state = {
      id: tokenId,
      userId,
      expiresAt: expiresAt ? expiresAt.toISOString() : null,
      tokenHash,
    };
    await this.cacheTokenState(state);

    return this.toVerificationResult(state, tokenHash);
  }

  async validateAccessPayload(payload: AuthTokenPayload): Promise<boolean> {
    if (payload?.tokenType !== 'api_token') return true;
    if (!payload?.tokenId || !payload?.id) return false;

    const tokenId = String(payload.tokenId);
    const userId = String(payload.id);
    let state = await this.cacheService.get<ApiTokenState>(
      this.tokenCacheKey(tokenId),
    );

    if (!this.isCachedTokenState(state)) {
      const record = await this.findTokenById(tokenId);
      if (!record) return false;
      state = {
        id: tokenId,
        userId: String(this.tokenUserId(record)),
        expiresAt: this.recordExpiresAt(record)?.toISOString() ?? null,
        tokenHash: typeof record.tokenHash === 'string' ? record.tokenHash : undefined,
      };
      await this.cacheTokenState(state);
    }

    if (state.userId !== userId) return false;
    if (state.expiresAt && new Date(state.expiresAt).getTime() <= Date.now()) {
      await this.invalidateTokenCache(tokenId, state.tokenHash);
      return false;
    }

    return true;
  }

  hashToken(token: string): string {
    return createHash('sha256').update(token).digest('hex');
  }

  async handleTokenRevoked(tokenId: string, tokenHash?: string): Promise<void> {
    await this.invalidateTokenCache(tokenId, tokenHash);
    await this.redisPubSubService.publish(API_TOKEN_REVOKED_CHANNEL, {
      tokenId: String(tokenId),
      ...(tokenHash ? { tokenHash } : {}),
    });
  }

  private tokenCacheKey(tokenId: string): string {
    return `${API_TOKEN_CACHE_PREFIX}:${tokenId}`;
  }

  private tokenHashCacheKey(tokenHash: string): string {
    return `${API_TOKEN_CACHE_PREFIX}:hash:${tokenHash}`;
  }

  private async cacheTokenState(
    state: ApiTokenState,
  ): Promise<void> {
    const expiresAtMs = state.expiresAt
      ? new Date(state.expiresAt).getTime() - Date.now()
      : API_TOKEN_STATE_TTL_MS;
    const ttlMs = Math.max(1, Math.min(API_TOKEN_STATE_TTL_MS, expiresAtMs));
    const writes = [
      this.cacheService.set(this.tokenCacheKey(state.id), state, ttlMs),
    ];
    if (state.tokenHash) {
      writes.push(
        this.cacheService.set(this.tokenHashCacheKey(state.tokenHash), state, ttlMs),
      );
    }
    await Promise.all(writes);
  }

  private async invalidateTokenCache(
    tokenId: unknown,
    tokenHash?: string,
  ): Promise<void> {
    const id = String(tokenId);
    const cachedState = tokenHash
      ? null
      : await this.cacheService.get<ApiTokenState>(this.tokenCacheKey(id));
    const resolvedTokenHash =
      tokenHash ??
      (this.isCachedTokenState(cachedState) ? cachedState.tokenHash : undefined);
    const keys = [this.tokenCacheKey(id)];
    if (resolvedTokenHash) keys.push(this.tokenHashCacheKey(resolvedTokenHash));
    await Promise.all(keys.map((key) => this.cacheService.deleteKey(key)));
  }

  private isCachedTokenState(value: unknown): value is ApiTokenState {
    if (!value || typeof value !== 'object') return false;
    const state = value as ApiTokenState;
    if (typeof state.id !== 'string' || typeof state.userId !== 'string') {
      return false;
    }
    return (
      state.expiresAt === null ||
      (typeof state.expiresAt === 'string' &&
        !Number.isNaN(new Date(state.expiresAt).getTime()))
    );
  }

  private toVerificationResult(
    state: ApiTokenState,
    tokenHash?: string,
  ): PatVerificationResult {
    const expiresAt = state.expiresAt ? new Date(state.expiresAt) : null;
    if (expiresAt && expiresAt.getTime() <= Date.now()) {
      void this.invalidateTokenCache(state.id, tokenHash ?? state.tokenHash);
      throw new UnauthorizedException('API token has expired');
    }

    return {
      payload: {
        id: state.userId,
        loginProvider: 'api_token',
        tokenType: 'api_token',
        tokenId: state.id,
      },
      expiresAt,
    };
  }

  private async findTokenById(tokenId: string): Promise<any> {
    const id = this.queryBuilder.isMongoDb()
      ? this.toMongoId(tokenId)
      : tokenId;
    return this.queryBuilder.findOne({
      table: API_TOKEN_TABLE,
      where: { [this.queryBuilder.getPkField()]: id },
    });
  }

  private recordId(record: any): any {
    return DatabaseConfigService.getRecordId(record);
  }

  private tokenUserId(record: any): any {
    return this.queryBuilder.isMongoDb()
      ? record.user?._id || record.user
      : record.userId || record.user?.id || record.user;
  }

  private recordExpiresAt(record: any): Date | null {
    if (!record?.expiresAt) return null;
    const date = new Date(record.expiresAt);
    return Number.isNaN(date.getTime()) ? null : date;
  }

  private toMongoId(value: unknown): ObjectId | unknown {
    if (typeof value === 'string' && ObjectId.isValid(value)) {
      return new ObjectId(value);
    }
    return value;
  }
}
