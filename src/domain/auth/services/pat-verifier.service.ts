import { createHash, randomUUID } from 'crypto';
import { ObjectId } from 'mongodb';
import { DatabaseConfigService } from '../../../shared/services';
import type { ICache } from '../../shared/interfaces/cache.interface';
import type { IQueryBuilder } from '../../shared/interfaces/query-builder.interface';
import type { IRedisPubSub } from '../../shared/interfaces/redis-pubsub.interface';
import { UnauthorizedException } from '../../exceptions';
import { Logger } from '../../../shared/logger';
import {
  API_TOKEN_HARD_REVALIDATION_MS,
  API_TOKEN_CACHE_PREFIX,
  API_TOKEN_LAST_USED_FLUSH_BATCH_SIZE,
  API_TOKEN_LAST_USED_FLUSH_MS,
  API_TOKEN_LAST_USED_WRITE_INTERVAL_MS,
  API_TOKEN_REVOKED_CHANNEL,
  API_TOKEN_REVOCATION_TOMBSTONE_TTL_MS,
  API_TOKEN_STATE_REVALIDATION_LOCK_TTL_MS,
  API_TOKEN_STATE_REVALIDATION_POLL_MS,
  API_TOKEN_STATE_REVALIDATION_WAIT_MS,
  API_TOKEN_STATE_REFRESH_WINDOW_MS,
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
  hardRevalidateAt?: string;
  cacheExpiresAt?: string;
};

export class PatVerifierService {
  private readonly logger = new Logger(PatVerifierService.name);
  private readonly queryBuilder: IQueryBuilder;
  private readonly cacheService: ICache;
  private readonly redisPubSubService: IRedisPubSub;
  private readonly pendingLastUsed = new Set<string>();
  private readonly lastUsedCooldowns = new Map<string, number>();
  private readonly rawVerificationInFlight = new Map<
    string,
    Promise<PatVerificationResult>
  >();
  private readonly accessValidationInFlight = new Map<string, Promise<boolean>>();
  private lastUsedFlushTimer: ReturnType<typeof setInterval> | null = null;
  private lastUsedFlushInFlight = false;
  private initialized = false;

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
    if (this.initialized) return;
    this.initialized = true;
    this.redisPubSubService.subscribeWithHandler(
      API_TOKEN_REVOKED_CHANNEL,
      (_channel, message) => {
        try {
          const payload = JSON.parse(message);
          if (payload?.tokenId !== undefined) {
            const tokenHash =
              typeof payload.tokenHash === 'string' ? payload.tokenHash : undefined;
            this.handleRevokedToken(payload.tokenId, tokenHash).catch((err) => {
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
    this.lastUsedFlushTimer = setInterval(() => {
      void this.flushLastUsedAt();
    }, API_TOKEN_LAST_USED_FLUSH_MS);
  }

  onDestroy(): void {
    if (this.lastUsedFlushTimer) {
      clearInterval(this.lastUsedFlushTimer);
      this.lastUsedFlushTimer = null;
    }
    this.pendingLastUsed.clear();
    this.lastUsedCooldowns.clear();
    this.rawVerificationInFlight.clear();
    this.accessValidationInFlight.clear();
    this.initialized = false;
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
      const verification = this.toVerificationResult(cachedState, tokenHash);
      if (!this.requiresHardRevalidation(cachedState)) {
        this.refreshCachedStateIfNeeded(
          this.tokenHashCacheKey(tokenHash),
          cachedState,
        );
        this.scheduleLastUsedAt(cachedState.id);
        return verification;
      }
    }

    if (await this.isTokenHashRevoked(tokenHash)) {
      throw new UnauthorizedException('Invalid API token');
    }

    return this.singleFlight(
      this.rawVerificationInFlight,
      tokenHash,
      () =>
        this.verifyAfterRevalidation(
          tokenHash,
          this.isCachedTokenState(cachedState) ? cachedState : null,
        ),
    );
  }

  async validateAccessPayload(payload: AuthTokenPayload): Promise<boolean> {
    if (payload?.tokenType !== 'api_token') return true;
    if (!payload?.tokenId || !payload?.id) return false;

    const tokenId = String(payload.tokenId);
    const userId = String(payload.id);
    const state = await this.cacheService.get<ApiTokenState>(
      this.tokenCacheKey(tokenId),
    );
    const cachedResult = await this.validateCachedAccessState(
      state,
      tokenId,
      userId,
    );
    if (cachedResult !== null) return cachedResult;

    if (await this.isTokenRevoked(tokenId)) return false;

    return this.singleFlight(
      this.accessValidationInFlight,
      `${tokenId}:${userId}`,
      () => this.validateAfterRevalidation(tokenId, userId),
    );
  }

  hashToken(token: string): string {
    return createHash('sha256').update(token).digest('hex');
  }

  async handleTokenRevoked(tokenId: string, tokenHash?: string): Promise<void> {
    await this.handleRevokedToken(tokenId, tokenHash);
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

  private tokenRevocationKey(tokenId: string): string {
    return `${API_TOKEN_CACHE_PREFIX}:revoked:${tokenId}`;
  }

  private tokenHashRevocationKey(tokenHash: string): string {
    return `${API_TOKEN_CACHE_PREFIX}:revoked:hash:${tokenHash}`;
  }

  private tokenStateLockKey(tokenId: string): string {
    return `${API_TOKEN_CACHE_PREFIX}:state-lock:${tokenId}`;
  }

  private tokenHashStateLockKey(tokenHash: string): string {
    return `${API_TOKEN_CACHE_PREFIX}:state-lock:hash:${tokenHash}`;
  }

  private lastUsedLockKey(tokenId: string): string {
    return `${API_TOKEN_CACHE_PREFIX}:last-used-lock:${tokenId}`;
  }

  private async verifyAfterRevalidation(
    tokenHash: string,
    staleState: ApiTokenState | null,
  ): Promise<PatVerificationResult> {
    const state = await this.loadTokenStateWithLock(
      staleState
        ? this.tokenStateLockKey(staleState.id)
        : this.tokenHashStateLockKey(tokenHash),
      this.tokenHashCacheKey(tokenHash),
      async () => {
        const record = await this.queryBuilder.findOne({
          table: API_TOKEN_TABLE,
          where: { tokenHash },
        });
        if (!record) {
          if (staleState) {
            await this.invalidateTokenCache(staleState.id, tokenHash);
          }
          return null;
        }

        const state = this.createTokenState(record, tokenHash);
        this.toVerificationResult(state, tokenHash);
        return (await this.cacheTokenState(state)) ? state : null;
      },
    );
    if (!state) throw new UnauthorizedException('Invalid API token');

    const verification = this.toVerificationResult(state, tokenHash);
    this.scheduleLastUsedAt(state.id);
    return verification;
  }

  private async validateAfterRevalidation(
    tokenId: string,
    userId: string,
  ): Promise<boolean> {
    const state = await this.loadTokenStateWithLock(
      this.tokenStateLockKey(tokenId),
      this.tokenCacheKey(tokenId),
      async () => {
        const record = await this.findTokenById(tokenId);
        if (!record) {
          await this.invalidateTokenCache(tokenId);
          return null;
        }

        const state = this.createTokenState(record);
        if (this.isTokenExpired(state)) {
          await this.invalidateTokenCache(tokenId, state.tokenHash);
          return null;
        }
        return (await this.cacheTokenState(state)) ? state : null;
      },
    );
    return state?.userId === userId;
  }

  private async validateCachedAccessState(
    state: unknown,
    tokenId: string,
    userId: string,
  ): Promise<boolean | null> {
    if (!this.isCachedTokenState(state)) return null;
    if (state.userId !== userId) return false;
    if (this.isTokenExpired(state)) {
      await this.invalidateTokenCache(tokenId, state.tokenHash);
      return false;
    }
    if (this.requiresHardRevalidation(state)) return null;

    this.refreshCachedStateIfNeeded(this.tokenCacheKey(tokenId), state);
    return true;
  }

  private async loadTokenStateWithLock(
    lockKey: string,
    cacheKey: string,
    loadFromDatabase: () => Promise<ApiTokenState | null>,
  ): Promise<ApiTokenState | null> {
    const lockValue = randomUUID();
    let acquired = false;
    try {
      acquired = await this.cacheService.acquire(
        lockKey,
        lockValue,
        API_TOKEN_STATE_REVALIDATION_LOCK_TTL_MS,
      );
    } catch (error) {
      this.logger.warn(`API token state lock failed: ${String(error)}`);
      return loadFromDatabase();
    }

    if (acquired) {
      try {
        const cachedState = await this.readUsableCachedTokenState(cacheKey);
        return cachedState ?? (await loadFromDatabase());
      } finally {
        await this.cacheService.release(lockKey, lockValue).catch((error) => {
          this.logger.warn(`API token state lock release failed: ${String(error)}`);
        });
      }
    }

    const cachedState = await this.waitForUsableCachedTokenState(cacheKey);
    return cachedState ?? (await loadFromDatabase());
  }

  private async readUsableCachedTokenState(
    cacheKey: string,
  ): Promise<ApiTokenState | null> {
    const state = await this.cacheService.get<ApiTokenState>(cacheKey);
    if (
      !this.isCachedTokenState(state) ||
      this.isTokenExpired(state) ||
      this.requiresHardRevalidation(state)
    ) {
      return null;
    }
    return state;
  }

  private async waitForUsableCachedTokenState(
    cacheKey: string,
  ): Promise<ApiTokenState | null> {
    const deadline = Date.now() + API_TOKEN_STATE_REVALIDATION_WAIT_MS;
    while (Date.now() < deadline) {
      await new Promise<void>((resolve) => {
        setTimeout(resolve, API_TOKEN_STATE_REVALIDATION_POLL_MS);
      });
      const state = await this.readUsableCachedTokenState(cacheKey);
      if (state) return state;
    }
    return null;
  }

  private async cacheTokenState(state: ApiTokenState): Promise<boolean> {
    const cachedState = this.withCacheExpiry(state);
    const ttlMs = this.cacheTtlMs(cachedState);
    const entries = [
      {
        key: this.tokenCacheKey(cachedState.id),
        value: cachedState,
        ttlMs,
      },
    ];
    if (cachedState.tokenHash) {
      entries.push({
        key: this.tokenHashCacheKey(cachedState.tokenHash),
        value: cachedState,
        ttlMs,
      });
    }
    return this.cacheService.setManyIfKeyAbsent(
      this.tokenRevocationKey(cachedState.id),
      entries,
    );
  }

  private async invalidateTokenCache(
    tokenId: unknown,
    tokenHash?: string,
  ): Promise<void> {
    const id = String(tokenId);
    this.pendingLastUsed.delete(id);
    this.lastUsedCooldowns.delete(id);
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

  private async handleRevokedToken(
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
    const tombstones = [
      {
        key: this.tokenRevocationKey(id),
        value: true,
        ttlMs: API_TOKEN_REVOCATION_TOMBSTONE_TTL_MS,
      },
    ];
    const keysToDelete = [this.tokenCacheKey(id)];
    if (resolvedTokenHash) {
      tombstones.push({
        key: this.tokenHashRevocationKey(resolvedTokenHash),
        value: true,
        ttlMs: API_TOKEN_REVOCATION_TOMBSTONE_TTL_MS,
      });
      keysToDelete.push(this.tokenHashCacheKey(resolvedTokenHash));
    }
    await this.cacheService.setManyAndDelete(tombstones, keysToDelete);
    this.pendingLastUsed.delete(id);
    this.lastUsedCooldowns.delete(id);
  }

  private async isTokenRevoked(tokenId: string): Promise<boolean> {
    return (await this.cacheService.get(this.tokenRevocationKey(tokenId))) === true;
  }

  private async isTokenHashRevoked(tokenHash: string): Promise<boolean> {
    return (
      (await this.cacheService.get(this.tokenHashRevocationKey(tokenHash))) === true
    );
  }

  private async singleFlight<T>(
    inFlight: Map<string, Promise<T>>,
    key: string,
    load: () => Promise<T>,
  ): Promise<T> {
    const existing = inFlight.get(key);
    if (existing) return existing;

    const task = load().finally(() => {
      inFlight.delete(key);
    });
    inFlight.set(key, task);
    return task;
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

  private createTokenState(record: any, tokenHash?: string): ApiTokenState {
    const expiresAt = this.recordExpiresAt(record);
    const hardRevalidateAtMs = Math.min(
      Date.now() + API_TOKEN_HARD_REVALIDATION_MS,
      expiresAt?.getTime() ?? Number.POSITIVE_INFINITY,
    );
    return {
      id: String(this.recordId(record)),
      userId: String(this.tokenUserId(record)),
      expiresAt: expiresAt ? expiresAt.toISOString() : null,
      tokenHash:
        tokenHash ??
        (typeof record.tokenHash === 'string' ? record.tokenHash : undefined),
      hardRevalidateAt: new Date(hardRevalidateAtMs).toISOString(),
    };
  }

  private cacheTtlMs(state: ApiTokenState): number {
    const expiresAtMs = state.expiresAt
      ? new Date(state.expiresAt).getTime() - Date.now()
      : API_TOKEN_STATE_TTL_MS;
    return Math.max(1, Math.min(API_TOKEN_STATE_TTL_MS, expiresAtMs));
  }

  private withCacheExpiry(state: ApiTokenState): ApiTokenState {
    const ttlMs = this.cacheTtlMs(state);
    return {
      ...state,
      cacheExpiresAt: new Date(Date.now() + ttlMs).toISOString(),
    };
  }

  private requiresHardRevalidation(state: ApiTokenState): boolean {
    const hardRevalidateAt = Date.parse(state.hardRevalidateAt ?? '');
    return !Number.isFinite(hardRevalidateAt) || hardRevalidateAt <= Date.now();
  }

  private isTokenExpired(state: ApiTokenState): boolean {
    return (
      state.expiresAt !== null &&
      new Date(state.expiresAt).getTime() <= Date.now()
    );
  }

  private refreshCachedStateIfNeeded(
    cacheKey: string,
    state: ApiTokenState,
  ): void {
    const cacheExpiresAt = Date.parse(state.cacheExpiresAt ?? '');
    if (
      !Number.isFinite(cacheExpiresAt) ||
      cacheExpiresAt - Date.now() > API_TOKEN_STATE_REFRESH_WINDOW_MS
    ) {
      return;
    }

    const refreshedState = this.withCacheExpiry(state);
    void this.cacheService
      .compareAndSet(
        cacheKey,
        state,
        refreshedState,
        this.cacheTtlMs(refreshedState),
      )
      .catch((error) => {
        this.logger.warn(`API token cache refresh failed: ${String(error)}`);
      });
  }

  private scheduleLastUsedAt(tokenId: string): void {
    if (!this.initialized) return;

    const now = Date.now();
    if (this.pendingLastUsed.has(tokenId)) {
      return;
    }

    const cooldownUntil = this.lastUsedCooldowns.get(tokenId);
    if (cooldownUntil && cooldownUntil > now) return;

    this.lastUsedCooldowns.set(
      tokenId,
      now + API_TOKEN_LAST_USED_WRITE_INTERVAL_MS,
    );
    void this.cacheService
      .acquire(
        this.lastUsedLockKey(tokenId),
        `${tokenId}:${now}`,
        API_TOKEN_LAST_USED_WRITE_INTERVAL_MS,
      )
      .then((acquired) => {
        if (acquired) this.pendingLastUsed.add(tokenId);
      })
      .catch((error) => {
        this.logger.warn(`API token last-used scheduling failed: ${String(error)}`);
      });
  }

  private async flushLastUsedAt(): Promise<void> {
    if (this.lastUsedFlushInFlight) return;
    this.pruneLastUsedCooldowns();
    if (this.pendingLastUsed.size === 0) return;

    this.lastUsedFlushInFlight = true;
    const batch = Array.from(this.pendingLastUsed).slice(
      0,
      API_TOKEN_LAST_USED_FLUSH_BATCH_SIZE,
    );
    for (const tokenId of batch) this.pendingLastUsed.delete(tokenId);

    try {
      const ids = batch.map((tokenId) =>
        this.queryBuilder.isMongoDb() ? this.toMongoId(tokenId) : tokenId,
      );
      const observedAt = new Date();
      await this.queryBuilder.updateMany(
        API_TOKEN_TABLE,
        ids,
        { lastUsedAt: observedAt, updatedAt: observedAt },
        this.queryBuilder.getPkField(),
      );
    } catch (error) {
      for (const tokenId of batch) this.pendingLastUsed.add(tokenId);
      this.logger.warn(`API token last-used batch update failed: ${String(error)}`);
    } finally {
      this.lastUsedFlushInFlight = false;
    }
  }

  private pruneLastUsedCooldowns(): void {
    const now = Date.now();
    for (const [tokenId, cooldownUntil] of this.lastUsedCooldowns) {
      if (cooldownUntil <= now) this.lastUsedCooldowns.delete(tokenId);
    }
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
