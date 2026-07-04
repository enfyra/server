import { randomBytes } from 'crypto';
import type { Redis } from 'ioredis';
import { BadRequestException } from '../../exceptions';
import type { ICache } from '../../shared/interfaces/cache.interface';
import type { IQueryBuilder } from '../../shared/interfaces/query-builder.interface';
import { EnvService } from '../../../shared/services';
import { Logger } from '../../../shared/logger';
import type {
  OAuthExchangePendingPayload,
  OAuthExchangeTokenPayload,
} from '../types/oauth-exchange-code.types';

const CODE_TTL_MS = 10 * 60 * 1000;
const CLEANUP_INTERVAL_MS = 60 * 1000;
const CODE_PREFIX = 'auth:oauth-exchange:code';
const PENDING_PREFIX = 'auth:oauth-exchange:pending';
const PENDING_INDEX_KEY = 'auth:oauth-exchange:pending-index';

export class OAuthExchangeCodeService {
  private readonly logger = new Logger(OAuthExchangeCodeService.name);
  private readonly cacheService: ICache;
  private readonly queryBuilderService: IQueryBuilder;
  private readonly redis: Redis;
  private readonly nodeName: string | null;
  private cleanupTimer?: ReturnType<typeof setInterval>;

  constructor(deps: {
    cacheService: ICache;
    queryBuilderService: IQueryBuilder;
    redis: Redis;
    envService: EnvService;
  }) {
    this.cacheService = deps.cacheService;
    this.queryBuilderService = deps.queryBuilderService;
    this.redis = deps.redis;
    this.nodeName = deps.envService.get('NODE_NAME') || null;
  }

  async init(): Promise<void> {
    await this.cleanupExpired();
    this.cleanupTimer = setInterval(() => {
      this.cleanupExpired().catch((err) => {
        this.logger.error('OAuth exchange code cleanup failed', err);
      });
    }, CLEANUP_INTERVAL_MS);
  }

  async createCodeForTokens(
    payload: OAuthExchangeTokenPayload,
  ): Promise<string> {
    const code = randomBytes(32).toString('base64url');
    const expiresAt = Date.now() + CODE_TTL_MS;
    const pending: OAuthExchangePendingPayload = {
      sessionId: payload.sessionId,
      expiresAt,
    };

    await this.cacheService.set(this.codeKey(code), payload, CODE_TTL_MS);
    await this.cacheService.set(
      this.pendingKey(code),
      pending,
      CODE_TTL_MS * 2,
    );
    const pipeline = this.redisTransaction();
    pipeline.zadd(this.pendingIndexKey(), expiresAt, code);
    pipeline.pexpire(this.pendingIndexKey(), CODE_TTL_MS * 2);
    await pipeline.exec();

    return code;
  }

  async exchange(code: unknown): Promise<OAuthExchangeTokenPayload> {
    if (typeof code !== 'string' || code.length === 0) {
      throw new BadRequestException('OAuth exchange code is required');
    }

    const payload = await this.cacheService.get<OAuthExchangeTokenPayload>(
      this.codeKey(code),
    );
    if (!payload) {
      throw new BadRequestException(
        'OAuth exchange code is invalid or expired',
      );
    }

    await this.deleteCode(code);
    return payload;
  }

  async cleanupExpired(): Promise<{ deleted: number }> {
    const indexKey = this.pendingIndexKey();
    const expiredCodes = await this.redis.zrangebyscore(
      indexKey,
      0,
      Date.now(),
      'LIMIT',
      0,
      100,
    );

    let deleted = 0;
    for (const code of expiredCodes) {
      const pending = await this.cacheService.get<OAuthExchangePendingPayload>(
        this.pendingKey(code),
      );
      await this.deleteCode(code);

      if (pending?.sessionId) {
        try {
          await this.queryBuilderService.delete(
            'enfyra_session',
            pending.sessionId,
          );
          deleted++;
        } catch (err) {
          this.logger.warn(
            `Failed to delete unexchanged OAuth session ${pending.sessionId}`,
          );
        }
      }
    }

    const remaining = await this.redis.zcard(indexKey);
    if (remaining > 0) {
      await this.redis.pexpire(indexKey, CODE_TTL_MS * 2);
    } else {
      await this.redis.del(indexKey);
    }

    return { deleted };
  }

  async onDestroy(): Promise<void> {
    if (this.cleanupTimer) clearInterval(this.cleanupTimer);
  }

  private async deleteCode(code: string): Promise<void> {
    await Promise.all([
      this.cacheService.deleteKey(this.codeKey(code)),
      this.cacheService.deleteKey(this.pendingKey(code)),
      this.redis.zrem(this.pendingIndexKey(), code),
    ]);
  }

  private codeKey(code: string): string {
    return `${CODE_PREFIX}:${code}`;
  }

  private pendingKey(code: string): string {
    return `${PENDING_PREFIX}:${code}`;
  }

  private pendingIndexKey(): string {
    return this.nodeName
      ? `${this.nodeName}:${PENDING_INDEX_KEY}`
      : PENDING_INDEX_KEY;
  }

  private redisTransaction(): ReturnType<Redis['pipeline']> {
    const redis = this.redis as Redis & {
      multi?: () => ReturnType<Redis['pipeline']>;
    };
    return typeof redis.multi === 'function'
      ? redis.multi()
      : this.redis.pipeline();
  }
}
