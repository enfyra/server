import { Logger } from '../../../shared/logger';
import { Redis } from 'ioredis';
import { EnvService } from '../../../shared/services';

export interface RateLimitOptions {
  maxRequests: number;
  perSeconds: number;
}

export interface RateLimitResult {
  allowed: boolean;
  remaining: number;
  resetAt: number;
  retryAfter: number;
  limit: number;
  window: number;
}

export class RateLimitService {
  private readonly logger = new Logger(RateLimitService.name);
  private readonly redis: Redis;
  private readonly nodeName: string | null;
  private readonly envService: EnvService;

  private readonly luaScript = `
    local key = KEYS[1]
    local limit = tonumber(ARGV[1])
    local window = tonumber(ARGV[2])
    local now = tonumber(ARGV[3])

    redis.call('ZREMRANGEBYSCORE', key, 0, now - window * 1000)

    local current = redis.call('ZCARD', key)

    if current < limit then
      redis.call('ZADD', key, now, now .. '-' .. math.random())
      redis.call('PEXPIRE', key, window * 1000)
      return {1, limit - current - 1, now + window * 1000, 0}
    else
      local oldest = redis.call('ZRANGE', key, 0, 0, 'WITHSCORES')
      local resetAt = tonumber(oldest[2]) + window * 1000
      return {0, 0, resetAt, math.ceil((resetAt - now) / 1000)}
    end
  `;

  constructor(deps: { redis: Redis; envService: EnvService }) {
    this.redis = deps.redis;
    this.envService = deps.envService;
    if (!this.redis) {
      this.logger.warn(
        'Redis connection not available - RateLimitService will not work',
      );
    }
    this.nodeName = this.envService.get('NODE_NAME') || null;
  }

  private decorateKey(key: string): string {
    const prefix = 'rl';
    if (this.nodeName) {
      return `${this.nodeName}:${prefix}:${key}`;
    }
    return `${prefix}:${key}`;
  }

  async check(
    key: string,
    options: RateLimitOptions,
  ): Promise<RateLimitResult> {
    if (!this.redis) {
      this.logger.warn('Redis not available, allowing request');
      return {
        allowed: true,
        remaining: options.maxRequests,
        resetAt: Date.now() + options.perSeconds * 1000,
        retryAfter: 0,
        limit: options.maxRequests,
        window: options.perSeconds,
      };
    }

    const decoratedKey = this.decorateKey(key);
    const now = Date.now();

    try {
      const result = await this.redis.eval(
        this.luaScript,
        1,
        decoratedKey,
        options.maxRequests,
        options.perSeconds,
        now,
      );

      const [allowed, remaining, resetAt, retryAfter] = result as [
        number,
        number,
        number,
        number,
      ];

      return {
        allowed: allowed === 1,
        remaining,
        resetAt,
        retryAfter,
        limit: options.maxRequests,
        window: options.perSeconds,
      };
    } catch (error) {
      this.logger.error(`Rate limit check failed: ${error}`);
      return {
        allowed: true,
        remaining: options.maxRequests,
        resetAt: Date.now() + options.perSeconds * 1000,
        retryAfter: 0,
        limit: options.maxRequests,
        window: options.perSeconds,
      };
    }
  }

  async reset(key: string): Promise<void> {
    if (!this.redis) return;
    const decoratedKey = this.decorateKey(key);
    await this.redis.del(decoratedKey);
  }

  async status(
    key: string,
    options: RateLimitOptions,
  ): Promise<RateLimitResult> {
    if (!this.redis) {
      return {
        allowed: true,
        remaining: options.maxRequests,
        resetAt: Date.now() + options.perSeconds * 1000,
        retryAfter: 0,
        limit: options.maxRequests,
        window: options.perSeconds,
      };
    }

    const decoratedKey = this.decorateKey(key);
    const now = Date.now();
    const windowStart = now - options.perSeconds * 1000;

    try {
      const pipeline = this.redis.pipeline();
      pipeline.zremrangebyscore(decoratedKey, 0, windowStart);
      pipeline.zcard(decoratedKey);
      pipeline.zrange(decoratedKey, 0, 0, 'WITHSCORES');
      const results = await pipeline.exec();

      if (!results) {
        throw new Error('Redis pipeline execution failed');
      }

      const count = results[1][1] as number;
      const oldestResult = results[2][1] as string[];
      let resetAt = now + options.perSeconds * 1000;
      if (oldestResult && oldestResult.length >= 2) {
        const oldestTimestamp = parseFloat(oldestResult[1]);
        if (!Number.isNaN(oldestTimestamp)) {
          resetAt = oldestTimestamp + options.perSeconds * 1000;
        }
      }
      const remaining = Math.max(0, options.maxRequests - count);

      return {
        allowed: count < options.maxRequests,
        remaining,
        resetAt,
        retryAfter: 0,
        limit: options.maxRequests,
        window: options.perSeconds,
      };
    } catch (error) {
      this.logger.error(`Rate limit status failed: ${error}`);
      return {
        allowed: true,
        remaining: options.maxRequests,
        resetAt: Date.now() + options.perSeconds * 1000,
        retryAfter: 0,
        limit: options.maxRequests,
        window: options.perSeconds,
      };
    }
  }
}
