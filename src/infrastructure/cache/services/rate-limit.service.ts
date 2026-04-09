import { Injectable, Logger } from '@nestjs/common';
import { RedisService } from '@liaoliaots/nestjs-redis';
import { Redis } from 'ioredis';
import { ConfigService } from '@nestjs/config';

export interface RateLimitResult {
  allowed: boolean;
  remaining: number;
  resetAt: number;
  retryAfter: number;
  limit: number;
  window: number;
}

export interface RateLimitOptions {
  maxRequests: number;
  perSeconds: number;
}

@Injectable()
export class RateLimitService {
  private readonly logger = new Logger(RateLimitService.name);
  private readonly redis: Redis;
  private readonly nodeName: string | null;

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

  constructor(
    private readonly redisService: RedisService,
    private readonly configService: ConfigService,
  ) {
    this.redis = this.redisService.getOrNil();
    if (!this.redis) {
      this.logger.warn(
        'Redis connection not available - RateLimitService will not work',
      );
    }
    this.nodeName = this.configService.get<string>('NODE_NAME') || null;
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

    try {
      await this.redis.zremrangebyscore(
        decoratedKey,
        0,
        now - options.perSeconds * 1000,
      );
      const current = await this.redis.zcard(decoratedKey);
      const remaining = Math.max(0, options.maxRequests - current);
      const ttl = await this.redis.pttl(decoratedKey);
      const resetAt = ttl > 0 ? now + ttl : now + options.perSeconds * 1000;

      return {
        allowed: current < options.maxRequests,
        remaining,
        resetAt,
        retryAfter: 0,
        limit: options.maxRequests,
        window: options.perSeconds,
      };
    } catch (error) {
      this.logger.error(`Rate limit status check failed: ${error}`);
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
