import { Logger } from '../../../shared/logger';
import { Redis } from 'ioredis';
import { EnvService } from '../../../shared/services/env.service';

export interface RateLimitResult {
  allowed: boolean;
  limit: number;
  remaining: number;
  resetAt: number;
  retryAfter?: number;
}

export interface RateLimitOptions {
  maxRequests: number;
  perSeconds: number;
}

export class RateLimitService {
  private readonly logger = new Logger(RateLimitService.name);
  private readonly redis: Redis;
  private readonly nodeName: string | null;
  private readonly envService: EnvService;

  constructor(deps: { redis: Redis; envService: EnvService }) {
    this.redis = deps.redis;
    this.envService = deps.envService;
    if (!this.redis) {
      throw new Error(
        'Redis connection not available - RateLimitService cannot initialize',
      );
    }
    this.nodeName = this.envService.get('NODE_NAME') || null;
  }

  private decorateKey(key: string): string {
    if (!this.nodeName) {
      return `ratelimit:${key}`;
    }
    return `ratelimit:${this.nodeName}:${key}`;
  }

  async check(
    key: string,
    options: RateLimitOptions,
  ): Promise<RateLimitResult> {
    const decoratedKey = this.decorateKey(key);
    const { maxRequests, perSeconds } = options;
    const windowMs = perSeconds * 1000;
    const now = Date.now();
    const windowStart = now - windowMs;

    try {
      const pipeline = this.redis.pipeline();
      pipeline.zremrangebyscore(decoratedKey, 0, windowStart);
      pipeline.zcard(decoratedKey);
      pipeline.zadd(decoratedKey, now, `${now}:${Math.random()}`);
      pipeline.pexpire(decoratedKey, windowMs);
      pipeline.zrange(decoratedKey, 0, -1, 'WITHSCORES');

      const results = await pipeline.exec();

      if (!results) {
        throw new Error('Redis pipeline execution failed');
      }

      const count = (results[1][1] as number) + 1;
      const remaining = Math.max(0, maxRequests - count);
      const allowed = count <= maxRequests;

      const resetAt = now + windowMs;

      let retryAfter: number | undefined;
      if (!allowed && count > maxRequests) {
        const oldestResult = results[4][1] as string[];
        if (oldestResult && oldestResult.length > 0) {
          const oldestTimestamp = parseFloat(oldestResult[0]);
          retryAfter = Math.ceil((oldestTimestamp + windowMs - now) / 1000);
          retryAfter = Math.max(1, retryAfter);
        }
      }

      return {
        allowed,
        limit: maxRequests,
        remaining,
        resetAt,
        retryAfter,
      };
    } catch (error) {
      this.logger.error(`Rate limit check failed for key ${key}:`, error);
      throw error;
    }
  }

  async reset(key: string): Promise<void> {
    const decoratedKey = this.decorateKey(key);
    await this.redis.del(decoratedKey);
  }

  async status(
    key: string,
    options: RateLimitOptions,
  ): Promise<RateLimitResult> {
    const decoratedKey = this.decorateKey(key);
    const { perSeconds } = options;
    const windowMs = perSeconds * 1000;
    const now = Date.now();
    const windowStart = now - windowMs;

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

      return {
        allowed: true,
        limit: options.maxRequests,
        remaining: Math.max(0, options.maxRequests - count),
        resetAt: now + windowMs,
      };
    } catch (error) {
      this.logger.error(`Rate limit status check failed for key ${key}:`, error);
      throw error;
    }
  }
}
