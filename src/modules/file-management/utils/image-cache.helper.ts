import { Logger } from '@nestjs/common';
import { Redis } from 'ioredis';
import { REDIS_TTL } from '../../../shared/utils/constant';
import * as crypto from 'crypto';
import * as fs from 'fs';

export class ImageCacheHelper {
  private readonly logger = new Logger(ImageCacheHelper.name);
  private readonly cachePrefix = 'image:cache:';
  private readonly statsKey = 'image:cache:stats';
  private readonly frequencyKey = 'image:freq';
  private readonly hotKeysKey = 'image:hot';
  private readonly minHitsToCache = 3;
  private readonly maxCacheMemory = 2 * 1024 * 1024 * 1024;
  private readonly evictionThreshold = 0.9;
  private readonly maxCacheAge = {
    small: REDIS_TTL.FILE_CACHE_TTL.SMALL / 1000,
    medium: REDIS_TTL.FILE_CACHE_TTL.MEDIUM / 1000,
    large: REDIS_TTL.FILE_CACHE_TTL.LARGE / 1000,
    xlarge: REDIS_TTL.FILE_CACHE_TTL.XLARGE / 1000,
  };

  constructor(private readonly redis: Redis | null) {}

  async configureRedis(): Promise<void> {
    if (!this.redis) return;

    try {
      const [maxMemory, policy] = await Promise.all([
        this.redis.config('GET', 'maxmemory'),
        this.redis.config('GET', 'maxmemory-policy'),
      ]);

      if (maxMemory[1] === '0') this.logger.warn('Redis maxmemory unlimited');
      else
        this.logger.log(
          `Redis: ${(parseInt(maxMemory[1]) / (1024 * 1024)).toFixed(0)}MB, ${policy[1]}`,
        );

      if (policy[1] === 'noeviction')
        this.logger.warn('Consider allkeys-lru policy');
    } catch (error) {
      this.logger.error(`Redis config error: ${error.message}`);
    }
  }

  generateCacheKey(
    filePath: string,
    format?: string,
    width?: number,
    height?: number,
    quality?: number,
  ): string {
    let mtime = 0;
    try {
      const stats = fs.statSync(filePath);
      mtime = stats.mtime.getTime();
    } catch {
      mtime = 0;
    }

    return crypto
      .createHash('md5')
      .update(
        `${filePath}-${mtime}-${format || 'original'}-${width || 0}x${height || 0}-q${quality || 80}`,
      )
      .digest('hex');
  }

  async getFromCache(
    key: string,
  ): Promise<{ buffer: Buffer; contentType: string } | null> {
    if (!this.redis) return null;
    try {
      const isHot = await this.redis.sismember(this.hotKeysKey, key);
      if (!isHot) return null;

      const [bufferData, metaData] = await Promise.all([
        this.redis.getBuffer(`${this.cachePrefix}${key}`),
        this.redis.get(`${this.cachePrefix}${key}:meta`),
      ]);

      if (!bufferData || !metaData) {
        await this.redis.srem(this.hotKeysKey, key);
        return null;
      }

      return {
        buffer: bufferData,
        contentType: JSON.parse(metaData).contentType,
      };
    } catch (error) {
      this.logger.error(`Cache error: ${error.message}`);
      return null;
    }
  }

  async addToCache(
    key: string,
    buffer: Buffer,
    contentType: string,
  ): Promise<void> {
    if (!this.redis) return;
    try {
      const size = buffer.length;
      const ttl = this.getMaxAge(size);
      const pipeline = this.redis.pipeline();

      pipeline.setex(`${this.cachePrefix}${key}`, ttl, buffer);
      pipeline.setex(
        `${this.cachePrefix}${key}:meta`,
        ttl,
        JSON.stringify({ contentType, size, timestamp: Date.now() }),
      );

      await pipeline.exec();
    } catch (error) {
      this.logger.error(`Cache add error: ${error.message}`);
    }
  }

  private getMaxAge(size: number): number {
    if (size < 100 * 1024) return this.maxCacheAge.small;
    if (size < 500 * 1024) return this.maxCacheAge.medium;
    if (size < 2 * 1024 * 1024) return this.maxCacheAge.large;
    return this.maxCacheAge.xlarge;
  }

  async incrementStats(type: 'hits' | 'misses'): Promise<void> {
    if (!this.redis) return;
    try {
      await this.redis.hincrby(this.statsKey, type, 1);
    } catch (error) {
      this.logger.error(`Stats error: ${error.message}`);
    }
  }

  async logCacheStats(): Promise<void> {
    if (!this.redis) return;

    try {
      const [stats, hotKeysCount, currentMemory] = await Promise.all([
        this.redis.hgetall(this.statsKey),
        this.redis.scard(this.hotKeysKey),
        this.getCurrentCacheMemory(),
      ]);

      const hits = parseInt(stats.hits || '0');
      const misses = parseInt(stats.misses || '0');
      if (hits + misses === 0) return;

      const hitRate = ((hits / (hits + misses)) * 100).toFixed(1);
      this.logger.log(
        `Cache: ${hitRate}% hit rate, ${hotKeysCount} hot keys, ${(currentMemory / 1024 / 1024).toFixed(0)}MB`,
      );

      await this.redis.del(this.statsKey);
    } catch (error) {
      this.logger.error(`Stats error: ${error.message}`);
    }
  }

  async incrementFrequency(key: string): Promise<number> {
    if (!this.redis) return 1;
    try {
      const frequency = await this.redis.hincrby(this.frequencyKey, key, 1);
      if (frequency === 1) await this.redis.expire(this.frequencyKey, 86400);
      return frequency;
    } catch (error) {
      this.logger.error(`Frequency error: ${error.message}`);
      return 1;
    }
  }

  async addToHotKeys(key: string, size: number): Promise<void> {
    if (!this.redis) return;
    try {
      const pipeline = this.redis.pipeline();
      pipeline.sadd(this.hotKeysKey, key);
      pipeline.hset(
        `${this.hotKeysKey}:meta`,
        key,
        JSON.stringify({ size, timestamp: Date.now() }),
      );
      await pipeline.exec();
    } catch (error) {
      this.logger.error(`Hot keys error: ${error.message}`);
    }
  }

  async cleanupLeastUsedCache(): Promise<void> {
    if (!this.redis) return;

    try {
      const currentMemory = await this.getCurrentCacheMemory();
      const memoryThreshold = this.maxCacheMemory * this.evictionThreshold;

      if (currentMemory < memoryThreshold) return;

      const targetMemory = this.maxCacheMemory * 0.7;
      const memoryToFree = currentMemory - targetMemory;

      const hotKeys = await this.redis.smembers(this.hotKeysKey);
      if (hotKeys.length === 0) return;

      const [frequencies, hotKeysMeta] = await Promise.all([
        this.redis.hmget(this.frequencyKey, ...hotKeys),
        this.redis.hmget(`${this.hotKeysKey}:meta`, ...hotKeys),
      ]);

      const candidates: Array<{
        key: string;
        frequency: number;
        size: number;
        timestamp: number;
      }> = [];

      for (let i = 0; i < hotKeys.length; i++) {
        const key = hotKeys[i];
        const freq = parseInt(frequencies[i] || '0');
        const meta = hotKeysMeta[i] ? JSON.parse(hotKeysMeta[i]) : null;

        if (meta) {
          candidates.push({
            key,
            frequency: freq,
            size: meta.size,
            timestamp: meta.timestamp,
          });
        }
      }

      candidates.sort((a, b) =>
        a.frequency !== b.frequency
          ? a.frequency - b.frequency
          : a.timestamp - b.timestamp,
      );

      let freedMemory = 0;
      let evictCount = 0;
      const pipeline = this.redis.pipeline();

      for (const item of candidates) {
        if (freedMemory >= memoryToFree) break;

        pipeline.del(`${this.cachePrefix}${item.key}`);
        pipeline.del(`${this.cachePrefix}${item.key}:meta`);
        pipeline.srem(this.hotKeysKey, item.key);
        pipeline.hdel(`${this.hotKeysKey}:meta`, item.key);

        freedMemory += item.size;
        evictCount++;
      }

      if (evictCount > 0) {
        await pipeline.exec();
        this.logger.log(
          `Evicted ${evictCount} items, freed ${(freedMemory / 1024 / 1024).toFixed(2)}MB`,
        );
      }
    } catch (error) {
      this.logger.error(`LRU cleanup error: ${error.message}`);
    }
  }

  private async getCurrentCacheMemory(): Promise<number> {
    if (!this.redis) return 0;

    try {
      const hotKeys = await this.redis.smembers(this.hotKeysKey);
      if (hotKeys.length === 0) return 0;

      const metaDataArray = await this.redis.hmget(
        `${this.hotKeysKey}:meta`,
        ...hotKeys,
      );
      let totalMemory = 0;

      for (const metaData of metaDataArray) {
        if (metaData) {
          try {
            totalMemory += JSON.parse(metaData).size || 0;
          } catch {}
        }
      }

      return totalMemory;
    } catch (error) {
      this.logger.error(`Memory calc error: ${error.message}`);
      return 0;
    }
  }

  shouldCache(frequency: number): boolean {
    return frequency >= this.minHitsToCache;
  }

  getMinHitsToCache(): number {
    return this.minHitsToCache;
  }
}

