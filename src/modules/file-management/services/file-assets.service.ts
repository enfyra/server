import { Injectable, NotFoundException, Logger } from '@nestjs/common';
import {
  AuthenticationException,
  AuthorizationException,
} from '../../../core/exceptions/custom-exceptions';
import { QueryBuilderService } from '../../../infrastructure/query-builder/query-builder.service';
import { FileManagementService } from './file-management.service';
import { Response } from 'express';
import { RequestWithRouteData } from '../../../shared/interfaces/dynamic-context.interface';
import * as fs from 'fs';
import * as path from 'path';
import * as sharp from 'sharp';
import * as crypto from 'crypto';
import { RedisService } from '@liaoliaots/nestjs-redis';
import { Redis } from 'ioredis';
import { REDIS_TTL } from '../../../shared/utils/constant';

interface CacheHitTracker {
  hits: number;
  lastAccessed: number;
  size: number;
}

@Injectable()
export class FileAssetsService {
  private readonly logger = new Logger(FileAssetsService.name);
  private readonly redis: Redis | null;
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

  constructor(
    private queryBuilder: QueryBuilderService,
    private fileManagementService: FileManagementService,
    private redisService: RedisService,
  ) {
    this.redis = this.redisService.getOrNil();
    if (!this.redis)
      this.logger.warn('Redis not available - image caching disabled');

    sharp.cache({ memory: 50, files: 20 });
    sharp.concurrency(2);
    sharp.simd(true);

    if (this.redis) {
      this.configureRedis();
      setInterval(() => this.logCacheStats(), REDIS_TTL.CACHE_STATS_INTERVAL);
      setInterval(() => this.cleanupLeastUsedCache(), REDIS_TTL.CACHE_CLEANUP_INTERVAL);
    }
  }

  private async configureRedis(): Promise<void> {
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

  async streamFile(req: RequestWithRouteData, res: Response): Promise<void> {
    const fileId = req.routeData?.params?.id || req.params.id;
    if (!fileId)
      return void res.status(400).json({ error: 'File ID is required' });

    const fileResult = await this.queryBuilder.select({
      tableName: 'file_definition',
      filter: { id: { _eq: fileId } },
      fields: ['*', 'storageConfig.*'],
    });

    const file = fileResult.data?.[0];
    if (!file) throw new NotFoundException(`File not found: ${fileId}`);

    if (file.isPublished !== true) {
      const permissionsResult = await this.queryBuilder.select({
        tableName: 'file_permission_definition',
        filter: {
          fileId: { _eq: fileId },
          isEnabled: { _eq: true }
        },
      });
      const permissions = permissionsResult.data;

      for (const perm of permissions) {
        if (perm.userId) {
          perm.allowedUsers = await this.queryBuilder.findOneWhere('user_definition', { id: perm.userId });
        }
        if (perm.roleId) {
          perm.role = await this.queryBuilder.findOneWhere('role_definition', { id: perm.roleId });
        }
      }

      file.permissions = permissions;
    }

    await this.checkFilePermissions(file, req);

    const { location, storageConfig, filename, mimetype, type: fileType } = file as any;
    const storageType = storageConfig?.type || 'local';
    const storageConfigId = storageConfig?.id || null;

    if (storageType === 'gcs') {
      if (this.isImageFile(mimetype, fileType) && this.hasImageQueryParams(req)) {
        return void (await this.processImageWithQuery(
          location,
          req,
          res,
          filename,
          storageConfigId,
        ));
      }

      const stream = await this.fileManagementService.getStreamFromGCS(
        location,
        storageConfigId,
      );
      return void (await this.streamCloudFile(stream, res, filename, mimetype));
    }

    if (storageType === 's3') {
      throw new NotFoundException('S3 storage not implemented yet');
    }

    const filePath = this.fileManagementService.getFilePath(
      path.basename(location),
    );

    if (!(await this.fileExists(filePath))) {
      this.logger.error(`File not found: ${filePath}`);
      throw new NotFoundException('Physical file not found');
    }

    if (this.isImageFile(mimetype, fileType) && this.hasImageQueryParams(req)) {
      return void (await this.processImageWithQuery(
        filePath,
        req,
        res,
        filename,
      ));
    }

    await this.streamRegularFile(filePath, res, filename, mimetype);
  }

  private isImageFile(mimetype: string, fileType: string): boolean {
    return mimetype.startsWith('image/') || fileType === 'image';
  }

  private hasImageQueryParams(req: RequestWithRouteData): boolean {
    const query = req.routeData?.context?.$query || req.query;
    return !!(query.format || query.width || query.height || query.quality);
  }

  private async processImageWithQuery(
    filePath: string,
    req: RequestWithRouteData,
    res: Response,
    filename: string,
    storageConfigId?: number | string,
  ): Promise<void> {
    try {
      const query = req.routeData?.context?.$query || req.query;
      const format = query.format as string;
      const width = query.width
        ? parseInt(query.width as string, 10)
        : undefined;
      const height = query.height
        ? parseInt(query.height as string, 10)
        : undefined;
      const quality = query.quality
        ? parseInt(query.quality as string, 10)
        : undefined;
      const cache = query.cache
        ? parseInt(query.cache as string, 10)
        : undefined;

      const cacheKey = this.generateCacheKey(
        filePath,
        format,
        width,
        height,
        quality,
      );
      const frequency = await this.incrementFrequency(cacheKey);
      const cachedImage = await this.getFromCache(cacheKey);
      if (cachedImage) {
        await this.incrementStats('hits');
        res.setHeader('Content-Type', cachedImage.contentType);
        res.setHeader('Content-Length', cachedImage.buffer.length);
        res.setHeader('X-Cache', 'HIT');
        res.setHeader('Content-Disposition', `inline; filename="${filename}"`);
        if (cache && cache > 0)
          res.setHeader('Cache-Control', `public, max-age=${cache}`);
        return void res.send(cachedImage.buffer);
      }

      await this.incrementStats('misses');

      if (width && (width < 1 || width > 4000))
        return void res.status(400).json({ error: 'Width 1-4000' });
      if (height && (height < 1 || height > 4000))
        return void res.status(400).json({ error: 'Height 1-4000' });
      if (quality && (quality < 1 || quality > 100))
        return void res.status(400).json({ error: 'Quality 1-100' });

      let imageInput: Buffer | string;
      if (storageConfigId) {
        const config = await this.fileManagementService.getStorageConfigById(
          storageConfigId,
        );
        if (config.type === 'gcs') {
          imageInput = await this.fileManagementService.getBufferFromGCS(
            filePath,
            storageConfigId,
          );
        } else {
          imageInput = filePath;
        }
      } else {
        imageInput = filePath;
      }

      let imageProcessor = sharp(imageInput, {
        failOnError: false,
        density: 72,
        limitInputPixels: 268402689,
      })
        .rotate()
        .withMetadata();

      if (width || height) {
        imageProcessor = imageProcessor.resize(width, height, {
          fit: 'inside',
          withoutEnlargement: true,
          fastShrinkOnLoad: true,
        });
      }

      if (format) {
        const supportedFormats = ['jpeg', 'jpg', 'png', 'webp', 'avif', 'gif'];
        if (!supportedFormats.includes(format.toLowerCase())) {
          return void res
            .status(400)
            .json({
              error: `Unsupported format: ${supportedFormats.join(', ')}`,
            });
        }
        imageProcessor = this.setImageFormat(
          imageProcessor,
          format.toLowerCase(),
          quality,
        );
      } else if (quality) {
        const originalFormat = path.extname(filePath).toLowerCase().slice(1);
        imageProcessor = this.setImageFormat(
          imageProcessor,
          originalFormat,
          quality,
        );
      }

      const outputFormat = format || this.getOriginalFormat(filePath);
      const mimeType = this.getMimeType(outputFormat);
      const processedBuffer = await imageProcessor.toBuffer();

      if (
        processedBuffer.length < 10 * 1024 * 1024 &&
        frequency >= this.minHitsToCache
      ) {
        await this.addToCache(cacheKey, processedBuffer, mimeType);
        await this.addToHotKeys(cacheKey, processedBuffer.length);
      }

      const etag = `"${crypto.createHash('md5').update(processedBuffer).digest('hex')}"`;

      res.setHeader('Content-Type', mimeType);
      res.setHeader('Content-Length', processedBuffer.length);
      res.setHeader('X-Cache', 'MISS');
      res.setHeader('Content-Disposition', `inline; filename="${filename}"`);
      res.setHeader('ETag', etag);

      if (cache && cache > 0)
        res.setHeader('Cache-Control', `public, max-age=${cache}`);
      if (req.headers['if-none-match'] === etag)
        return void res.status(304).end();

      res.send(processedBuffer);
    } catch (error) {
      this.logger.error('Image processing error:', error);
      if (!res.headersSent)
        res.status(500).json({ error: 'Image processing failed' });
    }
  }

  private setImageFormat(
    imageProcessor: sharp.Sharp,
    format: string,
    quality = 80,
  ): sharp.Sharp {
    const formatMap = {
      jpeg: () =>
        imageProcessor.jpeg({
          quality,
          progressive: true,
          mozjpeg: true,
          trellisQuantisation: true,
          overshootDeringing: true,
          optimizeScans: true,
        }),
      jpg: () =>
        imageProcessor.jpeg({
          quality,
          progressive: true,
          mozjpeg: true,
          trellisQuantisation: true,
          overshootDeringing: true,
          optimizeScans: true,
        }),
      png: () =>
        imageProcessor.png({ quality, compressionLevel: 9, progressive: true }),
      webp: () =>
        imageProcessor.webp({ quality, effort: 4, smartSubsample: true }),
      avif: () => imageProcessor.avif({ quality, effort: 4 }),
      gif: () => imageProcessor.gif(),
    };
    return formatMap[format]?.() || imageProcessor;
  }

  private async streamRegularFile(
    filePath: string,
    res: Response,
    filename: string,
    mimetype: string,
  ): Promise<void> {
    const stats = await fs.promises.stat(filePath);
    res.setHeader('Content-Type', mimetype);
    res.setHeader('Content-Length', stats.size);
    res.setHeader('Content-Disposition', `inline; filename="${filename}"`);

    const fileStream = fs.createReadStream(filePath);
    fileStream.on('error', (error) => {
      this.logger.error('File stream error:', error);
      if (!res.headersSent) res.status(500).json({ error: 'Stream error' });
    });
    fileStream.pipe(res);
  }

  private async streamCloudFile(
    stream: any,
    res: Response,
    filename: string,
    mimetype: string,
  ): Promise<void> {
    res.setHeader('Content-Type', mimetype);
    res.setHeader('Content-Disposition', `inline; filename="${filename}"`);

    stream.on('error', (error) => {
      this.logger.error('Cloud stream error:', error);
      if (!res.headersSent) res.status(500).json({ error: 'Stream error' });
    });

    stream.pipe(res);
  }

  private getOriginalFormat(filePath: string): string {
    const ext = path.extname(filePath).toLowerCase().slice(1);
    return ext === 'jpg' ? 'jpeg' : ext;
  }

  private getMimeType(format: string): string {
    const types = {
      jpeg: 'image/jpeg',
      jpg: 'image/jpeg',
      png: 'image/png',
      webp: 'image/webp',
      avif: 'image/avif',
      gif: 'image/gif',
    };
    return types[format] || 'image/jpeg';
  }

  private async fileExists(filePath: string): Promise<boolean> {
    try {
      const stats = await fs.promises.stat(filePath);
      return stats.isFile();
    } catch {
      return false;
    }
  }

  private async checkFilePermissions(
    file: any,
    req: RequestWithRouteData,
  ): Promise<void> {
    if (file.isPublished === true) return;

    const user = req.routeData?.context?.$user || req.user;
    if (!user?.id) throw new AuthenticationException('Authentication required');
    if (user.isRootAdmin === true) return;

    const hasAccess = (file.permissions || []).some(
      (p: any) =>
        p.isEnabled !== false &&
        (p.allowedUsers?.id === user.id ||
          (p.role && user.role?.id === p.role.id)),
    );

    if (!hasAccess) throw new AuthorizationException('Access denied');
  }

  private generateCacheKey(
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

  private async getFromCache(
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

  private async addToCache(
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

  private async incrementStats(type: 'hits' | 'misses'): Promise<void> {
    if (!this.redis) return;
    try {
      await this.redis.hincrby(this.statsKey, type, 1);
    } catch (error) {
      this.logger.error(`Stats error: ${error.message}`);
    }
  }

  private async logCacheStats(): Promise<void> {
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

  private async incrementFrequency(key: string): Promise<number> {
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

  private async addToHotKeys(key: string, size: number): Promise<void> {
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

  private async cleanupLeastUsedCache(): Promise<void> {
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
}
