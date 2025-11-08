import { Injectable, NotFoundException, Logger } from '@nestjs/common';
import { QueryBuilderService } from '../../../infrastructure/query-builder/query-builder.service';
import { FileManagementService } from './file-management.service';
import { Response } from 'express';
import { RequestWithRouteData } from '../../../shared/interfaces/dynamic-context.interface';
import * as path from 'path';
import * as crypto from 'crypto';
import { RedisService } from '@liaoliaots/nestjs-redis';
import { Redis } from 'ioredis';
import { REDIS_TTL } from '../../../shared/utils/constant';
import { ImageCacheHelper } from '../utils/image-cache.helper';
import { ImageProcessorHelper } from '../utils/image-processor.helper';
import { StreamHelper } from '../utils/stream.helper';
import { FileValidationHelper } from '../utils/file-validation.helper';
import { ImageFormatHelper } from '../utils/image-format.helper';

@Injectable()
export class FileAssetsService {
  private readonly logger = new Logger(FileAssetsService.name);
  private readonly redis: Redis | null;
  private readonly cacheHelper: ImageCacheHelper;
  private readonly streamHelper: StreamHelper;

  constructor(
    private queryBuilder: QueryBuilderService,
    private fileManagementService: FileManagementService,
    private redisService: RedisService,
  ) {
    this.redis = this.redisService.getOrNil();
    this.cacheHelper = new ImageCacheHelper(this.redis);
    this.streamHelper = new StreamHelper();

    if (!this.redis)
      this.logger.warn('Redis not available - image caching disabled');

    ImageProcessorHelper.configureSharp();

    if (this.redis) {
      this.cacheHelper.configureRedis();
      setInterval(() => this.cacheHelper.logCacheStats(), REDIS_TTL.CACHE_STATS_INTERVAL);
      setInterval(() => this.cacheHelper.cleanupLeastUsedCache(), REDIS_TTL.CACHE_CLEANUP_INTERVAL);
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

    await FileValidationHelper.checkFilePermissions(file, req);

    const { location, storageConfig, filename, mimetype, type: fileType } = file as any;
    const storageType = storageConfig?.type || 'Local Storage';
    const storageConfigId = storageConfig?.id || null;

    if (storageType === 'Google Cloud Storage') {
      if (FileValidationHelper.isImageFile(mimetype, fileType) && FileValidationHelper.hasImageQueryParams(req)) {
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
      return void (await this.streamHelper.streamCloudFile(stream, res, filename, mimetype));
    }

    if (storageType === 'Amazon S3') {
      throw new NotFoundException('S3 storage not implemented yet');
    }

    const filePath = this.fileManagementService.getFilePath(
      path.basename(location),
    );

    if (!(await FileValidationHelper.fileExists(filePath))) {
      this.logger.error(`File not found: ${filePath}`);
      throw new NotFoundException('Physical file not found');
    }

    if (FileValidationHelper.isImageFile(mimetype, fileType) && FileValidationHelper.hasImageQueryParams(req)) {
      return void (await this.processImageWithQuery(
        filePath,
        req,
        res,
        filename,
      ));
    }

    await this.streamHelper.streamRegularFile(filePath, res, filename, mimetype);
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

      const cacheKey = this.cacheHelper.generateCacheKey(
        filePath,
        format,
        width,
        height,
        quality,
      );
      const frequency = await this.cacheHelper.incrementFrequency(cacheKey);
      const cachedImage = await this.cacheHelper.getFromCache(cacheKey);
      if (cachedImage) {
        await this.cacheHelper.incrementStats('hits');
        res.setHeader('Content-Type', cachedImage.contentType);
        res.setHeader('Content-Length', cachedImage.buffer.length);
        res.setHeader('X-Cache', 'HIT');
        res.setHeader('Content-Disposition', `inline; filename="${filename}"`);
        if (cache && cache > 0)
          res.setHeader('Cache-Control', `public, max-age=${cache}`);
        return void res.send(cachedImage.buffer);
      }

      await this.cacheHelper.incrementStats('misses');

      const validation = ImageProcessorHelper.validateImageParams(width, height, quality);
      if (!validation.valid) {
        return void res.status(400).json({ error: validation.error });
      }

      const outputFormat = format || ImageFormatHelper.getOriginalFormat(filePath);
      const mimeType = ImageFormatHelper.getMimeType(outputFormat);

      let shouldStream = false;
      if (storageConfigId) {
        const config = await this.fileManagementService.getStorageConfigById(
          storageConfigId,
        );
        shouldStream = config.type === 'Google Cloud Storage';
      }

      if (shouldStream) {
        return void (await this.streamImageFromGCS(
          filePath,
          storageConfigId,
          req,
          res,
          filename,
          format,
          width,
          height,
          quality,
          cache,
          cacheKey,
          frequency,
          mimeType,
        ));
      }

      let imageInput: Buffer | string = filePath;

      let imageProcessor = ImageProcessorHelper.createProcessor(imageInput);

      imageProcessor = ImageProcessorHelper.applyResize(imageProcessor, width, height);

      if (format) {
        const formatValidation = ImageProcessorHelper.validateFormat(format);
        if (!formatValidation.valid) {
          return void res.status(400).json({ error: formatValidation.error });
        }
        imageProcessor = ImageProcessorHelper.setImageFormat(
          imageProcessor,
          format.toLowerCase(),
          quality,
        );
      } else if (quality) {
        const originalFormat = ImageFormatHelper.getOriginalFormat(filePath);
        imageProcessor = ImageProcessorHelper.setImageFormat(
          imageProcessor,
          originalFormat,
          quality,
        );
      }

      const processedBuffer = await imageProcessor.toBuffer();

      if (
        processedBuffer.length < 10 * 1024 * 1024 &&
        this.cacheHelper.shouldCache(frequency)
      ) {
        await this.cacheHelper.addToCache(cacheKey, processedBuffer, mimeType);
        await this.cacheHelper.addToHotKeys(cacheKey, processedBuffer.length);
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

  private async streamImageFromGCS(
    filePath: string,
    storageConfigId: number | string,
    req: RequestWithRouteData,
    res: Response,
    filename: string,
    format?: string,
    width?: number,
    height?: number,
    quality?: number,
    cache?: number,
    cacheKey?: string,
    frequency?: number,
    mimeType?: string,
  ): Promise<void> {
    try {
      const gcsStream = await this.fileManagementService.getStreamFromGCS(
        filePath,
        storageConfigId,
      );

      let imageProcessor = ImageProcessorHelper.createStreamProcessor();

      imageProcessor = ImageProcessorHelper.applyResize(imageProcessor, width, height);

      if (format) {
        const formatValidation = ImageProcessorHelper.validateFormat(format);
        if (!formatValidation.valid) {
          return void res.status(400).json({ error: formatValidation.error });
        }
        imageProcessor = ImageProcessorHelper.setImageFormat(
          imageProcessor,
          format.toLowerCase(),
          quality,
        );
      } else if (quality) {
        const originalFormat = ImageFormatHelper.getOriginalFormat(filePath);
        imageProcessor = ImageProcessorHelper.setImageFormat(
          imageProcessor,
          originalFormat,
          quality,
        );
      }

      res.setHeader('Content-Type', mimeType || 'image/jpeg');
      res.setHeader('X-Cache', 'MISS');
      res.setHeader('Content-Disposition', `inline; filename="${filename}"`);

      if (cache && cache > 0)
        res.setHeader('Cache-Control', `public, max-age=${cache}`);

      const shouldCache =
        cacheKey &&
        frequency &&
        this.cacheHelper.shouldCache(frequency) &&
        this.redis;

      const sharpStream = gcsStream.pipe(imageProcessor);

      sharpStream.on('error', (error) => {
        this.logger.error('Sharp processing error:', error);
        if (!res.headersSent)
          res.status(500).json({ error: 'Image processing failed' });
      });

      if (shouldCache && cacheKey) {
        this.streamHelper.setupImageStream(
          sharpStream,
          res,
          true,
          async (buffer: Buffer) => {
            await this.cacheHelper.addToCache(cacheKey, buffer, mimeType || 'image/jpeg');
            await this.cacheHelper.addToHotKeys(cacheKey, buffer.length);
          },
        );
      } else {
        this.streamHelper.setupImageStream(sharpStream, res, false);
      }

      this.streamHelper.handleStreamError(
        gcsStream,
        res,
        'Failed to stream from GCS',
      );
    } catch (error) {
      this.logger.error('Stream image from GCS error:', error);
      if (!res.headersSent)
        res.status(500).json({ error: 'Image streaming failed' });
    }
  }
}
