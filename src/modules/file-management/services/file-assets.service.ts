import { Injectable, NotFoundException, Logger } from '@nestjs/common';
import { QueryBuilderService } from '../../../infrastructure/query-builder/query-builder.service';
import { FileManagementService } from './file-management.service';
import { Response } from 'express';
import { RequestWithRouteData } from '../../../shared/interfaces/dynamic-context.interface';
import * as path from 'path';
import * as crypto from 'crypto';
import { ImageProcessorHelper } from '../utils/image-processor.helper';
import { StreamHelper } from '../utils/stream.helper';
import { FileValidationHelper } from '../utils/file-validation.helper';
import { ImageFormatHelper } from '../utils/image-format.helper';
import { StorageFactoryService } from '../storage/storage-factory.service';

@Injectable()
export class FileAssetsService {
  private readonly logger = new Logger(FileAssetsService.name);
  private readonly streamHelper: StreamHelper;

  constructor(
    private queryBuilder: QueryBuilderService,
    private fileManagementService: FileManagementService,
    private storageFactory: StorageFactoryService,
  ) {
    this.streamHelper = new StreamHelper();
    ImageProcessorHelper.configureSharp();
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

    if (storageType === 'Google Cloud Storage' || storageType === 'Cloudflare R2') {
      if (FileValidationHelper.isImageFile(mimetype, fileType) && FileValidationHelper.hasImageQueryParams(req)) {
        return void (await this.processImageWithQuery(
          location,
          req,
          res,
          filename,
          storageConfigId,
        ));
      }

      const stream = await this.fileManagementService.getStreamFromStorage(
        location,
        storageConfigId,
      );
      return void (await this.streamHelper.streamCloudFile(stream, res, filename, mimetype));
    }

    if (storageType === 'Amazon S3') {
      throw new NotFoundException('S3 storage not implemented yet');
    }

    // For local storage, use storage service to stream
    if (storageType === 'Local Storage') {
      const storageService = this.storageFactory.getStorageService('Local Storage');
      let storageConfig;
      
      if (storageConfigId) {
        storageConfig = await this.fileManagementService.getStorageConfigById(storageConfigId);
      } else {
        // Create default local storage config
        storageConfig = {
          type: 'Local Storage',
          name: 'Local',
          isEnabled: true,
        };
      }
      
      const stream = await storageService.getStream(location, storageConfig);
      
      if (FileValidationHelper.isImageFile(mimetype, fileType) && FileValidationHelper.hasImageQueryParams(req)) {
        // For images with query params, need to process
        // But we have stream, so we need to handle differently
        // For now, stream directly
        return void (await this.streamHelper.streamCloudFile(stream, res, filename, mimetype));
      }
      
      return void (await this.streamHelper.streamCloudFile(stream, res, filename, mimetype));
    }

    // Fallback to old method for backward compatibility
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

      const validation = ImageProcessorHelper.validateImageParams(width, height, quality);
      if (!validation.valid) {
        return void res.status(400).json({ error: validation.error });
      }

      const outputFormat = format || ImageFormatHelper.getOriginalFormat(filePath);
      const mimeType = ImageFormatHelper.getMimeType(outputFormat);

      // Check if streaming from cloud storage
      let shouldStream = false;
      if (storageConfigId) {
        const config = await this.fileManagementService.getStorageConfigById(
          storageConfigId,
        );
        shouldStream = config.type === 'Google Cloud Storage' || config.type === 'Cloudflare R2';
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

      const etag = `"${crypto.createHash('md5').update(processedBuffer).digest('hex')}"`;

      res.setHeader('Content-Type', mimeType);
      res.setHeader('Content-Length', processedBuffer.length);
      res.setHeader('Content-Disposition', `inline; filename="${filename}"`);
      res.setHeader('ETag', etag);

      // Browser cache headers
      if (cache && cache > 0)
        res.setHeader('Cache-Control', `public, max-age=${cache}`);
      else
        res.setHeader('Cache-Control', 'public, max-age=31536000'); // Default 1 year
      
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
    mimeType?: string,
  ): Promise<void> {
    try {
      const gcsStream = await this.fileManagementService.getStreamFromStorage(
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
      res.setHeader('Content-Disposition', `inline; filename="${filename}"`);

      // Browser cache headers - sufficient for streaming
      if (cache && cache > 0)
        res.setHeader('Cache-Control', `public, max-age=${cache}`);
      else
        res.setHeader('Cache-Control', 'public, max-age=31536000'); // Default 1 year

      const sharpStream = gcsStream.pipe(imageProcessor);

      sharpStream.on('error', (error) => {
        this.logger.error('Sharp processing error:', error);
        if (!res.headersSent)
          res.status(500).json({ error: 'Image processing failed' });
      });

      // Stream directly - no caching needed (streaming is fast enough)
      // Browser cache headers (Cache-Control) are sufficient
      this.streamHelper.setupImageStream(sharpStream, res, false);

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
