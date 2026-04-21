import { Response } from 'express';
import * as fs from 'fs';
import * as path from 'path';
import { Logger } from '../../../shared/logger';
import { NotFoundException } from '../../../core/exceptions/custom-exceptions';
import { QueryBuilderService } from '../../../infrastructure/query-builder/query-builder.service';
import { FileManagementService } from './file-management.service';
import { StorageFactoryService } from '../storage/storage-factory.service';
import { ImageProcessorHelper } from '../utils/image-processor.helper';
import { StreamHelper } from '../utils/stream.helper';
import { FileValidationHelper } from '../utils/file-validation.helper';
import { ImageFormatHelper } from '../utils/image-format.helper';

export class FileAssetsService {
  private readonly logger = new Logger(FileAssetsService.name);
  private readonly streamHelper: StreamHelper;
  private readonly queryBuilderService: QueryBuilderService;
  private readonly fileManagementService: FileManagementService;
  private readonly storageFactoryService: StorageFactoryService;

  constructor(deps: {
    queryBuilderService: QueryBuilderService;
    fileManagementService: FileManagementService;
    storageFactoryService: StorageFactoryService;
  }) {
    this.queryBuilderService = deps.queryBuilderService;
    this.fileManagementService = deps.fileManagementService;
    this.storageFactoryService = deps.storageFactoryService;
    this.streamHelper = new StreamHelper();
    ImageProcessorHelper.configureSharp();
  }

  async streamFile(req: any, res: Response): Promise<void> {
    const fileId = req.routeData?.params?.id || req.params.id;
    if (!fileId)
      return void res.status(400).json({ error: 'File ID is required' });

    const fileResult = await this.queryBuilderService.find({
      table: 'file_definition',
      filter: { id: { _eq: fileId } },
      fields: ['*', 'storageConfig.*'],
    });

    const file = fileResult.data?.[0];
    if (!file) throw new NotFoundException(`File not found: ${fileId}`);

    if (!file.isPublished) {
      const permissionsResult = await this.queryBuilderService.find({
        table: 'file_permission_definition',
        filter: {
          fileId: { _eq: fileId },
          isEnabled: { _eq: true },
        },
        deep: {
          allowedUsers: { fields: ['id', 'email'] },
          role: { fields: ['id', 'name'] },
        },
      });
      const permissions = permissionsResult.data;

      for (const perm of permissions) {
        if (perm.roleId && !perm.role) {
          perm.role = await this.queryBuilderService.findOne({
            table: 'role_definition',
            where: { id: perm.roleId },
          });
        }
      }

      file.permissions = permissions;
    }

    await FileValidationHelper.checkFilePermissions(file, req);

    const {
      location,
      storageConfig,
      filename,
      mimetype,
      type: fileType,
    } = file as any;
    const storageType = storageConfig?.type || 'Local Storage';
    const storageConfigId = storageConfig?._id || storageConfig?.id || null;

    if (
      storageType === 'Google Cloud Storage' ||
      storageType === 'Cloudflare R2' ||
      storageType === 'Amazon S3'
    ) {
      if (
        FileValidationHelper.isImageFile(mimetype, fileType) &&
        FileValidationHelper.hasImageQueryParams(req)
      ) {
        return void (await this.processImageWithQuery(
          location,
          req,
          res,
          filename,
          storageConfigId,
        ));
      }

      const query = req.routeData?.context?.$query || req.query;
      const shouldDownload =
        query.download === 'true' || query.download === true;
      const stream = await this.fileManagementService.getStreamFromStorage(
        location,
        storageConfigId,
      );
      return void (await this.streamHelper.streamCloudFile(
        stream,
        res,
        filename,
        mimetype,
        shouldDownload,
      ));
    }

    if (storageType === 'Local Storage') {
      if (
        FileValidationHelper.isImageFile(mimetype, fileType) &&
        FileValidationHelper.hasImageQueryParams(req)
      ) {
        const basePath = path.join(process.cwd(), 'public');
        const relativePath = location.startsWith('/')
          ? location.slice(1)
          : location;
        const filePath = path.join(basePath, relativePath);

        return void (await this.processImageWithQuery(
          filePath,
          req,
          res,
          filename,
          storageConfigId,
        ));
      }

      const storageService =
        this.storageFactoryService.getStorageService('Local Storage');
      let sc: any;

      if (storageConfigId) {
        sc =
          await this.fileManagementService.getStorageConfigById(
            storageConfigId,
          );
      } else {
        sc = {
          type: 'Local Storage',
          name: 'Local',
          isEnabled: true,
        };
      }

      const query = req.routeData?.context?.$query || req.query;
      const shouldDownload =
        query.download === 'true' || query.download === true;
      const stream = await storageService.getStream(location, sc);
      return void (await this.streamHelper.streamCloudFile(
        stream,
        res,
        filename,
        mimetype,
        shouldDownload,
      ));
    }

    const filePath = this.fileManagementService.getFilePath(
      path.basename(location),
    );

    if (!(await FileValidationHelper.fileExists(filePath))) {
      this.logger.error(`File not found: ${filePath}`);
      throw new NotFoundException('Physical file not found');
    }

    if (
      FileValidationHelper.isImageFile(mimetype, fileType) &&
      FileValidationHelper.hasImageQueryParams(req)
    ) {
      return void (await this.processImageWithQuery(
        filePath,
        req,
        res,
        filename,
      ));
    }

    const query = req.routeData?.context?.$query || req.query;
    const shouldDownload = query.download === 'true' || query.download === true;
    await this.streamHelper.streamRegularFile(
      filePath,
      res,
      filename,
      mimetype,
      shouldDownload,
    );
  }

  private async processImageWithQuery(
    filePath: string,
    req: any,
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
      const shouldDownload =
        query.download === 'true' || query.download === true;
      const fit = query.fit as string;
      const gravity = query.gravity as string;
      const rotate = query.rotate
        ? parseInt(query.rotate as string, 10)
        : undefined;
      const flip = query.flip as string;
      const blur = query.blur ? parseFloat(query.blur as string) : undefined;
      const sharpen = query.sharpen
        ? parseFloat(query.sharpen as string)
        : undefined;
      const brightness = query.brightness
        ? parseInt(query.brightness as string, 10)
        : undefined;
      const contrast = query.contrast
        ? parseInt(query.contrast as string, 10)
        : undefined;
      const saturation = query.saturation
        ? parseInt(query.saturation as string, 10)
        : undefined;
      const grayscale = query.grayscale === 'true' || query.grayscale === true;

      const validation = ImageProcessorHelper.validateImageParams(
        width,
        height,
        quality,
      );
      if (!validation.valid) {
        return void res.status(400).json({ error: validation.error });
      }

      const fitValidation = ImageProcessorHelper.validateFit(fit);
      if (!fitValidation.valid) {
        return void res.status(400).json({ error: fitValidation.error });
      }

      const gravityValidation = ImageProcessorHelper.validateGravity(gravity);
      if (!gravityValidation.valid) {
        return void res.status(400).json({ error: gravityValidation.error });
      }

      const transformValidation = ImageProcessorHelper.validateTransformParams(
        rotate,
        flip,
        blur,
        sharpen,
        brightness,
        contrast,
        saturation,
      );
      if (!transformValidation.valid) {
        return void res.status(400).json({ error: transformValidation.error });
      }

      let shouldStream = false;
      if (storageConfigId) {
        const config =
          await this.fileManagementService.getStorageConfigById(
            storageConfigId,
          );
        shouldStream =
          config.type === 'Google Cloud Storage' ||
          config.type === 'Cloudflare R2' ||
          config.type === 'Amazon S3';
      }

      if (shouldStream) {
        const finalFormat =
          format || ImageFormatHelper.getOriginalFormat(filePath);
        const finalMimeType = ImageFormatHelper.getMimeType(finalFormat);
        return void (await this.streamImageFromCloud(
          filePath,
          storageConfigId!,
          req,
          res,
          filename,
          format,
          width,
          height,
          quality,
          cache,
          finalMimeType,
          shouldDownload,
          fit,
          gravity,
          rotate,
          flip,
          blur,
          sharpen,
          brightness,
          contrast,
          saturation,
          grayscale,
        ));
      }

      const fileStream = fs.createReadStream(filePath);

      let imageProcessor = ImageProcessorHelper.createStreamProcessor();

      imageProcessor = ImageProcessorHelper.applyResize(
        imageProcessor,
        width,
        height,
        fit,
        gravity,
      );
      imageProcessor = ImageProcessorHelper.applyTransformations(
        imageProcessor,
        rotate,
        flip,
        blur,
        sharpen,
      );
      imageProcessor = ImageProcessorHelper.applyEffects(
        imageProcessor,
        brightness,
        contrast,
        saturation,
        grayscale,
      );

      let outFilename = filename;

      if (format) {
        const formatValidation = ImageProcessorHelper.validateFormat(format);
        if (!formatValidation.valid) {
          return void res.status(400).json({ error: formatValidation.error });
        }
        const formatLower = format.toLowerCase();
        if (formatLower === 'avif' && quality !== undefined) {
          imageProcessor = ImageProcessorHelper.setImageFormat(
            imageProcessor,
            formatLower,
            undefined,
          );
        } else {
          imageProcessor = ImageProcessorHelper.setImageFormat(
            imageProcessor,
            formatLower,
            quality,
          );
        }
        outFilename = ImageFormatHelper.updateFilenameWithFormat(
          outFilename,
          format,
        );
      } else if (quality) {
        const originalFormat = ImageFormatHelper.getOriginalFormat(filePath);
        if (originalFormat === 'avif') {
          imageProcessor = ImageProcessorHelper.setImageFormat(
            imageProcessor,
            originalFormat,
            undefined,
          );
        } else {
          imageProcessor = ImageProcessorHelper.setImageFormat(
            imageProcessor,
            originalFormat,
            quality,
          );
        }
      }

      const finalFormat =
        format || ImageFormatHelper.getOriginalFormat(filePath);
      const finalMimeType = ImageFormatHelper.getMimeType(finalFormat);

      res.setHeader('Content-Type', finalMimeType);
      res.setHeader(
        'Content-Disposition',
        shouldDownload
          ? `attachment; filename="${outFilename}"`
          : `inline; filename="${outFilename}"`,
      );

      if (cache && cache > 0)
        res.setHeader('Cache-Control', `public, max-age=${cache}`);
      else if (format) res.setHeader('Cache-Control', 'public, max-age=86400');
      else res.setHeader('Cache-Control', 'public, max-age=31536000');

      const sharpStream = fileStream.pipe(imageProcessor);

      sharpStream.on('error', (error: any) => {
        this.logger.error('Sharp processing error:', error);
        if (!res.headersSent)
          res.status(500).json({ error: 'Image processing failed' });
      });

      this.streamHelper.setupImageStream(sharpStream, res, false);

      this.streamHelper.handleStreamError(
        fileStream,
        res,
        'Failed to stream from local storage',
      );
    } catch (error) {
      this.logger.error('Image processing error:', error);
      if (!res.headersSent)
        res.status(500).json({
          error: 'Image processing failed',
          details: error instanceof Error ? error.message : String(error),
        });
    }
  }

  private async streamImageFromCloud(
    filePath: string,
    storageConfigId: number | string,
    req: any,
    res: Response,
    filename: string,
    format?: string,
    width?: number,
    height?: number,
    quality?: number,
    cache?: number,
    mimeType?: string,
    shouldDownload?: boolean,
    fit?: string,
    gravity?: string,
    rotate?: number,
    flip?: string,
    blur?: number,
    sharpen?: number,
    brightness?: number,
    contrast?: number,
    saturation?: number,
    grayscale?: boolean,
  ): Promise<void> {
    try {
      const cloudStream = await this.fileManagementService.getStreamFromStorage(
        filePath,
        storageConfigId,
      );

      let imageProcessor = ImageProcessorHelper.createStreamProcessor();

      imageProcessor = ImageProcessorHelper.applyResize(
        imageProcessor,
        width,
        height,
        fit,
        gravity,
      );
      imageProcessor = ImageProcessorHelper.applyTransformations(
        imageProcessor,
        rotate,
        flip,
        blur,
        sharpen,
      );
      imageProcessor = ImageProcessorHelper.applyEffects(
        imageProcessor,
        brightness,
        contrast,
        saturation,
        grayscale,
      );

      let outFilename = filename;

      if (format) {
        const formatValidation = ImageProcessorHelper.validateFormat(format);
        if (!formatValidation.valid) {
          return void res.status(400).json({ error: formatValidation.error });
        }
        const formatLower = format.toLowerCase();
        if (formatLower === 'avif' && quality !== undefined) {
          imageProcessor = ImageProcessorHelper.setImageFormat(
            imageProcessor,
            formatLower,
            undefined,
          );
        } else {
          imageProcessor = ImageProcessorHelper.setImageFormat(
            imageProcessor,
            formatLower,
            quality,
          );
        }
        outFilename = ImageFormatHelper.updateFilenameWithFormat(
          outFilename,
          format,
        );
      } else if (quality) {
        const originalFormat = ImageFormatHelper.getOriginalFormat(filePath);
        if (originalFormat === 'avif') {
          imageProcessor = ImageProcessorHelper.setImageFormat(
            imageProcessor,
            originalFormat,
            undefined,
          );
        } else {
          imageProcessor = ImageProcessorHelper.setImageFormat(
            imageProcessor,
            originalFormat,
            quality,
          );
        }
      }

      const finalFormat =
        format || ImageFormatHelper.getOriginalFormat(filePath);
      const finalMimeType = ImageFormatHelper.getMimeType(finalFormat);

      res.setHeader('Content-Type', finalMimeType);
      res.setHeader(
        'Content-Disposition',
        shouldDownload
          ? `attachment; filename="${outFilename}"`
          : `inline; filename="${outFilename}"`,
      );

      if (cache && cache > 0)
        res.setHeader('Cache-Control', `public, max-age=${cache}`);
      else if (format) res.setHeader('Cache-Control', 'public, max-age=86400');
      else res.setHeader('Cache-Control', 'public, max-age=31536000');

      const sharpStream = cloudStream.pipe(imageProcessor);

      sharpStream.on('error', (error: any) => {
        this.logger.error('Sharp processing error:', error);
        if (!res.headersSent)
          res.status(500).json({ error: 'Image processing failed' });
      });

      this.streamHelper.setupImageStream(sharpStream, res, false);

      this.streamHelper.handleStreamError(
        cloudStream,
        res,
        'Failed to stream from cloud storage',
      );
    } catch (error) {
      this.logger.error('Stream image from cloud error:', error);
      if (!res.headersSent)
        res.status(500).json({ error: 'Image streaming failed' });
    }
  }
}
