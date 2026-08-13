import type { Response } from 'express';
import * as fs from 'fs';
import * as path from 'path';
import { Logger } from '../../../shared/logger';
import { NotFoundException } from '../../../domain/exceptions';
import type { FileManagementService } from './file-management.service';
import { ImageProcessorHelper } from '../utils/image-processor.helper';
import type { StreamHelper } from '../utils/stream.helper';
import { ImageFormatHelper } from '../utils/image-format.helper';
import { FileSignatureHelper } from '../utils/file-signature.helper';
import type { HeicConvert, LocalFileSignature } from '../types/file-asset.types';

const heicConvert = require('heic-convert') as HeicConvert;

export class ImageAssetProcessorService {
  private readonly logger = new Logger(ImageAssetProcessorService.name);
  private readonly fileManagementService: FileManagementService;
  private readonly streamHelper: StreamHelper;

  constructor(deps: {
    fileManagementService: FileManagementService;
    streamHelper: StreamHelper;
  }) {
    this.fileManagementService = deps.fileManagementService;
    this.streamHelper = deps.streamHelper;
    ImageProcessorHelper.configureSharp();
  }

  resolveLocalAssetPath(location: string): string {
    const basePath = path.resolve(process.cwd(), 'public');
    const relativePath = location.startsWith('/')
      ? location.slice(1)
      : location;
    const filePath = path.resolve(basePath, relativePath);
    if (
      filePath !== basePath &&
      !filePath.startsWith(`${basePath}${path.sep}`)
    ) {
      throw new NotFoundException('Physical file not found');
    }
    return filePath;
  }

  async detectLocalFileSignature(
    location: string,
  ): Promise<LocalFileSignature> {
    try {
      const filePath = this.resolveLocalAssetPath(location);
      const handle = await fs.promises.open(filePath, 'r');
      try {
        const buffer = Buffer.alloc(32);
        const { bytesRead } = await handle.read(buffer, 0, buffer.length, 0);
        return FileSignatureHelper.detect(buffer.subarray(0, bytesRead));
      } finally {
        await handle.close();
      }
    } catch {
      return null;
    }
  }

  isHeicMimeType(mimetype: string): boolean {
    return mimetype === 'image/heic' || mimetype === 'image/heif';
  }

  async processHeicInline(
    filePath: string,
    req: any,
    res: Response,
    filename: string,
  ): Promise<void> {
    try {
      const query = req.routeData?.context?.$query || req.query;
      const requestedFormat = (
        query.format as string | undefined
      )?.toLowerCase();
      const outputFormat = requestedFormat || 'jpeg';
      const quality = query.quality
        ? parseInt(query.quality as string, 10)
        : undefined;
      const width = query.width
        ? parseInt(query.width as string, 10)
        : undefined;
      const height = query.height
        ? parseInt(query.height as string, 10)
        : undefined;
      const fit = query.fit as string;
      const gravity = query.gravity as string;

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

      const formatValidation =
        ImageProcessorHelper.validateFormat(outputFormat);
      if (!formatValidation.valid) {
        return void res.status(400).json({ error: formatValidation.error });
      }

      const input = await fs.promises.readFile(filePath);
      const converted = await heicConvert({
        buffer: input,
        format: outputFormat === 'png' ? 'PNG' : 'JPEG',
        quality: quality ? quality / 100 : 0.85,
      });
      const decoded = Buffer.from(
        converted instanceof ArrayBuffer
          ? new Uint8Array(converted)
          : converted,
      );

      let output: Buffer<ArrayBufferLike> = decoded;
      if (
        outputFormat !== 'jpeg' ||
        width ||
        height ||
        fit ||
        gravity ||
        quality
      ) {
        let processor = ImageProcessorHelper.createProcessor(decoded);
        processor = ImageProcessorHelper.applyResize(
          processor,
          width,
          height,
          fit,
          gravity,
        );
        processor = ImageProcessorHelper.setImageFormat(
          processor,
          outputFormat,
          quality,
        );
        output = await processor.toBuffer();
      }

      const outFormat = outputFormat === 'jpg' ? 'jpeg' : outputFormat;
      const outFilename = ImageFormatHelper.updateFilenameWithFormat(
        filename,
        outFormat,
      );

      res.setHeader('Content-Type', ImageFormatHelper.getMimeType(outFormat));
      res.setHeader('Content-Length', output.length);
      res.setHeader('Content-Disposition', `inline; filename="${outFilename}"`);
      res.setHeader('Cache-Control', 'public, max-age=86400');
      res.end(output);
    } catch (error) {
      this.logger.error('HEIC image conversion error:', error);
      if (!res.headersSent)
        res.status(415).json({ error: 'HEIC image conversion failed' });
    }
  }

  async processImageWithQuery(
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
