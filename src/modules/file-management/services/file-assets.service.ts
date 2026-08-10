import type { Response } from 'express';
import * as path from 'path';
import { Logger } from '../../../shared/logger';
import { NotFoundException } from '../../../domain/exceptions';
import type { QueryBuilderService } from '@enfyra/kernel';
import { FileManagementService } from './file-management.service';
import { StorageFactoryService } from '../storage/storage-factory.service';
import { StreamHelper } from '../utils/stream.helper';
import { FileValidationHelper } from '../utils/file-validation.helper';
import { FileSignatureHelper } from '../utils/file-signature.helper';
import { FileAssetAccessService } from './file-asset-access.service';
import { ImageAssetProcessorService } from './image-asset-processor.service';

export class FileAssetsService {
  private readonly logger = new Logger(FileAssetsService.name);
  private readonly streamHelper: StreamHelper;
  private readonly fileManagementService: FileManagementService;
  private readonly storageFactoryService: StorageFactoryService;
  private readonly fileAssetAccessService: FileAssetAccessService;
  private readonly imageAssetProcessorService: ImageAssetProcessorService;

  constructor(deps: {
    queryBuilderService: QueryBuilderService;
    fileManagementService: FileManagementService;
    storageFactoryService: StorageFactoryService;
    eventEmitter?: import('eventemitter2').EventEmitter2;
  }) {
    this.fileManagementService = deps.fileManagementService;
    this.storageFactoryService = deps.storageFactoryService;
    this.streamHelper = new StreamHelper();

    this.fileAssetAccessService = new FileAssetAccessService({
      queryBuilderService: deps.queryBuilderService,
      eventEmitter: deps.eventEmitter,
    });

    this.imageAssetProcessorService = new ImageAssetProcessorService({
      fileManagementService: deps.fileManagementService,
      streamHelper: this.streamHelper,
    });
  }

  async streamFile(req: any, res: Response): Promise<void> {
    const fileId = req.routeData?.params?.id || req.params.id;
    if (!fileId)
      return void res.status(400).json({ error: 'File ID is required' });

    const file = await this.fileAssetAccessService.resolveAuthorizedFile(
      req,
      fileId,
    );
    if (!file) throw new NotFoundException(`File not found: ${fileId}`);

    const {
      location,
      storageConfig,
      filename,
      mimetype,
      type: fileType,
    } = file as any;
    const storageType = storageConfig?.type || 'Local Storage';
    const storageConfigId = storageConfig?._id || storageConfig?.id || null;
    const rangeHeader = req.headers?.range as string | undefined;
    const totalSize = this.fileAssetAccessService.getFileSize(file);

    if (
      storageType === 'Google Cloud Storage' ||
      storageType === 'Cloudflare R2' ||
      storageType === 'Amazon S3'
    ) {
      if (
        FileValidationHelper.isImageFile(mimetype, fileType) &&
        FileValidationHelper.hasImageQueryParams(req)
      ) {
        return void (await this.imageAssetProcessorService.processImageWithQuery(
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
      const parsedRange = this.streamHelper.parseHttpRange(
        rangeHeader,
        totalSize,
      );
      if (parsedRange.type === 'invalid') {
        return void this.streamHelper.sendRangeNotSatisfiable(res, totalSize);
      }
      const stream = await this.fileManagementService.getStreamFromStorage(
        location,
        storageConfigId,
        parsedRange.type === 'partial'
          ? { range: parsedRange.range }
          : undefined,
      );
      return void (await this.streamHelper.streamCloudFile(
        stream,
        res,
        filename,
        mimetype,
        shouldDownload,
        parsedRange.type === 'partial' ? parsedRange.range : undefined,
        totalSize,
      ));
    }

    if (storageType === 'Local Storage') {
      const query = req.routeData?.context?.$query || req.query;
      const shouldDownload =
        query.download === 'true' || query.download === true;
      const actualSignature =
        await this.imageAssetProcessorService.detectLocalFileSignature(location);
      const actualMimeType = actualSignature?.mimetype || mimetype;
      const actualFilename = actualSignature
        ? FileSignatureHelper.replaceExtension(
            filename,
            actualSignature.extension,
          )
        : filename;
      if (
        this.imageAssetProcessorService.isHeicMimeType(actualMimeType) &&
        !shouldDownload
      ) {
        return void (await this.imageAssetProcessorService.processHeicInline(
          this.imageAssetProcessorService.resolveLocalAssetPath(location),
          req,
          res,
          actualFilename,
        ));
      }

      if (
        FileValidationHelper.isImageFile(mimetype, fileType) &&
        FileValidationHelper.hasImageQueryParams(req)
      ) {
        const filePath = this.imageAssetProcessorService.resolveLocalAssetPath(
          location,
        );

        return void (await this.imageAssetProcessorService.processImageWithQuery(
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

      let stream;
      const parsedRange = this.streamHelper.parseHttpRange(
        rangeHeader,
        totalSize,
      );
      if (parsedRange.type === 'invalid') {
        return void this.streamHelper.sendRangeNotSatisfiable(res, totalSize);
      }
      try {
        stream = await storageService.getStream(
          location,
          sc,
          parsedRange.type === 'partial'
            ? { range: parsedRange.range }
            : undefined,
        );
      } catch (error) {
        this.logger.error(`Local file not found: ${location}`, error);
        throw new NotFoundException('Physical file not found');
      }
      return void (await this.streamHelper.streamCloudFile(
        stream,
        res,
        actualFilename,
        actualMimeType,
        shouldDownload,
        parsedRange.type === 'partial' ? parsedRange.range : undefined,
        totalSize,
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
      return void (await this.imageAssetProcessorService.processImageWithQuery(
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
      rangeHeader,
    );
  }
}
