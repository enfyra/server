import { BadRequestException } from '../../../core/exceptions/custom-exceptions';
import { FileUploadDto, ProcessedFileInfo } from '../../../shared/types';
import * as crypto from 'crypto';
import * as path from 'path';
import { autoSlug } from '../../../shared/utils/auto-slug.helper';
import { QueryBuilderService } from '../../../infrastructure/query-builder/query-builder.service';
import { StorageConfigCacheService } from '../../../infrastructure/cache/services/storage-config-cache.service';
import { StorageFactoryService } from '../storage/storage-factory.service';

export class UploadFileHelper {
  private readonly queryBuilderService: QueryBuilderService;
  private readonly storageConfigCacheService: StorageConfigCacheService;
  private readonly storageFactoryService: StorageFactoryService;

  constructor(deps: {
    queryBuilderService: QueryBuilderService;
    storageConfigCacheService: StorageConfigCacheService;
    storageFactoryService: StorageFactoryService;
  }) {
    this.queryBuilderService = deps.queryBuilderService;
    this.storageConfigCacheService = deps.storageConfigCacheService;
    this.storageFactoryService = deps.storageFactoryService;
  }

  async processFileUpload(
    fileData: FileUploadDto,
    storageConfigId?: number | string,
  ): Promise<ProcessedFileInfo> {
    const uniqueFilename = this.generateUniqueFilename(fileData.filename);
    const relativePath = `uploads/${uniqueFilename}`;
    const fileType = this.getFileType(fileData.mimetype);

    try {
      const storageConfig = await this.getStorageConfig(storageConfigId);

      const idField = this.queryBuilderService.getPkField();
      const storageConfigIdValue = storageConfig?.[idField];

      if (!storageConfig || !storageConfigIdValue) {
        throw new BadRequestException('Storage config not found or invalid');
      }

      const storageService =
        this.storageFactoryService.getStorageServiceByConfig(storageConfig);

      let normalizedStorageConfigId: string | number = storageConfigIdValue;
      if (this.queryBuilderService.isMongoDb() && normalizedStorageConfigId) {
        if (
          typeof normalizedStorageConfigId === 'object' &&
          normalizedStorageConfigId !== null &&
          typeof (normalizedStorageConfigId as any).toString === 'function'
        ) {
          normalizedStorageConfigId = (
            normalizedStorageConfigId as any
          ).toString();
        } else {
          normalizedStorageConfigId = String(normalizedStorageConfigId);
        }
      }

      if (
        !normalizedStorageConfigId ||
        (typeof normalizedStorageConfigId === 'string' &&
          normalizedStorageConfigId.trim() === '')
      ) {
        throw new BadRequestException(
          'Invalid storage config ID after normalization',
        );
      }

      const uploadResult = await storageService.upload(
        fileData.buffer,
        relativePath,
        fileData.mimetype,
        storageConfig,
      );

      const processedInfo: ProcessedFileInfo = {
        filename: fileData.filename,
        mimetype: fileData.mimetype,
        type: fileType,
        filesize: fileData.size,
        storage_config_id: normalizedStorageConfigId,
        location: uploadResult.location,
        description: fileData.description,
        status: 'active',
      };

      return processedInfo;
    } catch (error: any) {
      throw new BadRequestException(
        `Failed to process file upload: ${error.message}`,
      );
    }
  }

  private generateUniqueFilename(originalFilename: string): string {
    const ext = path.extname(originalFilename);
    const baseName = path.basename(originalFilename, ext);
    const sanitizedName = autoSlug(baseName, {
      separator: '_',
      lowercase: false,
      maxLength: 50,
    });
    const timestamp = Date.now();
    const randomString = crypto.randomBytes(6).toString('hex');
    return `${sanitizedName}_${timestamp}_${randomString}${ext}`;
  }

  private getFileType(mimetype: string): string {
    if (mimetype.startsWith('image/')) return 'image';
    if (mimetype.startsWith('video/')) return 'video';
    if (mimetype.startsWith('audio/')) return 'audio';
    if (
      mimetype.includes('pdf') ||
      mimetype.includes('document') ||
      mimetype.includes('text')
    )
      return 'document';
    if (
      mimetype.includes('zip') ||
      mimetype.includes('tar') ||
      mimetype.includes('gzip')
    )
      return 'archive';
    return 'other';
  }

  private async getStorageConfig(
    storageConfigId?: number | string,
  ): Promise<any> {
    let config;

    if (storageConfigId) {
      config = await this.storageConfigCacheService.getStorageConfigById(storageConfigId);
    } else {
      config =
        await this.storageConfigCacheService.getStorageConfigByType('Local Storage');

      if (!config) {
        throw new BadRequestException('No local storage configured');
      }
    }

    return config;
  }
}
