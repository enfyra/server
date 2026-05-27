import { Logger } from '../../../shared/logger';
import { BadRequestException } from '../../../domain/exceptions';
import { FileUploadDto, ProcessedFileInfo } from '../../../shared/types';
import * as crypto from 'crypto';
import * as path from 'path';
import { autoSlug } from '../../../shared/utils/auto-slug.helper';
import { getErrorMessage } from '../../../shared/utils/error.util';
import { QueryBuilderService } from '@enfyra/kernel';
import { StorageConfigCacheService } from '../../../engines/cache';
import { StorageFactoryService } from '../storage/storage-factory.service';
import { Readable } from 'stream';
import { FileSignatureHelper } from '../utils/file-signature.helper';
import type { StorageStreamOptions } from '../storage/storage.interface';

export class FileManagementService {
  private readonly logger = new Logger(FileManagementService.name);
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

  private getIdField(): string {
    return this.queryBuilderService.getPkField();
  }

  private getEntityId(value: any): number | string | null {
    if (value === null || value === undefined) return null;
    if (typeof value === 'object') {
      const id = value.id ?? value._id;
      if (id === null || id === undefined) return null;
      return id;
    }
    return value;
  }

  private getFileStorageConfigId(file: any): number | string | null {
    return this.getEntityId(file?.storageConfig);
  }

  private normalizeRelationValue(value: any): any {
    if (value === undefined) return undefined;
    if (value === null || value === 'null' || value === '') return null;
    return typeof value === 'object' ? value : this.createIdReference(value);
  }

  private sameId(left: any, right: any): boolean {
    const leftId = this.getEntityId(left);
    const rightId = this.getEntityId(right);
    if (leftId === null && rightId === null) return true;
    if (leftId === null || rightId === null) return false;
    return String(leftId) === String(rightId);
  }

  public createIdReference(id: number | string | null | undefined): any {
    if (!id || id === null || id === undefined) {
      return null;
    }

    if (typeof id === 'string' && id.trim() === '') {
      return null;
    }

    const idField = this.getIdField();

    let normalizedId: string | number = id;
    if (this.queryBuilderService.isMongoDb()) {
      if (
        typeof id === 'object' &&
        id !== null &&
        typeof (id as any).toString === 'function'
      ) {
        normalizedId = (id as any).toString();
      } else {
        normalizedId = String(id);
      }

      if (
        !normalizedId ||
        (typeof normalizedId === 'string' && normalizedId.trim() === '')
      ) {
        return null;
      }
    }

    return { [idField]: normalizedId };
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

  getFilePath(filename: string): string {
    return `uploads/${filename}`;
  }

  async processFileUpload(
    fileData: FileUploadDto,
    storageConfigId?: number | string,
  ): Promise<ProcessedFileInfo> {
    const normalizedFile = FileSignatureHelper.normalizeUploadMetadata(
      fileData.filename,
      fileData.mimetype,
      fileData.buffer,
    );
    const uniqueFilename = this.generateUniqueFilename(normalizedFile.filename);
    const relativePath = `uploads/${uniqueFilename}`;
    const fileType = this.getFileType(normalizedFile.mimetype);

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
        normalizedFile.mimetype,
        storageConfig,
      );

      const processedInfo: ProcessedFileInfo = {
        filename: normalizedFile.filename,
        mimetype: normalizedFile.mimetype,
        type: fileType,
        filesize: fileData.size,
        storage_config_id: normalizedStorageConfigId,
        location: uploadResult.location,
        description: fileData.description,
        status: 'active',
      };

      return processedInfo;
    } catch (error) {
      this.logger.error(
        `Failed to process file upload: ${fileData.filename}`,
        error,
      );
      throw new BadRequestException(
        `Failed to process file upload: ${getErrorMessage(error)}`,
      );
    }
  }

  async uploadFileAndCreateRecord(
    fileData: {
      filename: string;
      mimetype: string;
      buffer: Buffer;
      size: number;
    },
    options: {
      folder?: number | string | { id: number | string };
      storageConfig?: number | string | { id: number | string };
      title?: string;
      description?: string;
      userId?: number | string;
    },
    fileRepo: any,
  ): Promise<any> {
    let folderData = null;
    if (options.folder) {
      folderData =
        typeof options.folder === 'object'
          ? options.folder
          : { id: options.folder };
    }

    let storageConfigId: number | string | null = null;
    if (options.storageConfig) {
      storageConfigId = this.getEntityId(options.storageConfig);
    }

    const processedFile = await this.processFileUpload(
      {
        filename: fileData.filename,
        mimetype: fileData.mimetype,
        buffer: fileData.buffer,
        size: fileData.size,
        folder: folderData,
        title: options.title || fileData.filename,
        description: options.description || undefined,
      },
      storageConfigId ?? undefined,
    );

    let fileUploadedToCloud = false;
    try {
      fileUploadedToCloud = true;

      const savedFile = await fileRepo.create({
        data: {
          filename: processedFile.filename,
          mimetype: processedFile.mimetype,
          type: processedFile.type,
          filesize: processedFile.filesize,
          location: processedFile.location,
          description: processedFile.description || null,
          folder: folderData,
          uploadedBy: options.userId
            ? this.createIdReference(options.userId)
            : null,
          storageConfig: processedFile.storage_config_id
            ? this.createIdReference(processedFile.storage_config_id)
            : null,
        },
      });

      return savedFile;
    } catch (error) {
      if (fileUploadedToCloud) {
        this.logger.warn(
          `Database save failed for file ${processedFile.location}, rolling back cloud storage upload`,
        );
        try {
          await this.rollbackFileCreation(
            processedFile.location,
            processedFile.storage_config_id,
          );
        } catch (rollbackError) {
          this.logger.error(
            `Failed to rollback cloud storage upload for ${processedFile.location}: ${getErrorMessage(rollbackError)}`,
          );
        }
      }
      throw error;
    }
  }

  async replaceFileAndUpdateRecord(
    fileRepo: any,
    id: number | string,
    currentFile: any,
    fileData: {
      filename: string;
      mimetype: string;
      buffer: Buffer;
      size: number;
    },
    options: {
      folder?: any;
      storageConfig?: any;
      title?: string;
      description?: string;
      status?: string;
      isPublished?: boolean;
    } = {},
  ): Promise<any> {
    const currentStorageConfigId = this.getFileStorageConfigId(currentFile);
    const nextStorageConfigId =
      options.storageConfig !== undefined && options.storageConfig !== null
        ? this.getEntityId(options.storageConfig)
        : currentStorageConfigId;

    const nextFolder =
      options.folder !== undefined
        ? this.normalizeRelationValue(options.folder)
        : currentFile.folder;
    const nextDescription =
      options.description !== undefined
        ? options.description
        : currentFile.description;
    const nextStatus =
      options.status !== undefined ? options.status : currentFile.status;
    const nextIsPublished =
      options.isPublished !== undefined
        ? options.isPublished
        : currentFile.isPublished;

    const processedFile = await this.processFileUpload(
      {
        filename: fileData.filename,
        mimetype: fileData.mimetype,
        buffer: fileData.buffer,
        size: fileData.size,
        folder: nextFolder,
        title: options.title || fileData.filename,
        description: nextDescription,
      },
      nextStorageConfigId ?? undefined,
    );

    try {
      const updateData = {
        filename: processedFile.filename,
        mimetype: processedFile.mimetype,
        type: processedFile.type,
        filesize: processedFile.filesize,
        location: processedFile.location,
        description: nextDescription,
        folder: nextFolder,
        uploadedBy: currentFile.uploadedBy,
        status: nextStatus,
        isPublished: nextIsPublished,
        storageConfig: processedFile.storage_config_id
          ? this.createIdReference(processedFile.storage_config_id)
          : null,
      };

      const result = await fileRepo.update({ id, data: updateData });

      await this.deleteOldPhysicalFileAfterReplace(
        currentFile.location,
        currentStorageConfigId,
        processedFile.location,
        processedFile.storage_config_id,
      );

      return result;
    } catch (error) {
      await this.rollbackFileCreation(
        processedFile.location,
        processedFile.storage_config_id,
      );
      throw error;
    }
  }

  async updateFileMetadataRecord(
    fileRepo: any,
    id: number | string,
    currentFile: any,
    options: {
      folder?: any;
      storageConfig?: any;
      title?: string;
      description?: string;
      status?: string;
      isPublished?: boolean;
    },
  ): Promise<any> {
    const updateData: any = {};

    if (options.storageConfig !== undefined && options.storageConfig !== null) {
      const currentStorageConfigId = this.getFileStorageConfigId(currentFile);
      const nextStorageConfigId = this.getEntityId(options.storageConfig);
      if (!this.sameId(currentStorageConfigId, nextStorageConfigId)) {
        throw new BadRequestException(
          'Changing storageConfig requires replacing the file blob in the same request',
        );
      }
    }

    if (options.folder !== undefined) {
      updateData.folder = this.normalizeRelationValue(options.folder);
    }

    if (options.title !== undefined) updateData.title = options.title;
    if (options.description !== undefined)
      updateData.description = options.description;
    if (options.status !== undefined) updateData.status = options.status;
    if (options.isPublished !== undefined)
      updateData.isPublished = options.isPublished;

    if (Object.keys(updateData).length === 0) return currentFile;

    return fileRepo.update({ id, data: updateData });
  }

  async deleteFileAndRecord(
    fileRepo: any,
    id: number | string,
    currentFile: any,
  ): Promise<any> {
    await this.deletePhysicalFile(
      currentFile.location,
      this.getFileStorageConfigId(currentFile) ?? undefined,
    );
    return fileRepo.delete({ id });
  }

  private async deleteOldPhysicalFileAfterReplace(
    oldLocation: string,
    oldStorageConfigId: number | string | null,
    newLocation: string,
    newStorageConfigId: number | string,
  ): Promise<void> {
    if (
      oldLocation === newLocation &&
      this.sameId(oldStorageConfigId, newStorageConfigId)
    ) {
      return;
    }

    try {
      await this.deletePhysicalFile(
        oldLocation,
        oldStorageConfigId ?? undefined,
      );
    } catch (error) {
      this.logger.warn(
        `Failed to delete old physical file after replace: ${oldLocation}: ${getErrorMessage(error)}`,
      );
    }
  }

  async deletePhysicalFile(
    location: string,
    storageConfigId?: number | string,
  ): Promise<void> {
    try {
      const config = await this.getStorageConfig(storageConfigId);
      const storageService =
        this.storageFactoryService.getStorageServiceByConfig(config);

      await storageService.delete(location, config);
    } catch (error: any) {
      this.logger.error(`Failed to delete physical file: ${location}`, error);
      throw new BadRequestException(
        `Failed to delete physical file: ${getErrorMessage(error)}`,
      );
    }
  }

  async rollbackFileCreation(
    location: string,
    storageConfigId?: number | string,
  ): Promise<void> {
    try {
      const config = await this.getStorageConfig(storageConfigId);
      const storageService =
        this.storageFactoryService.getStorageServiceByConfig(config);

      await storageService.delete(location, config);
    } catch (error: any) {
      this.logger.error(`Failed to rollback file creation:`, error);
    }
  }

  async getStorageConfigById(storageConfigId: number | string): Promise<any> {
    const config =
      await this.storageConfigCacheService.getStorageConfigById(
        storageConfigId,
      );

    if (!config) {
      throw new BadRequestException(
        `Storage config with ID ${storageConfigId} not found or disabled`,
      );
    }

    return config;
  }

  private async getStorageConfig(
    storageConfigId?: number | string,
  ): Promise<any> {
    let config;

    if (storageConfigId) {
      config = await this.getStorageConfigById(storageConfigId);
    } else {
      config =
        await this.storageConfigCacheService.getStorageConfigByType(
          'Local Storage',
        );

      if (!config) {
        throw new BadRequestException('No local storage configured');
      }
    }

    return config;
  }

  async getStreamFromStorage(
    location: string,
    storageConfigId?: number | string,
    options?: StorageStreamOptions,
  ): Promise<Readable> {
    const config = await this.getStorageConfig(storageConfigId);
    const storageService =
      this.storageFactoryService.getStorageServiceByConfig(config);
    return storageService.getStream(location, config, options);
  }

  async getBufferFromStorage(
    location: string,
    storageConfigId?: number | string,
  ): Promise<Buffer> {
    const config = await this.getStorageConfig(storageConfigId);
    const storageService =
      this.storageFactoryService.getStorageServiceByConfig(config);
    return storageService.getBuffer(location, config);
  }
}
