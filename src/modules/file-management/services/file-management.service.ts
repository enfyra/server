import { Logger } from '../../../shared/logger';
import { BadRequestException } from '../../../domain/exceptions';
import { FileUploadDto, ProcessedFileInfo } from '../../../shared/types';
import * as crypto from 'crypto';
import * as fs from 'fs';
import * as path from 'path';
import { autoSlug } from '../../../shared/utils/auto-slug.helper';
import { getErrorMessage } from '../../../shared/utils/error.util';
import { QueryBuilderService } from '../../../kernel/query';
import { StorageConfigCacheService } from '../../../engine/cache';
import { StorageFactoryService } from '../storage/storage-factory.service';
import { Readable } from 'stream';

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
      storageConfigId =
        typeof options.storageConfig === 'object'
          ? options.storageConfig.id
          : options.storageConfig;
    }

    const processedFile = await this.processFileUpload(
      {
        filename: fileData.filename,
        mimetype: fileData.mimetype,
        buffer: fileData.buffer,
        size: fileData.size,
        folder: folderData,
        title: options.title || fileData.filename,
        description: options.description || null,
      },
      storageConfigId,
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

  private convertToAbsolutePath(location: string): string {
    return location.startsWith('/')
      ? path.join(process.cwd(), 'public', location.slice(1))
      : path.join(process.cwd(), 'public', location);
  }

  private async fileExists(filePath: string): Promise<boolean> {
    try {
      const stats = await fs.promises.stat(filePath);
      return stats.isFile();
    } catch {
      return false;
    }
  }

  async backupFile(location: string): Promise<string> {
    const absolutePath = this.convertToAbsolutePath(location);
    const backupPath = `${absolutePath}.backup.${Date.now()}`;

    try {
      if (await this.fileExists(absolutePath)) {
        await fs.promises.copyFile(absolutePath, backupPath);
        return backupPath;
      }
      throw new Error(`Source file not found: ${absolutePath}`);
    } catch (error) {
      this.logger.error(`Failed to backup file: ${absolutePath}`, error);
      throw new BadRequestException(
        `Failed to backup file: ${getErrorMessage(error)}`,
      );
    }
  }

  async replacePhysicalFile(
    oldLocation: string,
    newLocation: string,
  ): Promise<void> {
    const oldAbsolutePath = this.convertToAbsolutePath(oldLocation);
    const newAbsolutePath = this.convertToAbsolutePath(newLocation);

    try {
      if (!(await this.fileExists(newAbsolutePath))) {
        throw new Error(`New file not found: ${newAbsolutePath}`);
      }

      if (!(await this.fileExists(oldAbsolutePath))) {
        throw new Error(`Old file not found: ${oldAbsolutePath}`);
      }

      await fs.promises.unlink(oldAbsolutePath);
      await fs.promises.copyFile(newAbsolutePath, oldAbsolutePath);
    } catch (error) {
      this.logger.error(`Failed to replace file: ${oldLocation}`, error);
      throw new BadRequestException(
        `Failed to replace file: ${getErrorMessage(error)}`,
      );
    }
  }

  async deleteBackupFile(backupPath: string): Promise<void> {
    try {
      if (await this.fileExists(backupPath)) {
        await fs.promises.unlink(backupPath);
      }
    } catch (error) {
      this.logger.error(`Failed to delete backup file: ${backupPath}`, error);
    }
  }

  async restoreFromBackup(
    originalLocation: string,
    backupPath: string,
  ): Promise<void> {
    const originalAbsolutePath = this.convertToAbsolutePath(originalLocation);

    try {
      if (await this.fileExists(backupPath)) {
        await fs.promises.copyFile(backupPath, originalAbsolutePath);

        await this.deleteBackupFile(backupPath);
      } else {
        this.logger.warn(`Backup file not found for restore: ${backupPath}`);
      }
    } catch (error) {
      this.logger.error(
        `Failed to restore file from backup: ${backupPath}`,
        error,
      );
      throw new BadRequestException(
        `Failed to restore file: ${getErrorMessage(error)}`,
      );
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
  ): Promise<Readable> {
    const config = await this.getStorageConfig(storageConfigId);
    const storageService =
      this.storageFactoryService.getStorageServiceByConfig(config);
    return storageService.getStream(location, config);
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

  async replaceFileOnStorage(
    location: string,
    buffer: Buffer,
    mimetype: string,
    storageConfigId?: number | string,
  ): Promise<void> {
    const config = await this.getStorageConfig(storageConfigId);
    const storageService =
      this.storageFactoryService.getStorageServiceByConfig(config);
    await storageService.replaceFile(location, buffer, mimetype, config);
  }
}
