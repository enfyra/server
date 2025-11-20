import { Injectable, BadRequestException, Logger } from '@nestjs/common';
import {
  FileUploadDto,
  ProcessedFileInfo,
} from '../../../shared/interfaces/file-management.interface';
import * as crypto from 'crypto';
import * as fs from 'fs';
import * as path from 'path';
import { autoSlug } from '../../../shared/utils/auto-slug.helper';
import { QueryBuilderService } from '../../../infrastructure/query-builder/query-builder.service';
import { StorageConfigCacheService } from '../../../infrastructure/cache/services/storage-config-cache.service';
import { StorageFactoryService } from '../storage/storage-factory.service';
import { Readable } from 'stream';

@Injectable()
export class FileManagementService {
  private readonly logger = new Logger(FileManagementService.name);

  constructor(
    private queryBuilder: QueryBuilderService,
    private storageConfigCache: StorageConfigCacheService,
    private storageFactory: StorageFactoryService,
  ) {}

  private getIdField(): string {
    return this.queryBuilder.isMongoDb() ? '_id' : 'id';
  }

  public createIdReference(id: number | string): any {
    const idField = this.getIdField();
    return { [idField]: id };
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

    this.logger.log(
      `Processing file upload: ${fileData.filename} → ${uniqueFilename} (storage config ID: ${storageConfigId || 'default'})`,
    );

    try {
      const storageConfig = await this.getStorageConfig(storageConfigId);
      const storageService = this.storageFactory.getStorageServiceByConfig(storageConfig);

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
        storage_config_id: storageConfig.id,
        location: uploadResult.location,
        description: fileData.description,
        status: 'active',
      };

      this.logger.log(
        `File processed successfully: ${uniqueFilename} on storage config ${storageConfig.id} (${storageConfig.type})`,
      );
      return processedInfo;
    } catch (error) {
      this.logger.error(
        `Failed to process file upload: ${fileData.filename}`,
        error,
      );
      throw new BadRequestException(
        `Failed to process file upload: ${error.message}`,
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
        typeof options.folder === 'object' ? options.folder : { id: options.folder };
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

    try {
      const savedFile = await fileRepo.create({
        data: {
          filename: processedFile.filename,
          mimetype: processedFile.mimetype,
          type: processedFile.type,
          filesize: processedFile.filesize,
          location: processedFile.location,
          description: processedFile.description || null,
          folder: folderData,
          uploaded_by: options.userId
            ? this.createIdReference(options.userId)
            : null,
          storageConfig: processedFile.storage_config_id
            ? this.createIdReference(processedFile.storage_config_id)
            : null,
        },
      });

      return savedFile;
    } catch (error) {
      await this.rollbackFileCreation(
        processedFile.location,
        processedFile.storage_config_id,
      );
      throw error;
    }
  }

  async deletePhysicalFile(
    location: string,
    storageConfigId?: number | string,
  ): Promise<void> {
    this.logger.log(
      `Deleting physical file: ${location} (storage config ID: ${storageConfigId || 'Local Storage'})`,
    );

    try {
      const config = await this.getStorageConfig(storageConfigId);
      const storageService = this.storageFactory.getStorageServiceByConfig(config);
      
      await storageService.delete(location, config);
      this.logger.log(`Deleted file: ${location}`);
    } catch (error: any) {
      this.logger.error(`Failed to delete physical file: ${location}`, error);
      throw new BadRequestException(
        `Failed to delete physical file: ${error.message}`,
      );
    }
  }

  async rollbackFileCreation(
    location: string,
    storageConfigId?: number | string,
  ): Promise<void> {
    try {
      const config = await this.getStorageConfig(storageConfigId);
      const storageService = this.storageFactory.getStorageServiceByConfig(config);
      
      await storageService.delete(location, config);
      this.logger.log(`Rolled back file creation: ${location}`);
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
        this.logger.log(`File backed up: ${absolutePath} → ${backupPath}`);
        return backupPath;
      }
      throw new Error(`Source file not found: ${absolutePath}`);
    } catch (error) {
      this.logger.error(`Failed to backup file: ${absolutePath}`, error);
      throw new BadRequestException(`Failed to backup file: ${error.message}`);
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

      this.logger.log(
        `File replaced successfully: ${oldAbsolutePath} ← ${newAbsolutePath}`,
      );
    } catch (error) {
      this.logger.error(`Failed to replace file: ${oldLocation}`, error);
      throw new BadRequestException(`Failed to replace file: ${error.message}`);
    }
  }

  async deleteBackupFile(backupPath: string): Promise<void> {
    try {
      if (await this.fileExists(backupPath)) {
        await fs.promises.unlink(backupPath);
        this.logger.log(`Backup file deleted: ${backupPath}`);
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
        this.logger.log(
          `File restored from backup: ${backupPath} → ${originalAbsolutePath}`,
        );

        await this.deleteBackupFile(backupPath);
      } else {
        this.logger.warn(`Backup file not found for restore: ${backupPath}`);
      }
    } catch (error) {
      this.logger.error(
        `Failed to restore file from backup: ${backupPath}`,
        error,
      );
      throw new BadRequestException(`Failed to restore file: ${error.message}`);
    }
  }

  async getStorageConfigById(storageConfigId: number | string): Promise<any> {
    const config = await this.storageConfigCache.getStorageConfigById(storageConfigId);

    if (!config) {
      throw new BadRequestException(
        `Storage config with ID ${storageConfigId} not found or disabled`,
      );
    }

    return config;
  }

  private async getStorageConfig(storageConfigId?: number | string): Promise<any> {
    let config;

    if (storageConfigId) {
      config = await this.getStorageConfigById(storageConfigId);
    } else {
      config = await this.storageConfigCache.getStorageConfigByType('Local Storage');

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
    const storageService = this.storageFactory.getStorageServiceByConfig(config);
    return storageService.getStream(location, config);
  }

  async getBufferFromStorage(
    location: string,
    storageConfigId?: number | string,
  ): Promise<Buffer> {
      const config = await this.getStorageConfig(storageConfigId);
    const storageService = this.storageFactory.getStorageServiceByConfig(config);
    return storageService.getBuffer(location, config);
  }

  async replaceFileOnStorage(
    location: string,
    buffer: Buffer,
    mimetype: string,
    storageConfigId?: number | string,
  ): Promise<void> {
      const config = await this.getStorageConfig(storageConfigId);
    const storageService = this.storageFactory.getStorageServiceByConfig(config);
    await storageService.replaceFile(location, buffer, mimetype, config);
  }
}
