import { Injectable, BadRequestException, Logger } from '@nestjs/common';
import {
  FileUploadDto,
  ProcessedFileInfo,
} from '../../../shared/interfaces/file-management.interface';
import * as fs from 'fs';
import * as path from 'path';
import * as crypto from 'crypto';
import { autoSlug } from '../../../shared/utils/auto-slug.helper';
import { QueryBuilderService } from '../../../infrastructure/query-builder/query-builder.service';
import { Storage } from '@google-cloud/storage';
import { Readable } from 'stream';

@Injectable()
export class FileManagementService {
  private readonly basePath = path.join(process.cwd(), 'public');
  private readonly logger = new Logger(FileManagementService.name);

  constructor(private queryBuilder: QueryBuilderService) {
    this.ensurePublicDirExists();
  }

  private getIdField(): string {
    return this.queryBuilder.isMongoDb() ? '_id' : 'id';
  }

  public createIdReference(id: number | string): any {
    const idField = this.getIdField();
    return { [idField]: id };
  }

  private async ensurePublicDirExists(): Promise<void> {
    try {
      await fs.promises.mkdir(this.basePath, { recursive: true });
    } catch (error) {
      this.logger.error('Failed to create public directory', error);
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

  getFilePath(filename: string): string {
    return path.join(this.basePath, 'uploads', filename);
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

      let location: string;
      switch (storageConfig.type) {
        case 'local':
          location = await this.uploadToLocal(fileData.buffer, relativePath);
          break;
        case 'gcs':
          location = await this.uploadToGCS(
            fileData.buffer,
            relativePath,
            fileData.mimetype,
            storageConfig,
          );
          break;
        case 's3':
          throw new BadRequestException('S3 storage not implemented yet');
        default:
          throw new BadRequestException(
            `Unknown storage type: ${storageConfig.type}`,
          );
      }

      const processedInfo: ProcessedFileInfo = {
        filename: fileData.filename,
        mimetype: fileData.mimetype,
        type: fileType,
        filesize: fileData.size,
        storage_config_id: storageConfig.id,
        location: location,
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

  async deletePhysicalFile(
    location: string,
    storageConfigId?: number | string,
  ): Promise<void> {
    this.logger.log(
      `Deleting physical file: ${location} (storage config ID: ${storageConfigId || 'local'})`,
    );

    try {
      let storageType = 'local';
      if (storageConfigId) {
        const config = await this.getStorageConfigById(storageConfigId);
        storageType = config.type;
      }

      if (storageType === 'gcs') {
        await this.deleteFromGCS(location, storageConfigId);
        return;
      }

      if (storageType === 's3') {
        throw new BadRequestException('S3 storage not implemented yet');
      }

      const absolutePath = this.convertToAbsolutePath(location);

      if (await this.fileExists(absolutePath)) {
        await fs.promises.unlink(absolutePath);
        this.logger.log(`Physical file deleted: ${absolutePath}`);
        return;
      }

      const altPath = path.join(
        process.cwd(),
        'public',
        'uploads',
        path.basename(location),
      );
      if (await this.fileExists(altPath)) {
        await fs.promises.unlink(altPath);
        this.logger.log(
          `Physical file deleted from alternative path: ${altPath}`,
        );
        return;
      }

      this.logger.warn(
        `Physical file not found (will sync metadata): ${location}`,
      );
    } catch (error) {
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
      let storageType = 'local';
      if (storageConfigId) {
        const config = await this.getStorageConfigById(storageConfigId);
        storageType = config.type;
      }

      if (storageType === 'gcs') {
        await this.deleteFromGCS(location, storageConfigId);
        this.logger.log(`Rolled back GCS file creation: ${location}`);
        return;
      }

      const absolutePath = this.convertToAbsolutePath(location);
      if (await this.fileExists(absolutePath)) {
        await fs.promises.unlink(absolutePath);
        this.logger.log(`Rolled back file creation: ${absolutePath}`);
      }
    } catch (error) {
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

  // File replacement methods
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
    const idField = this.getIdField();
    const config = await this.queryBuilder.findOneWhere(
      'storage_config_definition',
      {
        [idField]: storageConfigId,
        isEnabled: true,
      },
    );

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
      config = await this.queryBuilder.findOneWhere(
        'storage_config_definition',
        {
          type: 'local',
          isEnabled: true,
        },
      );

      if (!config) {
        throw new BadRequestException('No local storage configured');
      }
    }

    return config;
  }

  private async uploadToLocal(
    buffer: Buffer,
    relativePath: string,
  ): Promise<string> {
    const filePath = path.join(this.basePath, relativePath);

    await fs.promises.mkdir(path.dirname(filePath), { recursive: true });
    await fs.promises.writeFile(filePath, buffer);

    return `/${relativePath}`;
  }

  private async uploadToGCS(
    buffer: Buffer,
    relativePath: string,
    mimetype: string,
    config: any,
  ): Promise<string> {
    try {
      const credentials = typeof config.credentials === 'string'
        ? JSON.parse(config.credentials)
        : config.credentials;

      const storage = new Storage({
        credentials: credentials,
      });

      const bucket = storage.bucket(config.bucket);
      const file = bucket.file(relativePath);

      await file.save(buffer, {
        metadata: {
          contentType: mimetype,
        },
      });

      this.logger.log(
        `File uploaded to GCS: gs://${config.bucket}/${relativePath}`,
      );

      return relativePath;
    } catch (error) {
      this.logger.error(`Failed to upload to GCS: ${error.message}`, error);
      throw new BadRequestException(
        `Failed to upload to GCS: ${error.message}`,
      );
    }
  }

  async getStreamFromGCS(
    location: string,
    storageConfigId?: number | string,
  ): Promise<Readable> {
    try {
      const config = await this.getStorageConfig(storageConfigId);
      const credentials = typeof config.credentials === 'string'
        ? JSON.parse(config.credentials)
        : config.credentials;

      const storage = new Storage({
        credentials: credentials,
      });

      const bucket = storage.bucket(config.bucket);
      const file = bucket.file(location);

      // Check if file exists
      const [exists] = await file.exists();
      if (!exists) {
        throw new BadRequestException(`File not found in GCS: ${location}`);
      }

      const stream = file.createReadStream();

      this.logger.log(`Streaming file from GCS: gs://${config.bucket}/${location}`);

      return stream;
    } catch (error) {
      this.logger.error(`Failed to stream from GCS: ${error.message}`, error);
      throw new BadRequestException(
        `Failed to stream from GCS: ${error.message}`,
      );
    }
  }

  async getBufferFromGCS(
    location: string,
    storageConfigId?: number | string,
  ): Promise<Buffer> {
    try {
      const config = await this.getStorageConfig(storageConfigId);
      const credentials = typeof config.credentials === 'string'
        ? JSON.parse(config.credentials)
        : config.credentials;

      const storage = new Storage({
        credentials: credentials,
      });

      const bucket = storage.bucket(config.bucket);
      const file = bucket.file(location);

      const [exists] = await file.exists();
      if (!exists) {
        throw new BadRequestException(`File not found in GCS: ${location}`);
      }

      const [buffer] = await file.download();

      this.logger.log(`Downloaded buffer from GCS: gs://${config.bucket}/${location} (${buffer.length} bytes)`);

      return buffer;
    } catch (error) {
      this.logger.error(`Failed to download from GCS: ${error.message}`, error);
      throw new BadRequestException(
        `Failed to download from GCS: ${error.message}`,
      );
    }
  }

  async deleteFromGCS(
    location: string,
    storageConfigId?: number | string,
  ): Promise<void> {
    try {
      const config = await this.getStorageConfig(storageConfigId);
      const credentials = typeof config.credentials === 'string'
        ? JSON.parse(config.credentials)
        : config.credentials;

      const storage = new Storage({
        credentials: credentials,
      });

      const bucket = storage.bucket(config.bucket);
      const file = bucket.file(location);

      await file.delete({ ignoreNotFound: true });

      this.logger.log(`Deleted file from GCS: gs://${config.bucket}/${location}`);
    } catch (error) {
      this.logger.error(`Failed to delete from GCS: ${error.message}`, error);
      throw new BadRequestException(
        `Failed to delete from GCS: ${error.message}`,
      );
    }
  }

  async replaceFileOnGCS(
    location: string,
    buffer: Buffer,
    mimetype: string,
    storageConfigId?: number | string,
  ): Promise<void> {
    try {
      const config = await this.getStorageConfig(storageConfigId);
      const credentials = typeof config.credentials === 'string'
        ? JSON.parse(config.credentials)
        : config.credentials;

      const storage = new Storage({
        credentials: credentials,
      });

      const bucket = storage.bucket(config.bucket);
      const file = bucket.file(location);

      await file.save(buffer, {
        metadata: {
          contentType: mimetype,
        },
      });

      this.logger.log(`Replaced file on GCS: gs://${config.bucket}/${location}`);
    } catch (error) {
      this.logger.error(`Failed to replace file on GCS: ${error.message}`, error);
      throw new BadRequestException(
        `Failed to replace file on GCS: ${error.message}`,
      );
    }
  }
}
