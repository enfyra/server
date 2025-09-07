import { Injectable, BadRequestException, Logger } from '@nestjs/common';
import { DataSourceService } from '../../../core/database/data-source/data-source.service';
import {
  FileUploadDto,
  ProcessedFileInfo,
} from '../../../shared/interfaces/file-management.interface';
import * as fs from 'fs';
import * as path from 'path';
import * as crypto from 'crypto';
import { autoSlug } from '../../../shared/utils/auto-slug.helper';

@Injectable()
export class FileManagementService {
  private readonly basePath = path.join(process.cwd(), 'public');
  private readonly logger = new Logger(FileManagementService.name);

  constructor(private dataSourceService: DataSourceService) {
    this.ensurePublicDirExists();
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

  async processFileUpload(fileData: FileUploadDto): Promise<ProcessedFileInfo> {
    const uniqueFilename = this.generateUniqueFilename(fileData.filename);
    const filePath = this.getFilePath(uniqueFilename);
    const fileType = this.getFileType(fileData.mimetype);

    this.logger.log(
      `Processing file upload: ${fileData.filename} → ${uniqueFilename}`,
    );

    try {
      await fs.promises.mkdir(path.join(this.basePath, 'uploads'), {
        recursive: true,
      });
      await fs.promises.writeFile(filePath, fileData.buffer);

      const processedInfo: ProcessedFileInfo = {
        filename: fileData.filename,
        mimetype: fileData.mimetype,
        type: fileType,
        filesize: fileData.size,
        storage: 'local',
        location: `/uploads/${uniqueFilename}`,
        description: fileData.description,
        status: 'active',
      };

      this.logger.log(`File processed successfully: ${uniqueFilename}`);
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

  async deletePhysicalFile(location: string): Promise<void> {
    this.logger.log(`Deleting physical file: ${location}`);

    try {
      const absolutePath = this.convertToAbsolutePath(location);

      if (await this.fileExists(absolutePath)) {
        await fs.promises.unlink(absolutePath);
        this.logger.log(`Physical file deleted: ${absolutePath}`);
        return;
      }

      // Try alternative path
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

      this.logger.warn(`Physical file not found: ${location}`);
    } catch (error) {
      this.logger.error(`Failed to delete physical file: ${location}`, error);
      throw new BadRequestException(
        `Failed to delete physical file: ${error.message}`,
      );
    }
  }

  async rollbackFileCreation(location: string): Promise<void> {
    try {
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

  // ✅ File replacement methods
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
      // Kiểm tra file mới có tồn tại không
      if (!(await this.fileExists(newAbsolutePath))) {
        throw new Error(`New file not found: ${newAbsolutePath}`);
      }

      // Kiểm tra file cũ có tồn tại không
      if (!(await this.fileExists(oldAbsolutePath))) {
        throw new Error(`Old file not found: ${oldAbsolutePath}`);
      }

      // Xóa file cũ và copy file mới
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
      // Không throw error vì đây chỉ là cleanup
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

        // Xóa backup sau khi restore thành công
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
}
