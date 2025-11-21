import { Injectable } from '@nestjs/common';
import { FileManagementService } from '../../modules/file-management/services/file-management.service';
import { TDynamicContext } from '../../shared/interfaces/dynamic-context.interface';

@Injectable()
export class UploadFileHelper {
  constructor(private readonly fileManagementService: FileManagementService) {}

  private getFileRepo(context: TDynamicContext) {
    const fileRepo = context.$repos?.file_definition || context.$repos?.main;
    if (!fileRepo) {
      const availableRepos = context.$repos ? Object.keys(context.$repos) : [];
      throw new Error(
        `File repository not found in context. Available repos: ${availableRepos.join(', ') || 'none'}. ` +
        `Context has $repos: ${!!context.$repos}, $repos type: ${typeof context.$repos}`
      );
    }
    return fileRepo;
  }

  private normalizeBuffer(buffer: any): Buffer {
    if (buffer && typeof buffer === 'object' && !Buffer.isBuffer(buffer)) {
      if (typeof buffer.toBuffer === 'function') {
        return buffer.toBuffer();
      } else if (buffer.type === 'Buffer' && Array.isArray(buffer.data)) {
        return Buffer.from(buffer.data);
      } else {
        const keys = Object.keys(buffer);
        const numericKeys = keys.filter(k => /^\d+$/.test(k));
        if (numericKeys.length > 0) {
          const sortedKeys = numericKeys.map(k => parseInt(k, 10)).sort((a, b) => a - b);
          const arr = new Array(sortedKeys.length);
          for (let i = 0; i < sortedKeys.length; i++) {
            arr[i] = buffer[sortedKeys[i].toString()];
          }
          return Buffer.from(arr);
        } else {
          throw new Error('Invalid buffer format: buffer object has no numeric keys');
        }
      }
    }
    return buffer;
  }

  private handleError(error: any, operation: string): never {
    let errorMessage = `Unknown error in $${operation}`;
    
    if (error?.response?.message) {
      errorMessage = error.response.message;
    } else if (error?.message) {
      errorMessage = error.message;
    } else if (error?.toString && error.toString() !== '[object Object]') {
      errorMessage = error.toString();
    }
    
    const uploadError = new Error(errorMessage);
    if (error?.stack) uploadError.stack = error.stack;
    if (error?.response) {
      (uploadError as any).response = error.response;
    }
    if (error?.statusCode) {
      (uploadError as any).statusCode = error.statusCode;
    }
    throw uploadError;
  }

  createUploadFileHelper(context: TDynamicContext) {
    return async (options: any) => {
      try {
        const buffer = this.normalizeBuffer(options.buffer);
        const fileRepo = this.getFileRepo(context);

        return await this.fileManagementService.uploadFileAndCreateRecord(
          {
            filename: options.originalname || options.filename,
            mimetype: options.mimetype,
            buffer: buffer,
            size: options.size,
          },
          {
            folder: options.folder,
            storageConfig: options.storageConfig,
            title: options.title,
            description: options.description,
            userId: context.$user?.id,
          },
          fileRepo,
        );
      } catch (error: any) {
        this.handleError(error, 'uploadFile');
      }
    };
  }

  createUpdateFileHelper(context: TDynamicContext) {
    return async (fileId: string | number, options: any) => {
      try {
        const fileRepo = this.getFileRepo(context);

        const files = await fileRepo.find({ where: { id: { _eq: fileId } } });
        const currentFile = files.data?.[0];

        if (!currentFile) {
          throw new Error(`File with ID ${fileId} not found`);
        }

        if (options.buffer) {
          const buffer = this.normalizeBuffer(options.buffer);
          const filename = options.originalname || options.filename || currentFile.filename;
          const mimetype = options.mimetype || currentFile.mimetype;
          const size = options.size || buffer.length;

          let storageConfigId = currentFile.storageConfig?.id || null;
          if (options.storageConfig) {
            storageConfigId = typeof options.storageConfig === 'object'
              ? options.storageConfig.id
              : options.storageConfig;
          }

          let storageConfig = null;
          if (storageConfigId) {
            storageConfig = await this.fileManagementService.getStorageConfigById(storageConfigId);
          }

          if (storageConfig && (storageConfig.type === 'Google Cloud Storage' || storageConfig.type === 'Cloudflare R2' || storageConfig.type === 'Amazon S3')) {
            await this.fileManagementService.replaceFileOnStorage(
              currentFile.location,
              buffer,
              mimetype,
              storageConfigId,
            );

            const updateData = {
              filename: filename,
              mimetype: mimetype,
              filesize: size,
              storageConfig: this.fileManagementService.createIdReference(storageConfigId),
              description: options.description !== undefined ? options.description : currentFile.description,
              folder: currentFile.folder,
              uploaded_by: currentFile.uploaded_by,
              status: currentFile.status,
            };

            return await fileRepo.update({ id: fileId, data: updateData });
          }

          const processedFile = await this.fileManagementService.processFileUpload(
            {
              filename: filename,
              mimetype: mimetype,
              buffer: buffer,
              size: size,
              folder: currentFile.folder,
              title: options.title || filename,
              description: options.description !== undefined ? options.description : currentFile.description,
            },
            storageConfigId,
          );

          const backupPath = await this.fileManagementService.backupFile(currentFile.location);

          try {
            await this.fileManagementService.replacePhysicalFile(
              currentFile.location,
              processedFile.location,
            );

            const updateData = {
              filename: processedFile.filename,
              mimetype: processedFile.mimetype,
              type: processedFile.type,
              filesize: processedFile.filesize,
              location: currentFile.location,
              description: processedFile.description,
              folder: currentFile.folder,
              uploaded_by: currentFile.uploaded_by,
              status: currentFile.status,
              storageConfig: processedFile.storage_config_id
                ? this.fileManagementService.createIdReference(processedFile.storage_config_id)
                : null,
            };

            const result = await fileRepo.update({ id: fileId, data: updateData });

            await this.fileManagementService.rollbackFileCreation(
              processedFile.location,
              processedFile.storage_config_id,
            );
            await this.fileManagementService.deleteBackupFile(backupPath);

            return result;
          } catch (error) {
            await this.fileManagementService.restoreFromBackup(
              currentFile.location,
              backupPath,
            );
            throw error;
          }
        }

        const updateData: any = {};
        if (options.folder !== undefined) {
          updateData.folder = typeof options.folder === 'object' ? options.folder : { id: options.folder };
        }
        if (options.title !== undefined) updateData.title = options.title;
        if (options.description !== undefined) updateData.description = options.description;
        if (options.storageConfig !== undefined) {
          updateData.storageConfig = typeof options.storageConfig === 'object'
            ? options.storageConfig
            : this.fileManagementService.createIdReference(options.storageConfig);
        }

        if (Object.keys(updateData).length === 0) {
          return currentFile;
        }

        return await fileRepo.update({ id: fileId, data: updateData });
      } catch (error: any) {
        this.handleError(error, 'updateFile');
      }
    };
  }

  createDeleteFileHelper(context: TDynamicContext) {
    return async (fileId: string | number) => {
      try {
        const fileRepo = this.getFileRepo(context);

        const files = await fileRepo.find({ where: { id: { _eq: fileId } } });
        const file = files.data?.[0];

        if (!file) {
          throw new Error(`File with ID ${fileId} not found`);
        }

        const { location, storageConfig } = file;

        await this.fileManagementService.deletePhysicalFile(
          location,
          storageConfig?.id || null,
        );

        return await fileRepo.delete({ id: fileId });
      } catch (error: any) {
        this.handleError(error, 'deleteFile');
      }
    };
  }

}

