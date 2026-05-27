import { FileManagementService } from '../../modules/file-management';
import { TDynamicContext } from '../types';

export class UploadFileHelper {
  private readonly fileManagementService: FileManagementService;
  constructor(deps: { fileManagementService: FileManagementService }) {
    this.fileManagementService = deps.fileManagementService;
  }

  private getFileRepo(context: TDynamicContext) {
    const fileRepo = context.$repos?.file_definition || context.$repos?.main;
    if (!fileRepo) {
      throw new Error(
        `File repository not found in context. ` +
          `Ensure table "file_definition" exists in metadata.`,
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
        const numericKeys = keys.filter((k) => /^\d+$/.test(k));
        if (numericKeys.length > 0) {
          const sortedKeys = numericKeys
            .map((k) => parseInt(k, 10))
            .sort((a, b) => a - b);
          const arr = new Array(sortedKeys.length);
          for (let i = 0; i < sortedKeys.length; i++) {
            arr[i] = buffer[sortedKeys[i].toString()];
          }
          return Buffer.from(arr);
        } else {
          throw new Error(
            'Invalid buffer format: buffer object has no numeric keys',
          );
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

        const files = await fileRepo.find({ filter: { id: { _eq: fileId } } });
        const currentFile = files.data?.[0];

        if (!currentFile) {
          throw new Error(`File with ID ${fileId} not found`);
        }

        if (options.buffer) {
          const buffer = this.normalizeBuffer(options.buffer);
          return await this.fileManagementService.replaceFileAndUpdateRecord(
            fileRepo,
            fileId,
            currentFile,
            {
              filename:
                options.originalname ||
                options.filename ||
                currentFile.filename,
              mimetype: options.mimetype || currentFile.mimetype,
              buffer: buffer,
              size: options.size || buffer.length,
            },
            {
              folder:
                options.folder !== undefined
                  ? options.folder
                  : currentFile.folder,
              storageConfig: options.storageConfig,
              title: options.title,
              description: options.description,
              status: options.status,
              isPublished: options.isPublished,
            },
          );
        }

        return await this.fileManagementService.updateFileMetadataRecord(
          fileRepo,
          fileId,
          currentFile,
          {
            folder: options.folder,
            storageConfig: options.storageConfig,
            title: options.title,
            description: options.description,
            status: options.status,
            isPublished: options.isPublished,
          },
        );
      } catch (error: any) {
        this.handleError(error, 'updateFile');
      }
    };
  }

  createDeleteFileHelper(context: TDynamicContext) {
    return async (fileId: string | number) => {
      try {
        const fileRepo = this.getFileRepo(context);

        const files = await fileRepo.find({ filter: { id: { _eq: fileId } } });
        const file = files.data?.[0];

        if (!file) {
          throw new Error(`File with ID ${fileId} not found`);
        }

        return await this.fileManagementService.deleteFileAndRecord(
          fileRepo,
          fileId,
          file,
        );
      } catch (error: any) {
        this.handleError(error, 'deleteFile');
      }
    };
  }
}
