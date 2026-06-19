import { FileManagementService } from '../../modules/file-management';
import type { TDynamicContext, UploadedFileInfo } from '../types';
import { Readable } from 'stream';
import { createReadStream } from 'fs';

type UploadFileInput = {
  filename: string;
  mimetype: string;
  stream: Readable;
  signatureBuffer?: Buffer;
  size: number;
};

type RegisterFileInput = {
  filename: string;
  mimetype: string;
  location: string;
  size: number;
  type?: string;
};

export class UploadFileHelper {
  private readonly fileManagementService: FileManagementService;
  constructor(deps: { fileManagementService: FileManagementService }) {
    this.fileManagementService = deps.fileManagementService;
  }

  private getFileRepo(context: TDynamicContext) {
    const fileRepo = context.$repos?.enfyra_file || context.$repos?.main;
    if (!fileRepo) {
      throw new Error(
        `File repository not found in context. ` +
          `Ensure table "enfyra_file" exists in metadata.`,
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

  private normalizeUploadedFile(file: any): UploadedFileInfo | null {
    if (!file) return null;
    if (!file.path) {
      throw new Error('Invalid uploaded file: path is required');
    }
    if (!file.originalname && !file.filename) {
      throw new Error('Invalid uploaded file: originalname is required');
    }
    if (!file.mimetype) {
      throw new Error('Invalid uploaded file: mimetype is required');
    }
    if (typeof file.size !== 'number') {
      throw new Error('Invalid uploaded file: size is required');
    }
    return file;
  }

  private createUploadInput(options: any): UploadFileInput {
    const uploadedFile = this.normalizeUploadedFile(options.file);
    const hasBuffer = options.buffer !== undefined && options.buffer !== null;

    if (uploadedFile && hasBuffer) {
      throw new Error(
        'Pass either file or buffer to $storage.$upload, not both',
      );
    }

    if (uploadedFile) {
      const filePath = uploadedFile.path;
      if (!filePath) {
        throw new Error('Invalid uploaded file: path is required');
      }
      return {
        filename:
          options.originalname ||
          options.filename ||
          uploadedFile.originalname ||
          (uploadedFile as any).filename,
        mimetype: options.mimetype || uploadedFile.mimetype,
        stream: createReadStream(filePath),
        size: options.size || uploadedFile.size,
      };
    }

    if (!hasBuffer) {
      throw new Error('Either file or buffer is required for $storage.$upload');
    }

    const buffer = this.normalizeBuffer(options.buffer);
    if (!Buffer.isBuffer(buffer)) {
      throw new Error('Invalid buffer format for $storage.$upload');
    }
    if (!options.originalname && !options.filename) {
      throw new Error(
        'filename is required for $storage.$upload buffer uploads',
      );
    }
    if (!options.mimetype) {
      throw new Error(
        'mimetype is required for $storage.$upload buffer uploads',
      );
    }
    return {
      filename: options.originalname || options.filename,
      mimetype: options.mimetype,
      stream: Readable.from(buffer),
      signatureBuffer: buffer,
      size: options.size || buffer.length,
    };
  }

  private createRegisterFileInput(options: any): RegisterFileInput {
    const filename = options.originalname || options.filename;
    const size = options.size ?? options.filesize;

    if (!filename || typeof filename !== 'string') {
      throw new Error('filename is required for $storage.$registerFile');
    }
    if (!options.mimetype || typeof options.mimetype !== 'string') {
      throw new Error('mimetype is required for $storage.$registerFile');
    }
    if (!options.location || typeof options.location !== 'string') {
      throw new Error('location is required for $storage.$registerFile');
    }
    if (!Number.isFinite(Number(size)) || Number(size) < 0) {
      throw new Error(
        'size must be a non-negative number for $storage.$registerFile',
      );
    }
    if (!options.storageConfig) {
      throw new Error('storageConfig is required for $storage.$registerFile');
    }

    return {
      filename,
      mimetype: options.mimetype,
      location: options.location,
      size: Number(size),
      type: options.type,
    };
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

  createStorageHelper(context: TDynamicContext) {
    return {
      $upload: this.createUpload(context),
      $update: this.createUpdate(context),
      $delete: this.createDelete(context),
      $registerFile: this.createRegisterFile(context),
    };
  }

  private createUpload(context: TDynamicContext) {
    return async (options: any) => {
      try {
        const uploadInput = this.createUploadInput(options);
        const fileRepo = this.getFileRepo(context);

        return await this.fileManagementService.uploadFileAndCreateRecord(
          uploadInput,
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
        this.handleError(error, 'storage.$upload');
      }
    };
  }

  private createUpdate(context: TDynamicContext) {
    return async (fileId: string | number, options: any) => {
      try {
        const fileRepo = this.getFileRepo(context);

        const files = await fileRepo.find({ filter: { id: { _eq: fileId } } });
        const currentFile = files.data?.[0];

        if (!currentFile) {
          throw new Error(`File with ID ${fileId} not found`);
        }

        if (options.file || options.buffer) {
          const uploadInput = this.createUploadInput({
            filename: currentFile.filename,
            mimetype: currentFile.mimetype,
            ...options,
          });
          return await this.fileManagementService.replaceFileAndUpdateRecord(
            fileRepo,
            fileId,
            currentFile,
            uploadInput,
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
        this.handleError(error, 'storage.$update');
      }
    };
  }

  private createDelete(context: TDynamicContext) {
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
        this.handleError(error, 'storage.$delete');
      }
    };
  }

  private createRegisterFile(context: TDynamicContext) {
    return async (options: any) => {
      try {
        const fileInput = this.createRegisterFileInput(options);
        const fileRepo = this.getFileRepo(context);

        return await this.fileManagementService.registerExternalFileRecord(
          fileInput,
          {
            folder: options.folder,
            storageConfig: options.storageConfig,
            title: options.title,
            description: options.description,
            userId: context.$user?.id,
            verifyExists: options.verifyExists,
          },
          fileRepo,
        );
      } catch (error: any) {
        this.handleError(error, 'storage.$registerFile');
      }
    };
  }
}
