import { Injectable } from '@nestjs/common';
import { FileManagementService } from '../../modules/file-management/services/file-management.service';
import { TDynamicContext } from '../../shared/interfaces/dynamic-context.interface';

@Injectable()
export class UploadFileHelper {
  constructor(private readonly fileManagementService: FileManagementService) {}

  createUploadFileHelper(context: TDynamicContext) {
    return async (options: any) => {
      try {
        let buffer = options.buffer;
        if (buffer && typeof buffer === 'object' && !Buffer.isBuffer(buffer)) {
          if (typeof buffer.toBuffer === 'function') {
            buffer = buffer.toBuffer();
          } else if (buffer.type === 'Buffer' && Array.isArray(buffer.data)) {
            buffer = Buffer.from(buffer.data);
          } else {
            const keys = Object.keys(buffer);
            const numericKeys = keys.filter(k => /^\d+$/.test(k));
            if (numericKeys.length > 0) {
              const sortedKeys = numericKeys.map(k => parseInt(k, 10)).sort((a, b) => a - b);
              const arr = new Array(sortedKeys.length);
              for (let i = 0; i < sortedKeys.length; i++) {
                arr[i] = buffer[sortedKeys[i].toString()];
              }
              buffer = Buffer.from(arr);
            } else {
              throw new Error('Invalid buffer format: buffer object has no numeric keys');
            }
          }
        }

        const fileRepo = context.$repos?.file_definition || context.$repos?.main;

        if (!fileRepo) {
          throw new Error('File repository not found in context');
        }

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
      } catch (error) {
        let errorMessage = 'Unknown error in $uploadFile';
        
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
    };
  }
}

