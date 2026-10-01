import type { UploadedFileInfo } from './file-management.types';

export interface MultipartFileField {
  name: string;
  required?: boolean;
  maxCount?: number;
  maxFileSize?: number | null;
  allowedMimeTypes?: string[] | null;
}

export interface MultipartUploadConfig {
  fields: MultipartFileField[];
  maxFiles: number;
  maxUploadFileSize?: number | null;
}

export type UploadedFileFields = Record<string, UploadedFileInfo | UploadedFileInfo[]>;
