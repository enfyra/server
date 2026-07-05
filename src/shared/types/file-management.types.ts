import type { Readable } from 'stream';

export type FileUploadProgressPhase =
  | 'receiving'
  | 'storing'
  | 'completed'
  | 'failed';

export interface FileUploadProgressEvent {
  uploadId?: string;
  phase: FileUploadProgressPhase;
  loaded: number;
  total: number;
  percent: number;
  fileName?: string;
}

export type FileUploadProgressHandler = (
  event: FileUploadProgressEvent,
) => void | Promise<void>;

export interface FileUploadDto {
  filename: string;
  mimetype: string;
  stream: Readable;
  signatureBuffer?: Buffer;
  size: number;
  folder?: any; // Can be ID or object {id: ...}
  title?: string;
  description?: string;
  onProgress?: FileUploadProgressHandler;
}

export interface ProcessedFileInfo {
  filename: string;
  mimetype: string;
  type: string;
  filesize: number;
  storage_config_id: number | string; // number for SQL, string for MongoDB
  location: string;
  description?: string;
  status: 'active' | 'archived' | 'quarantine';
}

export interface UploadedFileInfo {
  originalname: string;
  mimetype: string;
  encoding: string;
  path?: string;
  size: number;
  fieldname: string;
}
