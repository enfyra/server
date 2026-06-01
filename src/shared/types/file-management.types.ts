import type { Readable } from 'stream';

export interface FileUploadDto {
  filename: string;
  mimetype: string;
  stream: Readable;
  signatureBuffer?: Buffer;
  size: number;
  folder?: any; // Can be ID or object {id: ...}
  title?: string;
  description?: string;
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
