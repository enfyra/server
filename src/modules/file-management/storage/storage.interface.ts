import { Readable } from 'stream';

export interface StorageConfig {
  id?: number | string;
  name: string;
  type: 'Local Storage' | 'Amazon S3' | 'Google Cloud Storage' | 'Cloudflare R2';
  bucket?: string;
  region?: string;
  accessKeyId?: string;
  secretAccessKey?: string;
  accountId?: string; // Cloudflare account ID for R2 (auto-generates endpoint)
  credentials?: any; // For GCS JSON credentials
  endpoint?: string; // For S3 custom endpoints (deprecated for R2, use accountId instead)
  [key: string]: any;
}

export interface UploadResult {
  location: string; // Relative path or key
  url?: string; // Full URL if available
}

export interface IStorageService {
  /**
   * Upload file to storage
   */
  upload(
    buffer: Buffer,
    relativePath: string,
    mimetype: string,
    config: StorageConfig,
  ): Promise<UploadResult>;

  /**
   * Delete file from storage
   */
  delete(location: string, config: StorageConfig): Promise<void>;

  /**
   * Get file stream from storage
   */
  getStream(location: string, config: StorageConfig): Promise<Readable>;

  /**
   * Get file buffer from storage
   */
  getBuffer(location: string, config: StorageConfig): Promise<Buffer>;

  /**
   * Replace/update existing file in storage
   */
  replaceFile(
    location: string,
    buffer: Buffer,
    mimetype: string,
    config: StorageConfig,
  ): Promise<void>;

  /**
   * Check if file exists in storage
   */
  exists(location: string, config: StorageConfig): Promise<boolean>;
}

