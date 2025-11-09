import { Injectable, Logger, BadRequestException } from '@nestjs/common';
import { Storage } from '@google-cloud/storage';
import { Readable } from 'stream';
import { IStorageService, StorageConfig, UploadResult } from './storage.interface';

@Injectable()
export class GCSStorageService implements IStorageService {
  private readonly logger = new Logger(GCSStorageService.name);

  async upload(
    buffer: Buffer,
    relativePath: string,
    mimetype: string,
    config: StorageConfig,
  ): Promise<UploadResult> {
    try {
      const credentials = typeof config.credentials === 'string'
        ? JSON.parse(config.credentials)
        : config.credentials;

      const storage = new Storage({
        credentials: credentials,
      });

      const bucket = storage.bucket(config.bucket!);
      const file = bucket.file(relativePath);

      await file.save(buffer, {
        metadata: {
          contentType: mimetype,
        },
      });

      this.logger.log(
        `File uploaded to GCS: gs://${config.bucket}/${relativePath}`,
      );

      return {
        location: relativePath,
      };
    } catch (error: any) {
      const cloudError = error.message || error.code || error.name || 'Unknown error';
      const errorMessage = `Failed to upload to GCS: ${cloudError}`;
      this.logger.error(errorMessage, error);
      throw new BadRequestException(errorMessage);
    }
  }

  async delete(location: string, config: StorageConfig): Promise<void> {
    try {
      const credentials = typeof config.credentials === 'string'
        ? JSON.parse(config.credentials)
        : config.credentials;

      const storage = new Storage({
        credentials: credentials,
      });

      const bucket = storage.bucket(config.bucket!);
      const file = bucket.file(location);

      await file.delete({ ignoreNotFound: true });

      this.logger.log(`Deleted file from GCS: gs://${config.bucket}/${location}`);
    } catch (error: any) {
      const cloudError = error.message || error.code || error.name || 'Unknown error';
      const errorMessage = `Failed to delete from GCS: ${cloudError}`;
      this.logger.error(errorMessage, error);
      throw new BadRequestException(errorMessage);
    }
  }

  async getStream(location: string, config: StorageConfig): Promise<Readable> {
    try {
      const credentials = typeof config.credentials === 'string'
        ? JSON.parse(config.credentials)
        : config.credentials;

      const storage = new Storage({
        credentials: credentials,
      });

      const bucket = storage.bucket(config.bucket!);
      const file = bucket.file(location);

      const [exists] = await file.exists();
      if (!exists) {
        throw new BadRequestException(`File not found in GCS: ${location}`);
      }

      const stream = file.createReadStream();

      this.logger.log(`Streaming file from GCS: gs://${config.bucket}/${location}`);

      return stream;
    } catch (error: any) {
      const cloudError = error.message || error.code || error.name || 'Unknown error';
      const errorMessage = `Failed to stream from GCS: ${cloudError}`;
      this.logger.error(errorMessage, error);
      throw new BadRequestException(errorMessage);
    }
  }

  async getBuffer(location: string, config: StorageConfig): Promise<Buffer> {
    try {
      const credentials = typeof config.credentials === 'string'
        ? JSON.parse(config.credentials)
        : config.credentials;

      const storage = new Storage({
        credentials: credentials,
      });

      const bucket = storage.bucket(config.bucket!);
      const file = bucket.file(location);

      const [exists] = await file.exists();
      if (!exists) {
        throw new BadRequestException(`File not found in GCS: ${location}`);
      }

      const [buffer] = await file.download();

      this.logger.log(`Downloaded buffer from GCS: gs://${config.bucket}/${location} (${buffer.length} bytes)`);

      return buffer;
    } catch (error: any) {
      const cloudError = error.message || error.code || error.name || 'Unknown error';
      const errorMessage = `Failed to download from GCS: ${cloudError}`;
      this.logger.error(errorMessage, error);
      throw new BadRequestException(errorMessage);
    }
  }

  async replaceFile(
    location: string,
    buffer: Buffer,
    mimetype: string,
    config: StorageConfig,
  ): Promise<void> {
    try {
      const credentials = typeof config.credentials === 'string'
        ? JSON.parse(config.credentials)
        : config.credentials;

      const storage = new Storage({
        credentials: credentials,
      });

      const bucket = storage.bucket(config.bucket!);
      const file = bucket.file(location);

      await file.save(buffer, {
        metadata: {
          contentType: mimetype,
        },
      });

      this.logger.log(`Replaced file on GCS: gs://${config.bucket}/${location}`);
    } catch (error: any) {
      const cloudError = error.message || error.code || error.name || 'Unknown error';
      const errorMessage = `Failed to replace file on GCS: ${cloudError}`;
      this.logger.error(errorMessage, error);
      throw new BadRequestException(errorMessage);
    }
  }

  async exists(location: string, config: StorageConfig): Promise<boolean> {
    try {
      const credentials = typeof config.credentials === 'string'
        ? JSON.parse(config.credentials)
        : config.credentials;

      const storage = new Storage({
        credentials: credentials,
      });

      const bucket = storage.bucket(config.bucket!);
      const file = bucket.file(location);

      const [exists] = await file.exists();
      return exists;
    } catch {
      return false;
    }
  }
}

