import { Injectable, Logger, BadRequestException } from '@nestjs/common';
import { S3Client, PutObjectCommand, DeleteObjectCommand, GetObjectCommand, HeadObjectCommand } from '@aws-sdk/client-s3';
import { Readable } from 'stream';
import { IStorageService, StorageConfig, UploadResult } from './storage.interface';

@Injectable()
export class R2StorageService implements IStorageService {
  private readonly logger = new Logger(R2StorageService.name);

  private getS3Client(config: StorageConfig): S3Client {
    if (!config.accessKeyId || !config.secretAccessKey) {
      throw new BadRequestException('R2 requires accessKeyId and secretAccessKey');
    }

    if (!config.bucket) {
      throw new BadRequestException('R2 requires bucket name');
    }

    if (!config.accountId) {
      throw new BadRequestException('R2 requires accountId to generate endpoint');
    }

    const endpoint = `https://${config.accountId}.r2.cloudflarestorage.com`;

    return new S3Client({
      region: config.region || 'auto',
      endpoint: endpoint,
      credentials: {
        accessKeyId: config.accessKeyId,
        secretAccessKey: config.secretAccessKey,
      },
      forcePathStyle: false,
    });
  }

  async upload(
    buffer: Buffer,
    relativePath: string,
    mimetype: string,
    config: StorageConfig,
  ): Promise<UploadResult> {
    try {
      const s3Client = this.getS3Client(config);

      const command = new PutObjectCommand({
        Bucket: config.bucket,
        Key: relativePath,
        Body: buffer,
        ContentType: mimetype,
      });

      await s3Client.send(command);

      this.logger.log(
        `File uploaded to R2: ${config.bucket}/${relativePath}`,
      );

      return {
        location: relativePath,
      };
    } catch (error: any) {
      const cloudError = error.message || error.name || 'Unknown error';
      const errorMessage = `Failed to upload to R2: ${cloudError}`;
      this.logger.error(errorMessage, error);
      throw new BadRequestException(errorMessage);
    }
  }

  async delete(location: string, config: StorageConfig): Promise<void> {
    try {
      const s3Client = this.getS3Client(config);

      const command = new DeleteObjectCommand({
        Bucket: config.bucket!,
        Key: location,
      });

      await s3Client.send(command);

      this.logger.log(`Deleted file from R2: ${config.bucket}/${location}`);
    } catch (error: any) {
      const cloudError = error.message || error.name || 'Unknown error';
      const errorMessage = `Failed to delete from R2: ${cloudError}`;
      this.logger.error(errorMessage, error);
      throw new BadRequestException(errorMessage);
    }
  }

  async getStream(location: string, config: StorageConfig): Promise<Readable> {
    try {
      const s3Client = this.getS3Client(config);

      const command = new GetObjectCommand({
        Bucket: config.bucket!,
        Key: location,
      });

      const response = await s3Client.send(command);

      if (!response.Body) {
        throw new BadRequestException(`File not found in R2: ${location}`);
      }

      const stream = response.Body as Readable;

      this.logger.log(`Streaming file from R2: ${config.bucket}/${location}`);

      return stream;
    } catch (error: any) {
      const cloudError = error.message || error.name || 'Unknown error';
      const errorMessage = `Failed to stream from R2: ${cloudError}`;
      this.logger.error(errorMessage, error);
      throw new BadRequestException(errorMessage);
    }
  }

  async getBuffer(location: string, config: StorageConfig): Promise<Buffer> {
    try {
      const s3Client = this.getS3Client(config);

      const command = new GetObjectCommand({
        Bucket: config.bucket!,
        Key: location,
      });

      const response = await s3Client.send(command);

      if (!response.Body) {
        throw new BadRequestException(`File not found in R2: ${location}`);
      }

      const chunks: Uint8Array[] = [];
      const stream = response.Body as Readable;

      for await (const chunk of stream) {
        chunks.push(chunk);
      }

      const buffer = Buffer.concat(chunks);

      this.logger.log(`Downloaded buffer from R2: ${config.bucket}/${location} (${buffer.length} bytes)`);

      return buffer;
    } catch (error: any) {
      const cloudError = error.message || error.name || 'Unknown error';
      const errorMessage = `Failed to download from R2: ${cloudError}`;
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
      await this.upload(buffer, location, mimetype, config);
      this.logger.log(`Replaced file on R2: ${config.bucket}/${location}`);
    } catch (error: any) {
      const cloudError = error.message || error.name || 'Unknown error';
      const errorMessage = `Failed to replace file on R2: ${cloudError}`;
      this.logger.error(errorMessage, error);
      throw new BadRequestException(errorMessage);
    }
  }

  async exists(location: string, config: StorageConfig): Promise<boolean> {
    try {
      const s3Client = this.getS3Client(config);

      const command = new HeadObjectCommand({
        Bucket: config.bucket!,
        Key: location,
      });

      await s3Client.send(command);
      return true;
    } catch (error: any) {
      if (error.name === 'NotFound' || error.$metadata?.httpStatusCode === 404) {
        return false;
      }
      this.logger.warn(`Error checking file existence in R2: ${error.message}`);
      return false;
    }
  }
}

