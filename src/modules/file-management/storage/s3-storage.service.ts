import { Logger } from '../../../shared/logger';
import { BadRequestException } from '../../../domain/exceptions';
import {
  S3Client,
  DeleteObjectCommand,
  GetObjectCommand,
  HeadObjectCommand,
} from '@aws-sdk/client-s3';
import { Upload } from '@aws-sdk/lib-storage';
import { Readable } from 'stream';
import {
  IStorageService,
  StorageConfig,
  StorageStreamOptions,
  UploadResult,
} from './storage.interface';

export class S3StorageService implements IStorageService {
  private readonly logger = new Logger(S3StorageService.name);

  private getS3Client(config: StorageConfig): S3Client {
    if (!config.accessKeyId || !config.secretAccessKey) {
      throw new BadRequestException(
        'S3 requires accessKeyId and secretAccessKey',
      );
    }

    if (!config.bucket) {
      throw new BadRequestException('S3 requires bucket name');
    }

    if (!config.region) {
      throw new BadRequestException('S3 requires region');
    }

    return new S3Client({
      region: config.region,
      endpoint: config.endpoint,
      credentials: {
        accessKeyId: config.accessKeyId,
        secretAccessKey: config.secretAccessKey,
      },
      forcePathStyle: config.endpoint ? true : false,
    });
  }

  async upload(
    stream: Readable,
    relativePath: string,
    mimetype: string,
    config: StorageConfig,
  ): Promise<UploadResult> {
    try {
      const s3Client = this.getS3Client(config);
      const upload = new Upload({
        client: s3Client,
        params: {
          Bucket: config.bucket,
          Key: relativePath,
          Body: stream,
          ContentType: mimetype,
        },
      });

      await upload.done();

      return {
        location: relativePath,
      };
    } catch (error: any) {
      const cloudError = error.message || error.name || 'Unknown error';
      const errorMessage = `Failed to upload to S3: ${cloudError}`;
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
    } catch (error: any) {
      const cloudError = error.message || error.name || 'Unknown error';
      const errorMessage = `Failed to delete from S3: ${cloudError}`;
      this.logger.error(errorMessage, error);
      throw new BadRequestException(errorMessage);
    }
  }

  async getStream(
    location: string,
    config: StorageConfig,
    options?: StorageStreamOptions,
  ): Promise<Readable> {
    try {
      const s3Client = this.getS3Client(config);

      const command = new GetObjectCommand({
        Bucket: config.bucket!,
        Key: location,
        Range: options?.range
          ? `bytes=${options.range.start}-${options.range.end}`
          : undefined,
      });

      const response = await s3Client.send(command);

      if (!response.Body) {
        throw new BadRequestException(`File not found in S3: ${location}`);
      }

      const stream = response.Body as Readable;
      if (response.ContentLength !== undefined) {
        (stream as any).contentLength = response.ContentLength;
      }

      return stream;
    } catch (error: any) {
      const cloudError = error.message || error.name || 'Unknown error';
      const errorMessage = `Failed to stream from S3: ${cloudError}`;
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
        throw new BadRequestException(`File not found in S3: ${location}`);
      }

      const chunks: Uint8Array[] = [];
      const stream = response.Body as Readable;

      for await (const chunk of stream) {
        chunks.push(chunk);
      }

      const buffer = Buffer.concat(chunks);

      return buffer;
    } catch (error: any) {
      const cloudError = error.message || error.name || 'Unknown error';
      const errorMessage = `Failed to download from S3: ${cloudError}`;
      this.logger.error(errorMessage, error);
      throw new BadRequestException(errorMessage);
    }
  }

  async replaceFile(
    location: string,
    stream: Readable,
    mimetype: string,
    config: StorageConfig,
  ): Promise<void> {
    await this.upload(stream, location, mimetype, config);
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
      if (
        error.name === 'NotFound' ||
        error.$metadata?.httpStatusCode === 404
      ) {
        return false;
      }
      this.logger.warn(`Error checking file existence in S3: ${error.message}`);
      return false;
    }
  }
}
