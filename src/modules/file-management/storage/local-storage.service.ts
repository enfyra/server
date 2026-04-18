import { Logger } from '../../../shared/logger';
import * as fs from 'fs';
import * as path from 'path';
import { Readable } from 'stream';
import {
  IStorageService,
  StorageConfig,
  UploadResult,
} from './storage.interface';

export class LocalStorageService implements IStorageService {
  private readonly logger = new Logger(LocalStorageService.name);
  private readonly basePath = path.join(process.cwd(), 'public');

  constructor() {
    this.ensurePublicDirExists();
  }

  private async ensurePublicDirExists(): Promise<void> {
    try {
      await fs.promises.mkdir(this.basePath, { recursive: true });
    } catch (error) {
      this.logger.error('Failed to create public directory', error);
    }
  }

  async upload(
    buffer: Buffer,
    relativePath: string,
    mimetype: string,
    config: StorageConfig,
  ): Promise<UploadResult> {
    const filePath = path.join(this.basePath, relativePath);
    await fs.promises.mkdir(path.dirname(filePath), { recursive: true });
    await fs.promises.writeFile(filePath, buffer);
    return {
      location: `/${relativePath}`,
    };
  }

  async delete(location: string, config: StorageConfig): Promise<void> {
    const relativePath = location.startsWith('/')
      ? location.slice(1)
      : location;
    const absolutePath = path.join(this.basePath, relativePath);
    if (await this.exists(location, config)) {
      await fs.promises.unlink(absolutePath);
    }
  }

  async getStream(location: string, config: StorageConfig): Promise<Readable> {
    const relativePath = location.startsWith('/')
      ? location.slice(1)
      : location;
    const absolutePath = path.join(this.basePath, relativePath);
    if (!(await this.exists(location, config))) {
      throw new Error(`File not found in local storage: ${location}`);
    }
    const stream = fs.createReadStream(absolutePath);
    try {
      const stats = await fs.promises.stat(absolutePath);
      (stream as any).contentLength = stats.size;
    } catch (error) {}
    return stream;
  }

  async getBuffer(location: string, config: StorageConfig): Promise<Buffer> {
    const relativePath = location.startsWith('/')
      ? location.slice(1)
      : location;
    const absolutePath = path.join(this.basePath, relativePath);
    if (!(await this.exists(location, config))) {
      throw new Error(`File not found in local storage: ${location}`);
    }
    const buffer = await fs.promises.readFile(absolutePath);
    return buffer;
  }

  async replaceFile(
    location: string,
    buffer: Buffer,
    mimetype: string,
    config: StorageConfig,
  ): Promise<void> {
    const relativePath = location.startsWith('/')
      ? location.slice(1)
      : location;
    const absolutePath = path.join(this.basePath, relativePath);
    await fs.promises.writeFile(absolutePath, buffer);
  }

  async exists(location: string, config: StorageConfig): Promise<boolean> {
    try {
      const relativePath = location.startsWith('/')
        ? location.slice(1)
        : location;
      const absolutePath = path.join(this.basePath, relativePath);
      await fs.promises.access(absolutePath);
      return true;
    } catch {
      return false;
    }
  }
}
