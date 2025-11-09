import { Injectable, BadRequestException } from '@nestjs/common';
import { IStorageService, StorageConfig } from './storage.interface';
import { LocalStorageService } from './local-storage.service';
import { GCSStorageService } from './gcs-storage.service';
import { R2StorageService } from './r2-storage.service';
import { S3StorageService } from './s3-storage.service';

@Injectable()
export class StorageFactoryService {
  constructor(
    private localStorageService: LocalStorageService,
    private gcsStorageService: GCSStorageService,
    private r2StorageService: R2StorageService,
    private s3StorageService: S3StorageService,
  ) {}

  getStorageService(storageType: string): IStorageService {
    switch (storageType) {
      case 'Local Storage':
        return this.localStorageService;
      case 'Google Cloud Storage':
        return this.gcsStorageService;
      case 'Amazon S3':
        return this.s3StorageService;
      case 'Cloudflare R2':
        return this.r2StorageService;
      default:
        throw new BadRequestException(`Unknown storage type: ${storageType}`);
    }
  }

  getStorageServiceByConfig(config: StorageConfig): IStorageService {
    return this.getStorageService(config.type);
  }
}

