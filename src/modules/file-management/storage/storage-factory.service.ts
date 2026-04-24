import { BadRequestException } from '../../../domain/exceptions/custom-exceptions';
import { IStorageService, StorageConfig } from './storage.interface';
import { LocalStorageService } from './local-storage.service';
import { GCSStorageService } from './gcs-storage.service';
import { R2StorageService } from './r2-storage.service';
import { S3StorageService } from './s3-storage.service';

export class StorageFactoryService {
  private readonly localStorageService: LocalStorageService;
  private readonly gcsStorageService: GCSStorageService;
  private readonly r2StorageService: R2StorageService;
  private readonly s3StorageService: S3StorageService;

  constructor(deps: {
    localStorageService: LocalStorageService;
    gcsStorageService: GCSStorageService;
    r2StorageService: R2StorageService;
    s3StorageService: S3StorageService;
  }) {
    this.localStorageService = deps.localStorageService;
    this.gcsStorageService = deps.gcsStorageService;
    this.r2StorageService = deps.r2StorageService;
    this.s3StorageService = deps.s3StorageService;
  }

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
