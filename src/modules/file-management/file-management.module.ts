import { Module } from '@nestjs/common';
import { UploadFileHelper } from '../../infrastructure/helpers/upload-file.helper';
import { FileManagementService } from './services/file-management.service';
import { FileAssetsService } from './services/file-assets.service';
import { AssetsController } from './controllers/assets.controller';
import { FileController } from './controllers/file.controller';
import { LocalStorageService } from './storage/local-storage.service';
import { GCSStorageService } from './storage/gcs-storage.service';
import { R2StorageService } from './storage/r2-storage.service';
import { S3StorageService } from './storage/s3-storage.service';
import { StorageFactoryService } from './storage/storage-factory.service';

@Module({
  controllers: [AssetsController, FileController],
  providers: [
    FileManagementService,
    FileAssetsService,
    LocalStorageService,
    GCSStorageService,
    R2StorageService,
    S3StorageService,
    StorageFactoryService,
    UploadFileHelper,
  ],
  exports: [FileManagementService, FileAssetsService, StorageFactoryService, UploadFileHelper],
})
export class FileManagementModule {}