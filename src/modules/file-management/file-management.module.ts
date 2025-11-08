import { Module } from '@nestjs/common';
import { FileManagementService } from './services/file-management.service';
import { FileAssetsService } from './services/file-assets.service';
import { AssetsController } from './controllers/assets.controller';
import { FileController } from './controllers/file.controller';
import { LocalStorageService } from './storage/local-storage.service';
import { GCSStorageService } from './storage/gcs-storage.service';
import { R2StorageService } from './storage/r2-storage.service';
import { StorageFactoryService } from './storage/storage-factory.service';

@Module({
  imports: [],
  controllers: [AssetsController, FileController],
  providers: [
    FileManagementService,
    FileAssetsService,
    LocalStorageService,
    GCSStorageService,
    R2StorageService,
    StorageFactoryService,
  ],
  exports: [FileManagementService, FileAssetsService, StorageFactoryService],
})
export class FileManagementModule {}