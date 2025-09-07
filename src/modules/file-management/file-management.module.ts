import { Module } from '@nestjs/common';
import { FileManagementService } from './services/file-management.service';
import { FileAssetsService } from './services/file-assets.service';
import { AssetsController } from './controllers/assets.controller';
import { FileController } from './controllers/file.controller';

@Module({
  imports: [],
  controllers: [AssetsController, FileController],
  providers: [FileManagementService, FileAssetsService],
  exports: [FileManagementService, FileAssetsService],
})
export class FileManagementModule {}