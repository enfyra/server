import { Module } from '@nestjs/common';
import { PackageManagementService } from './services/package-management.service';
import { PackageController } from './controllers/package.controller';

@Module({
  controllers: [PackageController],
  providers: [PackageManagementService],
  exports: [PackageManagementService],
})
export class PackageManagementModule {}
