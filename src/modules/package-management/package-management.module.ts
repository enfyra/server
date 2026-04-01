import { Module } from '@nestjs/common';
import { PackageController } from './controllers/package.controller';

@Module({
  controllers: [PackageController],
})
export class PackageManagementModule {}
