import { Global, Module } from '@nestjs/common';
import { DynamicService } from './services/dynamic.service';
import { DynamicController } from './controllers/dynamic.controller';
import { SystemProtectionService } from './services/system-protection.service';
import { TableValidationService } from './services/table-validation.service';

@Global()
@Module({
  controllers: [DynamicController],
  providers: [DynamicService, SystemProtectionService, TableValidationService],
  exports: [DynamicService, SystemProtectionService, TableValidationService],
})
export class DynamicModule {}
