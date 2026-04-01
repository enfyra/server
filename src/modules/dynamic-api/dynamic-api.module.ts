import { Global, Module } from '@nestjs/common';
import { DynamicService } from './services/dynamic.service';
import { DynamicController } from './controllers/dynamic.controller';
import { TableValidationService } from './services/table-validation.service';

@Global()
@Module({
  controllers: [DynamicController],
  providers: [DynamicService, TableValidationService],
  exports: [DynamicService, TableValidationService],
})
export class DynamicApiModule {}
