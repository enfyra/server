import { CommonService } from './services/common.service';
import { DatabaseConfigService } from '../services/database-config.service';
import { InstanceService } from '../services/instance.service';
import { Global, Module } from '@nestjs/common';

@Global()
@Module({
  providers: [CommonService, DatabaseConfigService, InstanceService],
  exports: [CommonService, DatabaseConfigService, InstanceService],
})
export class CommonModule {}
