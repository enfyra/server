import { CommonService } from './services/common.service';
import { InstanceService } from '../services/instance.service';
import { Global, Module } from '@nestjs/common';

@Global()
@Module({
  providers: [CommonService, InstanceService],
  exports: [CommonService, InstanceService],
})
export class CommonModule {}
