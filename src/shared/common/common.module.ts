import { CommonService } from './services/common.service';
import { Global, Module } from '@nestjs/common';
import { RedisLockService } from '../../infrastructure/redis/services/redis-lock.service';

@Global()
@Module({
  providers: [CommonService, RedisLockService],
  exports: [CommonService, RedisLockService],
})
export class CommonModule {}
