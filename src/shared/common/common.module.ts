import { CommonService } from './services/common.service';
import { Global, Module } from '@nestjs/common';
import { CacheService } from '../../infrastructure/redis/services/cache.service';

@Global()
@Module({
  providers: [CommonService, CacheService],
  exports: [CommonService, CacheService],
})
export class CommonModule {}
