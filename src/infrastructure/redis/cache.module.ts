import { Module } from '@nestjs/common';
import { CacheService } from './services/cache.service';
import { RedisPubSubService } from './services/redis-pubsub.service';
import { RouteCacheService } from './services/route-cache.service';

@Module({
  providers: [CacheService, RedisPubSubService, RouteCacheService],
  exports: [CacheService, RedisPubSubService, RouteCacheService],
})
export class CacheModule {}
