import { Module } from '@nestjs/common';
import { CacheService } from './services/cache.service';
import { RedisPubSubService } from './services/redis-pubsub.service';
import { RouteCacheService } from './services/route-cache.service';
import { MetadataCacheService } from './services/metadata-cache.service';

@Module({
  imports: [],
  providers: [CacheService, RedisPubSubService, RouteCacheService, MetadataCacheService],
  exports: [CacheService, RedisPubSubService, RouteCacheService, MetadataCacheService],
})
export class CacheModule {}
