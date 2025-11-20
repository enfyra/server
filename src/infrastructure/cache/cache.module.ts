import { Global, Module } from '@nestjs/common';
import { CacheService } from './services/cache.service';
import { RedisPubSubService } from './services/redis-pubsub.service';
import { RouteCacheService } from './services/route-cache.service';
import { MetadataCacheService } from './services/metadata-cache.service';
import { StorageConfigCacheService } from './services/storage-config-cache.service';
import { AiConfigCacheService } from './services/ai-config-cache.service';

@Global()
@Module({
  providers: [CacheService, RedisPubSubService, MetadataCacheService, RouteCacheService, StorageConfigCacheService, AiConfigCacheService],
  exports: [CacheService, RedisPubSubService, MetadataCacheService, RouteCacheService, StorageConfigCacheService, AiConfigCacheService],
})
export class CacheModule {}
