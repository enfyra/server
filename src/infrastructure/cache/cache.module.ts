import { Global, Module } from '@nestjs/common';
import { CacheService } from './services/cache.service';
import { RedisPubSubService } from './services/redis-pubsub.service';
import { RouteCacheService } from './services/route-cache.service';
import { PackageCacheService } from './services/package-cache.service';
import { MetadataCacheService } from './services/metadata-cache.service';
import { StorageConfigCacheService } from './services/storage-config-cache.service';
import { AiConfigCacheService } from './services/ai-config-cache.service';
import { WebsocketCacheService } from './services/websocket-cache.service';
import { OAuthConfigCacheService } from './services/oauth-config-cache.service';

@Global()
@Module({
  providers: [CacheService, RedisPubSubService, MetadataCacheService, RouteCacheService, PackageCacheService, StorageConfigCacheService, AiConfigCacheService, WebsocketCacheService, OAuthConfigCacheService],
  exports: [CacheService, RedisPubSubService, MetadataCacheService, RouteCacheService, PackageCacheService, StorageConfigCacheService, AiConfigCacheService, WebsocketCacheService, OAuthConfigCacheService],
})
export class CacheModule {}
