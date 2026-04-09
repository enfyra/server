import { Global, Module } from '@nestjs/common';
import { CacheService } from './services/cache.service';
import { RedisPubSubService } from './services/redis-pubsub.service';
import { RouteCacheService } from './services/route-cache.service';
import { PackageCacheService } from './services/package-cache.service';
import { MetadataCacheService } from './services/metadata-cache.service';
import { StorageConfigCacheService } from './services/storage-config-cache.service';
import { WebsocketCacheService } from './services/websocket-cache.service';
import { OAuthConfigCacheService } from './services/oauth-config-cache.service';
import { RateLimitService } from './services/rate-limit.service';
import { FolderTreeCacheService } from './services/folder-tree-cache.service';
import { RepoRegistryService } from './services/repo-registry.service';
import { FlowCacheService } from './services/flow-cache.service';
import { PackageCdnLoaderService } from './services/package-cdn-loader.service';
import { GuardCacheService } from './services/guard-cache.service';
import { GuardEvaluatorService } from './services/guard-evaluator.service';
import { SettingCacheService } from './services/setting-cache.service';

@Global()
@Module({
  providers: [
    CacheService,
    RedisPubSubService,
    MetadataCacheService,
    RouteCacheService,
    PackageCacheService,
    StorageConfigCacheService,
    WebsocketCacheService,
    OAuthConfigCacheService,
    RateLimitService,
    FolderTreeCacheService,
    FlowCacheService,
    PackageCdnLoaderService,
    GuardCacheService,
    GuardEvaluatorService,
    SettingCacheService,
    RepoRegistryService,
  ],
  exports: [
    CacheService,
    RedisPubSubService,
    MetadataCacheService,
    RouteCacheService,
    PackageCacheService,
    StorageConfigCacheService,
    WebsocketCacheService,
    OAuthConfigCacheService,
    RateLimitService,
    FolderTreeCacheService,
    FlowCacheService,
    PackageCdnLoaderService,
    GuardCacheService,
    GuardEvaluatorService,
    SettingCacheService,
    RepoRegistryService,
  ],
})
export class CacheModule {}
