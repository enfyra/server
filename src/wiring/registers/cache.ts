import { asClass, asFunction } from 'awilix';
import type { Cradle } from '../cradle';
import { env } from '../../env';
import {
  AuthHeaderCacheBuilder,
  CacheOrchestratorService,
  FieldPermissionCacheBuilder,
  ColumnRuleCacheBuilder,
  FlowCacheBuilder,
  FolderTreeCacheService,
  GqlDefinitionCacheService,
  GuardAlertService,
  GuardCacheBuilder,
  GuardEvaluatorService,
  MetadataCacheService,
  OAuthConfigCacheBuilder,
  PackageCacheService,
  PackageCdnLoaderService,
  PackageRuntimeService,
  RateLimitService,
  RedisPubSubService,
  RedisRuntimeCacheStore,
  RedisCacheService,
  RepoRegistryService,
  RouteCacheService,
  RuntimeNamespaceLifecycleService,
  RuntimeRegistryService,
  RuntimeReloadAuditService,
  RuntimeScriptExecutorService,
  RuntimeScriptRepairService,
  SettingCacheService,
  StorageConfigCacheBuilder,
  WebsocketCacheBuilder,
} from '../../engines/cache';
import { DynamicRepositoryFactory } from '../../modules/dynamic-api';

export const cacheRegisters = {
  cacheService: asFunction(
    (cradle: Cradle) =>
      new RedisCacheService({
        redis: cradle.redis,
        envService: cradle.envService,
        runtimeNamespaceLifecycleService: cradle.runtimeNamespaceLifecycleService,
        policy: { keyPrefix: '', clearAllMode: 'namespace' },
      }),
  ).singleton(),
  userCacheService: asFunction(
    (cradle: Cradle) =>
      new RedisCacheService({
        redis: cradle.redis,
        envService: cradle.envService,
        runtimeNamespaceLifecycleService: cradle.runtimeNamespaceLifecycleService,
        policy: {
          keyPrefix: 'user_cache:',
          requireNamespace: true,
          quota: {
            limitBytes: env.REDIS_USER_CACHE_LIMIT_MB * 1024 * 1024,
            maxValueBytes: env.REDIS_USER_CACHE_MAX_VALUE_BYTES,
          },
          clearAllMode: 'prefix',
        },
      }),
  ).singleton(),
  redisPubSubService: asClass(RedisPubSubService)
    .singleton()
    .disposer((service: RedisPubSubService) => service.onDestroy()),
  runtimeNamespaceLifecycleService: asClass(RuntimeNamespaceLifecycleService)
    .singleton()
    .disposer((service: RuntimeNamespaceLifecycleService) => service.onDestroy()),
  redisRuntimeCacheStore: asClass(RedisRuntimeCacheStore).singleton(),
  metadataCacheService: asClass(MetadataCacheService).singleton(),
  routeCacheService: asClass(RouteCacheService).singleton(),
  packageCacheService: asClass(PackageCacheService).singleton(),
  packageRuntimeService: asClass(PackageRuntimeService).singleton(),
  storageConfigCacheBuilder: asClass(StorageConfigCacheBuilder).singleton(),
  websocketCacheBuilder: asClass(WebsocketCacheBuilder).singleton(),
  oauthConfigCacheBuilder: asClass(OAuthConfigCacheBuilder).singleton(),
  rateLimitService: asClass(RateLimitService).singleton(),
  folderTreeCacheService: asClass(FolderTreeCacheService).singleton(),
  flowCacheBuilder: asClass(FlowCacheBuilder).singleton(),
  packageCdnLoaderService: asClass(PackageCdnLoaderService).singleton(),
  guardCacheBuilder: asClass(GuardCacheBuilder).singleton(),
  guardEvaluatorService: asClass(GuardEvaluatorService).singleton(),
  guardAlertService: asClass(GuardAlertService).singleton(),
  settingCacheService: asClass(SettingCacheService).singleton(),
  authHeaderCacheBuilder: asClass(AuthHeaderCacheBuilder).singleton(),
  fieldPermissionCacheBuilder: asClass(FieldPermissionCacheBuilder).singleton(),
  columnRuleCacheBuilder: asClass(ColumnRuleCacheBuilder).singleton(),
  gqlDefinitionCacheService: asClass(GqlDefinitionCacheService).singleton(),
  repoRegistryService: asClass(RepoRegistryService).singleton(),
  runtimeRegistryService: asClass(RuntimeRegistryService).singleton(),
  runtimeReloadAuditService: asClass(RuntimeReloadAuditService).singleton(),
  runtimeScriptRepairService: asClass(RuntimeScriptRepairService).singleton(),
  dynamicRepositoryFactory: asClass(DynamicRepositoryFactory).singleton(),
  cacheOrchestratorService: asClass(CacheOrchestratorService)
    .singleton()
    .disposer((service: CacheOrchestratorService) => service.onDestroy()),
} as const;
