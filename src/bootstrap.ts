import type { AwilixContainer } from 'awilix';
import type { Cradle } from './container';

export async function bootstrap(
  container: AwilixContainer<Cradle>,
): Promise<void> {
  const c: any = container.cradle;

  await c.mongoService?.onInit?.();
  await c.replicationManager?.onInit?.();
  await c.knexService?.onInit?.();
  await c.sqlPoolClusterCoordinatorService?.onInit?.();
  await c.sqlFunctionService?.onInit?.();

  await c.redisPubSubService?.onInit?.();

  await c.isolatedExecutorService?.onInit?.();

  try {
    await c.mongoSagaCoordinator?.onInit?.();
  } catch (e: any) {
    console.warn('MongoSagaCoordinator init skipped:', e.message);
  }

  await c.provisionService.waitForDatabase();
  await c.provisionService.recoverJournals();

  if (await c.firstRunInitializer.isNeeded()) {
    await c.firstRunInitializer.run();
  }

  await c.cacheOrchestratorService?.onInit?.();
  await c.metadataCacheService?.reload?.();
  await c.repoRegistryService?.onInit?.();
  await c.routeCacheService?.reload?.();
  await c.fieldPermissionCacheService?.reload?.();
  await c.settingCacheService?.reload?.();
  await c.storageConfigCacheService?.reload?.();
  await c.oauthConfigCacheService?.reload?.();
  await c.websocketCacheService?.reload?.();
  await c.flowCacheService?.reload?.();
  await c.packageCacheService?.reload?.();
  await c.folderTreeCacheService?.reload?.();
  await c.guardCacheService?.reload?.();
  await c.gqlDefinitionCacheService?.reload?.();

  await c.cacheOrchestratorService?.onBootstrap?.();
  await c.sqlPoolClusterCoordinatorService?.onBootstrap?.();
  await c.sqlFunctionService?.onBootstrap?.();

  await c.graphqlService?.reloadSchema?.();

  await c.sessionCleanupService?.onInit?.();
  await c.userRevocationService?.onInit?.();
  await c.eventQueueService?.onInit?.();
  await c.connectionQueueService?.onInit?.();

  c.eventEmitter.emit('SYSTEM_READY');
}

export async function shutdown(
  container: AwilixContainer<Cradle>,
): Promise<void> {
  await container.dispose();
}
