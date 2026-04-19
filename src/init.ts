import type { AwilixContainer } from 'awilix';
import type { Cradle } from './container';
import { CACHE_EVENTS } from './shared/utils/cache-events.constants';

export async function init(
  container: AwilixContainer<Cradle>,
): Promise<void> {
  const c: any = container.cradle;

  await c.mongoService?.init?.();
  await c.replicationManager?.init?.();
  await c.knexService?.init?.();
  await c.sqlPoolClusterCoordinatorService?.init?.();

  await c.redisPubSubService?.init?.();

  try {
    await c.mongoSagaCoordinator?.init?.();
  } catch (e: any) {
    console.warn('MongoSagaCoordinator init skipped:', e.message);
  }

  await c.provisionService.waitForDatabase();
  await c.provisionService.recoverJournals();

  if (await c.firstRunInitializer.isNeeded()) {
    await c.firstRunInitializer.run();
  }

  await c.cacheOrchestratorService?.init?.();
  await c.metadataCacheService?.reload?.();
  c.eventEmitter.emit(CACHE_EVENTS.METADATA_LOADED);
  await c.repoRegistryService?.rebuildFromMetadata?.(c.metadataCacheService);

  await Promise.all([
    c.routeCacheService?.reload?.(),
    c.fieldPermissionCacheService?.reload?.(),
    c.columnRuleCacheService?.reload?.(),
    c.settingCacheService?.reload?.(),
    c.storageConfigCacheService?.reload?.(),
    c.oauthConfigCacheService?.reload?.(),
    c.websocketCacheService?.reload?.(),
    c.flowCacheService?.reload?.(),
    c.packageCacheService?.reload?.(),
    c.folderTreeCacheService?.reload?.(),
    c.guardCacheService?.reload?.(),
    c.gqlDefinitionCacheService?.reload?.(),
    c.bootstrapScriptService?.onMetadataLoaded?.(),
    c.sqlFunctionService?.installExtensions?.(),
  ]);

  await c.graphqlService?.reloadSchema?.();
  c.eventEmitter.emit(CACHE_EVENTS.GRAPHQL_LOADED);

  await Promise.all([
    c.sessionCleanupService?.init?.(),
    c.userRevocationService?.init?.(),
    c.eventQueueService?.init?.(),
    c.connectionQueueService?.init?.(),
  ]);

  c.eventEmitter.emit(CACHE_EVENTS.SYSTEM_READY);
}

export async function shutdown(
  container: AwilixContainer<Cradle>,
): Promise<void> {
  await container.dispose();
}
