import type { AwilixContainer } from 'awilix';
import type { Cradle } from './container';
import { ensureDatabaseExists } from './engines/knex';
import {
  CACHE_EVENTS,
  CACHE_IDENTIFIERS,
} from './shared/utils/cache-events.constants';
import { Logger } from './shared/logger';

const logger = new Logger('Init');

async function runInitStep(
  label: string,
  callback: () => Promise<unknown> | unknown,
): Promise<void> {
  const start = Date.now();
  logger.log(`Init step started: ${label}`);
  try {
    await callback();
    logger.log(`Init step completed: ${label} (${Date.now() - start}ms)`);
  } catch (error) {
    logger.error(`Init step failed: ${label}`, error);
    throw error;
  }
}

export async function init(container: AwilixContainer<Cradle>): Promise<void> {
  const c: any = container.cradle;

  await runInitStep('mongoService.init', () => c.mongoService?.init?.());
  await runInitStep('ensureDatabaseExists', async () => {
    const dbType = c.databaseConfigService?.getDbType?.();
    if (dbType === 'mysql' || dbType === 'postgres') {
      await ensureDatabaseExists();
    }
  });
  await runInitStep('replicationManager.init', () =>
    c.replicationManager?.init?.(),
  );
  await runInitStep('knexService.init', () => c.knexService?.init?.());
  await runInitStep('sqlPoolClusterCoordinatorService.init', () =>
    c.sqlPoolClusterCoordinatorService?.init?.(),
  );

  await runInitStep('redisPubSubService.init', () =>
    c.redisPubSubService?.init?.(),
  );
  await runInitStep('runtimeNamespaceLifecycleService.init', () =>
    c.runtimeNamespaceLifecycleService?.init?.(),
  );

  try {
    await runInitStep('mongoSagaCoordinator.init', () =>
      c.mongoSagaCoordinator?.init?.(),
    );
  } catch (e: any) {
    logger.warn(`MongoSagaCoordinator init skipped: ${e.message}`);
  }

  await runInitStep('provisionService.waitForDatabase', () =>
    c.provisionService.waitForDatabase(),
  );
  await runInitStep('provisionService.recoverJournals', () =>
    c.provisionService.recoverJournals(),
  );

  await runInitStep('firstRunInitializer', async () => {
    if (await c.firstRunInitializer.isNeeded()) {
      await c.firstRunInitializer.run();
    }
  });
  await runInitStep('runtimeReloadAuditService.repairInterruptedReloads', () =>
    c.runtimeReloadAuditService?.markInterruptedReloadsFailed?.(
      'Runtime reload was interrupted by process restart before activation',
    ),
  );

  await runInitStep('cacheOrchestratorService.init', () =>
    c.cacheOrchestratorService?.init?.(),
  );
  await runInitStep('runtimeRegistryService.init', () =>
    c.runtimeRegistryService?.init?.(),
  );
  await runInitStep('metadataCacheService.reload', () =>
    c.metadataCacheService?.reload?.(),
  );
  c.eventEmitter.emit(CACHE_EVENTS.METADATA_LOADED);
  logger.log('Init event emitted: METADATA_LOADED');
  await runInitStep('repoRegistryService.rebuildFromMetadata', () =>
    c.repoRegistryService?.rebuildFromMetadata?.(c.metadataCacheService),
  );
  await runInitStep('websocketRuntimeService.init', () =>
    c.websocketRuntimeService?.init?.(),
  );
  await runInitStep('packageRuntimeService.init', () =>
    c.packageRuntimeService?.init?.(),
  );

  await Promise.all([
    runInitStep('routeCacheService.reload', () =>
      c.routeCacheService?.reload?.(),
    ),
    runInitStep('fieldPermissionCacheBuilder.reload', () =>
      c.fieldPermissionCacheBuilder?.reload?.(),
    ),
    runInitStep('columnRuleCacheBuilder.reload', () =>
      c.columnRuleCacheBuilder?.reload?.(),
    ),
    runInitStep('settingCacheService.reload', () =>
      c.settingCacheService?.reload?.(),
    ),
    runInitStep('storageConfigCacheBuilder.reload', () =>
      c.storageConfigCacheBuilder?.reload?.(),
    ),
    runInitStep('oauthConfigCacheBuilder.reload', () =>
      c.oauthConfigCacheBuilder?.reload?.(),
    ),
    runInitStep('websocketCacheBuilder.reload', () =>
      c.websocketCacheBuilder?.reload?.(),
    ),
    runInitStep('flowCacheBuilder.reload', () =>
      c.flowCacheBuilder?.reload?.(),
    ),
    runInitStep('packageCacheService.reload', () =>
      c.packageCacheService?.reload?.(),
    ),
    runInitStep('folderTreeCacheService.reload', () =>
      c.folderTreeCacheService?.reload?.(),
    ),
    runInitStep('guardCacheBuilder.reload', () =>
      c.guardCacheBuilder?.reload?.(),
    ),
    runInitStep('gqlDefinitionCacheService.reload', () =>
      c.gqlDefinitionCacheService?.reload?.(),
    ),
    runInitStep('bootstrapScriptService.onMetadataLoaded', () =>
      c.bootstrapScriptService?.onMetadataLoaded?.(),
    ),
    runInitStep('sqlFunctionService.installExtensions', () =>
      c.sqlFunctionService?.installExtensions?.(),
    ),
  ]);

  await runInitStep('runtimeRegistryService.publishInitialCaches', async () => {
    const runtimeRegistry = c.runtimeRegistryService;
    if (!runtimeRegistry?.publishFromCache) return;
    const entries = [
      [CACHE_IDENTIFIERS.METADATA, c.metadataCacheService],
      [CACHE_IDENTIFIERS.ROUTE, c.routeCacheService],
      [CACHE_IDENTIFIERS.FIELD_PERMISSION, c.fieldPermissionCacheBuilder],
      [CACHE_IDENTIFIERS.COLUMN_RULE, c.columnRuleCacheBuilder],
      [CACHE_IDENTIFIERS.SETTING, c.settingCacheService],
      [CACHE_IDENTIFIERS.STORAGE, c.storageConfigCacheBuilder],
      [CACHE_IDENTIFIERS.OAUTH_CONFIG, c.oauthConfigCacheBuilder],
      [CACHE_IDENTIFIERS.WEBSOCKET, c.websocketCacheBuilder],
      [CACHE_IDENTIFIERS.FLOW, c.flowCacheBuilder],
      [CACHE_IDENTIFIERS.PACKAGE, c.packageCacheService],
      [CACHE_IDENTIFIERS.FOLDER_TREE, c.folderTreeCacheService],
      [CACHE_IDENTIFIERS.GUARD, c.guardCacheBuilder],
      [CACHE_IDENTIFIERS.GRAPHQL, c.gqlDefinitionCacheService],
    ] as const;
    for (const [identifier, service] of entries) {
      await runtimeRegistry.publishFromCache(identifier, service);
    }
  });

  await runInitStep('flowRuntimeService.init', () =>
    c.flowRuntimeService?.init?.(),
  );

  await runInitStep('graphqlService.reloadSchema', () =>
    c.graphqlService?.reloadSchema?.(),
  );
  c.eventEmitter.emit(CACHE_EVENTS.GRAPHQL_LOADED);
  logger.log('Init event emitted: GRAPHQL_LOADED');

  await Promise.all([
    runInitStep('sessionCleanupService.init', () =>
      c.sessionCleanupService?.init?.(),
    ),
    runInitStep('userRevocationService.init', () =>
      c.userRevocationService?.init?.(),
    ),
    runInitStep('apiTokenService.init', () => c.apiTokenService?.init?.()),
    runInitStep('oauthExchangeCodeService.init', () =>
      c.oauthExchangeCodeService?.init?.(),
    ),
    runInitStep('mongoPhysicalMigrationService.init', () =>
      c.mongoPhysicalMigrationService?.init?.(),
    ),
  ]);
  await runInitStep('runtimeNamespaceLifecycleService.renewReadyNamespace', () =>
    c.runtimeNamespaceLifecycleService?.renewCurrentNamespaceKeys?.(),
  );

  c.eventEmitter.emit(CACHE_EVENTS.SYSTEM_READY);
  logger.log('Init event emitted: SYSTEM_READY');
}

export async function shutdown(
  container: AwilixContainer<Cradle>,
): Promise<void> {
  await container.cradle.flowExecutionQueueService?.onDestroy?.();
  await container.cradle.queryBuilderService?.flushBatchInserts?.();
  await container.dispose();
}
