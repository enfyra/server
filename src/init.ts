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
    runInitStep('fieldPermissionCacheService.reload', () =>
      c.fieldPermissionCacheService?.reload?.(),
    ),
    runInitStep('columnRuleCacheService.reload', () =>
      c.columnRuleCacheService?.reload?.(),
    ),
    runInitStep('settingCacheService.reload', () =>
      c.settingCacheService?.reload?.(),
    ),
    runInitStep('storageConfigCacheService.reload', () =>
      c.storageConfigCacheService?.reload?.(),
    ),
    runInitStep('oauthConfigCacheService.reload', () =>
      c.oauthConfigCacheService?.reload?.(),
    ),
    runInitStep('websocketCacheService.reload', () =>
      c.websocketCacheService?.reload?.(),
    ),
    runInitStep('flowCacheService.reload', () =>
      c.flowCacheService?.reload?.(),
    ),
    runInitStep('packageCacheService.reload', () =>
      c.packageCacheService?.reload?.(),
    ),
    runInitStep('folderTreeCacheService.reload', () =>
      c.folderTreeCacheService?.reload?.(),
    ),
    runInitStep('guardCacheService.reload', () =>
      c.guardCacheService?.reload?.(),
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
      [CACHE_IDENTIFIERS.FIELD_PERMISSION, c.fieldPermissionCacheService],
      [CACHE_IDENTIFIERS.COLUMN_RULE, c.columnRuleCacheService],
      [CACHE_IDENTIFIERS.SETTING, c.settingCacheService],
      [CACHE_IDENTIFIERS.STORAGE, c.storageConfigCacheService],
      [CACHE_IDENTIFIERS.OAUTH_CONFIG, c.oauthConfigCacheService],
      [CACHE_IDENTIFIERS.WEBSOCKET, c.websocketCacheService],
      [CACHE_IDENTIFIERS.FLOW, c.flowCacheService],
      [CACHE_IDENTIFIERS.PACKAGE, c.packageCacheService],
      [CACHE_IDENTIFIERS.FOLDER_TREE, c.folderTreeCacheService],
      [CACHE_IDENTIFIERS.GUARD, c.guardCacheService],
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
