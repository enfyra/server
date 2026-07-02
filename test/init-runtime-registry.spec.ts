import { EventEmitter2 } from 'eventemitter2';
import { describe, expect, it, vi } from 'vitest';
import { init } from '../src/init';
import { RuntimeRegistryService } from '../src/engines/cache';
import { CACHE_IDENTIFIERS } from '../src/shared/utils/cache-events.constants';

function cacheService(data: unknown) {
  return {
    reload: vi.fn(async () => undefined),
    getCacheAsync: vi.fn(async () => data),
  };
}

describe('init runtime registry publish', () => {
  it('publishes initial metadata through getMetadata during boot', async () => {
    const eventEmitter = new EventEmitter2();
    const runtimeRegistryService = new RuntimeRegistryService({ eventEmitter });
    const metadata = {
      tables: new Map([['enfyra_user', { name: 'enfyra_user' }]]),
      tablesList: [{ name: 'enfyra_user' }],
      version: 1,
      timestamp: new Date('2026-07-02T00:00:00.000Z'),
    };
    const metadataCacheService = {
      reload: vi.fn(async () => undefined),
      getMetadata: vi.fn(async () => metadata),
    };

    await init({
      cradle: {
        eventEmitter,
        databaseConfigService: { getDbType: () => 'mongodb' },
        mongoService: { init: vi.fn(async () => undefined) },
        replicationManager: { init: vi.fn(async () => undefined) },
        knexService: { init: vi.fn(async () => undefined) },
        sqlPoolClusterCoordinatorService: {
          init: vi.fn(async () => undefined),
        },
        redisPubSubService: { init: vi.fn(async () => undefined) },
        mongoSagaCoordinator: { init: vi.fn(async () => undefined) },
        provisionService: {
          waitForDatabase: vi.fn(async () => undefined),
          recoverJournals: vi.fn(async () => undefined),
        },
        firstRunInitializer: {
          isNeeded: vi.fn(async () => false),
          run: vi.fn(async () => undefined),
        },
        cacheOrchestratorService: { init: vi.fn(async () => undefined) },
        runtimeRegistryService,
        metadataCacheService,
        repoRegistryService: {
          rebuildFromMetadata: vi.fn(async () => undefined),
        },
        websocketRuntimeService: { init: vi.fn(() => undefined) },
        packageRuntimeService: { init: vi.fn(() => undefined) },
        routeCacheService: cacheService([]),
        fieldPermissionCacheService: cacheService([]),
        columnRuleCacheService: cacheService([]),
        settingCacheService: cacheService([]),
        storageConfigCacheService: cacheService([]),
        oauthConfigCacheService: cacheService([]),
        websocketCacheService: cacheService([]),
        flowCacheService: cacheService([]),
        packageCacheService: cacheService([]),
        folderTreeCacheService: cacheService([]),
        guardCacheService: cacheService([]),
        gqlDefinitionCacheService: cacheService([]),
        bootstrapScriptService: {
          onMetadataLoaded: vi.fn(async () => undefined),
        },
        sqlFunctionService: {
          installExtensions: vi.fn(async () => undefined),
        },
        flowRuntimeService: { init: vi.fn(async () => undefined) },
        graphqlService: { reloadSchema: vi.fn(async () => undefined) },
        sessionCleanupService: { init: vi.fn(async () => undefined) },
        userRevocationService: { init: vi.fn(async () => undefined) },
        apiTokenService: { init: vi.fn(async () => undefined) },
        oauthExchangeCodeService: { init: vi.fn(async () => undefined) },
        mongoPhysicalMigrationService: { init: vi.fn(async () => undefined) },
      },
    } as any);

    expect(metadataCacheService.getMetadata).toHaveBeenCalled();
    expect(
      runtimeRegistryService.getSnapshot(CACHE_IDENTIFIERS.METADATA)?.data,
    ).toBe(metadata);
  });
});
