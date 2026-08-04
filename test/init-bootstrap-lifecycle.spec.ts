import { describe, expect, it, vi } from 'vitest';
import { init, initBootstrap } from '../src/init';

function bootstrapContainer(overrides: Record<string, unknown> = {}) {
  const calls: string[] = [];
  const record = (name: string) =>
    vi.fn(async () => {
      calls.push(name);
    });
  const container = {
    cradle: {
      databaseConfigService: { getDbType: () => 'mongodb' },
      mongoService: { init: record('mongoService.init') },
      replicationManager: { init: record('replicationManager.init') },
      knexService: { init: record('knexService.init') },
      sqlPoolClusterCoordinatorService: {
        init: record('sqlPoolClusterCoordinatorService.init'),
      },
      redisPubSubService: { init: record('redisPubSubService.init') },
      runtimeNamespaceLifecycleService: {
        init: record('runtimeNamespaceLifecycleService.init'),
      },
      mongoSagaCoordinator: { init: record('mongoSagaCoordinator.init') },
      provisionService: {
        waitForDatabase: record('provisionService.waitForDatabase'),
        recoverJournals: record('provisionService.recoverJournals'),
      },
      legacyStoreInventoryService: {
        inventory: record('legacyStoreInventoryService.inventory'),
      },
      legacyAssessmentService: {
        assess: vi.fn(() => {
          calls.push('legacyAssessmentService.assess');
          return {
            backend: 'mongodb',
            findings: [],
            hasBlockingFindings: false,
            assessedAt: new Date().toISOString(),
          };
        }),
      },
      firstRunInitializer: {
        isNeeded: vi.fn(async () => {
          calls.push('firstRunInitializer.isNeeded');
          return true;
        }),
        run: record('firstRunInitializer.run'),
      },
      ...overrides,
    },
  };
  return { container: container as any, calls };
}

function runtimeContainer(bootstrapOverrides: Record<string, unknown> = {}) {
  const { container, calls } = bootstrapContainer(bootstrapOverrides);
  const runtime: Record<string, unknown> = {
    eventEmitter: { emit: vi.fn() },
    cacheOrchestratorService: { init: vi.fn(async () => undefined) },
    runtimeRegistryService: { init: vi.fn(async () => undefined) },
    metadataCacheService: { reload: vi.fn(async () => undefined) },
    repoRegistryService: {
      rebuildFromMetadata: vi.fn(async () => undefined),
    },
    websocketRuntimeService: { init: vi.fn(() => undefined) },
    packageRuntimeService: { init: vi.fn(() => undefined) },
    routeCacheService: cacheService(),
    fieldPermissionCacheBuilder: cacheService(),
    columnRuleCacheBuilder: cacheService(),
    settingCacheService: cacheService(),
    storageConfigCacheBuilder: cacheService(),
    oauthConfigCacheBuilder: cacheService(),
    websocketCacheBuilder: cacheService(),
    flowCacheBuilder: cacheService(),
    packageCacheService: cacheService(),
    folderTreeCacheService: cacheService(),
    guardCacheBuilder: cacheService(),
    gqlDefinitionCacheService: cacheService(),
    bootstrapScriptService: {
      onMetadataLoaded: vi.fn(async () => undefined),
    },
    sqlFunctionService: { installExtensions: vi.fn(async () => undefined) },
    flowRuntimeService: { init: vi.fn(async () => undefined) },
    flowTriggerDispatcherService: { init: vi.fn(async () => undefined) },
    graphqlService: { reloadSchema: vi.fn(async () => undefined) },
    sessionCleanupService: { init: vi.fn(async () => undefined) },
    userRevocationService: { init: vi.fn(async () => undefined) },
    patVerifierService: { init: vi.fn(async () => undefined) },
    oauthExchangeCodeService: { init: vi.fn(async () => undefined) },
    mongoPhysicalMigrationService: { init: vi.fn(async () => undefined) },
  };
  Object.assign(container.cradle, runtime);
  return { container, calls };
}

function cacheService() {
  return {
    reload: vi.fn(async () => undefined),
    getCacheAsync: vi.fn(async () => undefined),
  };
}

describe('initBootstrap lifecycle', () => {
  it('runs bootstrap steps in exact order', async () => {
    const { container, calls } = bootstrapContainer();

    await initBootstrap(container);

    expect(calls).toEqual([
      'mongoService.init',
      'replicationManager.init',
      'knexService.init',
      'sqlPoolClusterCoordinatorService.init',
      'redisPubSubService.init',
      'runtimeNamespaceLifecycleService.init',
      'mongoSagaCoordinator.init',
      'provisionService.waitForDatabase',
      'provisionService.recoverJournals',
      'legacyStoreInventoryService.inventory',
      'legacyAssessmentService.assess',
      'firstRunInitializer.isNeeded',
      'firstRunInitializer.run',
    ]);
  });

  it('skips firstRunInitializer.run when isNeeded is false', async () => {
    const { container } = bootstrapContainer({
      firstRunInitializer: {
        isNeeded: vi.fn(async () => false),
        run: vi.fn(async () => undefined),
      },
    });

    await initBootstrap(container);

    expect(container.cradle.firstRunInitializer.run).not.toHaveBeenCalled();
  });

  it('aborts before firstRun when legacy assessment has blocking findings', async () => {
    const { container } = bootstrapContainer({
      legacyAssessmentService: {
        assess: vi.fn(() => ({
          backend: 'mongodb',
          findings: [
            {
              coreKey: 'legacy.users',
              outcome: 'blocked',
              detail: 'legacy table conflicts',
              blocking: true,
            },
          ],
          hasBlockingFindings: true,
          assessedAt: new Date().toISOString(),
        })),
      },
    });

    await expect(initBootstrap(container)).rejects.toThrow(
      'Legacy system metadata assessment found blocking issues',
    );
    expect(
      container.cradle.firstRunInitializer.isNeeded,
    ).not.toHaveBeenCalled();
    expect(container.cradle.firstRunInitializer.run).not.toHaveBeenCalled();
  });

  it('propagates a bootstrap step failure', async () => {
    const failure = new Error('bootstrap failed');
    const { container } = bootstrapContainer({
      provisionService: {
        waitForDatabase: vi.fn(async () => {
          throw failure;
        }),
        recoverJournals: vi.fn(async () => undefined),
      },
    });

    await expect(initBootstrap(container)).rejects.toBe(failure);
  });

  it('init awaits bootstrap before running runtime steps', async () => {
    const { container, calls } = runtimeContainer();

    await init(container);

    expect(calls).toEqual([
      'mongoService.init',
      'replicationManager.init',
      'knexService.init',
      'sqlPoolClusterCoordinatorService.init',
      'redisPubSubService.init',
      'runtimeNamespaceLifecycleService.init',
      'mongoSagaCoordinator.init',
      'provisionService.waitForDatabase',
      'provisionService.recoverJournals',
      'legacyStoreInventoryService.inventory',
      'legacyAssessmentService.assess',
      'firstRunInitializer.isNeeded',
      'firstRunInitializer.run',
    ]);
    expect(container.cradle.cacheOrchestratorService.init).toHaveBeenCalled();
    expect(container.cradle.metadataCacheService.reload).toHaveBeenCalled();
  });
});
