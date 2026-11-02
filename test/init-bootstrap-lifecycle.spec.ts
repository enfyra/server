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
      runtimeLogWriterService: {
        start: vi.fn(),
        assertReady: vi.fn(async () => {}),
      },
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
        renewCurrentNamespaceKeys: record(
          'runtimeNamespaceLifecycleService.renewCurrentNamespaceKeys',
        ),
      },
      mongoSagaCoordinator: { init: record('mongoSagaCoordinator.init') },
      provisionService: {
        waitForDatabase: record('provisionService.waitForDatabase'),
        recoverJournals: record('provisionService.recoverJournals'),
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
    runtimeReloadAuditService: {
      markInterruptedReloadsFailed: vi.fn(async () => undefined),
    },
    cacheOrchestratorService: { init: vi.fn(async () => undefined) },
    redisRuntimeCacheStore: {
      runWithPublication: async (work: () => Promise<void>) => work(),
      commitPublication: async () => {},
    },
    runtimeRegistryService: {
      init: vi.fn(async () => undefined),
      publishFromCache: vi.fn(async () => undefined),
    },
    metadataCacheService: { reload: vi.fn(async () => undefined) },
    sharedHelperCacheService: cacheService(),
    corsOriginCacheBuilder: cacheService(),
    fileCleanupService: { init: vi.fn(async () => undefined) },
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
    sqlFunctionService: { installExtensions: vi.fn(async () => undefined) },
    flowRuntimeService: { init: vi.fn(async () => undefined) },
    flowTriggerDispatcherService: { init: vi.fn(async () => undefined) },
    graphqlService: { reloadSchema: vi.fn(async () => undefined) },
    sessionCleanupService: { init: vi.fn(async () => undefined) },
    userRevocationService: { init: vi.fn(async () => undefined) },
    patVerifierService: { init: vi.fn(async () => undefined) },
    oauthExchangeCodeService: { init: vi.fn(async () => undefined) },
    mongoPhysicalMigrationService: { init: vi.fn(async () => undefined) },
    runtimeSchemaJournalService: {
      completeRecoveredActivations: vi.fn(async () => undefined),
    },
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
      'firstRunInitializer.isNeeded',
      'firstRunInitializer.run',
      'runtimeNamespaceLifecycleService.renewCurrentNamespaceKeys',
    ]);
    expect(container.cradle.cacheOrchestratorService.init).toHaveBeenCalled();
    expect(container.cradle.metadataCacheService.reload).toHaveBeenCalled();
  });
});

it('does not publish readiness until installed package artifacts are prepared', async () => {
  const { container } = runtimeContainer();
  let release!: () => void;
  const blocked = new Promise<void>((resolve) => {
    release = resolve;
  });
  let entered = false;
  container.cradle.packageRuntimeService.init.mockImplementation(async () => {
    entered = true;
    await blocked;
  });
  let ready = false;
  container.cradle.eventEmitter.emit.mockImplementation((event: string) => {
    if (event === 'system:ready') ready = true;
  });
  const initialization = init(container);
  try {
    await vi.waitFor(() => {
      expect(entered).toBe(true);
    });
    expect(container.cradle.packageCacheService.reload).not.toHaveBeenCalled();
    expect(ready).toBe(false);
  } finally {
    release();
    await initialization;
  }
  expect(ready).toBe(true);
});

it('rejects boot before cache publication when package preparation fails', async () => {
  const { container } = runtimeContainer();
  const failure = new Error('installed package unavailable');
  container.cradle.packageRuntimeService.init.mockRejectedValue(failure);
  await expect(init(container)).rejects.toBe(failure);
  expect(container.cradle.packageCacheService.reload).not.toHaveBeenCalled();
  expect(
    container.cradle.runtimeRegistryService.publishFromCache,
  ).toHaveBeenCalledTimes(1);
  expect(
    container.cradle.runtimeNamespaceLifecycleService.renewCurrentNamespaceKeys,
  ).not.toHaveBeenCalled();
});

it('activates flows only after runtime snapshots and SQL functions are ready', async () => {
  const { container } = runtimeContainer();
  let snapshots = 0;
  let sqlReady = false;
  container.cradle.runtimeRegistryService.publishFromCache.mockImplementation(
    async () => {
      snapshots++;
    },
  );
  container.cradle.sqlFunctionService.installExtensions.mockImplementation(
    async () => {
      sqlReady = true;
    },
  );
  container.cradle.flowRuntimeService.init.mockImplementation(async () => {
    expect(snapshots).toBe(16);
    expect(sqlReady).toBe(true);
  });
  await init(container);
  expect(container.cradle.flowRuntimeService.init).toHaveBeenCalledTimes(1);
});
