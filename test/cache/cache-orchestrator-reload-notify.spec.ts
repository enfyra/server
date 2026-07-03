import { describe, expect, it, vi } from 'vitest';
import { EventEmitter2 } from 'eventemitter2';
import {
  CacheOrchestratorService,
  RuntimeRegistryService,
} from '../../src/engines/cache';
import { CACHE_IDENTIFIERS } from '../../src/shared/utils/cache-events.constants';

function cacheMock(overrides: Record<string, any> = {}) {
  return {
    reload: async () => undefined,
    isLoaded: () => true,
    supportsPartialReload: () => false,
    ...overrides,
  };
}

function createOrchestrator(overrides: Record<string, any> = {}) {
  const emitted: Array<{ event: string; data: any }> = [];
  const orchestrator = new CacheOrchestratorService({
    redisPubSubService: { publish: async () => undefined } as any,
    instanceService: { getInstanceId: () => 'test-instance' } as any,
    eventEmitter: new EventEmitter2(),
    metadataCacheService: cacheMock() as any,
    routeCacheService: cacheMock() as any,
    guardCacheBuilder: cacheMock() as any,
    flowCacheBuilder: cacheMock() as any,
    websocketCacheBuilder: cacheMock() as any,
    packageCacheService: cacheMock() as any,
    settingCacheService: cacheMock() as any,
    storageConfigCacheBuilder: cacheMock() as any,
    oauthConfigCacheBuilder: cacheMock() as any,
    folderTreeCacheService: cacheMock() as any,
    fieldPermissionCacheBuilder: cacheMock() as any,
    columnRuleCacheBuilder: cacheMock() as any,
    gqlDefinitionCacheService: cacheMock() as any,
    repoRegistryService: { rebuildFromMetadata: () => undefined } as any,
    graphqlService: { reloadSchema: async () => undefined } as any,
    bootstrapScriptService: {
      reloadBootstrapScripts: async () => undefined,
    } as any,
    dynamicWebSocketGateway: {
      emitToNamespace: (_path: string, event: string, data: any) => {
        emitted.push({ event, data });
      },
    } as any,
    ...overrides,
  });
  return { orchestrator, emitted };
}

describe('CacheOrchestratorService reload notifications', () => {
  it('emits done after package reload failure so admin reload UI cannot hang', async () => {
    const runtimeRegistryService = {
      publishFromCache: vi.fn(async () => undefined),
    };
    const { orchestrator, emitted } = createOrchestrator({
      packageCacheService: cacheMock({
        reload: async () => {
          throw new Error('package reload failed');
        },
      }) as any,
      runtimeRegistryService,
    });

    await expect(
      (orchestrator as any).executeChain(
        {
          table: 'enfyra_package',
          action: 'reload',
          scope: 'full',
          timestamp: Date.now(),
        },
        true,
      ),
    ).rejects.toThrow('package reload failed');

    expect(emitted).toEqual([
      {
        event: '$system:reload',
        data: expect.objectContaining({
          flow: 'package',
          status: 'pending',
          steps: ['package'],
          instanceId: 'test-instance',
          reloadId: expect.any(String),
        }),
      },
      {
        event: '$system:reload',
        data: expect.objectContaining({
          flow: 'package',
          status: 'done',
          steps: ['package'],
          instanceId: 'test-instance',
          reloadId: expect.any(String),
        }),
      },
    ]);
    expect(runtimeRegistryService.publishFromCache).not.toHaveBeenCalled();
  });

  it('publishes package cache to runtime registry after a successful chain', async () => {
    const runtimeRegistryService = {
      publishFromCache: vi.fn(async () => undefined),
    };
    const packageCacheService = cacheMock({
      getCacheAsync: vi.fn(async () => ['demo-package']),
    });
    const { orchestrator } = createOrchestrator({
      packageCacheService: packageCacheService as any,
      runtimeRegistryService,
    });

    await (orchestrator as any).executeChain(
      {
        table: 'enfyra_package',
        action: 'reload',
        scope: 'full',
        timestamp: Date.now(),
      },
      true,
    );

    expect(runtimeRegistryService.publishFromCache).toHaveBeenCalledWith(
      'package',
      packageCacheService,
    );
  });

  it('persists activated audit status after a successful reload transaction', async () => {
    const runtimeRegistryService = {
      publishFromCache: vi.fn(async () => undefined),
    };
    const runtimeReloadAuditService = {
      markBuilding: vi.fn(async () => true),
      markActivated: vi.fn(async () => undefined),
      markFailed: vi.fn(async () => undefined),
    };
    const packageCacheService = cacheMock({
      getCacheAsync: vi.fn(async () => ['demo-package']),
    });
    const { orchestrator } = createOrchestrator({
      packageCacheService: packageCacheService as any,
      runtimeRegistryService,
      runtimeReloadAuditService,
    });

    await (orchestrator as any).executeChain(
      {
        table: 'enfyra_package',
        action: 'reload',
        scope: 'full',
        timestamp: Date.now(),
      },
      true,
    );

    expect(runtimeReloadAuditService.markBuilding).toHaveBeenCalledWith(
      expect.objectContaining({
        flow: 'package',
        table: 'enfyra_package',
        scope: 'full',
        action: 'reload',
        chain: ['package'],
        instanceId: 'test-instance',
      }),
    );
    expect(runtimeReloadAuditService.markActivated).toHaveBeenCalledWith(
      expect.objectContaining({
        reloadId:
          runtimeReloadAuditService.markBuilding.mock.calls[0]?.[0].reloadId,
        steps: [
          expect.objectContaining({ name: 'package', status: 'success' }),
        ],
      }),
    );
    expect(runtimeReloadAuditService.markFailed).not.toHaveBeenCalled();
  });

  it('persists failed audit status when a reload transaction fails', async () => {
    const runtimeRegistryService = {
      publishFromCache: vi.fn(async () => undefined),
    };
    const runtimeReloadAuditService = {
      markBuilding: vi.fn(async () => true),
      markActivated: vi.fn(async () => undefined),
      markFailed: vi.fn(async () => undefined),
    };
    const { orchestrator } = createOrchestrator({
      packageCacheService: cacheMock({
        reload: async () => {
          throw new Error('package reload failed');
        },
      }) as any,
      runtimeRegistryService,
      runtimeReloadAuditService,
    });

    await expect(
      (orchestrator as any).executeChain(
        {
          table: 'enfyra_package',
          action: 'reload',
          scope: 'full',
          timestamp: Date.now(),
        },
        true,
      ),
    ).rejects.toThrow('package reload failed');

    expect(runtimeReloadAuditService.markActivated).not.toHaveBeenCalled();
    expect(runtimeReloadAuditService.markFailed).toHaveBeenCalledWith(
      expect.objectContaining({
        reloadId:
          runtimeReloadAuditService.markBuilding.mock.calls[0]?.[0].reloadId,
        error: 'package reload failed',
        steps: [expect.objectContaining({ name: 'package', status: 'failed' })],
      }),
    );
  });

  it('publishes graphql definition cache after graphql reload chain succeeds', async () => {
    const runtimeRegistryService = {
      publishFromCache: vi.fn(async () => undefined),
    };
    const gqlDefinitionCacheService = cacheMock({
      getCacheAsync: vi.fn(async () => new Map([['posts', { id: 1 }]])),
    });
    const { orchestrator } = createOrchestrator({
      gqlDefinitionCacheService: gqlDefinitionCacheService as any,
      runtimeRegistryService,
    });

    await (orchestrator as any).executeChain(
      {
        table: 'enfyra_graphql',
        action: 'reload',
        scope: 'full',
        timestamp: Date.now(),
      },
      true,
    );

    expect(runtimeRegistryService.publishFromCache).toHaveBeenCalledWith(
      'graphql',
      gqlDefinitionCacheService,
    );
  });

  it('reloads GraphQL runtime only after staged snapshots are activated', async () => {
    const runtimeRegistryService = new RuntimeRegistryService();
    const oldDefinitions = new Map([
      ['posts', { id: 1, isEnabled: true, tableName: 'posts' }],
    ]);
    const nextDefinitions = new Map([
      ['posts', { id: 2, isEnabled: true, tableName: 'posts' }],
    ]);
    await runtimeRegistryService.publishFromCache(CACHE_IDENTIFIERS.GRAPHQL, {
      getCacheAsync: vi.fn(async () => oldDefinitions),
    });

    const gqlDefinitionCacheService = cacheMock({
      reload: vi.fn(async () => undefined),
      getCacheAsync: vi.fn(async () => nextDefinitions),
    });
    const graphqlService = {
      reloadSchema: vi.fn(async () => {
        expect(
          runtimeRegistryService.getActiveData(CACHE_IDENTIFIERS.GRAPHQL),
        ).toEqual(nextDefinitions);
      }),
    };
    const { orchestrator } = createOrchestrator({
      gqlDefinitionCacheService: gqlDefinitionCacheService as any,
      graphqlService: graphqlService as any,
      runtimeRegistryService,
    });

    await (orchestrator as any).executeChain(
      {
        table: 'enfyra_graphql',
        action: 'reload',
        scope: 'full',
        timestamp: Date.now(),
      },
      true,
    );

    expect(
      runtimeRegistryService.getActiveData(CACHE_IDENTIFIERS.GRAPHQL),
    ).toEqual(nextDefinitions);
    expect(graphqlService.reloadSchema).toHaveBeenCalled();
  });

  it('keeps old active snapshots when staging cache snapshots fails', async () => {
    const runtimeRegistryService = new RuntimeRegistryService();
    const oldDefinitions = new Map([
      ['posts', { id: 1, isEnabled: true, tableName: 'posts' }],
    ]);
    await runtimeRegistryService.publishFromCache(CACHE_IDENTIFIERS.GRAPHQL, {
      getCacheAsync: vi.fn(async () => oldDefinitions),
    });

    const gqlDefinitionCacheService = cacheMock({
      reload: vi.fn(async () => undefined),
      getCacheAsync: vi.fn(async () => {
        throw new Error('snapshot stage failed');
      }),
    });
    const graphqlService = {
      reloadSchema: vi.fn(),
    };
    const { orchestrator } = createOrchestrator({
      gqlDefinitionCacheService: gqlDefinitionCacheService as any,
      graphqlService: graphqlService as any,
      runtimeRegistryService,
    });

    await expect(
      (orchestrator as any).executeChain(
        {
          table: 'enfyra_graphql',
          action: 'reload',
          scope: 'full',
          timestamp: Date.now(),
        },
        true,
      ),
    ).rejects.toThrow('snapshot stage failed');

    expect(
      runtimeRegistryService.getActiveData(CACHE_IDENTIFIERS.GRAPHQL),
    ).toEqual(oldDefinitions);
    expect(graphqlService.reloadSchema).not.toHaveBeenCalled();
  });

  it('keeps committed snapshots when GraphQL post-commit runtime reload fails', async () => {
    const runtimeRegistryService = new RuntimeRegistryService();
    const oldDefinitions = new Map([
      ['posts', { id: 1, isEnabled: true, tableName: 'posts' }],
    ]);
    const nextDefinitions = new Map([
      ['posts', { id: 2, isEnabled: true, tableName: 'posts' }],
    ]);
    await runtimeRegistryService.publishFromCache(CACHE_IDENTIFIERS.GRAPHQL, {
      getCacheAsync: vi.fn(async () => oldDefinitions),
    });

    const gqlDefinitionCacheService = cacheMock({
      reload: vi.fn(async () => undefined),
      getCacheAsync: vi.fn(async () => nextDefinitions),
    });
    const graphqlService = {
      reloadSchema: vi.fn(async () => {
        throw new Error('yoga reload failed');
      }),
    };
    const { orchestrator } = createOrchestrator({
      gqlDefinitionCacheService: gqlDefinitionCacheService as any,
      graphqlService: graphqlService as any,
      runtimeRegistryService,
    });

    await expect(
      (orchestrator as any).executeChain(
        {
          table: 'enfyra_graphql',
          action: 'reload',
          scope: 'full',
          timestamp: Date.now(),
        },
        true,
      ),
    ).rejects.toThrow('yoga reload failed');

    expect(
      runtimeRegistryService.getActiveData(CACHE_IDENTIFIERS.GRAPHQL),
    ).toEqual(nextDefinitions);
    expect(graphqlService.reloadSchema).toHaveBeenCalled();
  });

  it('runs settingGraphql only after setting reload publishes to runtime registry', async () => {
    let settingFinished = false;
    const events: string[] = [];
    const settingCacheService = cacheMock({
      reload: vi.fn(async () => {
        events.push('setting:start');
        await new Promise((resolve) => setTimeout(resolve, 10));
        settingFinished = true;
        events.push('setting:done');
      }),
    });
    const graphqlService = {
      onSettingChanged: vi.fn(async () => {
        events.push('settingGraphql');
        expect(settingFinished).toBe(true);
      }),
    };
    const runtimeRegistryService = {
      publishFromCache: vi.fn(async () => {
        events.push('publish');
      }),
    };
    const { orchestrator } = createOrchestrator({
      settingCacheService: settingCacheService as any,
      graphqlService: graphqlService as any,
      runtimeRegistryService,
    });

    await (orchestrator as any).executeChain(
      {
        table: 'enfyra_setting',
        action: 'reload',
        scope: 'full',
        timestamp: Date.now(),
      },
      true,
    );

    expect(events).toEqual([
      'setting:start',
      'setting:done',
      'publish',
      'settingGraphql',
    ]);
  });

  it('emits extension reload notifications for extension definition changes', async () => {
    const { orchestrator, emitted } = createOrchestrator();

    await (orchestrator as any).executeChain(
      {
        table: 'enfyra_extension',
        action: 'reload',
        scope: 'partial',
        ids: [8],
        timestamp: Date.now(),
      },
      true,
    );

    expect(emitted).toEqual([
      {
        event: '$system:reload',
        data: expect.objectContaining({
          flow: 'extension',
          status: 'pending',
          steps: ['extension'],
          instanceId: 'test-instance',
          reloadId: expect.any(String),
        }),
      },
      {
        event: '$system:reload',
        data: expect.objectContaining({
          flow: 'extension',
          status: 'done',
          steps: ['extension'],
          instanceId: 'test-instance',
          reloadId: expect.any(String),
        }),
      },
    ]);
  });

  it('emits menu and extension reload notifications for menu definition changes', async () => {
    const { orchestrator, emitted } = createOrchestrator();

    await (orchestrator as any).executeChain(
      {
        table: 'enfyra_menu',
        action: 'reload',
        scope: 'partial',
        ids: [12],
        timestamp: Date.now(),
      },
      true,
    );

    expect(emitted).toEqual([
      {
        event: '$system:reload',
        data: expect.objectContaining({
          flow: 'menu',
          status: 'pending',
          steps: ['menu', 'extension'],
          instanceId: 'test-instance',
          reloadId: expect.any(String),
        }),
      },
      {
        event: '$system:reload',
        data: expect.objectContaining({
          flow: 'menu',
          status: 'done',
          steps: ['menu', 'extension'],
          instanceId: 'test-instance',
          reloadId: expect.any(String),
        }),
      },
    ]);
  });

  it('records cache reload metrics for granular admin reloads', async () => {
    const cases = [
      {
        method: 'reloadMetadataAndDeps',
        flow: 'metadata',
        table: 'enfyra_table',
        steps: ['metadata', 'repoRegistry', 'route', 'graphql'],
      },
      {
        method: 'reloadRoutesOnly',
        flow: 'route',
        table: 'enfyra_route',
        steps: ['route'],
      },
      {
        method: 'reloadGraphqlOnly',
        flow: 'graphql',
        table: 'enfyra_graphql',
        steps: ['graphql'],
      },
      {
        method: 'reloadGuardsOnly',
        flow: 'guard',
        table: 'enfyra_guard',
        steps: ['guard'],
      },
    ] as const;

    for (const item of cases) {
      const runtimeMetricsCollectorService = {
        recordCacheReload: vi.fn(),
        runWithQueryContext: vi.fn(
          (_context: string, callback: () => Promise<void>) => callback(),
        ),
      };
      const { orchestrator } = createOrchestrator({
        runtimeMetricsCollectorService,
      });

      await orchestrator[item.method]();

      expect(
        runtimeMetricsCollectorService.runWithQueryContext,
      ).toHaveBeenCalledWith('cache', expect.any(Function));
      expect(
        runtimeMetricsCollectorService.recordCacheReload,
      ).toHaveBeenCalledTimes(1);
      const metric =
        runtimeMetricsCollectorService.recordCacheReload.mock.calls[0]?.[0];
      expect(metric).toEqual(
        expect.objectContaining({
          flow: item.flow,
          table: item.table,
          scope: 'full',
          status: 'success',
        }),
      );
      expect(metric.steps.map((step: any) => step.name)).toEqual(item.steps);
      expect(metric.steps.every((step: any) => step.status === 'success')).toBe(
        true,
      );
    }
  });

  it('records failed cache reload metrics for granular admin reloads', async () => {
    const runtimeMetricsCollectorService = {
      recordCacheReload: vi.fn(),
      runWithQueryContext: vi.fn(
        (_context: string, callback: () => Promise<void>) => callback(),
      ),
    };
    const { orchestrator, emitted } = createOrchestrator({
      runtimeMetricsCollectorService,
      routeCacheService: cacheMock({
        reload: async () => {
          throw new Error('route reload failed');
        },
      }) as any,
    });

    await expect(orchestrator.reloadRoutesOnly()).rejects.toThrow(
      'route reload failed',
    );

    const metric =
      runtimeMetricsCollectorService.recordCacheReload.mock.calls[0]?.[0];
    expect(metric).toEqual(
      expect.objectContaining({
        flow: 'route',
        table: 'enfyra_route',
        status: 'failed',
        error: 'route reload failed',
      }),
    );
    expect(metric.steps).toEqual([
      expect.objectContaining({
        name: 'route',
        status: 'failed',
        error: 'route reload failed',
      }),
    ]);
    expect(emitted).toEqual([
      {
        event: '$system:reload',
        data: expect.objectContaining({
          flow: 'route',
          status: 'pending',
          steps: ['route'],
          instanceId: 'test-instance',
          reloadId: expect.any(String),
        }),
      },
      {
        event: '$system:reload',
        data: expect.objectContaining({
          flow: 'route',
          status: 'done',
          steps: ['route'],
          instanceId: 'test-instance',
          reloadId: expect.any(String),
        }),
      },
    ]);
  });

  it('bounds concurrent full reload builder steps', async () => {
    let active = 0;
    let maxActive = 0;
    const completed: string[] = [];
    const track = async (name: string) => {
      active += 1;
      maxActive = Math.max(maxActive, active);
      await new Promise((resolve) => setTimeout(resolve, 5));
      completed.push(name);
      active -= 1;
    };
    const trackedCache = (name: string) =>
      cacheMock({
        reload: () => track(name),
        syncFromSharedCache: () => track(`${name}:shared`),
      });
    const { orchestrator } = createOrchestrator({
      routeCacheService: trackedCache('route') as any,
      guardCacheBuilder: trackedCache('guard') as any,
      flowCacheBuilder: trackedCache('flow') as any,
      websocketCacheBuilder: trackedCache('websocket') as any,
      packageCacheService: trackedCache('package') as any,
      settingCacheService: trackedCache('setting') as any,
      storageConfigCacheBuilder: trackedCache('storage') as any,
      oauthConfigCacheBuilder: trackedCache('oauth') as any,
      folderTreeCacheService: trackedCache('folder') as any,
      fieldPermissionCacheBuilder: trackedCache('fieldPermission') as any,
      columnRuleCacheBuilder: trackedCache('columnRule') as any,
      gqlDefinitionCacheService: trackedCache('graphql') as any,
      repoRegistryService: {
        rebuildFromMetadata: () => track('repoRegistry'),
      } as any,
    });

    await orchestrator.reloadAll();

    expect(maxActive).toBeGreaterThan(1);
    expect(maxActive).toBeLessThanOrEqual(3);
    expect(completed.sort()).toEqual(
      [
        'columnRule',
        'fieldPermission',
        'flow',
        'folder',
        'graphql',
        'guard',
        'oauth',
        'package',
        'repoRegistry',
        'route',
        'setting',
        'storage',
        'websocket',
      ].sort(),
    );
  });

  it('creates unique reload ids for same-flow reloads queued in one process', async () => {
    const { orchestrator, emitted } = createOrchestrator();

    await Promise.all([
      (orchestrator as any).executeChain(
        {
          table: 'enfyra_extension',
          action: 'reload',
          scope: 'partial',
          ids: [1],
          timestamp: Date.now(),
        },
        true,
      ),
      (orchestrator as any).executeChain(
        {
          table: 'enfyra_extension',
          action: 'reload',
          scope: 'partial',
          ids: [2],
          timestamp: Date.now(),
        },
        true,
      ),
    ]);

    const pendingIds = emitted
      .filter((event) => event.data.status === 'pending')
      .map((event) => event.data.reloadId);
    expect(pendingIds).toHaveLength(2);
    expect(new Set(pendingIds).size).toBe(2);
  });
});
