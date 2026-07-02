import { describe, expect, it, vi } from 'vitest';
import { EventEmitter2 } from 'eventemitter2';
import { CacheOrchestratorService } from '../../src/engines/cache';

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
    guardCacheService: cacheMock() as any,
    flowCacheService: cacheMock() as any,
    websocketCacheService: cacheMock() as any,
    packageCacheService: cacheMock() as any,
    settingCacheService: cacheMock() as any,
    storageConfigCacheService: cacheMock() as any,
    oauthConfigCacheService: cacheMock() as any,
    folderTreeCacheService: cacheMock() as any,
    fieldPermissionCacheService: cacheMock() as any,
    columnRuleCacheService: cacheMock() as any,
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
});
