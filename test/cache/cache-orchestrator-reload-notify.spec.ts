import { describe, expect, it, vi } from 'vitest';
import { EventEmitter2 } from 'eventemitter2';
import {
  CacheOrchestratorService,
  RuntimeRegistryService,
} from '../../src/engines/cache';
import { CACHE_IDENTIFIERS } from '../../src/shared/utils/cache-events.constants';
import { RepoRegistryService } from '../../src/engines/cache/services/repo-registry.service';
import { GuardCacheBuilder } from '../../src/engines/cache/services/guard-cache-builder.service';
import { GuardEvaluatorService } from '../../src/engines/cache/services/guard-evaluator.service';

function cacheMock(overrides: Record<string, any> = {}) {
  return {
    reload: async () => undefined,
    isLoaded: () => true,
    supportsPartialReload: () => false,
    syncFromSharedCache: async () => undefined,
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
    sharedHelperCacheService: cacheMock() as any,
    settingCacheService: cacheMock() as any,
    authHeaderCacheBuilder: cacheMock() as any,
    storageConfigCacheBuilder: cacheMock() as any,
    oauthConfigCacheBuilder: cacheMock() as any,
    corsOriginCacheBuilder: cacheMock() as any,
    folderTreeCacheService: cacheMock() as any,
    fieldPermissionCacheBuilder: cacheMock() as any,
    columnRuleCacheBuilder: cacheMock() as any,
    gqlDefinitionCacheService: cacheMock() as any,
    repoRegistryService: { stageFromMetadata: async () => new Map(), activateAliases: () => undefined } as any,
    graphqlService: { reloadSchema: async () => undefined } as any,
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
  it('merges full invalidation without undefined optional fields', () => {
    const { orchestrator } = createOrchestrator();
    const internals = orchestrator as any;
    try {
      internals.mergePayload({ table: 'enfyra_route', action: 'reload', scope: 'partial', ids: [1], timestamp: 1 });
      internals.mergePayload({ table: 'enfyra_route', action: 'reload', scope: 'full', timestamp: 2 });
      expect(internals.pendingPayloads.get('enfyra_route')).toStrictEqual({ table: 'enfyra_route', action: 'reload', scope: 'full', timestamp: 2 });
      internals.mergePayload({ table: 'enfyra_route', action: 'reload', scope: 'partial', ids: [2], critical: true, timestamp: 3 });
      expect(internals.pendingPayloads.get('enfyra_route')).toStrictEqual({ table: 'enfyra_route', action: 'reload', scope: 'full', critical: true, timestamp: 3 });
    } finally { orchestrator.onDestroy(); }
  });

  it('omits absent tracking metadata and retains failure messages', () => {
    const recordCacheReload = vi.fn();
    const { orchestrator } = createOrchestrator({ runtimeMetricsCollectorService: { recordCacheReload } });
    try {
      const tracker = (orchestrator as any).createReloadTracker('route', 'enfyra_route', 'full');
      tracker.finish(null);
      expect(recordCacheReload).toHaveBeenLastCalledWith(expect.objectContaining({ status: 'success' }));
      const success = recordCacheReload.mock.calls[0]?.[0];
      expect(success).not.toHaveProperty('error');
      expect(success).not.toHaveProperty('reloadId');
      tracker.finish(new Error('failed'));
      expect(recordCacheReload).toHaveBeenLastCalledWith(expect.objectContaining({ status: 'failed', error: 'failed' }));
    } finally { orchestrator.onDestroy(); }
  });

  it('runs bounded steps once and settles sibling work before rejecting', async () => {
    const { orchestrator } = createOrchestrator();
    const started: number[] = [];
    const completed: number[] = [];
    let active = 0;
    let peak = 0;
    try {
      await expect((orchestrator as any).runBoundedReloadSteps([1, 2, 3, 4], 2, async (item: number) => {
        started.push(item);
        active++;
        peak = Math.max(peak, active);
        await new Promise<void>((resolve) => setImmediate(resolve));
        active--;
        if (item === 1) throw new Error('step failed');
        completed.push(item);
      })).rejects.toThrow('step failed');
      expect(peak).toBe(2);
      expect(active).toBe(0);
      expect(started).toEqual([1, 2, 3, 4]);
      expect(completed).toEqual([2, 3, 4]);
    } finally { orchestrator.onDestroy(); }
  });

  it('keeps route and GraphQL guards attached after a table rename', async () => {
    let name = 'old_items';
    const runtimeRegistryService = new RuntimeRegistryService();
    const guardCacheBuilder = new GuardCacheBuilder({ eventEmitter: new EventEmitter2(), queryBuilderService: { isMongoDb: () => false, find: async ({ table }: any) => ({ data: table === 'enfyra_guard' ? [{ id: 1, name: 'route', position: 'pre_auth', isEnabled: true, route: { id: 1, path: `/${name}` } }, { id: 2, name: 'gql', position: 'pre_auth', isEnabled: true, type: 'graphql', gqlOperation: 'QUERY', table: { id: 1, name } }] : [1, 2].map((guardId) => ({ id: guardId, guard: { id: guardId }, type: 'ip_whitelist', config: { ips: ['127.0.0.1'] }, isEnabled: true })) }) } } as any);
    await guardCacheBuilder.reload(false);
    await runtimeRegistryService.publishFromCache(CACHE_IDENTIFIERS.GUARD, guardCacheBuilder);
    const { orchestrator } = createOrchestrator({ metadataCacheService: cacheMock({ partialReload: async () => {} }), guardCacheBuilder, runtimeRegistryService: { publishFromCache: async (id: string, builder: any) => { if (id === CACHE_IDENTIFIERS.GUARD) await runtimeRegistryService.publishFromCache(id, builder); } } });
    try {
      name = 'new_items';
      await (orchestrator as any).executeChain({ table: 'enfyra_table', action: 'reload', scope: 'partial', ids: [1], tableRenames: [{ id: 1, oldName: 'old_items', newName: name }], timestamp: Date.now() }, true);
      expect(runtimeRegistryService.getGuardsForRoute('pre_auth', '/new_items', 'GET').map(guard => guard.id)).toEqual([1]);
      expect(runtimeRegistryService.getGuardsForGraphql('pre_auth', 'new_items', 'QUERY').map(guard => guard.id)).toEqual([2]);
      expect(runtimeRegistryService.getGuardsForRoute('pre_auth', '/old_items', 'GET')).toEqual([]);
      const evaluator = new GuardEvaluatorService({ rateLimitService: {} as any });
      const routeGuard = runtimeRegistryService.getGuardsForRoute('pre_auth', '/new_items', 'GET')[0];
      const graphqlGuard = runtimeRegistryService.getGuardsForGraphql('pre_auth', 'new_items', 'QUERY')[0];
      for (const guard of [routeGuard, graphqlGuard]) {
        expect((await evaluator.evaluateGuard(guard, { clientIp: '203.0.113.5', routePath: '/new_items', tableName: 'new_items', operation: 'QUERY' })).reject).toMatchObject({ statusCode: 403, errorCode: 'IP_NOT_ALLOWED' });
        expect((await evaluator.evaluateGuard(guard, { clientIp: '127.0.0.1', routePath: '/new_items', tableName: 'new_items', operation: 'QUERY' })).reject).toBeNull();
      }
    } finally { orchestrator.onDestroy(); }
  });

  it('does not expose candidate repository aliases when a sibling reload fails', async () => {
    let tables = [{ name: 'items', alias: 'oldAlias' }];
    const metadataCacheService = cacheMock({ getAllTablesMetadata: async () => tables });
    const repoRegistryService = new RepoRegistryService({ metadataCacheService, eventEmitter: new EventEmitter2(), dynamicRepositoryFactory: {} } as any);
    await repoRegistryService.rebuildFromMetadata();
    const { orchestrator } = createOrchestrator({ metadataCacheService, repoRegistryService, routeCacheService: cacheMock({ reload: async () => { await new Promise(resolve => setImmediate(resolve)); throw new Error('route failed'); } }) });
    try {
      tables = [{ name: 'items', alias: 'newAlias' }];
      await expect(orchestrator.reloadMetadataAndDeps()).rejects.toThrow('route failed');
      expect(repoRegistryService.resolveTableName('oldAlias')).toBe('items');
      expect(repoRegistryService.resolveTableName('newAlias')).toBeNull();
    } finally { orchestrator.onDestroy(); }
  });

  it.each([false, true])('activates repository aliases only after a successful shared commit (commit fails=%s)', async (failCommit) => {
    let tables = [{ name: 'items', alias: 'oldAlias' }];
    const metadataCacheService = cacheMock({ getAllTablesMetadata: async () => tables });
    const repoRegistryService = new RepoRegistryService({ metadataCacheService, eventEmitter: new EventEmitter2(), dynamicRepositoryFactory: {} } as any);
    await repoRegistryService.rebuildFromMetadata();
    const { orchestrator } = createOrchestrator({ metadataCacheService, repoRegistryService, redisRuntimeCacheStore: {
      isEnabled: () => true,
      runWithPublication: async (work: () => Promise<void>) => work(),
      commitPublication: async () => {
        expect(repoRegistryService.resolveTableName('oldAlias')).toBe('items');
        expect(repoRegistryService.resolveTableName('newAlias')).toBeNull();
        if (failCommit) throw new Error('commit failed');
      },
    } });
    try {
      tables = [{ name: 'items', alias: 'newAlias' }];
      const reload = orchestrator.reloadMetadataAndDeps();
      if (failCommit) await expect(reload).rejects.toThrow('commit failed');
      else await reload;
      expect(repoRegistryService.resolveTableName('oldAlias')).toBe(failCommit ? 'items' : null);
      expect(repoRegistryService.resolveTableName('newAlias')).toBe(failCommit ? null : 'items');
    } finally { orchestrator.onDestroy(); }
  });

  it.each(['mutation', 'admin', 'all'])('notifies peers after committed snapshots even when GraphQL artifacts fail (%s)', async mode => {
    let committed = false;
    const publish = vi.fn(async () => { expect(committed).toBe(true); });
    const { orchestrator } = createOrchestrator({ redisRuntimeCacheStore: { isEnabled: () => true, runWithPublication: async (work: () => Promise<void>) => work(), commitPublication: async () => { committed = true; } }, redisPubSubService: { publish }, graphqlService: { reloadSchema: async () => { throw new Error('artifact failed'); } } });
    try {
      const reload = mode === 'admin' ? orchestrator.reloadGraphqlOnly() : mode === 'all' ? orchestrator.reloadAll() : (orchestrator as any).executeChain({ table: 'enfyra_graphql', action: 'reload', scope: 'full', timestamp: Date.now() }, true);
      await expect(reload).rejects.toThrow('artifact failed');
      expect(publish).toHaveBeenCalledOnce();
    } finally { orchestrator.onDestroy(); }
  });

  it('pins a shared replay only after earlier local reloads release the queue', async () => {
    let release!: () => void;
    const blocked = new Promise<void>(resolve => { release = resolve; });
    let generation = 1;
    const pinned: number[] = [];
    const { orchestrator } = createOrchestrator({
      redisRuntimeCacheStore: {
        isEnabled: () => true,
        commitPublication: async () => {},
        runWithPublication: async (work: () => Promise<void>) => work(),
        runWithCommittedSnapshots: async (work: () => Promise<void>) => {
          pinned.push(generation);
          return work();
        },
      },
    });
    const first = (orchestrator as any).runWithReloadLock(async () => {
      await blocked;
      generation = 2;
    });
    await new Promise(resolve => setImmediate(resolve));
    const replay = (orchestrator as any).reloadAllLocal(false, true);
    release();
    await first;
    await replay;
    expect(pinned).toEqual([2]);
  });

  it('retains client-only menu notifications on shared snapshot replay', async () => {
    const { orchestrator, emitted } = createOrchestrator({
      redisPubSubService: {
        isChannelForBase: () => true,
        subscribeWithHandler: () => {},
      },
      redisRuntimeCacheStore: { isEnabled: () => true },
    });
    const hydrate = vi.spyOn(orchestrator as any, 'reloadAllLocal').mockResolvedValue(undefined);
    (orchestrator as any).subscribeToRedis();
    await (orchestrator as any).messageHandler('test', JSON.stringify({
      instanceId: 'peer',
      timestamp: 1,
      payload: { table: 'enfyra_menu', scope: 'full' },
    }));
    expect(hydrate).toHaveBeenCalledWith(false, true);
    expect(emitted).toEqual([expect.objectContaining({
      data: expect.objectContaining({ status: 'done', steps: ['menu', 'extension'] }),
    })]);
  });

  it('keeps independent invalidation origins and completes both chains', async () => {
    const settingReload = vi.fn(async () => undefined);
    const authReload = vi.fn(async () => undefined);
    const { orchestrator } = createOrchestrator({ settingCacheService: cacheMock({ reload: settingReload }), authHeaderCacheBuilder: cacheMock({ reload: authReload }) });
    try {
      await Promise.all(['enfyra_setting', 'enfyra_auth_header'].map(table => (orchestrator as any).handleInvalidation({ table, scope: 'full', action: 'update', timestamp: Date.now(), critical: true })));
      expect(settingReload).toHaveBeenCalledOnce();
      expect(authReload).toHaveBeenCalledOnce();
    } finally { orchestrator.onDestroy(); }
  });

  it.each([false, true])('activates every rebuilt full-reload cache (shared replay=%s)', async sharedReplay => {
    const published: string[] = [];
    const { orchestrator } = createOrchestrator({
      runtimeRegistryService: { publishFromCache: async (name: string) => { published.push(name); } },
      metadataCacheService: cacheMock({ syncFromSharedCache: async () => {} }),
      sharedHelperCacheService: cacheMock({ syncFromSharedCache: async () => {} }),
    });
    const builders = ['routeCacheService', 'guardCacheBuilder', 'flowCacheBuilder', 'websocketCacheBuilder', 'packageCacheService', 'settingCacheService', 'authHeaderCacheBuilder', 'storageConfigCacheBuilder', 'oauthConfigCacheBuilder', 'corsOriginCacheBuilder', 'folderTreeCacheService', 'fieldPermissionCacheBuilder', 'columnRuleCacheBuilder', 'gqlDefinitionCacheService'];
    for (const name of builders) (orchestrator as any)[name].syncFromSharedCache = async () => {};
    await (orchestrator as any).reloadAllLocal(false, sharedReplay);
    expect(published).toContain(CACHE_IDENTIFIERS.AUTH_HEADER);
    expect(published).toContain(CACHE_IDENTIFIERS.CORS_ORIGIN);
  });

  it('does not let one failed origin acknowledge or discard a different origin', async () => {
    const authReload = vi.fn(async () => {});
    const { orchestrator } = createOrchestrator({ settingCacheService: cacheMock({ reload: async () => { throw new Error('setting failed'); } }), authHeaderCacheBuilder: cacheMock({ reload: authReload }) });
    try {
      const results = await Promise.allSettled(['enfyra_setting', 'enfyra_auth_header'].map(table => (orchestrator as any).handleInvalidation({ table, scope: 'full', action: 'update', timestamp: Date.now(), critical: true })));
      expect(results.map(result => result.status)).toEqual(['rejected', 'fulfilled']);
      expect(authReload).toHaveBeenCalledOnce();
    } finally { orchestrator.onDestroy(); }
  });

  it('keeps invalidation fire-and-forget when cache activation fails', async () => {
    const { orchestrator, emitted } = createOrchestrator({
      packageCacheService: cacheMock({
        reload: async () => {
          throw new Error('package reload failed');
        },
      }) as any,
    });

    await expect(
      (orchestrator as any).handleInvalidation({
        table: 'enfyra_package',
        action: 'reload',
        scope: 'full',
        timestamp: Date.now(),
      }),
    ).resolves.toBeUndefined();

    expect(emitted.map(({ data }) => data.status)).toEqual([
      'pending',
      'failed',
    ]);
  });

  it('emits failed after an admin full reload fails', async () => {
    const { orchestrator, emitted } = createOrchestrator({
      metadataCacheService: cacheMock({
        reload: async () => {
          throw new Error('metadata reload failed');
        },
      }) as any,
    });

    await expect(
      (orchestrator as any).reloadAllLocal(true, false),
    ).rejects.toThrow('metadata reload failed');

    expect(emitted.map(({ data }) => data.status)).toEqual([
      'pending',
      'failed',
    ]);
  });

  it('emits failed after package reload failure so admin reload UI can surface it', async () => {
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
          status: 'failed',
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
        steps: ['metadata', 'repoRegistry', 'route', 'guard', 'graphql'],
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

  it('records failed cache reload metrics and notifies clients for granular admin reloads', async () => {
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
          status: 'failed',
          steps: ['route'],
          instanceId: 'test-instance',
          reloadId: expect.any(String),
        }),
      },
    ]);
  });

  it.each([false, true])(
    'loads metadata and helpers first, then bounds full reload builders with shared replay %s',
    async (sharedReplay) => {
      let active = 0;
      let maxActive = 0;
      const started: string[] = [];
      const completed: string[] = [];
      const track = async (name: string) => {
        if (!name.startsWith('metadata') && !name.startsWith('helper')) {
          expect(completed.slice(0, 2)).toEqual(
            sharedReplay
              ? ['metadata:shared', 'helper:shared']
              : ['metadata', 'helper'],
          );
        }
        started.push(name);
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
        metadataCacheService: trackedCache('metadata') as any,
        sharedHelperCacheService: trackedCache('helper') as any,
        routeCacheService: trackedCache('route') as any,
        guardCacheBuilder: trackedCache('guard') as any,
        flowCacheBuilder: trackedCache('flow') as any,
        websocketCacheBuilder: trackedCache('websocket') as any,
        packageCacheService: trackedCache('package') as any,
        settingCacheService: trackedCache('setting') as any,
        authHeaderCacheBuilder: trackedCache('authHeader') as any,
        storageConfigCacheBuilder: trackedCache('storage') as any,
        oauthConfigCacheBuilder: trackedCache('oauth') as any,
        corsOriginCacheBuilder: trackedCache('corsOrigin') as any,
        folderTreeCacheService: trackedCache('folder') as any,
        fieldPermissionCacheBuilder: trackedCache('fieldPermission') as any,
        columnRuleCacheBuilder: trackedCache('columnRule') as any,
        gqlDefinitionCacheService: trackedCache('graphql') as any,
        repoRegistryService: {
          stageFromMetadata: async () => { await track('repoRegistry'); return new Map(); },
          activateAliases: () => undefined,
        } as any,
      });

      await (orchestrator as any).reloadAllLocal(true, sharedReplay);

      expect(started.slice(0, 2)).toEqual(
        sharedReplay
          ? ['metadata:shared', 'helper:shared']
          : ['metadata', 'helper'],
      );
      expect(maxActive).toBeGreaterThan(1);
      expect(maxActive).toBeLessThanOrEqual(3);
      expect(completed.sort()).toEqual(
        [
          'metadata',
          'helper',
          'authHeader',
          'columnRule',
          'corsOrigin',
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
        ]
          .map((name) =>
            sharedReplay && name !== 'repoRegistry' ? `${name}:shared` : name,
          )
          .sort(),
      );
    },
  );

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
