import { describe, expect, it, vi } from 'vitest';
import { EventEmitter2 } from 'eventemitter2';
import { CacheOrchestratorService } from '../../src/engines/cache/services/cache-orchestrator.service';
import { CACHE_ORCHESTRATOR_DEFAULTS } from '../../src/engines/cache/config/cache-orchestrator.config';

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
  const deps: Record<string, any> = {
    redisPubSubService: { publish: async () => undefined },
    instanceService: { getInstanceId: () => 'audit-instance' },
    eventEmitter: new EventEmitter2(),
    metadataCacheService: cacheMock(),
    routeCacheService: cacheMock(),
    guardCacheBuilder: cacheMock(),
    flowCacheBuilder: cacheMock(),
    websocketCacheBuilder: cacheMock(),
    packageCacheService: cacheMock(),
    sharedHelperCacheService: cacheMock(),
    settingCacheService: cacheMock(),
    storageConfigCacheBuilder: cacheMock(),
    oauthConfigCacheBuilder: cacheMock(),
    corsOriginCacheBuilder: cacheMock(),
    folderTreeCacheService: cacheMock(),
    fieldPermissionCacheBuilder: cacheMock(),
    columnRuleCacheBuilder: cacheMock(),
    gqlDefinitionCacheService: cacheMock(),
    repoRegistryService: {
      stageFromMetadata: async () => new Map(),
      activateAliases: () => undefined,
    },
    graphqlService: { reloadSchema: async () => undefined },
    dynamicWebSocketGateway: { emitToNamespace: () => undefined },
    ...overrides,
  };
  return new CacheOrchestratorService(deps as any);
}

describe('shared runtime cache generation reconcile', () => {
  it('replays committed snapshots when the shared generation advanced without a signal', async () => {
    let generation: string | null = 'g1';
    const orchestrator = createOrchestrator({
      redisRuntimeCacheStore: {
        isEnabled: () => true,
        readCommittedGeneration: async () => generation,
        runWithPublication: async (work: () => Promise<void>) => work(),
        commitPublication: async () => undefined,
        runWithCommittedSnapshots: async (work: () => Promise<void>) => work(),
      },
    });
    const replay = vi
      .spyOn(orchestrator as any, 'reloadAllLocal')
      .mockResolvedValue(undefined);

    try {
      await (orchestrator as any).reconcileSharedGeneration();
      expect(replay).not.toHaveBeenCalled();

      generation = 'g2';
      await (orchestrator as any).reconcileSharedGeneration();
      expect(replay).toHaveBeenCalledWith(false, true);

      await (orchestrator as any).reconcileSharedGeneration();
      expect(replay).toHaveBeenCalledTimes(1);
    } finally {
      orchestrator.onDestroy();
    }
  });

  it('keeps the applied generation when the replay fails so the next tick retries', async () => {
    let generation: string | null = 'g1';
    const orchestrator = createOrchestrator({
      redisRuntimeCacheStore: {
        isEnabled: () => true,
        readCommittedGeneration: async () => generation,
        runWithPublication: async (work: () => Promise<void>) => work(),
        commitPublication: async () => undefined,
        runWithCommittedSnapshots: async (work: () => Promise<void>) => work(),
      },
    });
    const replay = vi
      .spyOn(orchestrator as any, 'reloadAllLocal')
      .mockRejectedValueOnce(new Error('transient'));

    try {
      await (orchestrator as any).reconcileSharedGeneration();
      generation = 'g2';
      await (orchestrator as any).reconcileSharedGeneration();
      expect(replay).toHaveBeenCalledTimes(1);

      (replay as any).mockResolvedValue(undefined);
      await (orchestrator as any).reconcileSharedGeneration();
      expect(replay).toHaveBeenCalledTimes(2);
    } finally {
      orchestrator.onDestroy();
    }
  });

  it('does not reconcile when the shared runtime cache is disabled', async () => {
    const readCommittedGeneration = vi.fn(async () => 'g2');
    const orchestrator = createOrchestrator({
      redisRuntimeCacheStore: {
        isEnabled: () => false,
        readCommittedGeneration,
        runWithPublication: async (work: () => Promise<void>) => work(),
        commitPublication: async () => undefined,
      },
    });
    const replay = vi
      .spyOn(orchestrator as any, 'reloadAllLocal')
      .mockResolvedValue(undefined);

    try {
      await (orchestrator as any).reconcileSharedGeneration();
      expect(readCommittedGeneration).not.toHaveBeenCalled();
      expect(replay).not.toHaveBeenCalled();
    } finally {
      orchestrator.onDestroy();
    }
  });

  it('keeps a positive reconcile interval', () => {
    expect(CACHE_ORCHESTRATOR_DEFAULTS.sharedGenerationReconcileMs).toBeGreaterThan(0);
    expect(CACHE_ORCHESTRATOR_DEFAULTS.signalRetryBudgetMs).toBeGreaterThan(
      CACHE_ORCHESTRATOR_DEFAULTS.signalRetryMaxDelayMs,
    );
  });
});
