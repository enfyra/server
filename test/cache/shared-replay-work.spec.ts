import { EventEmitter2 } from 'eventemitter2';
import { expect, it, vi } from 'vitest';
import { CacheOrchestratorService } from '../../src/engines/cache/services/cache-orchestrator.service';

it('replays every changed committed cache, not just the signal origin, and skips unchanged artifacts', async () => {
  const hydrated: string[] = [];
  const revisions = new Map<string, string>();
  const builder = (name: string) => ({
    syncFromSharedCache: async () => {
      hydrated.push(name);
    },
    isLoaded: () => true,
  });
  const deps: any = {
    eventEmitter: new EventEmitter2(),
    instanceService: { getInstanceId: () => 'test' },
    redisPubSubService: {},
    graphqlService: { reloadSchema: vi.fn(async () => {}) },
    repoRegistryService: {
      stageFromMetadata: async () => new Map(),
      activateAliases: () => {},
    },
    redisRuntimeCacheStore: {
      isEnabled: () => true,
      runWithCommittedSnapshots: async (work: () => Promise<void>) => work(),
      commitPublication: async () => {},
      getCommittedSnapshotRevision: (id: string) =>
        revisions.get(id) ?? 'initial',
    },
  };
  for (const [key, name] of Object.entries({
    metadataCacheService: 'metadata',
    sharedHelperCacheService: 'helper',
    routeCacheService: 'route',
    guardCacheBuilder: 'guard',
    flowCacheBuilder: 'flow',
    websocketCacheBuilder: 'websocket',
    packageCacheService: 'package',
    settingCacheService: 'setting',
    storageConfigCacheBuilder: 'storage',
    oauthConfigCacheBuilder: 'oauth',
    corsOriginCacheBuilder: 'cors',
    folderTreeCacheService: 'folder',
    fieldPermissionCacheBuilder: 'fieldPermission',
    columnRuleCacheBuilder: 'columnRule',
    gqlDefinitionCacheService: 'graphql',
  }))
    deps[key] = builder(name);
  const orchestrator = new CacheOrchestratorService(deps);
  try {
    await (orchestrator as any).reloadAllLocal(false, true);
    expect(hydrated).toHaveLength(15);
    hydrated.length = 0;
    deps.graphqlService.reloadSchema.mockClear();
    revisions.set('route', 'route-changed');
    revisions.set('flow', 'missed-earlier-flow-signal');
    await (orchestrator as any).reloadAllLocal(false, true);
    expect(hydrated.sort()).toEqual(['flow', 'route']);
    expect(deps.graphqlService.reloadSchema).not.toHaveBeenCalled();
    hydrated.length = 0;
    await (orchestrator as any).reloadAllLocal(false, true);
    expect(hydrated).toEqual([]);
    revisions.set('graphql', 'new-graphql');
    deps.graphqlService.reloadSchema.mockRejectedValueOnce(
      new Error('artifact failed'),
    );
    await expect(
      (orchestrator as any).reloadAllLocal(false, true),
    ).rejects.toThrow('artifact failed');
    hydrated.length = 0;
    await (orchestrator as any).reloadAllLocal(false, true);
    expect(hydrated).toEqual(['graphql']);
  } finally {
    orchestrator.onDestroy();
  }
});
