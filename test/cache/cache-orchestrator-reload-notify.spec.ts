import { EventEmitter2 } from 'eventemitter2';
import { CacheOrchestratorService } from 'src/engines/cache';

function cacheMock(overrides: Record<string, any> = {}) {
  return {
    reload: async () => undefined,
    isLoaded: () => true,
    supportsPartialReload: () => false,
    ...overrides,
  };
}

describe('CacheOrchestratorService reload notifications', () => {
  it('emits done after package reload failure so admin reload UI cannot hang', async () => {
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
      packageCacheService: cacheMock({
        reload: async () => {
          throw new Error('package reload failed');
        },
      }) as any,
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
    });

    await expect(
      (orchestrator as any).executeChain(
        {
          table: 'package_definition',
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
        data: { flow: 'package', status: 'pending', steps: ['package'] },
      },
      {
        event: '$system:reload',
        data: { flow: 'package', status: 'done', steps: ['package'] },
      },
    ]);
  });
});
