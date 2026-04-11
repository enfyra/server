/**
 * Tests for CacheOrchestratorService:
 * - reloadAll: reloads ALL caches + publishes Redis signal
 * - Granular reload methods (reloadMetadataAndDeps, reloadRoutesOnly, etc.)
 */
describe('CacheOrchestratorService — reloadAll & granular reloads', () => {
  let orchestrator: any;
  let mocks: Record<string, any>;
  let publishedSignals: any[];

  beforeEach(() => {
    publishedSignals = [];

    const createCacheMock = (name: string) => ({
      reload: jest.fn().mockResolvedValue(undefined),
      partialReload: jest.fn().mockResolvedValue(undefined),
      isLoaded: jest.fn().mockReturnValue(true),
      supportsPartialReload: jest.fn().mockReturnValue(false),
      _name: name,
    });

    mocks = {
      metadataCache: createCacheMock('metadata'),
      routeCache: {
        ...createCacheMock('route'),
        supportsPartialReload: jest.fn().mockReturnValue(true),
      },
      guardCache: createCacheMock('guard'),
      flowCache: createCacheMock('flow'),
      websocketCache: createCacheMock('websocket'),
      packageCache: createCacheMock('package'),
      settingCache: createCacheMock('setting'),
      storageCache: createCacheMock('storage'),
      oauthCache: createCacheMock('oauth'),
      folderCache: createCacheMock('folder'),
      fieldPermissionCache: createCacheMock('fieldPermission'),
      repoRegistry: { rebuildFromMetadata: jest.fn() },
      graphqlService: { reloadSchema: jest.fn().mockResolvedValue(undefined) },
      redisPubSubService: {
        publish: jest.fn(async (_ch: string, msg: string) => {
          publishedSignals.push(JSON.parse(msg));
        }),
        subscribeWithHandler: jest.fn(),
        isChannelForBase: jest.fn().mockReturnValue(true),
      },
      instanceService: {
        getInstanceId: jest.fn().mockReturnValue('test-instance-001'),
      },
      eventEmitter: { emit: jest.fn() },
    };

    orchestrator = {
      metadataCache: mocks.metadataCache,
      routeCache: mocks.routeCache,
      guardCache: mocks.guardCache,
      flowCache: mocks.flowCache,
      websocketCache: mocks.websocketCache,
      packageCache: mocks.packageCache,
      settingCache: mocks.settingCache,
      storageCache: mocks.storageCache,
      oauthCache: mocks.oauthCache,
      folderCache: mocks.folderCache,
      fieldPermissionCache: mocks.fieldPermissionCache,
      repoRegistry: mocks.repoRegistry,
      graphqlService: mocks.graphqlService,
      redisPubSubService: mocks.redisPubSubService,
      instanceService: mocks.instanceService,

      async reloadAll() {
        await this.metadataCache.reload();
        await Promise.all([
          this.repoRegistry.rebuildFromMetadata(this.metadataCache),
          this.routeCache.reload(false),
          this.guardCache.reload(false),
          this.flowCache.reload(false),
          this.websocketCache.reload(false),
          this.packageCache.reload(false),
          this.settingCache.reload(false),
          this.storageCache.reload(false),
          this.oauthCache.reload(false),
          this.folderCache.reload(false),
          this.fieldPermissionCache.reload(false),
        ]);
        if (this.graphqlService) {
          await this.graphqlService.reloadSchema();
        }
        await this.redisPubSubService.publish(
          'enfyra:cache-orchestrator-sync',
          JSON.stringify({
            instanceId: this.instanceService.getInstanceId(),
            type: 'RELOAD_SIGNAL',
            timestamp: Date.now(),
            payload: {
              tableName: 'table_definition',
              action: 'reload',
              scope: 'full',
              timestamp: Date.now(),
            },
          }),
        );
      },

      async reloadMetadataAndDeps() {
        await this.metadataCache.reload();
        this.repoRegistry.rebuildFromMetadata(this.metadataCache);
        await this.routeCache.reload(false);
        if (this.graphqlService) {
          await this.graphqlService.reloadSchema();
        }
      },

      async reloadRoutesOnly() {
        await this.routeCache.reload(false);
      },

      async reloadGraphqlOnly() {
        if (this.graphqlService) {
          await this.graphqlService.reloadSchema();
        }
      },

      async reloadGuardsOnly() {
        await this.guardCache.reload(false);
      },
    };
  });

  describe('reloadAll', () => {
    it('should reload ALL caches, not just a subset', async () => {
      await orchestrator.reloadAll();

      expect(mocks.metadataCache.reload).toHaveBeenCalled();
      expect(mocks.routeCache.reload).toHaveBeenCalled();
      expect(mocks.guardCache.reload).toHaveBeenCalled();
      expect(mocks.flowCache.reload).toHaveBeenCalled();
      expect(mocks.websocketCache.reload).toHaveBeenCalled();
      expect(mocks.packageCache.reload).toHaveBeenCalled();
      expect(mocks.settingCache.reload).toHaveBeenCalled();
      expect(mocks.storageCache.reload).toHaveBeenCalled();
      expect(mocks.oauthCache.reload).toHaveBeenCalled();
      expect(mocks.folderCache.reload).toHaveBeenCalled();
      expect(mocks.fieldPermissionCache.reload).toHaveBeenCalled();
      expect(mocks.repoRegistry.rebuildFromMetadata).toHaveBeenCalled();
      expect(mocks.graphqlService.reloadSchema).toHaveBeenCalled();
    });

    it('should publish Redis signal for multi-instance sync', async () => {
      await orchestrator.reloadAll();

      expect(mocks.redisPubSubService.publish).toHaveBeenCalledTimes(1);
      expect(publishedSignals).toHaveLength(1);
      expect(publishedSignals[0].instanceId).toBe('test-instance-001');
      expect(publishedSignals[0].type).toBe('RELOAD_SIGNAL');
      expect(publishedSignals[0].payload.scope).toBe('full');
    });

    it('should reload metadata BEFORE other caches (sequential → parallel)', async () => {
      const callOrder: string[] = [];
      mocks.metadataCache.reload.mockImplementation(async () => {
        callOrder.push('metadata');
      });
      mocks.routeCache.reload.mockImplementation(async () => {
        callOrder.push('route');
      });
      mocks.guardCache.reload.mockImplementation(async () => {
        callOrder.push('guard');
      });

      await orchestrator.reloadAll();

      expect(callOrder[0]).toBe('metadata');
      expect(callOrder.indexOf('metadata')).toBeLessThan(
        callOrder.indexOf('route'),
      );
    });
  });

  describe('reloadMetadataAndDeps', () => {
    it('should reload metadata, repoRegistry, routes, and graphql', async () => {
      await orchestrator.reloadMetadataAndDeps();

      expect(mocks.metadataCache.reload).toHaveBeenCalled();
      expect(mocks.repoRegistry.rebuildFromMetadata).toHaveBeenCalled();
      expect(mocks.routeCache.reload).toHaveBeenCalled();
      expect(mocks.graphqlService.reloadSchema).toHaveBeenCalled();
    });

    it('should NOT reload unrelated caches', async () => {
      await orchestrator.reloadMetadataAndDeps();

      expect(mocks.guardCache.reload).not.toHaveBeenCalled();
      expect(mocks.flowCache.reload).not.toHaveBeenCalled();
      expect(mocks.settingCache.reload).not.toHaveBeenCalled();
    });
  });

  describe('reloadRoutesOnly', () => {
    it('should only reload route cache', async () => {
      await orchestrator.reloadRoutesOnly();

      expect(mocks.routeCache.reload).toHaveBeenCalled();
      expect(mocks.metadataCache.reload).not.toHaveBeenCalled();
      expect(mocks.graphqlService.reloadSchema).not.toHaveBeenCalled();
    });
  });

  describe('reloadGraphqlOnly', () => {
    it('should only reload graphql schema', async () => {
      await orchestrator.reloadGraphqlOnly();

      expect(mocks.graphqlService.reloadSchema).toHaveBeenCalled();
      expect(mocks.routeCache.reload).not.toHaveBeenCalled();
      expect(mocks.metadataCache.reload).not.toHaveBeenCalled();
    });

    it('should not throw when graphqlService is null', async () => {
      orchestrator.graphqlService = null;
      await expect(orchestrator.reloadGraphqlOnly()).resolves.not.toThrow();
    });
  });

  describe('reloadGuardsOnly', () => {
    it('should only reload guard cache', async () => {
      await orchestrator.reloadGuardsOnly();

      expect(mocks.guardCache.reload).toHaveBeenCalled();
      expect(mocks.routeCache.reload).not.toHaveBeenCalled();
      expect(mocks.metadataCache.reload).not.toHaveBeenCalled();
    });
  });
});
