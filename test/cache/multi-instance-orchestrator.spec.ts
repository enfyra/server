describe('CacheOrchestrator — multi-instance Redis sync', () => {
  const RELOAD_CHAINS: Record<string, string[]> = {
    table_definition: ['metadata', 'repoRegistry', 'route', 'graphql', 'fieldPermission'],
    column_definition: ['metadata', 'repoRegistry', 'route', 'graphql', 'fieldPermission'],
    route_definition: ['route', 'graphql', 'guard'],
    guard_definition: ['guard'],
    package_definition: ['package'],
    setting_definition: ['setting', 'settingGraphql'],
  };

  type Signal = { instanceId: string; payload: any };

  function createInstance(id: string, redisChannel: Signal[]) {
    const reloaded: string[] = [];
    const wsEvents: any[] = [];

    const stepMap: Record<string, () => Promise<void>> = {
      metadata: async () => { reloaded.push('metadata'); },
      repoRegistry: async () => { reloaded.push('repoRegistry'); },
      route: async () => { reloaded.push('route'); },
      graphql: async () => { reloaded.push('graphql'); },
      guard: async () => { reloaded.push('guard'); },
      fieldPermission: async () => { reloaded.push('fieldPermission'); },
      package: async () => { reloaded.push('package'); },
      setting: async () => { reloaded.push('setting'); },
      settingGraphql: async () => { reloaded.push('settingGraphql'); },
      flow: async () => { reloaded.push('flow'); },
      websocket: async () => { reloaded.push('websocket'); },
      storage: async () => { reloaded.push('storage'); },
      oauth: async () => { reloaded.push('oauth'); },
      folder: async () => { reloaded.push('folder'); },
      bootstrap: async () => { reloaded.push('bootstrap'); },
    };

    function notifyClients(status: string) {
      wsEvents.push({ status });
    }

    async function executeChain(payload: any) {
      const chain = RELOAD_CHAINS[payload.tableName];
      if (!chain) return;
      if (chain.includes('metadata')) notifyClients('pending');

      if (chain.includes('metadata')) await stepMap['metadata']();
      const middle = chain.filter(s => s !== 'metadata' && s !== 'graphql');
      await Promise.all(middle.map(s => stepMap[s]?.() || Promise.resolve()));
      if (chain.includes('graphql')) await stepMap['graphql']();

      if (chain.includes('metadata')) notifyClients('done');
    }

    async function reloadAllLocal() {
      notifyClients('pending');
      reloaded.push('metadata');
      const allCaches = [
        'repoRegistry', 'route', 'guard', 'flow', 'websocket',
        'package', 'setting', 'storage', 'oauth', 'folder', 'fieldPermission',
      ];
      for (const c of allCaches) reloaded.push(c);
      reloaded.push('graphql');
      notifyClients('done');
    }

    async function publishSignal(payload: any) {
      redisChannel.push({ instanceId: id, payload });
    }

    async function receiveSignal(signal: Signal) {
      if (signal.instanceId === id) return;
      if (signal.payload.tableName === '__admin_reload_all') {
        await reloadAllLocal();
      } else {
        await executeChain(signal.payload);
      }
    }

    return {
      id,
      reloaded,
      wsEvents,
      executeChain,
      reloadAllLocal,
      publishSignal,
      receiveSignal,

      async handleInvalidation(payload: any) {
        await executeChain(payload);
        await publishSignal(payload);
      },

      async reloadAll() {
        await publishSignal({ tableName: '__admin_reload_all', scope: 'full' });
        await reloadAllLocal();
      },

      async reloadMetadataAndDeps() {
        notifyClients('pending');
        reloaded.push('metadata', 'repoRegistry', 'route', 'graphql');
        notifyClients('done');
        await publishSignal({ tableName: 'table_definition', scope: 'full' });
      },

      async reloadRoutesOnly() {
        notifyClients('pending');
        reloaded.push('route');
        notifyClients('done');
        await publishSignal({ tableName: 'route_definition', scope: 'full' });
      },

      async reloadGuardsOnly() {
        reloaded.push('guard');
        await publishSignal({ tableName: 'guard_definition', scope: 'full' });
      },
    };
  }

  function deliverSignals(redisChannel: Signal[], instances: ReturnType<typeof createInstance>[]) {
    return Promise.all(
      redisChannel.map(signal =>
        Promise.all(instances.map(inst => inst.receiveSignal(signal))),
      ),
    );
  }

  describe('create table → both instances reload surgical', () => {
    it('instance A creates table → instance B reloads same chain', async () => {
      const redis: Signal[] = [];
      const A = createInstance('A', redis);
      const B = createInstance('B', redis);

      await A.handleInvalidation({
        tableName: 'table_definition',
        scope: 'partial',
        ids: [99],
      });

      await deliverSignals(redis, [A, B]);

      expect(A.reloaded).toEqual(['metadata', 'repoRegistry', 'route', 'fieldPermission', 'graphql']);
      expect(B.reloaded).toEqual(['metadata', 'repoRegistry', 'route', 'fieldPermission', 'graphql']);
      expect(A.wsEvents).toEqual([{ status: 'pending' }, { status: 'done' }]);
      expect(B.wsEvents).toEqual([{ status: 'pending' }, { status: 'done' }]);
    });
  });

  describe('admin reload all → all instances reload ALL caches', () => {
    it('instance A calls reloadAll → instance B receives __admin_reload_all → reloadAllLocal', async () => {
      const redis: Signal[] = [];
      const A = createInstance('A', redis);
      const B = createInstance('B', redis);

      await A.reloadAll();
      await deliverSignals(redis, [A, B]);

      expect(A.reloaded).toContain('metadata');
      expect(A.reloaded).toContain('route');
      expect(A.reloaded).toContain('guard');
      expect(A.reloaded).toContain('flow');
      expect(A.reloaded).toContain('fieldPermission');
      expect(A.reloaded).toContain('graphql');

      expect(B.reloaded).toContain('metadata');
      expect(B.reloaded).toContain('route');
      expect(B.reloaded).toContain('guard');
      expect(B.reloaded).toContain('flow');
      expect(B.reloaded).toContain('fieldPermission');
      expect(B.reloaded).toContain('graphql');

      expect(A.wsEvents).toEqual([{ status: 'pending' }, { status: 'done' }]);
      expect(B.wsEvents).toEqual([{ status: 'pending' }, { status: 'done' }]);
    });
  });

  describe('admin reload metadata → remote gets table_definition chain', () => {
    it('instance A reloads metadata → instance B runs full table_definition chain', async () => {
      const redis: Signal[] = [];
      const A = createInstance('A', redis);
      const B = createInstance('B', redis);

      await A.reloadMetadataAndDeps();
      await deliverSignals(redis, [A, B]);

      expect(A.reloaded).toEqual(['metadata', 'repoRegistry', 'route', 'graphql']);
      expect(B.reloaded).toEqual(['metadata', 'repoRegistry', 'route', 'fieldPermission', 'graphql']);
    });
  });

  describe('admin reload routes → remote gets route_definition chain (includes guard)', () => {
    it('instance A reloads routes → instance B also reloads route + guard', async () => {
      const redis: Signal[] = [];
      const A = createInstance('A', redis);
      const B = createInstance('B', redis);

      await A.reloadRoutesOnly();
      await deliverSignals(redis, [A, B]);

      expect(A.reloaded).toEqual(['route']);
      expect(B.reloaded).toContain('route');
      expect(B.reloaded).toContain('guard');
      expect(B.reloaded).toContain('graphql');
    });
  });

  describe('self-echo filter', () => {
    it('instance A should NOT process its own signal', async () => {
      const redis: Signal[] = [];
      const A = createInstance('A', redis);

      await A.handleInvalidation({
        tableName: 'guard_definition',
        scope: 'full',
      });

      await deliverSignals(redis, [A]);

      expect(A.reloaded).toEqual(['guard']);
    });
  });

  describe('package install → multi-instance via INVALIDATE', () => {
    it('package install on A → B reloads package cache', async () => {
      const redis: Signal[] = [];
      const A = createInstance('A', redis);
      const B = createInstance('B', redis);

      await A.handleInvalidation({
        tableName: 'package_definition',
        scope: 'full',
      });

      await deliverSignals(redis, [A, B]);

      expect(A.reloaded).toEqual(['package']);
      expect(B.reloaded).toEqual(['package']);
    });
  });

  describe('setting change → settingGraphql, not full graphql rebuild', () => {
    it('setting change triggers setting + settingGraphql, not full graphql', async () => {
      const redis: Signal[] = [];
      const A = createInstance('A', redis);
      const B = createInstance('B', redis);

      await A.handleInvalidation({
        tableName: 'setting_definition',
        scope: 'full',
      });

      await deliverSignals(redis, [A, B]);

      expect(A.reloaded).toEqual(['setting', 'settingGraphql']);
      expect(A.reloaded).not.toContain('graphql');
      expect(B.reloaded).toEqual(['setting', 'settingGraphql']);
    });
  });

  describe('3 instances — signal propagates to all', () => {
    it('A creates table → B and C both reload', async () => {
      const redis: Signal[] = [];
      const A = createInstance('A', redis);
      const B = createInstance('B', redis);
      const C = createInstance('C', redis);

      await A.handleInvalidation({
        tableName: 'column_definition',
        scope: 'partial',
        ids: [5],
      });

      await deliverSignals(redis, [A, B, C]);

      for (const inst of [A, B, C]) {
        expect(inst.reloaded).toContain('metadata');
        expect(inst.reloaded).toContain('route');
        expect(inst.reloaded).toContain('fieldPermission');
      }
    });
  });
});
