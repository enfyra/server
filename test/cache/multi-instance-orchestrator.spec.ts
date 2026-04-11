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
    };

    function notifyClients(status: string) {
      wsEvents.push({ status });
    }

    async function executeChain(payload: any, publish: boolean) {
      const chain = RELOAD_CHAINS[payload.tableName];
      if (!chain) return;

      if (publish && chain.includes('metadata')) notifyClients('pending');

      if (chain.includes('metadata')) await stepMap['metadata']();
      const middle = chain.filter(s => s !== 'metadata' && s !== 'graphql');
      await Promise.all(middle.map(s => stepMap[s]?.() || Promise.resolve()));
      if (chain.includes('graphql')) await stepMap['graphql']();

      if (publish && chain.includes('metadata')) notifyClients('done');
    }

    async function reloadAllLocal(notify = false) {
      if (notify) notifyClients('pending');
      const allCaches = [
        'metadata', 'repoRegistry', 'route', 'guard', 'fieldPermission',
        'package', 'setting', 'graphql',
      ];
      for (const c of allCaches) reloaded.push(c);
      if (notify) notifyClients('done');
    }

    async function publishSignal(payload: any) {
      redisChannel.push({ instanceId: id, payload });
    }

    async function receiveSignal(signal: Signal) {
      if (signal.instanceId === id) return;
      if (signal.payload.tableName === '__admin_reload_all') {
        await reloadAllLocal(false);
      } else {
        await executeChain(signal.payload, false);
      }
    }

    return {
      id, reloaded, wsEvents,
      executeChain, reloadAllLocal, publishSignal, receiveSignal,

      async handleInvalidation(payload: any) {
        await executeChain(payload, true);
        await publishSignal(payload);
      },

      async reloadAll() {
        await publishSignal({ tableName: '__admin_reload_all', scope: 'full' });
        await reloadAllLocal(true);
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

  describe('create table → surgical reload on both instances', () => {
    it('A creates table → B reloads same chain, only A notifies clients', async () => {
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
      expect(B.wsEvents).toEqual([]);
    });
  });

  describe('admin reload all → no duplicate WS events', () => {
    it('A calls reloadAll → B reloads silently, only A notifies clients', async () => {
      const redis: Signal[] = [];
      const A = createInstance('A', redis);
      const B = createInstance('B', redis);

      await A.reloadAll();
      await deliverSignals(redis, [A, B]);

      expect(A.wsEvents).toEqual([{ status: 'pending' }, { status: 'done' }]);
      expect(B.wsEvents).toEqual([]);
      expect(B.reloaded.length).toBeGreaterThan(0);
    });
  });

  describe('race condition: no duplicate pending/done across instances', () => {
    it('client connected to A should receive exactly 1 pending + 1 done', async () => {
      const redis: Signal[] = [];
      const A = createInstance('A', redis);
      const B = createInstance('B', redis);

      await A.handleInvalidation({
        tableName: 'column_definition',
        scope: 'partial',
        ids: [5],
      });
      await deliverSignals(redis, [A, B]);

      const pendingCount = A.wsEvents.filter(e => e.status === 'pending').length;
      const doneCount = A.wsEvents.filter(e => e.status === 'done').length;
      expect(pendingCount).toBe(1);
      expect(doneCount).toBe(1);

      expect(B.wsEvents).toEqual([]);
    });
  });

  describe('self-echo filter', () => {
    it('instance should NOT process its own Redis signal', async () => {
      const redis: Signal[] = [];
      const A = createInstance('A', redis);

      await A.handleInvalidation({ tableName: 'guard_definition', scope: 'full' });
      await deliverSignals(redis, [A]);

      expect(A.reloaded).toEqual(['guard']);
    });
  });

  describe('3 instances — only originator notifies, all reload', () => {
    it('A creates table → B and C reload silently', async () => {
      const redis: Signal[] = [];
      const A = createInstance('A', redis);
      const B = createInstance('B', redis);
      const C = createInstance('C', redis);

      await A.handleInvalidation({
        tableName: 'table_definition',
        scope: 'partial',
        ids: [50],
      });
      await deliverSignals(redis, [A, B, C]);

      for (const inst of [A, B, C]) {
        expect(inst.reloaded).toContain('metadata');
        expect(inst.reloaded).toContain('route');
      }
      expect(A.wsEvents).toHaveLength(2);
      expect(B.wsEvents).toHaveLength(0);
      expect(C.wsEvents).toHaveLength(0);
    });
  });

  describe('package install → multi-instance', () => {
    it('package install on A → B reloads, no WS from B', async () => {
      const redis: Signal[] = [];
      const A = createInstance('A', redis);
      const B = createInstance('B', redis);

      await A.handleInvalidation({ tableName: 'package_definition', scope: 'full' });
      await deliverSignals(redis, [A, B]);

      expect(A.reloaded).toEqual(['package']);
      expect(B.reloaded).toEqual(['package']);
      expect(A.wsEvents).toEqual([]);
      expect(B.wsEvents).toEqual([]);
    });
  });

  describe('setting change → settingGraphql only', () => {
    it('both instances reload setting + settingGraphql, not full graphql', async () => {
      const redis: Signal[] = [];
      const A = createInstance('A', redis);
      const B = createInstance('B', redis);

      await A.handleInvalidation({ tableName: 'setting_definition', scope: 'full' });
      await deliverSignals(redis, [A, B]);

      expect(A.reloaded).toEqual(['setting', 'settingGraphql']);
      expect(B.reloaded).toEqual(['setting', 'settingGraphql']);
      expect(A.reloaded).not.toContain('graphql');
    });
  });

  describe('admin granular reloads → Redis propagation', () => {
    it('reloadMetadataAndDeps: A notifies + publishes, B runs chain silently', async () => {
      const redis: Signal[] = [];
      const A = createInstance('A', redis);
      const B = createInstance('B', redis);

      await A.reloadMetadataAndDeps();
      await deliverSignals(redis, [A, B]);

      expect(A.wsEvents).toEqual([{ status: 'pending' }, { status: 'done' }]);
      expect(B.wsEvents).toEqual([]);
      expect(B.reloaded).toContain('metadata');
    });

    it('reloadRoutesOnly: A notifies, B runs route+graphql+guard chain', async () => {
      const redis: Signal[] = [];
      const A = createInstance('A', redis);
      const B = createInstance('B', redis);

      await A.reloadRoutesOnly();
      await deliverSignals(redis, [A, B]);

      expect(A.wsEvents).toHaveLength(2);
      expect(B.wsEvents).toHaveLength(0);
      expect(B.reloaded).toContain('route');
      expect(B.reloaded).toContain('guard');
    });
  });
});
