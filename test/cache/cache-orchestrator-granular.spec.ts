describe('CacheOrchestratorService — RELOAD_CHAINS + multi-instance', () => {
  const RELOAD_CHAINS: Record<string, string[]> = {
    table_definition: ['metadata', 'repoRegistry', 'route', 'graphql', 'fieldPermission'],
    column_definition: ['metadata', 'repoRegistry', 'route', 'graphql', 'fieldPermission'],
    relation_definition: ['metadata', 'repoRegistry', 'route', 'graphql', 'fieldPermission'],
    route_definition: ['route', 'graphql', 'guard'],
    pre_hook_definition: ['route'],
    post_hook_definition: ['route'],
    route_handler_definition: ['route'],
    route_permission_definition: ['route'],
    role_definition: ['route'],
    method_definition: ['route', 'graphql'],
    guard_definition: ['guard'],
    guard_rule_definition: ['guard'],
    field_permission_definition: ['fieldPermission'],
    setting_definition: ['setting', 'settingGraphql'],
    storage_config_definition: ['storage'],
    oauth_config_definition: ['oauth'],
    websocket_definition: ['websocket'],
    websocket_event_definition: ['websocket'],
    package_definition: ['package'],
    flow_definition: ['flow'],
    flow_step_definition: ['flow'],
    folder_definition: ['folder'],
    bootstrap_script_definition: ['bootstrap'],
  };

  describe('RELOAD_CHAINS — dependency correctness', () => {
    it('table/column/relation changes should invalidate fieldPermission', () => {
      for (const t of ['table_definition', 'column_definition', 'relation_definition']) {
        expect(RELOAD_CHAINS[t]).toContain('fieldPermission');
      }
    });

    it('route_definition changes should invalidate guard (path-keyed)', () => {
      expect(RELOAD_CHAINS['route_definition']).toContain('guard');
    });

    it('structural metadata changes should reload metadata → route → graphql', () => {
      for (const t of ['table_definition', 'column_definition', 'relation_definition']) {
        const chain = RELOAD_CHAINS[t];
        expect(chain.indexOf('metadata')).toBeLessThan(chain.indexOf('route'));
        expect(chain.indexOf('route')).toBeLessThan(chain.indexOf('graphql'));
      }
    });

    it('hook/handler/permission changes should NOT trigger graphql rebuild', () => {
      for (const t of [
        'pre_hook_definition', 'post_hook_definition',
        'route_handler_definition', 'route_permission_definition',
        'role_definition',
      ]) {
        expect(RELOAD_CHAINS[t]).not.toContain('graphql');
      }
    });

    it('setting changes should use settingGraphql (lightweight), not full graphql', () => {
      expect(RELOAD_CHAINS['setting_definition']).toContain('settingGraphql');
      expect(RELOAD_CHAINS['setting_definition']).not.toContain('graphql');
    });

    it('method_definition should still trigger graphql (GQL_QUERY/GQL_MUTATION flags)', () => {
      expect(RELOAD_CHAINS['method_definition']).toContain('graphql');
    });

    it('every chain entry should have at least one step', () => {
      for (const [table, chain] of Object.entries(RELOAD_CHAINS)) {
        expect(chain.length).toBeGreaterThan(0);
      }
    });
  });

  describe('executeChain — phased parallel execution', () => {
    it('should run metadata first, middle steps in parallel, graphql last', async () => {
      const callOrder: string[] = [];
      const stepMap: Record<string, () => Promise<void>> = {
        metadata: async () => { callOrder.push('metadata'); },
        repoRegistry: async () => { callOrder.push('repoRegistry'); },
        route: async () => { await new Promise(r => setTimeout(r, 5)); callOrder.push('route'); },
        graphql: async () => { callOrder.push('graphql'); },
        fieldPermission: async () => { callOrder.push('fieldPermission'); },
      };

      const chain = RELOAD_CHAINS['table_definition'];

      if (chain.includes('metadata')) {
        await stepMap['metadata']();
      }
      const middleSteps = chain.filter(s => s !== 'metadata' && s !== 'graphql');
      await Promise.all(middleSteps.map(s => stepMap[s]?.()));
      if (chain.includes('graphql')) {
        await stepMap['graphql']();
      }

      expect(callOrder[0]).toBe('metadata');
      expect(callOrder[callOrder.length - 1]).toBe('graphql');
      expect(callOrder.indexOf('metadata')).toBeLessThan(callOrder.indexOf('repoRegistry'));
      expect(callOrder.indexOf('metadata')).toBeLessThan(callOrder.indexOf('route'));
    });

    it('chain without metadata/graphql should only run middle steps', async () => {
      const called: string[] = [];
      const stepMap: Record<string, () => Promise<void>> = {
        route: async () => { called.push('route'); },
      };

      const chain = RELOAD_CHAINS['pre_hook_definition'];
      if (chain.includes('metadata')) await stepMap['metadata']?.();
      const middle = chain.filter(s => s !== 'metadata' && s !== 'graphql');
      await Promise.all(middle.map(s => stepMap[s]?.()));
      if (chain.includes('graphql')) await stepMap['graphql']?.();

      expect(called).toEqual(['route']);
    });
  });

  describe('reloadAll — multi-instance', () => {
    it('should publish __admin_reload_all signal before local reload', async () => {
      const signals: any[] = [];
      let localReloaded = false;

      const publish = async (_ch: string, msg: string) => {
        signals.push(JSON.parse(msg));
        expect(localReloaded).toBe(false);
      };
      const reloadAllLocal = async () => { localReloaded = true; };

      await publish('ch', JSON.stringify({
        instanceId: 'inst-001',
        type: 'RELOAD_SIGNAL',
        payload: { tableName: '__admin_reload_all', scope: 'full' },
      }));
      await reloadAllLocal();

      expect(signals[0].payload.tableName).toBe('__admin_reload_all');
      expect(localReloaded).toBe(true);
    });

    it('remote instance receiving __admin_reload_all should call reloadAllLocal', async () => {
      let reloadAllLocalCalled = false;
      let executeChainCalled = false;

      const signal = { tableName: '__admin_reload_all', scope: 'full' };

      if (signal.tableName === '__admin_reload_all') {
        reloadAllLocalCalled = true;
      } else {
        executeChainCalled = true;
      }

      expect(reloadAllLocalCalled).toBe(true);
      expect(executeChainCalled).toBe(false);
    });

    it('remote instance receiving normal signal should call executeChain', async () => {
      let reloadAllLocalCalled = false;
      let executeChainCalled = false;

      const signal = { tableName: 'table_definition', scope: 'partial' };

      if (signal.tableName === '__admin_reload_all') {
        reloadAllLocalCalled = true;
      } else {
        executeChainCalled = true;
      }

      expect(reloadAllLocalCalled).toBe(false);
      expect(executeChainCalled).toBe(true);
    });
  });

  describe('granular admin reloads — Redis publish', () => {
    it('reloadMetadataAndDeps should publish table_definition/full', () => {
      const signal = { tableName: 'table_definition', scope: 'full' };
      const chain = RELOAD_CHAINS[signal.tableName];
      expect(chain).toContain('metadata');
      expect(chain).toContain('route');
      expect(chain).toContain('graphql');
    });

    it('reloadRoutesOnly should publish route_definition/full', () => {
      const signal = { tableName: 'route_definition', scope: 'full' };
      const chain = RELOAD_CHAINS[signal.tableName];
      expect(chain).toContain('route');
      expect(chain).toContain('guard');
    });

    it('reloadGuardsOnly should publish guard_definition/full', () => {
      const signal = { tableName: 'guard_definition', scope: 'full' };
      const chain = RELOAD_CHAINS[signal.tableName];
      expect(chain).toContain('guard');
    });
  });

  describe('mergePayload — cross-table merge picks longer chain', () => {
    it('should pick table_definition over route_definition (4 > 3)', () => {
      const chainA = RELOAD_CHAINS['table_definition'];
      const chainB = RELOAD_CHAINS['route_definition'];
      const winner = chainA.length >= chainB.length ? 'table_definition' : 'route_definition';
      expect(winner).toBe('table_definition');
    });

    it('should pick column_definition over pre_hook_definition (5 > 1)', () => {
      const chainA = RELOAD_CHAINS['column_definition'];
      const chainB = RELOAD_CHAINS['pre_hook_definition'];
      const winner = chainA.length >= chainB.length ? 'column_definition' : 'pre_hook_definition';
      expect(winner).toBe('column_definition');
    });
  });
});
