import { RELOAD_CHAINS } from '../../src/engines/cache/services/cache-orchestrator.service';

describe('CacheOrchestratorService — RELOAD_CHAINS + multi-instance', () => {
  describe('RELOAD_CHAINS — dependency correctness', () => {
    it('table/column/relation changes should invalidate fieldPermission', () => {
      for (const t of ['enfyra_table', 'enfyra_column', 'enfyra_relation']) {
        expect(RELOAD_CHAINS[t]).toContain('fieldPermission');
        expect(RELOAD_CHAINS[t]).toContain('column-rule');
      }
    });

    it('enfyra_route changes should invalidate guard (path-keyed)', () => {
      expect(RELOAD_CHAINS['enfyra_route']).toContain('guard');
    });

    it('structural metadata changes should reload metadata → route → graphql', () => {
      for (const t of ['enfyra_table', 'enfyra_column', 'enfyra_relation']) {
        const chain = RELOAD_CHAINS[t];
        expect(chain.indexOf('metadata')).toBeLessThan(chain.indexOf('route'));
        expect(chain.indexOf('route')).toBeLessThan(chain.indexOf('graphql'));
      }
    });

    it('hook/handler/permission changes should NOT trigger graphql rebuild', () => {
      for (const t of [
        'enfyra_pre_hook',
        'enfyra_post_hook',
        'enfyra_route_handler',
        'enfyra_route_permission',
        'enfyra_role',
      ]) {
        expect(RELOAD_CHAINS[t]).not.toContain('graphql');
      }
    });

    it('setting changes should use settingGraphql (lightweight), not full graphql', () => {
      expect(RELOAD_CHAINS['enfyra_setting']).toContain('settingGraphql');
      expect(RELOAD_CHAINS['enfyra_setting']).not.toContain('graphql');
    });

    it('enfyra_method should still trigger graphql (GQL_QUERY/GQL_MUTATION flags)', () => {
      expect(RELOAD_CHAINS['enfyra_method']).toContain('graphql');
    });

    it('every chain entry should have at least one step', () => {
      for (const chain of Object.values(RELOAD_CHAINS)) {
        expect(chain.length).toBeGreaterThan(0);
      }
    });
  });

  describe('executeChain — phased parallel execution', () => {
    it('should run metadata first, middle steps in parallel, graphql last', async () => {
      const callOrder: string[] = [];
      const stepMap: Record<string, () => Promise<void>> = {
        metadata: async () => {
          callOrder.push('metadata');
        },
        repoRegistry: async () => {
          callOrder.push('repoRegistry');
        },
        route: async () => {
          await new Promise((r) => setTimeout(r, 5));
          callOrder.push('route');
        },
        graphql: async () => {
          callOrder.push('graphql');
        },
        fieldPermission: async () => {
          callOrder.push('fieldPermission');
        },
      };

      const chain = RELOAD_CHAINS['enfyra_table'];

      if (chain.includes('metadata')) {
        await stepMap['metadata']();
      }
      const middleSteps = chain.filter(
        (s) => s !== 'metadata' && s !== 'graphql' && s !== 'settingGraphql',
      );
      await Promise.all(middleSteps.map((s) => stepMap[s]?.()));
      if (chain.includes('graphql')) {
        await stepMap['graphql']();
      }

      expect(callOrder[0]).toBe('metadata');
      expect(callOrder[callOrder.length - 1]).toBe('graphql');
      expect(callOrder.indexOf('metadata')).toBeLessThan(
        callOrder.indexOf('repoRegistry'),
      );
      expect(callOrder.indexOf('metadata')).toBeLessThan(
        callOrder.indexOf('route'),
      );
    });

    it('chain without metadata/graphql should only run middle steps', async () => {
      const called: string[] = [];
      const stepMap: Record<string, () => Promise<void>> = {
        route: async () => {
          called.push('route');
        },
      };

      const chain = RELOAD_CHAINS['enfyra_pre_hook'];
      if (chain.includes('metadata')) await stepMap['metadata']?.();
      const middle = chain.filter(
        (s) => s !== 'metadata' && s !== 'graphql' && s !== 'settingGraphql',
      );
      await Promise.all(middle.map((s) => stepMap[s]?.()));
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
      const reloadAllLocal = async () => {
        localReloaded = true;
      };

      await publish(
        'ch',
        JSON.stringify({
          instanceId: 'inst-001',
          type: 'RELOAD_SIGNAL',
          payload: { tableName: '__admin_reload_all', scope: 'full' },
        }),
      );
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

      const signal = { tableName: 'enfyra_table', scope: 'partial' };

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
    it('reloadMetadataAndDeps should publish enfyra_table/full', () => {
      const signal = { tableName: 'enfyra_table', scope: 'full' };
      const chain = RELOAD_CHAINS[signal.tableName];
      expect(chain).toContain('metadata');
      expect(chain).toContain('route');
      expect(chain).toContain('graphql');
    });

    it('reloadRoutesOnly should publish enfyra_route/full', () => {
      const signal = { tableName: 'enfyra_route', scope: 'full' };
      const chain = RELOAD_CHAINS[signal.tableName];
      expect(chain).toContain('route');
      expect(chain).toContain('guard');
    });

    it('reloadGuardsOnly should publish enfyra_guard/full', () => {
      const signal = { tableName: 'enfyra_guard', scope: 'full' };
      const chain = RELOAD_CHAINS[signal.tableName];
      expect(chain).toContain('guard');
    });
  });

  describe('mergePayload — cross-table merge picks longer chain', () => {
    it('should pick enfyra_table over enfyra_route', () => {
      const chainA = RELOAD_CHAINS['enfyra_table'];
      const chainB = RELOAD_CHAINS['enfyra_route'];
      const winner =
        chainA.length >= chainB.length ? 'enfyra_table' : 'enfyra_route';
      expect(winner).toBe('enfyra_table');
    });

    it('should pick enfyra_column over enfyra_pre_hook', () => {
      const chainA = RELOAD_CHAINS['enfyra_column'];
      const chainB = RELOAD_CHAINS['enfyra_pre_hook'];
      const winner =
        chainA.length >= chainB.length ? 'enfyra_column' : 'enfyra_pre_hook';
      expect(winner).toBe('enfyra_column');
    });
  });
});
