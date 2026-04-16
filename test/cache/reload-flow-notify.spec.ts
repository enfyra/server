import { EventEmitter2 } from '@nestjs/event-emitter';
import { CacheOrchestratorService } from '../../src/infrastructure/cache/services/cache-orchestrator.service';
import { TCacheInvalidationPayload } from '../../src/shared/types/cache.types';

type Emitted = { event: string; data: any };

function makeGateway() {
  const emitted: Emitted[] = [];
  return {
    emitted,
    emitToNamespace: jest.fn((_ns: string, event: string, data: any) => {
      emitted.push({ event, data });
    }),
  };
}

function makeReloadable() {
  return {
    reload: jest.fn().mockResolvedValue(undefined),
    partialReload: jest.fn().mockResolvedValue(undefined),
    isLoaded: jest.fn().mockReturnValue(true),
    supportsPartialReload: jest.fn().mockReturnValue(true),
  };
}

function makeService() {
  const gateway = makeGateway();
  const graphqlService = {
    reloadSchema: jest.fn().mockResolvedValue(undefined),
    onSettingChanged: jest.fn(),
  };

  const metadataCache = makeReloadable();
  const routeCache = makeReloadable();
  const guardCache = makeReloadable();
  const flowCache = makeReloadable();
  const websocketCache = makeReloadable();
  const packageCache = makeReloadable();
  const settingCache = makeReloadable();
  const storageCache = makeReloadable();
  const oauthCache = makeReloadable();
  const folderCache = makeReloadable();
  const fieldPermissionCache = makeReloadable();

  const repoRegistry = { rebuildFromMetadata: jest.fn() };
  const redisPubSub = {
    publish: jest.fn().mockResolvedValue(undefined),
    subscribeWithHandler: jest.fn(),
    isChannelForBase: jest.fn().mockReturnValue(true),
  } as any;
  const instanceService = { getInstanceId: () => 'test-instance' } as any;
  const moduleRef = { get: jest.fn() } as any;

  const svc = new CacheOrchestratorService(
    moduleRef,
    redisPubSub,
    instanceService,
    new EventEmitter2(),
    metadataCache as any,
    routeCache as any,
    guardCache as any,
    flowCache as any,
    websocketCache as any,
    packageCache as any,
    settingCache as any,
    storageCache as any,
    oauthCache as any,
    folderCache as any,
    fieldPermissionCache as any,
    repoRegistry as any,
  );

  (svc as any).websocketGateway = gateway;
  (svc as any).graphqlService = graphqlService;

  return { svc, gateway, graphqlService };
}

function mkPayload(table: string): TCacheInvalidationPayload {
  return {
    table,
    action: 'reload',
    scope: 'full',
    timestamp: Date.now(),
  } as TCacheInvalidationPayload;
}

describe('CacheOrchestrator reload notifications', () => {
  describe('resolveFlowName priority', () => {
    let svc: CacheOrchestratorService;
    beforeEach(() => {
      svc = makeService().svc;
    });

    const cases: Array<{ chain: string[]; expected: string }> = [
      { chain: ['metadata', 'repoRegistry', 'route', 'graphql', 'fieldPermission'], expected: 'metadata' },
      { chain: ['route', 'graphql', 'guard'], expected: 'route' },
      { chain: ['route', 'graphql'], expected: 'route' },
      { chain: ['route'], expected: 'route' },
      { chain: ['guard'], expected: 'guard' },
      { chain: ['fieldPermission', 'graphql'], expected: 'fieldPermission' },
      { chain: ['setting', 'settingGraphql'], expected: 'setting' },
      { chain: ['storage'], expected: 'storage' },
      { chain: ['oauth'], expected: 'oauth' },
      { chain: ['websocket'], expected: 'websocket' },
      { chain: ['package'], expected: 'package' },
      { chain: ['flow'], expected: 'flow' },
      { chain: ['folder'], expected: 'folder' },
      { chain: ['bootstrap'], expected: 'bootstrap' },
      { chain: ['graphql'], expected: 'graphql' },
    ];

    for (const c of cases) {
      it(`[${c.chain.join(',')}] → ${c.expected}`, () => {
        const flow = (svc as any).resolveFlowName(c.chain);
        expect(flow).toBe(c.expected);
      });
    }

    it('unknown chain falls back to first step', () => {
      expect((svc as any).resolveFlowName(['somethingCustom'])).toBe(
        'somethingCustom',
      );
    });

    it('empty chain returns "unknown"', () => {
      expect((svc as any).resolveFlowName([])).toBe('unknown');
    });
  });

  describe('notifyClients emits $system:reload with flow', () => {
    it('executeChain emits pending + done with correct flow for metadata chain', async () => {
      const { svc, gateway } = makeService();
      await (svc as any).executeChain(mkPayload('table_definition'), true);

      const events = gateway.emitted.filter((e) => e.event === '$system:reload');
      expect(events).toHaveLength(2);
      expect(events[0].data).toEqual(
        expect.objectContaining({ flow: 'metadata', status: 'pending' }),
      );
      expect(events[1].data).toEqual(
        expect.objectContaining({ flow: 'metadata', status: 'done' }),
      );
      expect(events[0].data.steps).toContain('metadata');
      expect(events[0].data.steps).toContain('graphql');
    });

    it('guard-only chain now notifies (was silent before)', async () => {
      const { svc, gateway } = makeService();
      await (svc as any).executeChain(mkPayload('guard_definition'), true);

      const events = gateway.emitted.filter((e) => e.event === '$system:reload');
      expect(events).toHaveLength(2);
      expect(events[0].data.flow).toBe('guard');
      expect(events[1].data.flow).toBe('guard');
    });

    it('storage-only chain notifies', async () => {
      const { svc, gateway } = makeService();
      await (svc as any).executeChain(mkPayload('storage_config_definition'), true);

      const events = gateway.emitted.filter((e) => e.event === '$system:reload');
      expect(events).toHaveLength(2);
      expect(events[0].data.flow).toBe('storage');
    });

    it('route_definition chain reports flow=route (not graphql)', async () => {
      const { svc, gateway } = makeService();
      await (svc as any).executeChain(mkPayload('route_definition'), true);

      const events = gateway.emitted.filter((e) => e.event === '$system:reload');
      expect(events[0].data.flow).toBe('route');
      expect(events[0].data.steps).toEqual(
        expect.arrayContaining(['route', 'graphql', 'guard']),
      );
    });

    it('publish=false (signal from other instance) does NOT emit to clients', async () => {
      const { svc, gateway } = makeService();
      await (svc as any).executeChain(mkPayload('table_definition'), false);

      const events = gateway.emitted.filter((e) => e.event === '$system:reload');
      expect(events).toHaveLength(0);
    });
  });

  describe('admin reload endpoints notify correctly', () => {
    it('reloadMetadataAndDeps emits flow=metadata', async () => {
      const { svc, gateway } = makeService();
      await svc.reloadMetadataAndDeps();
      const events = gateway.emitted.filter((e) => e.event === '$system:reload');
      expect(events).toHaveLength(2);
      expect(events[0].data.flow).toBe('metadata');
      expect(events[1].data.flow).toBe('metadata');
    });

    it('reloadRoutesOnly emits flow=route', async () => {
      const { svc, gateway } = makeService();
      await svc.reloadRoutesOnly();
      const events = gateway.emitted.filter((e) => e.event === '$system:reload');
      expect(events).toHaveLength(2);
      expect(events[0].data.flow).toBe('route');
    });

    it('reloadGraphqlOnly now emits (previously silent)', async () => {
      const { svc, gateway } = makeService();
      await svc.reloadGraphqlOnly();
      const events = gateway.emitted.filter((e) => e.event === '$system:reload');
      expect(events).toHaveLength(2);
      expect(events[0].data.flow).toBe('graphql');
    });

    it('reloadGuardsOnly now emits (previously silent)', async () => {
      const { svc, gateway } = makeService();
      await svc.reloadGuardsOnly();
      const events = gateway.emitted.filter((e) => e.event === '$system:reload');
      expect(events).toHaveLength(2);
      expect(events[0].data.flow).toBe('guard');
    });

    it('reloadAll emits flow=all', async () => {
      const { svc, gateway } = makeService();
      await svc.reloadAll();
      const events = gateway.emitted.filter((e) => e.event === '$system:reload');
      expect(events).toHaveLength(2);
      expect(events[0].data.flow).toBe('all');
      expect(events[0].data.steps).toEqual(
        expect.arrayContaining([
          'metadata',
          'route',
          'guard',
          'graphql',
          'fieldPermission',
        ]),
      );
    });
  });
});
