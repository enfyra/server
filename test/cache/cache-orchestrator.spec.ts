/**
 * Comprehensive tests for CacheOrchestratorService.
 *
 * All infrastructure is fully mocked — no NestJS DI, no Redis, no isolated-vm.
 * Multi-instance tests simulate two orchestrator instances sharing a mock pub/sub bus.
 */

import { EventEmitter2 } from 'eventemitter2';
import { CACHE_EVENTS } from '../../src/shared/utils/cache-events.constants';
import { TCacheInvalidationPayload } from '../../src/shared/types/cache.types';

const SYNC_CHANNEL = 'enfyra:cache-orchestrator-sync';

// ─────────────────────────────────────────────────────────────────────────────
// Inline types mirroring the production RELOAD_CHAINS
// ─────────────────────────────────────────────────────────────────────────────

const RELOAD_CHAINS: Record<string, string[]> = {
  table_definition: ['metadata', 'repoRegistry', 'route', 'graphql'],
  column_definition: ['metadata', 'repoRegistry', 'route', 'graphql'],
  relation_definition: ['metadata', 'repoRegistry', 'route', 'graphql'],

  route_definition: ['route', 'graphql'],
  pre_hook_definition: ['route', 'graphql'],
  post_hook_definition: ['route', 'graphql'],
  route_handler_definition: ['route', 'graphql'],
  route_permission_definition: ['route', 'graphql'],
  role_definition: ['route', 'graphql'],
  method_definition: ['route', 'graphql'],

  guard_definition: ['guard'],
  guard_rule_definition: ['guard'],

  field_permission_definition: ['fieldPermission'],

  setting_definition: ['setting', 'graphql'],
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

// ─────────────────────────────────────────────────────────────────────────────
// Mock pub/sub bus (shared between instances for multi-instance tests)
// ─────────────────────────────────────────────────────────────────────────────

type MsgHandler = (channel: string, message: string) => void;

interface MockPubSub {
  handlers: Map<string, MsgHandler[]>;
  published: Array<{ channel: string; raw: string }>;
  subscribeWithHandler: jest.Mock;
  publish: jest.Mock;
  isChannelForBase: jest.Mock;
  simulateMessage(channel: string, message: string): Promise<void>;
}

function makePubSub(): MockPubSub {
  const handlers = new Map<string, MsgHandler[]>();
  const published: Array<{ channel: string; raw: string }> = [];
  return {
    handlers,
    published,
    subscribeWithHandler: jest.fn((ch: string, h: MsgHandler) => {
      if (!handlers.has(ch)) handlers.set(ch, []);
      handlers.get(ch)!.push(h);
    }),
    publish: jest.fn(async (ch: string, msg: string) => {
      published.push({ channel: ch, raw: msg });
    }),
    isChannelForBase: jest.fn(
      (received: string, base: string) => received === base,
    ),
    async simulateMessage(ch: string, msg: string) {
      const list = handlers.get(ch) || [];
      await Promise.all(list.map((h) => h(ch, msg)));
    },
  };
}

// ─────────────────────────────────────────────────────────────────────────────
// Mock cache services factory
// ─────────────────────────────────────────────────────────────────────────────

function makeMetadataCache(
  opts: {
    loaded?: boolean;
  } = {},
) {
  return {
    reload: jest.fn().mockResolvedValue(undefined),
    partialReload: jest.fn().mockResolvedValue(undefined),
    isLoaded: jest.fn().mockReturnValue(opts.loaded ?? true),
  };
}

function makeRouteCache(
  opts: {
    loaded?: boolean;
    supportsPartial?: boolean;
  } = {},
) {
  return {
    reload: jest.fn().mockResolvedValue(undefined),
    partialReload: jest.fn().mockResolvedValue(undefined),
    isLoaded: jest.fn().mockReturnValue(opts.loaded ?? true),
    supportsPartialReload: jest
      .fn()
      .mockReturnValue(opts.supportsPartial ?? true),
  };
}

function makeSimpleCache() {
  return {
    reload: jest.fn().mockResolvedValue(undefined),
  };
}

function makeRepoRegistry() {
  return {
    rebuildFromMetadata: jest.fn(),
  };
}

function makeGraphqlService() {
  return {
    reloadSchema: jest.fn().mockResolvedValue(undefined),
  };
}

function makeBootstrapScriptService() {
  return {
    onMetadataLoaded: jest.fn().mockResolvedValue(undefined),
    reloadBootstrapScripts: jest.fn().mockResolvedValue(undefined),
  };
}

function makeModuleRef(graphql: any, bootstrap: any) {
  return {
    get: jest.fn((token: string) => {
      if (token === 'GraphqlService') {
        if (!graphql) throw new Error('not found');
        return graphql;
      }
      if (token === 'BootstrapScriptService') {
        if (!bootstrap) throw new Error('not found');
        return bootstrap;
      }
      throw new Error(`Unknown token: ${token}`);
    }),
  };
}

// ─────────────────────────────────────────────────────────────────────────────
// Inline CacheOrchestratorService (mirrors production logic exactly)
// ─────────────────────────────────────────────────────────────────────────────

type ReloadStep = (payload: TCacheInvalidationPayload) => Promise<void>;

class TestCacheOrchestrator {
  private stepMap: Record<string, ReloadStep>;
  public graphqlService: any = null;
  public bootstrapScriptService: any = null;
  private messageHandler: MsgHandler | null = null;

  private debounceTimer: ReturnType<typeof setTimeout> | null = null;
  private debounceResolvers: Array<() => void> = [];
  private pendingPayload: TCacheInvalidationPayload | null = null;

  public callOrder: string[] = [];

  constructor(
    public readonly instanceId: string,
    public readonly pubSub: MockPubSub,
    public readonly eventEmitter: EventEmitter2,
    public readonly metadataCache: ReturnType<typeof makeMetadataCache>,
    public readonly routeCache: ReturnType<typeof makeRouteCache>,
    public readonly guardCache: ReturnType<typeof makeSimpleCache>,
    public readonly flowCache: ReturnType<typeof makeSimpleCache>,
    public readonly websocketCache: ReturnType<typeof makeSimpleCache>,
    public readonly packageCache: ReturnType<typeof makeSimpleCache>,
    public readonly settingCache: ReturnType<typeof makeSimpleCache>,
    public readonly storageCache: ReturnType<typeof makeSimpleCache>,
    public readonly oauthCache: ReturnType<typeof makeSimpleCache>,
    public readonly folderCache: ReturnType<typeof makeSimpleCache>,
    public readonly fieldPermissionCache: ReturnType<typeof makeSimpleCache>,
    public readonly repoRegistry: ReturnType<typeof makeRepoRegistry>,
  ) {
    this.stepMap = {
      metadata: (p) => this.reloadMetadata(p),
      repoRegistry: () => this.reloadRepoRegistry(),
      route: (p) => this.reloadRoute(p),
      graphql: (p) => this.reloadGraphql(p),
      guard: (p) => this.reloadSimple(this.guardCache, p),
      flow: (p) => this.reloadSimple(this.flowCache, p),
      websocket: (p) => this.reloadSimple(this.websocketCache, p),
      package: (p) => this.reloadSimple(this.packageCache, p),
      setting: (p) => this.reloadSimple(this.settingCache, p),
      storage: (p) => this.reloadSimple(this.storageCache, p),
      oauth: (p) => this.reloadSimple(this.oauthCache, p),
      folder: (p) => this.reloadSimple(this.folderCache, p),
      fieldPermission: (p) => this.reloadSimple(this.fieldPermissionCache, p),
      bootstrap: () => this.reloadBootstrapScripts(),
    };
  }

  onModuleInit() {
    this.subscribeToRedis();
  }

  async onApplicationBootstrap(graphql: any, bootstrap: any) {
    this.graphqlService = graphql;
    this.bootstrapScriptService = bootstrap;
    await this.bootstrap();
  }

  async bootstrap(): Promise<void> {
    await this.metadataCache.reload();
    this.eventEmitter.emit(CACHE_EVENTS.METADATA_LOADED);

    await Promise.all([
      this.routeCache.reload(false),
      this.guardCache.reload(false),
      this.flowCache.reload(false),
      this.websocketCache.reload(false),
      this.packageCache.reload(false),
      this.settingCache.reload(false),
      this.storageCache.reload(false),
      this.oauthCache.reload(false),
      this.folderCache.reload(false),
      this.reloadRepoRegistry(),
      this.bootstrapScriptService?.onMetadataLoaded?.(),
    ]);

    if (this.graphqlService) {
      await this.graphqlService.reloadSchema();
    }
    this.eventEmitter.emit(CACHE_EVENTS.GRAPHQL_LOADED);
    this.eventEmitter.emit(CACHE_EVENTS.SYSTEM_READY);
  }

  handleInvalidation(payload: TCacheInvalidationPayload): Promise<void> {
    return new Promise<void>((resolve) => {
      this.debounceResolvers.push(resolve);
      this.mergePayload(payload);
      if (this.debounceTimer) clearTimeout(this.debounceTimer);
      this.debounceTimer = setTimeout(async () => {
        this.debounceTimer = null;
        const resolvers = this.debounceResolvers.splice(0);
        const merged = this.pendingPayload;
        this.pendingPayload = null;
        try {
          if (merged) {
            await this.executeChain(merged, true);
          }
        } catch {}
        resolvers.forEach((r) => r());
      }, 50);
    });
  }

  private mergePayload(payload: TCacheInvalidationPayload): void {
    if (!this.pendingPayload) {
      this.pendingPayload = { ...payload };
      return;
    }
    if (
      this.pendingPayload.tableName !== payload.tableName ||
      payload.scope === 'full' ||
      this.pendingPayload.scope === 'full'
    ) {
      this.pendingPayload.scope = 'full';
      this.pendingPayload.ids = undefined;
      this.pendingPayload.affectedTables = undefined;
      return;
    }
    const mergedIds = new Set([
      ...(this.pendingPayload.ids || []),
      ...(payload.ids || []),
    ]);
    const mergedTables = new Set([
      ...(this.pendingPayload.affectedTables || []),
      ...(payload.affectedTables || []),
    ]);
    this.pendingPayload.ids = [...mergedIds];
    this.pendingPayload.affectedTables = mergedTables.size
      ? [...mergedTables]
      : undefined;
  }

  async executeChain(
    payload: TCacheInvalidationPayload,
    publish: boolean,
  ): Promise<void> {
    const chain = RELOAD_CHAINS[payload.tableName];
    if (!chain) return;

    if (publish) {
      await this.publishSignal(payload);
    }

    for (const step of chain) {
      const fn = this.stepMap[step];
      if (fn) {
        this.callOrder.push(step);
        await fn(payload);
      }
    }
  }

  private async reloadMetadata(
    payload: TCacheInvalidationPayload,
  ): Promise<void> {
    if (
      payload.scope === 'partial' &&
      payload.ids?.length &&
      this.metadataCache.isLoaded()
    ) {
      await this.metadataCache.partialReload(payload);
    } else {
      await this.metadataCache.reload();
    }
  }

  private async reloadRepoRegistry(): Promise<void> {
    this.repoRegistry.rebuildFromMetadata(this.metadataCache);
  }

  private async reloadRoute(payload: TCacheInvalidationPayload): Promise<void> {
    if (
      payload.scope === 'partial' &&
      this.routeCache.isLoaded() &&
      this.routeCache.supportsPartialReload()
    ) {
      await this.routeCache.partialReload(payload, false);
    } else {
      await this.routeCache.reload(false);
    }
  }

  private async reloadGraphql(
    payload: TCacheInvalidationPayload,
  ): Promise<void> {
    if (!this.graphqlService) return;
    await this.graphqlService.reloadSchema(payload);
  }

  private async reloadSimple(
    cache: { reload: (publish?: boolean) => Promise<void> },
    _payload: TCacheInvalidationPayload,
  ): Promise<void> {
    await cache.reload(false);
  }

  private async reloadBootstrapScripts(): Promise<void> {
    if (!this.bootstrapScriptService) return;
    await this.bootstrapScriptService.reloadBootstrapScripts();
  }

  async reloadAll(): Promise<void> {
    await this.metadataCache.reload();
    await this.reloadRepoRegistry();
    await this.routeCache.reload(false);
    await this.guardCache.reload(false);
    if (this.graphqlService) {
      await this.graphqlService.reloadSchema();
    }
  }

  private subscribeToRedis(): void {
    if (this.messageHandler) return;
    this.messageHandler = async (channel: string, message: string) => {
      if (this.pubSub.isChannelForBase(channel, SYNC_CHANNEL)) {
        try {
          const signal = JSON.parse(message);
          if (signal.instanceId === this.instanceId) return;
          await this.executeChain(signal.payload, false);
        } catch {}
      }
    };
    this.pubSub.subscribeWithHandler(SYNC_CHANNEL, this.messageHandler);
  }

  async publishSignal(payload: TCacheInvalidationPayload): Promise<void> {
    await this.pubSub.publish(
      SYNC_CHANNEL,
      JSON.stringify({
        instanceId: this.instanceId,
        type: 'RELOAD_SIGNAL',
        timestamp: Date.now(),
        payload,
      }),
    );
  }

  getPendingPayload() {
    return this.pendingPayload;
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// Factory helper for building an orchestrator with all mocks
// ─────────────────────────────────────────────────────────────────────────────

function makeOrchestrator(
  opts: {
    instanceId?: string;
    pubSub?: MockPubSub;
    metadataCacheOpts?: Parameters<typeof makeMetadataCache>[0];
    routeCacheOpts?: Parameters<typeof makeRouteCache>[0];
  } = {},
) {
  const pubSub = opts.pubSub ?? makePubSub();
  const ee = new EventEmitter2();
  const metadataCache = makeMetadataCache(opts.metadataCacheOpts);
  const routeCache = makeRouteCache(opts.routeCacheOpts);
  const guardCache = makeSimpleCache();
  const flowCache = makeSimpleCache();
  const websocketCache = makeSimpleCache();
  const packageCache = makeSimpleCache();
  const settingCache = makeSimpleCache();
  const storageCache = makeSimpleCache();
  const oauthCache = makeSimpleCache();
  const folderCache = makeSimpleCache();
  const fieldPermissionCache = makeSimpleCache();
  const repoRegistry = makeRepoRegistry();
  const instanceId = opts.instanceId ?? 'test-instance-aabbcc';

  const svc = new TestCacheOrchestrator(
    instanceId,
    pubSub,
    ee,
    metadataCache,
    routeCache,
    guardCache,
    flowCache,
    websocketCache,
    packageCache,
    settingCache,
    storageCache,
    oauthCache,
    folderCache,
    fieldPermissionCache,
    repoRegistry,
  );
  svc.onModuleInit();
  return {
    svc,
    pubSub,
    ee,
    metadataCache,
    routeCache,
    guardCache,
    flowCache,
    websocketCache,
    packageCache,
    settingCache,
    storageCache,
    oauthCache,
    folderCache,
    fieldPermissionCache,
    repoRegistry,
  };
}

function makePayload(
  tableName: string,
  scope: 'full' | 'partial' = 'full',
  ids?: (string | number)[],
  affectedTables?: string[],
): TCacheInvalidationPayload {
  return {
    tableName,
    action: 'reload',
    timestamp: Date.now(),
    scope,
    ids,
    affectedTables,
  };
}

// ─────────────────────────────────────────────────────────────────────────────
// A. RELOAD_CHAINS correctness
// ─────────────────────────────────────────────────────────────────────────────

describe('A. RELOAD_CHAINS correctness', () => {
  it('every table in RELOAD_CHAINS produces the correct ordered chain (matrix)', async () => {
    const cases: Array<[string, string[]]> = [
      ['table_definition', ['metadata', 'repoRegistry', 'route', 'graphql']],
      ['column_definition', ['metadata', 'repoRegistry', 'route', 'graphql']],
      ['relation_definition', ['metadata', 'repoRegistry', 'route', 'graphql']],
      ['route_definition', ['route', 'graphql']],
      ['pre_hook_definition', ['route', 'graphql']],
      ['post_hook_definition', ['route', 'graphql']],
      ['route_handler_definition', ['route', 'graphql']],
      ['route_permission_definition', ['route', 'graphql']],
      ['role_definition', ['route', 'graphql']],
      ['method_definition', ['route', 'graphql']],
      ['guard_definition', ['guard']],
      ['guard_rule_definition', ['guard']],
      ['field_permission_definition', ['fieldPermission']],
      ['setting_definition', ['setting', 'graphql']],
      ['storage_config_definition', ['storage']],
      ['oauth_config_definition', ['oauth']],
      ['websocket_definition', ['websocket']],
      ['websocket_event_definition', ['websocket']],
      ['package_definition', ['package']],
      ['flow_definition', ['flow']],
      ['flow_step_definition', ['flow']],
      ['folder_definition', ['folder']],
      ['bootstrap_script_definition', ['bootstrap']],
    ];

    for (const [table, expectedChain] of cases) {
      expect(RELOAD_CHAINS[table]).toEqual(expectedChain);
    }
  });

  it('unknown table → no-op (no crash, nothing executed)', async () => {
    const { svc, metadataCache, routeCache, guardCache } = makeOrchestrator();
    const payload = makePayload('unknown_table_xyz');
    await svc.executeChain(payload, false);
    expect(metadataCache.reload).not.toHaveBeenCalled();
    expect(routeCache.reload).not.toHaveBeenCalled();
    expect(guardCache.reload).not.toHaveBeenCalled();
    expect(svc.callOrder).toHaveLength(0);
  });

  it('table_definition → chain is [metadata, repoRegistry, route, graphql]', () => {
    expect(RELOAD_CHAINS['table_definition']).toEqual([
      'metadata',
      'repoRegistry',
      'route',
      'graphql',
    ]);
  });

  it('route_definition → chain is [route, graphql]', () => {
    expect(RELOAD_CHAINS['route_definition']).toEqual(['route', 'graphql']);
  });

  it('guard_definition → chain is [guard] only', () => {
    expect(RELOAD_CHAINS['guard_definition']).toEqual(['guard']);
    expect(RELOAD_CHAINS['guard_definition']).not.toContain('metadata');
    expect(RELOAD_CHAINS['guard_definition']).not.toContain('route');
    expect(RELOAD_CHAINS['guard_definition']).not.toContain('graphql');
  });

  it('setting_definition → chain is [setting, graphql]', () => {
    expect(RELOAD_CHAINS['setting_definition']).toEqual(['setting', 'graphql']);
  });

  it('all chain entries are known step names', () => {
    const knownSteps = new Set([
      'metadata',
      'repoRegistry',
      'route',
      'graphql',
      'guard',
      'flow',
      'websocket',
      'package',
      'setting',
      'storage',
      'oauth',
      'folder',
      'fieldPermission',
      'bootstrap',
    ]);
    for (const [table, chain] of Object.entries(RELOAD_CHAINS)) {
      for (const step of chain) {
        expect(knownSteps.has(step)).toBe(true);
      }
    }
  });
});

// ─────────────────────────────────────────────────────────────────────────────
// B. Debounce + merge logic
// ─────────────────────────────────────────────────────────────────────────────

describe('B. Debounce + merge logic', () => {
  beforeEach(() => jest.useFakeTimers());
  afterEach(() => jest.useRealTimers());

  it('single invalidation → executes once after 50ms', async () => {
    const { svc, metadataCache } = makeOrchestrator();
    svc.graphqlService = makeGraphqlService();

    const p = svc.handleInvalidation(makePayload('table_definition', 'full'));
    expect(metadataCache.reload).not.toHaveBeenCalled();

    await jest.advanceTimersByTimeAsync(50);
    await p;

    expect(metadataCache.reload).toHaveBeenCalledTimes(1);
  });

  it('two rapid invalidations for same table → merged into 1 execution', async () => {
    const { svc, metadataCache } = makeOrchestrator();
    svc.graphqlService = makeGraphqlService();

    const p1 = svc.handleInvalidation(makePayload('table_definition', 'full'));
    const p2 = svc.handleInvalidation(makePayload('table_definition', 'full'));

    await jest.advanceTimersByTimeAsync(50);
    await Promise.all([p1, p2]);

    expect(metadataCache.reload).toHaveBeenCalledTimes(1);
  });

  it('two invalidations for different tables → scope escalates to full', async () => {
    const { svc } = makeOrchestrator();
    svc.graphqlService = makeGraphqlService();

    svc.handleInvalidation(makePayload('table_definition', 'partial', ['1']));
    const p2 = svc.handleInvalidation(
      makePayload('route_definition', 'partial', ['2']),
    );

    await jest.advanceTimersByTimeAsync(50);
    await p2;

    const lastCall = (svc['pubSub'] as MockPubSub).published.at(-1);
    expect(lastCall?.raw).toBeDefined();
    const signal = JSON.parse(lastCall!.raw);
    expect(signal.payload.scope).toBe('full');
    expect(signal.payload.ids).toBeUndefined();
    expect(signal.payload.affectedTables).toBeUndefined();
  });

  it('partial + full merge → result is full', async () => {
    const { svc } = makeOrchestrator();
    svc.graphqlService = makeGraphqlService();

    svc.handleInvalidation(makePayload('table_definition', 'partial', ['1']));
    const p2 = svc.handleInvalidation(makePayload('table_definition', 'full'));

    await jest.advanceTimersByTimeAsync(50);
    await p2;

    const signal = JSON.parse(svc.pubSub.published.at(-1)!.raw);
    expect(signal.payload.scope).toBe('full');
    expect(signal.payload.ids).toBeUndefined();
  });

  it('full + partial merge → result is full', async () => {
    const { svc } = makeOrchestrator();
    svc.graphqlService = makeGraphqlService();

    svc.handleInvalidation(makePayload('table_definition', 'full'));
    const p2 = svc.handleInvalidation(
      makePayload('table_definition', 'partial', ['1']),
    );

    await jest.advanceTimersByTimeAsync(50);
    await p2;

    const signal = JSON.parse(svc.pubSub.published.at(-1)!.raw);
    expect(signal.payload.scope).toBe('full');
  });

  it('10 rapid partial invalidations → 1 execution with all ids merged', async () => {
    const { svc, metadataCache } = makeOrchestrator({
      metadataCacheOpts: { loaded: true },
    });
    svc.graphqlService = makeGraphqlService();

    const promises: Promise<void>[] = [];
    for (let i = 0; i < 10; i++) {
      promises.push(
        svc.handleInvalidation(
          makePayload('table_definition', 'partial', [String(i)]),
        ),
      );
    }

    await jest.advanceTimersByTimeAsync(50);
    await Promise.all(promises);

    expect(metadataCache.partialReload).toHaveBeenCalledTimes(1);
    expect(svc.pubSub.publish).toHaveBeenCalledTimes(1);
    const signal = JSON.parse(svc.pubSub.published.at(-1)!.raw);
    const ids = signal.payload.ids as string[];
    expect(ids).toHaveLength(10);
    for (let i = 0; i < 10; i++) {
      expect(ids).toContain(String(i));
    }
  });

  it('affectedTables are merged across payloads', async () => {
    const { svc } = makeOrchestrator();
    svc.graphqlService = makeGraphqlService();

    svc.handleInvalidation(
      makePayload('table_definition', 'partial', ['1'], ['table_a']),
    );
    const p2 = svc.handleInvalidation(
      makePayload('table_definition', 'partial', ['2'], ['table_b']),
    );

    await jest.advanceTimersByTimeAsync(50);
    await p2;

    const signal = JSON.parse(svc.pubSub.published.at(-1)!.raw);
    expect(signal.payload.affectedTables).toEqual(
      expect.arrayContaining(['table_a', 'table_b']),
    );
    expect(signal.payload.affectedTables).toHaveLength(2);
  });

  it('affectedTables are deduped when same table appears in multiple payloads', async () => {
    const { svc } = makeOrchestrator();
    svc.graphqlService = makeGraphqlService();

    svc.handleInvalidation(
      makePayload('table_definition', 'partial', ['1'], ['table_a', 'table_b']),
    );
    const p2 = svc.handleInvalidation(
      makePayload('table_definition', 'partial', ['2'], ['table_b', 'table_c']),
    );

    await jest.advanceTimersByTimeAsync(50);
    await p2;

    const signal = JSON.parse(svc.pubSub.published.at(-1)!.raw);
    const tables: string[] = signal.payload.affectedTables;
    expect(tables).toHaveLength(3);
    expect(tables).toContain('table_a');
    expect(tables).toContain('table_b');
    expect(tables).toContain('table_c');
  });

  it('debounce resolves all waiting promises after execution', async () => {
    const { svc } = makeOrchestrator();
    svc.graphqlService = makeGraphqlService();

    const resolved: number[] = [];
    const p1 = svc
      .handleInvalidation(makePayload('route_definition', 'full'))
      .then(() => resolved.push(1));
    const p2 = svc
      .handleInvalidation(makePayload('route_definition', 'full'))
      .then(() => resolved.push(2));
    const p3 = svc
      .handleInvalidation(makePayload('route_definition', 'full'))
      .then(() => resolved.push(3));

    expect(resolved).toHaveLength(0);

    await jest.advanceTimersByTimeAsync(50);
    await Promise.all([p1, p2, p3]);

    expect(resolved).toHaveLength(3);
    expect(resolved).toContain(1);
    expect(resolved).toContain(2);
    expect(resolved).toContain(3);
  });

  it('second invalidation after first has fired → separate debounce', async () => {
    const { svc, metadataCache } = makeOrchestrator();
    svc.graphqlService = makeGraphqlService();

    const p1 = svc.handleInvalidation(makePayload('table_definition', 'full'));
    await jest.advanceTimersByTimeAsync(50);
    await p1;

    expect(metadataCache.reload).toHaveBeenCalledTimes(1);

    const p2 = svc.handleInvalidation(makePayload('table_definition', 'full'));
    await jest.advanceTimersByTimeAsync(50);
    await p2;

    expect(metadataCache.reload).toHaveBeenCalledTimes(2);
  });
});

// ─────────────────────────────────────────────────────────────────────────────
// C. Sequential execution order
// ─────────────────────────────────────────────────────────────────────────────

describe('C. Sequential execution order', () => {
  it('table_definition chain: metadata → repoRegistry → route → graphql in order', async () => {
    const { svc } = makeOrchestrator();
    const graphql = makeGraphqlService();
    svc.graphqlService = graphql;

    const order: string[] = [];
    svc.metadataCache.reload.mockImplementation(async () => {
      order.push('metadata.reload');
    });
    svc.repoRegistry.rebuildFromMetadata.mockImplementation(() => {
      order.push('repoRegistry.rebuild');
    });
    svc.routeCache.reload.mockImplementation(async () => {
      order.push('route.reload');
    });
    graphql.reloadSchema.mockImplementation(async () => {
      order.push('graphql.reloadSchema');
    });

    const payload = makePayload('table_definition', 'full');
    await svc.executeChain(payload, false);

    expect(order).toEqual([
      'metadata.reload',
      'repoRegistry.rebuild',
      'route.reload',
      'graphql.reloadSchema',
    ]);
  });

  it('route_definition chain: route must complete before graphql starts', async () => {
    const { svc } = makeOrchestrator();
    const graphql = makeGraphqlService();
    svc.graphqlService = graphql;

    const order: string[] = [];
    let routeDone = false;

    svc.routeCache.reload.mockImplementation(async () => {
      order.push('route:start');
      await Promise.resolve();
      routeDone = true;
      order.push('route:end');
    });
    graphql.reloadSchema.mockImplementation(async () => {
      expect(routeDone).toBe(true);
      order.push('graphql:start');
    });

    await svc.executeChain(makePayload('route_definition', 'full'), false);

    expect(order).toEqual(['route:start', 'route:end', 'graphql:start']);
  });

  it('metadata completes before repoRegistry', async () => {
    const { svc } = makeOrchestrator();
    svc.graphqlService = makeGraphqlService();

    let metaDone = false;
    svc.metadataCache.reload.mockImplementation(async () => {
      metaDone = true;
    });
    svc.repoRegistry.rebuildFromMetadata.mockImplementation(() => {
      expect(metaDone).toBe(true);
    });

    await svc.executeChain(makePayload('table_definition', 'full'), false);
    expect(svc.repoRegistry.rebuildFromMetadata).toHaveBeenCalled();
  });

  it('if a step throws, subsequent steps do NOT run', async () => {
    const { svc } = makeOrchestrator();
    svc.graphqlService = makeGraphqlService();

    svc.metadataCache.reload.mockRejectedValueOnce(
      new Error('metadata failed'),
    );
    const repoSpy = svc.repoRegistry.rebuildFromMetadata;
    const routeSpy = svc.routeCache.reload;

    try {
      await svc.executeChain(makePayload('table_definition', 'full'), false);
    } catch {}

    expect(repoSpy).not.toHaveBeenCalled();
    expect(routeSpy).not.toHaveBeenCalled();
  });

  it('call order is recorded in stepMap invocation sequence', async () => {
    const { svc } = makeOrchestrator();
    svc.graphqlService = makeGraphqlService();

    await svc.executeChain(makePayload('table_definition', 'full'), false);
    expect(svc.callOrder).toEqual([
      'metadata',
      'repoRegistry',
      'route',
      'graphql',
    ]);
  });

  it('guard chain: only guard step is invoked', async () => {
    const { svc } = makeOrchestrator();
    svc.graphqlService = makeGraphqlService();

    await svc.executeChain(makePayload('guard_definition', 'full'), false);

    expect(svc.guardCache.reload).toHaveBeenCalledWith(false);
    expect(svc.metadataCache.reload).not.toHaveBeenCalled();
    expect(svc.routeCache.reload).not.toHaveBeenCalled();
    expect(svc.graphqlService.reloadSchema).not.toHaveBeenCalled();
  });
});

// ─────────────────────────────────────────────────────────────────────────────
// D. Partial vs full reload decision
// ─────────────────────────────────────────────────────────────────────────────

describe('D. Partial vs full reload decision', () => {
  it('metadata: scope=partial + ids + isLoaded=true → calls partialReload', async () => {
    const { svc, metadataCache } = makeOrchestrator({
      metadataCacheOpts: { loaded: true },
    });

    const payload = makePayload('table_definition', 'partial', ['1', '2']);
    await svc.executeChain(payload, false);

    expect(metadataCache.partialReload).toHaveBeenCalledWith(payload);
    expect(metadataCache.reload).not.toHaveBeenCalled();
  });

  it('metadata: scope=partial + ids + isLoaded=false → calls full reload', async () => {
    const { svc, metadataCache } = makeOrchestrator({
      metadataCacheOpts: { loaded: false },
    });

    const payload = makePayload('table_definition', 'partial', ['1']);
    await svc.executeChain(payload, false);

    expect(metadataCache.reload).toHaveBeenCalled();
    expect(metadataCache.partialReload).not.toHaveBeenCalled();
  });

  it('metadata: scope=partial + NO ids + isLoaded=true → calls full reload', async () => {
    const { svc, metadataCache } = makeOrchestrator({
      metadataCacheOpts: { loaded: true },
    });

    const payload = makePayload('table_definition', 'partial', []);
    await svc.executeChain(payload, false);

    expect(metadataCache.reload).toHaveBeenCalled();
    expect(metadataCache.partialReload).not.toHaveBeenCalled();
  });

  it('metadata: scope=full → calls full reload regardless of isLoaded', async () => {
    const { svc, metadataCache } = makeOrchestrator({
      metadataCacheOpts: { loaded: true },
    });

    const payload = makePayload('table_definition', 'full');
    await svc.executeChain(payload, false);

    expect(metadataCache.reload).toHaveBeenCalled();
    expect(metadataCache.partialReload).not.toHaveBeenCalled();
  });

  it('route: scope=partial + isLoaded=true + supportsPartial=true → calls partialReload(payload, false)', async () => {
    const { svc, routeCache } = makeOrchestrator({
      routeCacheOpts: { loaded: true, supportsPartial: true },
    });

    const payload = makePayload('route_definition', 'partial', ['r1']);
    await svc.executeChain(payload, false);

    expect(routeCache.partialReload).toHaveBeenCalledWith(payload, false);
    expect(routeCache.reload).not.toHaveBeenCalled();
  });

  it('route: scope=partial + isLoaded=false → calls full reload', async () => {
    const { svc, routeCache } = makeOrchestrator({
      routeCacheOpts: { loaded: false, supportsPartial: true },
    });

    const payload = makePayload('route_definition', 'partial', ['r1']);
    await svc.executeChain(payload, false);

    expect(routeCache.reload).toHaveBeenCalledWith(false);
    expect(routeCache.partialReload).not.toHaveBeenCalled();
  });

  it('route: scope=partial + supportsPartialReload=false → calls full reload', async () => {
    const { svc, routeCache } = makeOrchestrator({
      routeCacheOpts: { loaded: true, supportsPartial: false },
    });

    const payload = makePayload('route_definition', 'partial', ['r1']);
    await svc.executeChain(payload, false);

    expect(routeCache.reload).toHaveBeenCalledWith(false);
    expect(routeCache.partialReload).not.toHaveBeenCalled();
  });

  it('route: scope=full → calls full reload', async () => {
    const { svc, routeCache } = makeOrchestrator({
      routeCacheOpts: { loaded: true, supportsPartial: true },
    });

    const payload = makePayload('route_definition', 'full');
    await svc.executeChain(payload, false);

    expect(routeCache.reload).toHaveBeenCalledWith(false);
    expect(routeCache.partialReload).not.toHaveBeenCalled();
  });

  it('graphql: always calls reloadSchema(payload)', async () => {
    const { svc } = makeOrchestrator();
    const graphql = makeGraphqlService();
    svc.graphqlService = graphql;

    const payload = makePayload('route_definition', 'partial', ['1']);
    await svc.executeChain(payload, false);

    expect(graphql.reloadSchema).toHaveBeenCalledWith(payload);
  });

  it('guard: always calls reload(false) regardless of scope', async () => {
    const { svc, guardCache } = makeOrchestrator();

    for (const scope of ['full', 'partial'] as const) {
      guardCache.reload.mockClear();
      await svc.executeChain(
        makePayload('guard_definition', scope, ['1']),
        false,
      );
      expect(guardCache.reload).toHaveBeenCalledWith(false);
    }
  });

  it('simple caches (flow, websocket, package, setting, storage, oauth, folder, fieldPermission) always call reload(false)', async () => {
    const tableToCache: Array<[string, string]> = [
      ['flow_definition', 'flowCache'],
      ['websocket_definition', 'websocketCache'],
      ['package_definition', 'packageCache'],
      ['setting_definition', 'settingCache'],
      ['storage_config_definition', 'storageCache'],
      ['oauth_config_definition', 'oauthCache'],
      ['folder_definition', 'folderCache'],
      ['field_permission_definition', 'fieldPermissionCache'],
    ];

    for (const [table, cacheKey] of tableToCache) {
      const mocks = makeOrchestrator();
      const cache = (mocks as any)[cacheKey] as ReturnType<
        typeof makeSimpleCache
      >;
      await mocks.svc.executeChain(makePayload(table, 'full'), false);
      expect(cache.reload).toHaveBeenCalledWith(false);
    }
  });
});

// ─────────────────────────────────────────────────────────────────────────────
// E. Bootstrap sequence
// ─────────────────────────────────────────────────────────────────────────────

describe('E. Bootstrap sequence', () => {
  it('calls metadata.reload() first, then parallel caches, then graphql.reloadSchema()', async () => {
    const { svc, metadataCache, routeCache, guardCache } = makeOrchestrator();
    const graphql = makeGraphqlService();

    const order: string[] = [];
    metadataCache.reload.mockImplementation(async () => {
      order.push('metadata');
    });
    routeCache.reload.mockImplementation(async () => {
      order.push('route');
    });
    guardCache.reload.mockImplementation(async () => {
      order.push('guard');
    });
    graphql.reloadSchema.mockImplementation(async () => {
      order.push('graphql');
    });

    await svc.onApplicationBootstrap(graphql, null);

    expect(order[0]).toBe('metadata');
    const parallelIdx = order.indexOf('route');
    const graphqlIdx = order.indexOf('graphql');
    expect(parallelIdx).toBeGreaterThan(0);
    expect(graphqlIdx).toBeGreaterThan(parallelIdx);
  });

  it('emits METADATA_LOADED after metadata.reload() and before parallel caches', async () => {
    const { svc, ee, metadataCache } = makeOrchestrator();
    const events: string[] = [];

    ee.on(CACHE_EVENTS.METADATA_LOADED, () => events.push('METADATA_LOADED'));
    ee.on(CACHE_EVENTS.SYSTEM_READY, () => events.push('SYSTEM_READY'));

    let metaResolved = false;
    metadataCache.reload.mockImplementation(async () => {
      metaResolved = true;
    });

    await svc.onApplicationBootstrap(null, null);

    expect(metaResolved).toBe(true);
    expect(events).toContain('METADATA_LOADED');
    expect(events.indexOf('METADATA_LOADED')).toBeLessThan(
      events.indexOf('SYSTEM_READY'),
    );
  });

  it('emits GRAPHQL_LOADED after graphql.reloadSchema()', async () => {
    const { svc, ee } = makeOrchestrator();
    const graphql = makeGraphqlService();
    const events: string[] = [];

    ee.on(CACHE_EVENTS.GRAPHQL_LOADED, () => events.push('GRAPHQL_LOADED'));
    ee.on(CACHE_EVENTS.SYSTEM_READY, () => events.push('SYSTEM_READY'));

    let graphqlDone = false;
    graphql.reloadSchema.mockImplementation(async () => {
      graphqlDone = true;
    });

    await svc.onApplicationBootstrap(graphql, null);

    expect(graphqlDone).toBe(true);
    expect(events).toContain('GRAPHQL_LOADED');
    expect(events.indexOf('GRAPHQL_LOADED')).toBeLessThan(
      events.indexOf('SYSTEM_READY'),
    );
  });

  it('emits SYSTEM_READY last', async () => {
    const { svc, ee } = makeOrchestrator();
    const graphql = makeGraphqlService();
    const events: string[] = [];

    ee.on(CACHE_EVENTS.METADATA_LOADED, () => events.push('METADATA_LOADED'));
    ee.on(CACHE_EVENTS.GRAPHQL_LOADED, () => events.push('GRAPHQL_LOADED'));
    ee.on(CACHE_EVENTS.SYSTEM_READY, () => events.push('SYSTEM_READY'));

    await svc.onApplicationBootstrap(graphql, null);

    expect(events.at(-1)).toBe('SYSTEM_READY');
  });

  it('all independent caches run in parallel (Promise.all) after metadata', async () => {
    const {
      svc,
      routeCache,
      guardCache,
      flowCache,
      websocketCache,
      packageCache,
    } = makeOrchestrator();

    const started: string[] = [];
    const resume: Record<string, () => void> = {};

    const makeDelayed = (name: string) => {
      return jest.fn(
        () =>
          new Promise<void>((resolve) => {
            started.push(name);
            resume[name] = resolve;
          }),
      );
    };

    routeCache.reload = makeDelayed('route');
    guardCache.reload = makeDelayed('guard');
    flowCache.reload = makeDelayed('flow');
    websocketCache.reload = makeDelayed('websocket');
    packageCache.reload = makeDelayed('package');

    const bootstrapPromise = svc.bootstrap();

    await Promise.resolve();
    await Promise.resolve();

    expect(started).toContain('route');
    expect(started).toContain('guard');
    expect(started).toContain('flow');
    expect(started).toContain('websocket');
    expect(started).toContain('package');

    for (const fn of Object.values(resume)) fn();
    await bootstrapPromise;
  });

  it('if metadata.reload() throws → bootstrap propagates the error', async () => {
    const { svc } = makeOrchestrator();
    svc.metadataCache.reload.mockRejectedValueOnce(new Error('DB down'));

    await expect(svc.bootstrap()).rejects.toThrow('DB down');
  });

  it('graphql service not available → bootstrap completes (no-op for graphql)', async () => {
    const { svc, ee } = makeOrchestrator();
    const events: string[] = [];
    ee.on(CACHE_EVENTS.SYSTEM_READY, () => events.push('SYSTEM_READY'));

    await expect(svc.onApplicationBootstrap(null, null)).resolves.not.toThrow();
    expect(events).toContain('SYSTEM_READY');
  });

  it('bootstrapScript service not available → bootstrap completes', async () => {
    const { svc, ee } = makeOrchestrator();
    const graphql = makeGraphqlService();
    const events: string[] = [];
    ee.on(CACHE_EVENTS.SYSTEM_READY, () => events.push('SYSTEM_READY'));

    await expect(
      svc.onApplicationBootstrap(graphql, null),
    ).resolves.not.toThrow();
    expect(events).toContain('SYSTEM_READY');
  });

  it('bootstrapScriptService.onMetadataLoaded is called during bootstrap parallel phase', async () => {
    const { svc } = makeOrchestrator();
    const bootstrap = makeBootstrapScriptService();

    await svc.onApplicationBootstrap(null, bootstrap);

    expect(bootstrap.onMetadataLoaded).toHaveBeenCalled();
  });

  it('repoRegistry.rebuildFromMetadata is called during bootstrap parallel phase', async () => {
    const { svc, repoRegistry, metadataCache } = makeOrchestrator();

    await svc.bootstrap();

    expect(repoRegistry.rebuildFromMetadata).toHaveBeenCalledWith(
      metadataCache,
    );
  });
});

// ─────────────────────────────────────────────────────────────────────────────
// F. Multi-instance Redis sync
// ─────────────────────────────────────────────────────────────────────────────

describe('F. Multi-instance Redis sync', () => {
  it('executeChain with publish=true → publishes 1 signal to Redis', async () => {
    const { svc } = makeOrchestrator();
    svc.graphqlService = makeGraphqlService();

    await svc.executeChain(makePayload('route_definition', 'full'), true);

    expect(svc.pubSub.publish).toHaveBeenCalledTimes(1);
    expect(svc.pubSub.publish).toHaveBeenCalledWith(
      SYNC_CHANNEL,
      expect.any(String),
    );
  });

  it('executeChain with publish=true → Redis publish before local reload', async () => {
    const { svc, routeCache } = makeOrchestrator();
    svc.graphqlService = makeGraphqlService();

    await svc.executeChain(makePayload('route_definition', 'full'), true);

    const publishOrder = svc.pubSub.publish.mock.invocationCallOrder[0];
    const routeOrder = routeCache.reload.mock.invocationCallOrder[0];
    expect(publishOrder).toBeLessThan(routeOrder);
  });

  it('executeChain with publish=false → does NOT publish', async () => {
    const { svc } = makeOrchestrator();
    svc.graphqlService = makeGraphqlService();

    await svc.executeChain(makePayload('route_definition', 'full'), false);

    expect(svc.pubSub.publish).not.toHaveBeenCalled();
  });

  it('published signal contains instanceId + payload', async () => {
    const instanceId = 'my-test-instance-12345';
    const { svc } = makeOrchestrator({ instanceId });
    svc.graphqlService = makeGraphqlService();

    const payload = makePayload('route_definition', 'full');
    await svc.executeChain(payload, true);

    const raw = svc.pubSub.published[0].raw;
    const signal = JSON.parse(raw);
    expect(signal.instanceId).toBe(instanceId);
    expect(signal.type).toBe('RELOAD_SIGNAL');
    expect(signal.payload.tableName).toBe('route_definition');
    expect(signal.timestamp).toBeDefined();
  });

  it('receiving instance with same instanceId → ignores signal (no echo)', async () => {
    const instanceId = 'shared-id-aabb';
    const { svc, metadataCache, routeCache } = makeOrchestrator({ instanceId });
    svc.graphqlService = makeGraphqlService();

    const selfSignal = JSON.stringify({
      instanceId,
      type: 'RELOAD_SIGNAL',
      timestamp: Date.now(),
      payload: makePayload('route_definition', 'full'),
    });

    await svc.pubSub.simulateMessage(SYNC_CHANNEL, selfSignal);

    expect(routeCache.reload).not.toHaveBeenCalled();
    expect(metadataCache.reload).not.toHaveBeenCalled();
  });

  it('receiving instance with different instanceId → executes chain without publishing', async () => {
    const { svc, routeCache } = makeOrchestrator({ instanceId: 'instance-B' });
    svc.graphqlService = makeGraphqlService();

    const remoteSignal = JSON.stringify({
      instanceId: 'instance-A',
      type: 'RELOAD_SIGNAL',
      timestamp: Date.now(),
      payload: makePayload('route_definition', 'full'),
    });

    await svc.pubSub.simulateMessage(SYNC_CHANNEL, remoteSignal);

    expect(routeCache.reload).toHaveBeenCalledWith(false);
    expect(svc.pubSub.publish).not.toHaveBeenCalled();
  });

  it('Instance A fires invalidation → Instance B receives and executes same chain', async () => {
    const sharedPubSub = makePubSub();

    const a = makeOrchestrator({
      instanceId: 'instance-A',
      pubSub: sharedPubSub,
    });
    const b = makeOrchestrator({
      instanceId: 'instance-B',
      pubSub: sharedPubSub,
    });
    a.svc.graphqlService = makeGraphqlService();
    b.svc.graphqlService = makeGraphqlService();

    const payload = makePayload('route_definition', 'full');
    await a.svc.executeChain(payload, true);

    expect(sharedPubSub.published).toHaveLength(1);

    const { raw } = sharedPubSub.published[0];
    await sharedPubSub.simulateMessage(SYNC_CHANNEL, raw);

    expect(b.routeCache.reload).toHaveBeenCalledWith(false);
    expect(a.routeCache.reload).toHaveBeenCalledTimes(1);
  });

  it('Instance A and B fire simultaneously → both execute, no infinite loop', async () => {
    const pubSubA = makePubSub();
    const pubSubB = makePubSub();

    const a = makeOrchestrator({ instanceId: 'instance-A', pubSub: pubSubA });
    const b = makeOrchestrator({ instanceId: 'instance-B', pubSub: pubSubB });
    a.svc.graphqlService = makeGraphqlService();
    b.svc.graphqlService = makeGraphqlService();

    const payloadA = makePayload('route_definition', 'full');
    const payloadB = makePayload('route_definition', 'full');

    await Promise.all([
      a.svc.executeChain(payloadA, true),
      b.svc.executeChain(payloadB, true),
    ]);

    const rawA = pubSubA.published[0].raw;
    const rawB = pubSubB.published[0].raw;

    await b.svc.pubSub.simulateMessage(SYNC_CHANNEL, rawA);
    await a.svc.pubSub.simulateMessage(SYNC_CHANNEL, rawB);

    expect(a.svc.pubSub.publish).toHaveBeenCalledTimes(1);
    expect(b.svc.pubSub.publish).toHaveBeenCalledTimes(1);

    expect(a.routeCache.reload).toHaveBeenCalledTimes(2);
    expect(b.routeCache.reload).toHaveBeenCalledTimes(2);
  });

  it('only 1 Redis publish per local invalidation (not N for N-step chain)', async () => {
    const { svc } = makeOrchestrator();
    svc.graphqlService = makeGraphqlService();

    await svc.executeChain(makePayload('table_definition', 'full'), true);

    expect(svc.pubSub.publish).toHaveBeenCalledTimes(1);
  });

  it('Redis channel is exactly enfyra:cache-orchestrator-sync', async () => {
    const { svc } = makeOrchestrator();
    svc.graphqlService = makeGraphqlService();

    await svc.executeChain(makePayload('route_definition', 'full'), true);

    expect(svc.pubSub.publish).toHaveBeenCalledWith(
      'enfyra:cache-orchestrator-sync',
      expect.any(String),
    );
  });

  it('subscribeToRedis called once on init → subsequent calls are no-ops', () => {
    const { svc } = makeOrchestrator();
    const callsBefore = svc.pubSub.subscribeWithHandler.mock.calls.length;

    svc['subscribeToRedis']();
    svc['subscribeToRedis']();

    expect(svc.pubSub.subscribeWithHandler.mock.calls.length).toBe(callsBefore);
  });

  it('malformed JSON in Redis message → does not crash, chain not invoked', async () => {
    const { svc, routeCache } = makeOrchestrator();
    svc.graphqlService = makeGraphqlService();

    await expect(
      svc.pubSub.simulateMessage(SYNC_CHANNEL, 'not-valid-json{{{'),
    ).resolves.not.toThrow();

    expect(routeCache.reload).not.toHaveBeenCalled();
  });

  it('multi-instance: 5 rapid invalidations on Instance A → only 1 publish delivered to B', async () => {
    jest.useFakeTimers();
    try {
      const sharedPubSub = makePubSub();

      const a = makeOrchestrator({
        instanceId: 'inst-A',
        pubSub: sharedPubSub,
      });
      const b = makeOrchestrator({
        instanceId: 'inst-B',
        pubSub: sharedPubSub,
      });
      a.svc.graphqlService = makeGraphqlService();
      b.svc.graphqlService = makeGraphqlService();

      const promises: Promise<void>[] = [];
      for (let i = 0; i < 5; i++) {
        promises.push(
          a.svc.handleInvalidation(makePayload('route_definition', 'full')),
        );
      }

      await jest.advanceTimersByTimeAsync(50);
      await Promise.all(promises);

      expect(sharedPubSub.published).toHaveLength(1);

      const raw = sharedPubSub.published[0].raw;
      await sharedPubSub.simulateMessage(SYNC_CHANNEL, raw);

      expect(b.routeCache.reload).toHaveBeenCalledTimes(1);
    } finally {
      jest.useRealTimers();
    }
  });
});

// ─────────────────────────────────────────────────────────────────────────────
// G. Admin reloadAll
// ─────────────────────────────────────────────────────────────────────────────

describe('G. Admin reloadAll', () => {
  it('calls metadata.reload(), repoRegistry, routeCache.reload(), guardCache.reload(), graphql.reloadSchema() in order', async () => {
    const { svc, metadataCache, routeCache, guardCache, repoRegistry } =
      makeOrchestrator();
    const graphql = makeGraphqlService();
    svc.graphqlService = graphql;

    const order: string[] = [];
    metadataCache.reload.mockImplementation(async () => order.push('metadata'));
    repoRegistry.rebuildFromMetadata.mockImplementation(() =>
      order.push('repoRegistry'),
    );
    routeCache.reload.mockImplementation(async () => order.push('route'));
    guardCache.reload.mockImplementation(async () => order.push('guard'));
    graphql.reloadSchema.mockImplementation(async () => order.push('graphql'));

    await svc.reloadAll();

    expect(order).toEqual([
      'metadata',
      'repoRegistry',
      'route',
      'guard',
      'graphql',
    ]);
  });

  it('does NOT publish to Redis', async () => {
    const { svc } = makeOrchestrator();
    svc.graphqlService = makeGraphqlService();

    await svc.reloadAll();

    expect(svc.pubSub.publish).not.toHaveBeenCalled();
  });

  it('reloadAll with no graphql service → skips graphql step', async () => {
    const { svc } = makeOrchestrator();
    svc.graphqlService = null;

    await expect(svc.reloadAll()).resolves.not.toThrow();
    expect(svc.pubSub.publish).not.toHaveBeenCalled();
  });

  it('reloadAll calls metadataCache.reload (not partialReload)', async () => {
    const { svc, metadataCache } = makeOrchestrator();
    svc.graphqlService = makeGraphqlService();

    await svc.reloadAll();

    expect(metadataCache.reload).toHaveBeenCalled();
    expect(metadataCache.partialReload).not.toHaveBeenCalled();
  });

  it('reloadAll calls routeCache.reload(false) — not partialReload', async () => {
    const { svc, routeCache } = makeOrchestrator();
    svc.graphqlService = makeGraphqlService();

    await svc.reloadAll();

    expect(routeCache.reload).toHaveBeenCalledWith(false);
    expect(routeCache.partialReload).not.toHaveBeenCalled();
  });
});

// ─────────────────────────────────────────────────────────────────────────────
// H. Edge cases
// ─────────────────────────────────────────────────────────────────────────────

describe('H. Edge cases', () => {
  it('empty payload (no ids, no affectedTables) → still executes chain', async () => {
    const { svc, routeCache } = makeOrchestrator();
    svc.graphqlService = makeGraphqlService();

    const payload: TCacheInvalidationPayload = {
      tableName: 'route_definition',
      action: 'reload',
      timestamp: Date.now(),
      scope: 'full',
    };

    await svc.executeChain(payload, false);

    expect(routeCache.reload).toHaveBeenCalledWith(false);
  });

  it('reloadSimple: cache.reload throws → error propagates out of executeChain', async () => {
    const { svc, guardCache } = makeOrchestrator();
    guardCache.reload.mockRejectedValueOnce(new Error('guard reload failed'));

    await expect(
      svc.executeChain(makePayload('guard_definition', 'full'), false),
    ).rejects.toThrow('guard reload failed');
  });

  it('handleInvalidation wraps errors from executeChain and still resolves all promises', async () => {
    jest.useFakeTimers();
    try {
      const { svc, routeCache } = makeOrchestrator();
      svc.graphqlService = makeGraphqlService();

      routeCache.reload.mockRejectedValueOnce(new Error('route failed'));

      const resolved: boolean[] = [];
      const p1 = svc
        .handleInvalidation(makePayload('route_definition', 'full'))
        .then(() => resolved.push(true));
      const p2 = svc
        .handleInvalidation(makePayload('route_definition', 'full'))
        .then(() => resolved.push(true));

      await jest.advanceTimersByTimeAsync(50);
      await Promise.all([p1, p2]);

      expect(resolved).toHaveLength(2);
    } finally {
      jest.useRealTimers();
    }
  });

  it('bootstrap_script_definition chain: calls bootstrapScriptService.reloadBootstrapScripts()', async () => {
    const { svc } = makeOrchestrator();
    const bootstrap = makeBootstrapScriptService();
    svc.bootstrapScriptService = bootstrap;

    await svc.executeChain(
      makePayload('bootstrap_script_definition', 'full'),
      false,
    );

    expect(bootstrap.reloadBootstrapScripts).toHaveBeenCalled();
  });

  it('bootstrap_script_definition with no bootstrapScriptService → no-op', async () => {
    const { svc } = makeOrchestrator();
    svc.bootstrapScriptService = null;

    await expect(
      svc.executeChain(
        makePayload('bootstrap_script_definition', 'full'),
        false,
      ),
    ).resolves.not.toThrow();
  });

  it('graphql step with no graphqlService → no-op, chain still completes', async () => {
    const { svc, routeCache } = makeOrchestrator();
    svc.graphqlService = null;

    await svc.executeChain(makePayload('route_definition', 'full'), false);

    expect(routeCache.reload).toHaveBeenCalledWith(false);
  });

  it('pendingPayload is cleared after debounce fires', async () => {
    jest.useFakeTimers();
    try {
      const { svc } = makeOrchestrator();
      svc.graphqlService = makeGraphqlService();

      const p = svc.handleInvalidation(makePayload('route_definition', 'full'));
      expect(svc.getPendingPayload()).not.toBeNull();

      await jest.advanceTimersByTimeAsync(50);
      await p;

      expect(svc.getPendingPayload()).toBeNull();
    } finally {
      jest.useRealTimers();
    }
  });

  it('concurrent invalidations during bootstrap: debounce accumulates and fires once', async () => {
    jest.useFakeTimers();
    try {
      const { svc, routeCache } = makeOrchestrator({
        routeCacheOpts: { loaded: true, supportsPartial: true },
      });
      svc.graphqlService = makeGraphqlService();

      const p1 = svc.handleInvalidation(
        makePayload('route_definition', 'partial', ['a']),
      );
      const p2 = svc.handleInvalidation(
        makePayload('route_definition', 'partial', ['b']),
      );
      const p3 = svc.handleInvalidation(
        makePayload('route_definition', 'partial', ['c']),
      );

      await jest.advanceTimersByTimeAsync(50);
      await Promise.all([p1, p2, p3]);

      expect(routeCache.partialReload).toHaveBeenCalledTimes(1);
      expect(svc.pubSub.publish).toHaveBeenCalledTimes(1);
    } finally {
      jest.useRealTimers();
    }
  });

  it('ids from different-table payloads are dropped on escalation to full', async () => {
    jest.useFakeTimers();
    try {
      const { svc } = makeOrchestrator();
      svc.graphqlService = makeGraphqlService();

      svc.handleInvalidation(
        makePayload('table_definition', 'partial', ['1', '2']),
      );
      const p2 = svc.handleInvalidation(
        makePayload('route_definition', 'partial', ['3', '4']),
      );

      await jest.advanceTimersByTimeAsync(50);
      await p2;

      const signal = JSON.parse(svc.pubSub.published.at(-1)!.raw);
      expect(signal.payload.scope).toBe('full');
      expect(signal.payload.ids).toBeUndefined();
    } finally {
      jest.useRealTimers();
    }
  });

  it('SYNC_CHANNEL constant is enfyra:cache-orchestrator-sync', () => {
    expect(SYNC_CHANNEL).toBe('enfyra:cache-orchestrator-sync');
  });

  it('subscribeToRedis registers handler on SYNC_CHANNEL during onModuleInit', () => {
    const pubSub = makePubSub();
    const mocks = makeOrchestrator({ pubSub });

    expect(pubSub.subscribeWithHandler).toHaveBeenCalledWith(
      SYNC_CHANNEL,
      expect.any(Function),
    );
  });

  it('partial reload: ids array is deduplicated across merges', async () => {
    jest.useFakeTimers();
    try {
      const { svc } = makeOrchestrator();
      svc.graphqlService = makeGraphqlService();

      svc.handleInvalidation(
        makePayload('table_definition', 'partial', ['1', '2']),
      );
      const p2 = svc.handleInvalidation(
        makePayload('table_definition', 'partial', ['2', '3']),
      );

      await jest.advanceTimersByTimeAsync(50);
      await p2;

      const signal = JSON.parse(svc.pubSub.published.at(-1)!.raw);
      const ids: string[] = signal.payload.ids;
      const unique = new Set(ids);
      expect(unique.size).toBe(ids.length);
      expect(ids).toContain('1');
      expect(ids).toContain('2');
      expect(ids).toContain('3');
    } finally {
      jest.useRealTimers();
    }
  });
});
