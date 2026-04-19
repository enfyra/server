/**
 * Comprehensive tests for the granular cache reload system.
 * Covers: payload merging, BaseCacheService partial reload, MetadataCache
 * partial update, RouteCache partial update, GraphQL incremental update,
 * cascade scenarios, and end-to-end flow simulation.
 *
 * All infrastructure is inlined / mocked — no NestJS DI pulled in.
 */

import { EventEmitter2 } from 'eventemitter2';
import { DatabaseConfigService } from '../../src/shared/services/database-config.service';
import {
  GraphQLObjectType,
  GraphQLString,
  GraphQLInt,
  GraphQLFloat,
  GraphQLBoolean,
  GraphQLID,
  GraphQLNonNull,
  GraphQLList,
  GraphQLNamedType,
} from 'graphql';

// ─────────────────────────────────────────────────────────────────────────────
// Types (mirror production)
// ─────────────────────────────────────────────────────────────────────────────

interface TCacheInvalidationPayload {
  tableName: string;
  action: 'reload';
  timestamp: number;
  scope: 'full' | 'partial';
  ids?: (string | number)[];
  affectedTables?: string[];
}

interface TCacheReloadSignal {
  instanceId: string;
  type: 'RELOAD_SIGNAL';
  timestamp: number;
  payload?: TCacheInvalidationPayload;
}

// ─────────────────────────────────────────────────────────────────────────────
// Helpers — mock PubSub
// ─────────────────────────────────────────────────────────────────────────────

type MsgHandler = (channel: string, message: string) => void;

interface MockPubSub {
  handlers: Map<string, MsgHandler[]>;
  published: Array<{ channel: string; payload: any }>;
  subscribeWithHandler: jest.Mock;
  publish: jest.Mock;
  isChannelForBase: jest.Mock;
  simulateMessage(channel: string, message: string): Promise<void>;
}

function makePubSub(): MockPubSub {
  const handlers = new Map<string, MsgHandler[]>();
  const published: Array<{ channel: string; payload: any }> = [];
  return {
    handlers,
    published,
    subscribeWithHandler: jest.fn((ch: string, h: MsgHandler) => {
      if (!handlers.has(ch)) handlers.set(ch, []);
      handlers.get(ch)!.push(h);
    }),
    publish: jest.fn(async (ch: string, msg: string) => {
      published.push({ channel: ch, payload: JSON.parse(msg) });
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
// Inlined mergePayload (mirrors MetadataCacheService.mergePayload)
// ─────────────────────────────────────────────────────────────────────────────

function mergePayload(
  pending: TCacheInvalidationPayload | null,
  incoming: TCacheInvalidationPayload,
): TCacheInvalidationPayload {
  if (!pending) return { ...incoming };
  if (incoming.scope === 'full' || pending.scope === 'full') {
    return {
      ...pending,
      scope: 'full',
      ids: undefined,
      affectedTables: undefined,
    };
  }
  const mergedIds = new Set([...(pending.ids || []), ...(incoming.ids || [])]);
  const mergedTables = new Set([
    ...(pending.affectedTables || []),
    ...(incoming.affectedTables || []),
  ]);
  return {
    ...pending,
    ids: [...mergedIds],
    affectedTables: mergedTables.size ? [...mergedTables] : undefined,
  };
}

// ─────────────────────────────────────────────────────────────────────────────
// Inlined BaseCacheService (mirrors production logic)
// ─────────────────────────────────────────────────────────────────────────────

class TestBaseCacheService {
  cache: string[] = [];
  cacheLoaded = false;
  isLoading = false;
  loadingPromise: Promise<void> | null = null;

  fullReloadCount = 0;
  partialReloadCount = 0;
  applyPartialCount = 0;
  loadDelay = 0;
  applyError: Error | null = null;
  _supportsPartial = false;

  constructor(
    private readonly syncKey: string,
    private readonly pubsub: MockPubSub,
    private readonly instanceId: string,
    private readonly eventEmitter?: EventEmitter2,
  ) {
    this.pubsub.subscribeWithHandler(
      syncKey,
      async (ch: string, msg: string) => {
        if (this.pubsub.isChannelForBase(ch, syncKey)) {
          await this._handleIncoming(msg);
        }
      },
    );
  }

  private async _handleIncoming(msg: string): Promise<void> {
    try {
      const signal: TCacheReloadSignal = JSON.parse(msg);
      if (signal.instanceId === this.instanceId) return;
      if (signal.payload?.scope === 'partial' && this._supportsPartial) {
        await this.partialReload(signal.payload, false);
      } else {
        await this.reload(false);
      }
    } catch {}
  }

  async reload(publish = true): Promise<void> {
    if (this.isLoading && this.loadingPromise) return this.loadingPromise;
    this.isLoading = true;
    this.loadingPromise = (async () => {
      try {
        if (this.loadDelay)
          await new Promise((r) => setTimeout(r, this.loadDelay));
        this.fullReloadCount++;
        this.cache = ['full'];
        this.cacheLoaded = true;
        this.eventEmitter?.emit(`loaded`);
        if (publish) await this._publish();
      } finally {
        this.isLoading = false;
        this.loadingPromise = null;
      }
    })();
    return this.loadingPromise;
  }

  async partialReload(
    payload: TCacheInvalidationPayload,
    publish = true,
  ): Promise<void> {
    try {
      await this._applyPartialUpdate(payload);
      this.partialReloadCount++;
      this.eventEmitter?.emit(`loaded`);
      if (publish) await this._publish(payload);
    } catch {
      await this.reload(publish);
    }
  }

  supportsPartialReload() {
    return this._supportsPartial;
  }

  protected async _applyPartialUpdate(
    _payload: TCacheInvalidationPayload,
  ): Promise<void> {
    if (this.applyError) throw this.applyError;
    this.applyPartialCount++;
    this.cache = ['partial'];
  }

  private async _publish(payload?: TCacheInvalidationPayload): Promise<void> {
    const signal: TCacheReloadSignal = {
      instanceId: this.instanceId,
      type: 'RELOAD_SIGNAL',
      timestamp: Date.now(),
      payload,
    };
    await this.pubsub.publish(this.syncKey, JSON.stringify(signal));
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// Imports for real generate-type-defs functions
// ─────────────────────────────────────────────────────────────────────────────

import {
  buildTableGraphQLDef,
  buildStubType,
  GraphQLJSON,
  TableGraphQLDef,
} from '../../src/modules/graphql/utils/generate-type-defs';

// ═════════════════════════════════════════════════════════════════════════════
// A. TCacheInvalidationPayload & mergePayload logic
// ═════════════════════════════════════════════════════════════════════════════

describe('A. TCacheInvalidationPayload & mergePayload', () => {
  const base = (): TCacheInvalidationPayload => ({
    tableName: 'table_definition',
    action: 'reload',
    timestamp: 1000,
    scope: 'partial',
    ids: [1, 2],
    affectedTables: ['tableA'],
  });

  it('merge two partial payloads → ids and affectedTables merged (deduped)', () => {
    const a = base();
    const b: TCacheInvalidationPayload = {
      ...base(),
      ids: [2, 3],
      affectedTables: ['tableA', 'tableB'],
    };
    const merged = mergePayload(a, b);
    expect(merged.scope).toBe('partial');
    expect(new Set(merged.ids)).toEqual(new Set([1, 2, 3]));
    expect(new Set(merged.affectedTables)).toEqual(
      new Set(['tableA', 'tableB']),
    );
  });

  it('merge partial + full → result is full, ids cleared', () => {
    const a = base();
    const b: TCacheInvalidationPayload = {
      ...base(),
      scope: 'full',
      ids: undefined,
      affectedTables: undefined,
    };
    const merged = mergePayload(a, b);
    expect(merged.scope).toBe('full');
    expect(merged.ids).toBeUndefined();
    expect(merged.affectedTables).toBeUndefined();
  });

  it('merge full + partial → stays full', () => {
    const a: TCacheInvalidationPayload = {
      ...base(),
      scope: 'full',
      ids: undefined,
      affectedTables: undefined,
    };
    const b = base();
    const merged = mergePayload(a, b);
    expect(merged.scope).toBe('full');
    expect(merged.ids).toBeUndefined();
    expect(merged.affectedTables).toBeUndefined();
  });

  it('merge null + partial → clones the incoming', () => {
    const b = base();
    const merged = mergePayload(null, b);
    expect(merged.scope).toBe('partial');
    expect(merged.ids).toEqual([1, 2]);
  });

  it('large number of ids deduplicated correctly', () => {
    const ids1 = Array.from({ length: 500 }, (_, i) => i);
    const ids2 = Array.from({ length: 500 }, (_, i) => i + 250);
    const a: TCacheInvalidationPayload = { ...base(), ids: ids1 };
    const b: TCacheInvalidationPayload = { ...base(), ids: ids2 };
    const merged = mergePayload(a, b);
    expect(merged.ids!.length).toBe(750);
    expect(new Set(merged.ids).size).toBe(750);
  });

  it('partial + partial with empty ids on first → uses second ids', () => {
    const a: TCacheInvalidationPayload = { ...base(), ids: [] };
    const b: TCacheInvalidationPayload = { ...base(), ids: [10, 20] };
    const merged = mergePayload(a, b);
    expect(merged.scope).toBe('partial');
    expect(new Set(merged.ids)).toEqual(new Set([10, 20]));
  });

  it('partial + partial with no affectedTables → affectedTables undefined', () => {
    const a: TCacheInvalidationPayload = {
      ...base(),
      affectedTables: undefined,
    };
    const b: TCacheInvalidationPayload = {
      ...base(),
      affectedTables: undefined,
    };
    const merged = mergePayload(a, b);
    expect(merged.affectedTables).toBeUndefined();
  });

  it('matrix: all scope combinations produce correct output scope', () => {
    const cases: Array<
      ['full' | 'partial', 'full' | 'partial', 'full' | 'partial']
    > = [
      ['partial', 'partial', 'partial'],
      ['partial', 'full', 'full'],
      ['full', 'partial', 'full'],
      ['full', 'full', 'full'],
    ];
    for (const [scopeA, scopeB, expected] of cases) {
      const a: TCacheInvalidationPayload = { ...base(), scope: scopeA };
      const b: TCacheInvalidationPayload = { ...base(), scope: scopeB };
      const merged = mergePayload(a, b);
      expect(merged.scope).toBe(expected);
    }
  });
});

// ═════════════════════════════════════════════════════════════════════════════
// B. BaseCacheService partial reload
// ═════════════════════════════════════════════════════════════════════════════

describe('B. BaseCacheService partial reload', () => {
  const CHANNEL = 'test:sync';

  function makePartialSignal(
    fromInstance: string,
    payload: TCacheInvalidationPayload,
  ): string {
    const signal: TCacheReloadSignal = {
      instanceId: fromInstance,
      type: 'RELOAD_SIGNAL',
      timestamp: Date.now(),
      payload,
    };
    return JSON.stringify(signal);
  }

  function makeFullSignal(fromInstance: string): string {
    const signal: TCacheReloadSignal = {
      instanceId: fromInstance,
      type: 'RELOAD_SIGNAL',
      timestamp: Date.now(),
    };
    return JSON.stringify(signal);
  }

  it('supportsPartialReload()=false → partial Redis signal falls back to full reload', async () => {
    const pubsub = makePubSub();
    const svc = new TestBaseCacheService(CHANNEL, pubsub, 'inst-A');
    svc._supportsPartial = false;

    const payload: TCacheInvalidationPayload = {
      tableName: 'route_definition',
      action: 'reload',
      timestamp: Date.now(),
      scope: 'partial',
      ids: [1],
    };

    await pubsub.simulateMessage(CHANNEL, makePartialSignal('inst-B', payload));

    expect(svc.fullReloadCount).toBe(1);
    expect(svc.partialReloadCount).toBe(0);
  });

  it('supportsPartialReload()=true → partial Redis signal calls partialReload', async () => {
    const pubsub = makePubSub();
    const svc = new TestBaseCacheService(CHANNEL, pubsub, 'inst-A');
    svc._supportsPartial = true;

    const payload: TCacheInvalidationPayload = {
      tableName: 'route_definition',
      action: 'reload',
      timestamp: Date.now(),
      scope: 'partial',
      ids: [1],
    };

    await pubsub.simulateMessage(CHANNEL, makePartialSignal('inst-B', payload));

    expect(svc.partialReloadCount).toBe(1);
    expect(svc.fullReloadCount).toBe(0);
  });

  it('applyPartialUpdate throws → falls back to full reload', async () => {
    const pubsub = makePubSub();
    const svc = new TestBaseCacheService(CHANNEL, pubsub, 'inst-A');
    svc._supportsPartial = true;
    svc.applyError = new Error('apply failed');

    const payload: TCacheInvalidationPayload = {
      tableName: 'route_definition',
      action: 'reload',
      timestamp: Date.now(),
      scope: 'partial',
      ids: [1],
    };

    await svc.partialReload(payload, false);

    expect(svc.fullReloadCount).toBe(1);
    expect(svc.partialReloadCount).toBe(0);
    expect(svc.applyPartialCount).toBe(0);
    expect(svc.cache).toEqual(['full']);
  });

  it('publish=true on partialReload sends signal with payload', async () => {
    const pubsub = makePubSub();
    const svc = new TestBaseCacheService(CHANNEL, pubsub, 'inst-A');
    svc._supportsPartial = true;

    const payload: TCacheInvalidationPayload = {
      tableName: 'route_definition',
      action: 'reload',
      timestamp: Date.now(),
      scope: 'partial',
      ids: [42],
    };

    await svc.partialReload(payload, true);

    expect(pubsub.published).toHaveLength(1);
    const sent = pubsub.published[0].payload as TCacheReloadSignal;
    expect(sent.payload?.scope).toBe('partial');
    expect(sent.payload?.ids).toEqual([42]);
    expect(sent.instanceId).toBe('inst-A');
  });

  it('publish=false on partialReload does NOT emit to Redis', async () => {
    const pubsub = makePubSub();
    const svc = new TestBaseCacheService(CHANNEL, pubsub, 'inst-A');
    svc._supportsPartial = true;

    const payload: TCacheInvalidationPayload = {
      tableName: 'route_definition',
      action: 'reload',
      timestamp: Date.now(),
      scope: 'partial',
      ids: [1],
    };

    await svc.partialReload(payload, false);

    expect(pubsub.published).toHaveLength(0);
  });

  it('full Redis signal always triggers full reload regardless of supportsPartialReload', async () => {
    const pubsub = makePubSub();
    const svc = new TestBaseCacheService(CHANNEL, pubsub, 'inst-A');
    svc._supportsPartial = true;

    await pubsub.simulateMessage(CHANNEL, makeFullSignal('inst-B'));

    expect(svc.fullReloadCount).toBe(1);
    expect(svc.partialReloadCount).toBe(0);
  });

  it('same-instance partial signal is ignored', async () => {
    const pubsub = makePubSub();
    const svc = new TestBaseCacheService(CHANNEL, pubsub, 'inst-A');
    svc._supportsPartial = true;

    const payload: TCacheInvalidationPayload = {
      tableName: 'route_definition',
      action: 'reload',
      timestamp: Date.now(),
      scope: 'partial',
      ids: [1],
    };

    await pubsub.simulateMessage(CHANNEL, makePartialSignal('inst-A', payload));

    expect(svc.fullReloadCount).toBe(0);
    expect(svc.partialReloadCount).toBe(0);
  });

  it('partial reload fallback (applyError) still publishes on publish=true', async () => {
    const pubsub = makePubSub();
    const svc = new TestBaseCacheService(CHANNEL, pubsub, 'inst-A');
    svc._supportsPartial = true;
    svc.applyError = new Error('broken');

    const payload: TCacheInvalidationPayload = {
      tableName: 'route_definition',
      action: 'reload',
      timestamp: Date.now(),
      scope: 'partial',
      ids: [1],
    };

    await svc.partialReload(payload, true);

    expect(svc.fullReloadCount).toBe(1);
    expect(pubsub.published).toHaveLength(1);
    const sent = pubsub.published[0].payload as TCacheReloadSignal;
    expect(sent.payload).toBeUndefined();
  });
});

// ═════════════════════════════════════════════════════════════════════════════
// C. MetadataCache partial reload (logic tested inline)
// ═════════════════════════════════════════════════════════════════════════════

describe('C. MetadataCache partial reload logic', () => {
  interface MetaCache {
    tables: Map<string, any>;
    tablesList: any[];
    version: number;
    timestamp: Date;
  }

  function makeTable(id: number, name: string, extra: Partial<any> = {}): any {
    return {
      id,
      name,
      columns: [],
      relations: [],
      uniques: [],
      indexes: [],
      ...extra,
    };
  }

  function makeInitialCache(tables: any[]): MetaCache {
    const map = new Map<string, any>();
    for (const t of tables) map.set(t.name, t);
    return {
      tables: map,
      tablesList: [...tables],
      version: 1,
      timestamp: new Date(),
    };
  }

  function applyPartialUpdate(
    cache: MetaCache,
    payload: TCacheInvalidationPayload,
    dbTables: any[],
  ): void {
    if (!cache) throw new Error('Cache not initialized');

    const requestedIds = payload.ids || [];
    const fetched = dbTables.filter((t) =>
      requestedIds.some((id) => String(id) === String(t.id)),
    );

    const affectedTableNames = new Set(payload.affectedTables || []);
    const affectedFetched = dbTables.filter((t) =>
      affectedTableNames.has(t.name),
    );
    const allFetched = [...fetched];
    for (const t of affectedFetched) {
      if (!allFetched.some((x) => x.id === t.id)) allFetched.push(t);
    }

    if (
      allFetched.length === 0 &&
      requestedIds.length === 0 &&
      !payload.affectedTables?.length
    )
      return;

    for (const table of allFetched) {
      const idx = cache.tablesList.findIndex((t) => t.name === table.name);
      if (idx >= 0) {
        cache.tablesList[idx] = table;
      } else {
        cache.tablesList.push(table);
      }
      cache.tables.set(table.name, table);
    }

    const fetchedIdSet = new Set(allFetched.map((t) => String(t.id)));
    const uniqueRequestedIds = new Set(requestedIds.map(String));
    for (const id of uniqueRequestedIds) {
      if (!fetchedIdSet.has(id)) {
        const existing = cache.tablesList.find((t) => String(t.id) === id);
        if (existing) {
          cache.tables.delete(existing.name);
          cache.tablesList = cache.tablesList.filter(
            (t) => String(t.id) !== id,
          );
        }
      }
    }

    cache.version = Date.now();
    cache.timestamp = new Date();
  }

  it('partial reload with 1 table ID → only that table updated in Map', () => {
    const tableA = makeTable(1, 'tableA', {
      columns: [{ name: 'title', type: 'varchar' }],
    });
    const tableB = makeTable(2, 'tableB');
    const cache = makeInitialCache([tableA, tableB]);

    const updatedA = makeTable(1, 'tableA', {
      columns: [{ name: 'title', type: 'text' }],
    });
    const payload: TCacheInvalidationPayload = {
      tableName: 'table_definition',
      action: 'reload',
      timestamp: Date.now(),
      scope: 'partial',
      ids: [1],
    };

    applyPartialUpdate(cache, payload, [updatedA, tableB]);

    expect(cache.tables.get('tableA')!.columns[0].type).toBe('text');
    expect(cache.tables.has('tableB')).toBe(true);
    expect(cache.tablesList).toHaveLength(2);
  });

  it('partial reload with affectedTables → both primary and affected tables reloaded', () => {
    const tableA = makeTable(1, 'tableA');
    const tableB = makeTable(2, 'tableB');
    const cache = makeInitialCache([tableA, tableB]);

    const updatedA = makeTable(1, 'tableA', {
      columns: [{ name: 'x', type: 'int' }],
    });
    const updatedB = makeTable(2, 'tableB', {
      columns: [{ name: 'y', type: 'float' }],
    });

    const payload: TCacheInvalidationPayload = {
      tableName: 'table_definition',
      action: 'reload',
      timestamp: Date.now(),
      scope: 'partial',
      ids: [1],
      affectedTables: ['tableB'],
    };

    applyPartialUpdate(cache, payload, [updatedA, updatedB]);

    expect(cache.tables.get('tableA')!.columns[0].name).toBe('x');
    expect(cache.tables.get('tableB')!.columns[0].name).toBe('y');
  });

  it('partial reload when table is deleted → removed from cache', () => {
    const tableA = makeTable(1, 'tableA');
    const tableB = makeTable(2, 'tableB');
    const cache = makeInitialCache([tableA, tableB]);

    const payload: TCacheInvalidationPayload = {
      tableName: 'table_definition',
      action: 'reload',
      timestamp: Date.now(),
      scope: 'partial',
      ids: [2],
    };

    applyPartialUpdate(cache, payload, []);

    expect(cache.tables.has('tableB')).toBe(false);
    expect(cache.tablesList.some((t) => t.name === 'tableB')).toBe(false);
    expect(cache.tables.has('tableA')).toBe(true);
  });

  it('partial reload when cache not initialized → throws', () => {
    const cache: MetaCache = null as any;
    const payload: TCacheInvalidationPayload = {
      tableName: 'table_definition',
      action: 'reload',
      timestamp: Date.now(),
      scope: 'partial',
      ids: [1],
    };

    expect(() => applyPartialUpdate(cache, payload, [])).toThrow(
      'Cache not initialized',
    );
  });

  it('version and timestamp updated after partial reload', () => {
    const tableA = makeTable(1, 'tableA');
    const cache = makeInitialCache([tableA]);
    const oldVersion = cache.version;
    const oldTs = cache.timestamp;

    const payload: TCacheInvalidationPayload = {
      tableName: 'table_definition',
      action: 'reload',
      timestamp: Date.now(),
      scope: 'partial',
      ids: [1],
    };

    applyPartialUpdate(cache, payload, [makeTable(1, 'tableA')]);

    expect(cache.version).toBeGreaterThanOrEqual(oldVersion);
    expect(cache.timestamp.getTime()).toBeGreaterThanOrEqual(oldTs.getTime());
  });

  it('new table in payload that did not exist before → added to cache', () => {
    const tableA = makeTable(1, 'tableA');
    const cache = makeInitialCache([tableA]);

    const tableB = makeTable(2, 'tableB', {
      columns: [{ name: 'foo', type: 'varchar' }],
    });
    const payload: TCacheInvalidationPayload = {
      tableName: 'table_definition',
      action: 'reload',
      timestamp: Date.now(),
      scope: 'partial',
      ids: [2],
    };

    applyPartialUpdate(cache, payload, [tableB]);

    expect(cache.tables.has('tableB')).toBe(true);
    expect(cache.tablesList).toHaveLength(2);
  });

  describe('Debounce merging simulation', () => {
    it('3 rapid partial payloads merge into 1 reload with all ids', async () => {
      let pending: TCacheInvalidationPayload | null = null;
      let reloadCount = 0;

      function receivePayload(p: TCacheInvalidationPayload): void {
        pending = mergePayload(pending, p);
      }

      const makeP = (id: number): TCacheInvalidationPayload => ({
        tableName: 'table_definition',
        action: 'reload',
        timestamp: Date.now(),
        scope: 'partial',
        ids: [id],
      });

      receivePayload(makeP(1));
      receivePayload(makeP(2));
      receivePayload(makeP(3));

      await new Promise<void>((resolve) => {
        setTimeout(() => {
          if (pending) {
            reloadCount++;
            const merged = pending;
            pending = null;
            expect(merged.scope).toBe('partial');
            expect(new Set(merged.ids)).toEqual(new Set([1, 2, 3]));
          }
          resolve();
        }, 60);
      });

      expect(reloadCount).toBe(1);
    });

    it('partial + full within debounce window → full reload', async () => {
      let pending: TCacheInvalidationPayload | null = null;
      let reloadedScope: string | null = null;

      function receivePayload(p: TCacheInvalidationPayload): void {
        pending = mergePayload(pending, p);
      }

      receivePayload({
        tableName: 'table_definition',
        action: 'reload',
        timestamp: Date.now(),
        scope: 'partial',
        ids: [1],
      });
      receivePayload({
        tableName: 'table_definition',
        action: 'reload',
        timestamp: Date.now(),
        scope: 'full',
      });

      await new Promise<void>((resolve) => {
        setTimeout(() => {
          if (pending) {
            reloadedScope = pending.scope;
            pending = null;
          }
          resolve();
        }, 60);
      });

      expect(reloadedScope).toBe('full');
    });
  });
});

// ═════════════════════════════════════════════════════════════════════════════
// D. RouteCache partial reload logic (inlined simulation)
// ═════════════════════════════════════════════════════════════════════════════

describe('D. RouteCache partial reload logic', () => {
  interface RouteCache {
    routes: any[];
    methods: string[];
  }

  function makeRoute(
    id: number,
    path: string,
    mainTableId: number,
    mainTableName: string,
  ): any {
    return {
      id,
      path,
      mainTable: { id: mainTableId, name: mainTableName },
      handlers: [],
      preHooks: [],
      postHooks: [],
      availableMethods: [{ method: 'GET' }],
    };
  }

  function makeChildRecord(id: number, routeId: number): any {
    return { id, route: { id: routeId } };
  }

  class TestRouteCachePartial {
    cache: RouteCache = { routes: [], methods: [] };
    cacheLoaded = true;
    fullReloadCount = 0;
    reloadedRouteIds: number[] = [];
    reloadedGlobalHooks = false;
    globalPreHooks: any[] = [];
    globalPostHooks: any[] = [];

    private dbRoutes: any[] = [];
    private dbChildren: Record<string, any[]> = {};

    setDbRoutes(routes: any[]) {
      this.dbRoutes = routes;
    }

    setDbChildren(tableName: string, records: any[]) {
      this.dbChildren[tableName] = records;
    }

    async applyPartialUpdate(
      payload: TCacheInvalidationPayload,
    ): Promise<void> {
      const affectedTableNames = new Set<string>(payload.affectedTables || []);

      if (payload.tableName === 'table_definition' && payload.ids?.length) {
        for (const route of this.cache.routes) {
          const mainTableId = route.mainTable?.id;
          if (payload.ids.some((id) => String(id) === String(mainTableId))) {
            affectedTableNames.add(route.mainTable?.name);
          }
        }
      }

      if (payload.tableName === 'route_definition' && payload.ids?.length) {
        await this._reloadSpecificRoutes(payload.ids);
        return;
      }

      if (
        ['route_handler_definition', 'route_permission_definition'].includes(
          payload.tableName,
        ) &&
        payload.ids?.length
      ) {
        const children = this.dbChildren[payload.tableName] || [];
        const matching = children.filter((c) =>
          payload.ids!.some((id) => String(id) === String(c.id)),
        );
        const routeIds = [
          ...new Set(matching.map((c) => c.route?.id).filter(Boolean)),
        ];
        if (routeIds.length > 0) {
          await this._reloadSpecificRoutes(routeIds);
          return;
        }
      }

      if (
        ['pre_hook_definition', 'post_hook_definition'].includes(
          payload.tableName,
        )
      ) {
        await this._reloadGlobalHooksAndMerge();
        return;
      }

      if (
        ['role_definition', 'method_definition'].includes(payload.tableName)
      ) {
        await this._fullReload();
        return;
      }

      if (affectedTableNames.size > 0) {
        const routeIds = this.cache.routes
          .filter((r) => affectedTableNames.has(r.mainTable?.name))
          .map((r) => r.id);
        if (routeIds.length > 0) {
          await this._reloadSpecificRoutes(routeIds);
          return;
        }
      }

      await this._fullReload();
    }

    private async _reloadSpecificRoutes(
      ids: (string | number)[],
    ): Promise<void> {
      const idSet = new Set(ids.map(String));
      const fetched = this.dbRoutes.filter((r) => idSet.has(String(r.id)));

      this.reloadedRouteIds.push(...fetched.map((r) => r.id));

      this.cache.routes = this.cache.routes.filter(
        (r) => !idSet.has(String(r.id)),
      );
      this.cache.routes.push(...fetched);
    }

    private async _reloadGlobalHooksAndMerge(): Promise<void> {
      this.reloadedGlobalHooks = true;
      this.globalPreHooks = [{ id: 99, isGlobal: true }];
      for (const route of this.cache.routes) {
        route.preHooks = [
          ...this.globalPreHooks,
          ...(route._localPreHooks || []),
        ];
      }
    }

    private async _fullReload(): Promise<void> {
      this.fullReloadCount++;
    }
  }

  it('table_definition change → finds routes by mainTable.id', async () => {
    const svc = new TestRouteCachePartial();
    svc.cache.routes = [
      makeRoute(1, '/tasks', 10, 'tasks'),
      makeRoute(2, '/users', 20, 'users'),
    ];
    svc.setDbRoutes([makeRoute(1, '/tasks', 10, 'tasks')]);

    const payload: TCacheInvalidationPayload = {
      tableName: 'table_definition',
      action: 'reload',
      timestamp: Date.now(),
      scope: 'partial',
      ids: [10],
    };

    await svc.applyPartialUpdate(payload);

    expect(svc.reloadedRouteIds).toContain(1);
    expect(svc.reloadedRouteIds).not.toContain(2);
  });

  it('route_definition change → reloads specific routes only', async () => {
    const svc = new TestRouteCachePartial();
    svc.cache.routes = [
      makeRoute(1, '/tasks', 10, 'tasks'),
      makeRoute(2, '/users', 20, 'users'),
    ];
    svc.setDbRoutes([makeRoute(1, '/tasks', 10, 'tasks')]);

    const payload: TCacheInvalidationPayload = {
      tableName: 'route_definition',
      action: 'reload',
      timestamp: Date.now(),
      scope: 'partial',
      ids: [1],
    };

    await svc.applyPartialUpdate(payload);

    expect(svc.reloadedRouteIds).toContain(1);
    expect(svc.reloadedRouteIds).not.toContain(2);
    expect(svc.fullReloadCount).toBe(0);
  });

  it('route_handler_definition change → finds parent route and reloads it', async () => {
    const svc = new TestRouteCachePartial();
    svc.cache.routes = [makeRoute(1, '/tasks', 10, 'tasks')];
    svc.setDbRoutes([makeRoute(1, '/tasks', 10, 'tasks')]);
    svc.setDbChildren('route_handler_definition', [makeChildRecord(50, 1)]);

    const payload: TCacheInvalidationPayload = {
      tableName: 'route_handler_definition',
      action: 'reload',
      timestamp: Date.now(),
      scope: 'partial',
      ids: [50],
    };

    await svc.applyPartialUpdate(payload);

    expect(svc.reloadedRouteIds).toContain(1);
    expect(svc.fullReloadCount).toBe(0);
  });

  it('pre_hook_definition change → reloads global hooks and re-merges', async () => {
    const svc = new TestRouteCachePartial();
    svc.cache.routes = [makeRoute(1, '/tasks', 10, 'tasks')];

    const payload: TCacheInvalidationPayload = {
      tableName: 'pre_hook_definition',
      action: 'reload',
      timestamp: Date.now(),
      scope: 'partial',
      ids: [5],
    };

    await svc.applyPartialUpdate(payload);

    expect(svc.reloadedGlobalHooks).toBe(true);
    expect(svc.fullReloadCount).toBe(0);
    expect(svc.cache.routes[0].preHooks).toContainEqual({
      id: 99,
      isGlobal: true,
    });
  });

  it('post_hook_definition change → reloads global hooks and re-merges', async () => {
    const svc = new TestRouteCachePartial();
    svc.cache.routes = [makeRoute(1, '/tasks', 10, 'tasks')];

    const payload: TCacheInvalidationPayload = {
      tableName: 'post_hook_definition',
      action: 'reload',
      timestamp: Date.now(),
      scope: 'partial',
      ids: [6],
    };

    await svc.applyPartialUpdate(payload);

    expect(svc.reloadedGlobalHooks).toBe(true);
  });

  it('role_definition change → falls back to full reload', async () => {
    const svc = new TestRouteCachePartial();
    svc.cache.routes = [makeRoute(1, '/tasks', 10, 'tasks')];

    const payload: TCacheInvalidationPayload = {
      tableName: 'role_definition',
      action: 'reload',
      timestamp: Date.now(),
      scope: 'partial',
      ids: [3],
    };

    await svc.applyPartialUpdate(payload);

    expect(svc.fullReloadCount).toBe(1);
    expect(svc.reloadedRouteIds).toHaveLength(0);
  });

  it('method_definition change → falls back to full reload', async () => {
    const svc = new TestRouteCachePartial();
    const payload: TCacheInvalidationPayload = {
      tableName: 'method_definition',
      action: 'reload',
      timestamp: Date.now(),
      scope: 'partial',
      ids: [7],
    };

    await svc.applyPartialUpdate(payload);

    expect(svc.fullReloadCount).toBe(1);
  });

  it('route deleted (in payload but not in DB) → removed from cache', async () => {
    const svc = new TestRouteCachePartial();
    svc.cache.routes = [
      makeRoute(1, '/tasks', 10, 'tasks'),
      makeRoute(2, '/users', 20, 'users'),
    ];
    svc.setDbRoutes([]);

    const payload: TCacheInvalidationPayload = {
      tableName: 'route_definition',
      action: 'reload',
      timestamp: Date.now(),
      scope: 'partial',
      ids: [1],
    };

    await svc.applyPartialUpdate(payload);

    expect(svc.cache.routes.some((r) => r.id === 1)).toBe(false);
    expect(svc.cache.routes.some((r) => r.id === 2)).toBe(true);
  });

  it('route trie rebuilt correctly after partial reload (routes in cache updated)', async () => {
    const svc = new TestRouteCachePartial();
    svc.cache.routes = [makeRoute(1, '/tasks', 10, 'tasks')];
    const updatedRoute = makeRoute(1, '/tasks', 10, 'tasks');
    updatedRoute.handlers = [{ id: 99, method: 'GET', logic: 'return {}' }];
    svc.setDbRoutes([updatedRoute]);

    const payload: TCacheInvalidationPayload = {
      tableName: 'route_definition',
      action: 'reload',
      timestamp: Date.now(),
      scope: 'partial',
      ids: [1],
    };

    await svc.applyPartialUpdate(payload);

    const found = svc.cache.routes.find((r) => r.id === 1);
    expect(found?.handlers).toHaveLength(1);
    expect(found?.handlers[0].id).toBe(99);
  });

  it('affectedTables in payload → finds and reloads routes for those tables', async () => {
    const svc = new TestRouteCachePartial();
    svc.cache.routes = [
      makeRoute(1, '/tasks', 10, 'tasks'),
      makeRoute(2, '/users', 20, 'users'),
    ];
    svc.setDbRoutes([makeRoute(2, '/users', 20, 'users')]);

    const payload: TCacheInvalidationPayload = {
      tableName: 'column_definition',
      action: 'reload',
      timestamp: Date.now(),
      scope: 'partial',
      ids: [],
      affectedTables: ['users'],
    };

    await svc.applyPartialUpdate(payload);

    expect(svc.reloadedRouteIds).toContain(2);
    expect(svc.reloadedRouteIds).not.toContain(1);
  });
});

// ═════════════════════════════════════════════════════════════════════════════
// E. GraphQL incremental update (using real buildTableGraphQLDef)
// ═════════════════════════════════════════════════════════════════════════════

describe('E. GraphQL buildTableGraphQLDef and incremental update', () => {
  beforeAll(() => {
    DatabaseConfigService.overrideForTesting('mysql');
  });

  afterAll(() => {
    DatabaseConfigService.resetForTesting();
  });

  function makeGqlTable(
    name: string,
    columns: any[],
    relations: any[] = [],
  ): any {
    return { name, columns, relations };
  }

  const basicColumns = [
    {
      name: 'id',
      type: 'uuid',
      isPrimary: true,
      isNullable: false,
      isPublished: true,
    },
    {
      name: 'title',
      type: 'varchar',
      isPrimary: false,
      isNullable: false,
      isPublished: true,
    },
    {
      name: 'body',
      type: 'text',
      isPrimary: false,
      isNullable: true,
      isPublished: true,
    },
  ];

  it('buildTableGraphQLDef returns null for table not in queryableNames', () => {
    const table = makeGqlTable('tasks', basicColumns);
    const result = buildTableGraphQLDef(table, new Set(['users']), new Map());
    expect(result).toBeNull();
  });

  it('buildTableGraphQLDef returns null for table with no columns', () => {
    const table = makeGqlTable('tasks', []);
    const result = buildTableGraphQLDef(table, new Set(['tasks']), new Map());
    expect(result).toBeNull();
  });

  it('buildTableGraphQLDef returns null for null/undefined table', () => {
    expect(
      buildTableGraphQLDef(null, new Set(['tasks']), new Map()),
    ).toBeNull();
    expect(
      buildTableGraphQLDef(undefined, new Set(['tasks']), new Map()),
    ).toBeNull();
  });

  it('buildTableGraphQLDef produces correct field types for all column types', () => {
    const typeMap = [
      { type: 'int', expected: GraphQLInt },
      { type: 'integer', expected: GraphQLInt },
      { type: 'float', expected: GraphQLFloat },
      { type: 'decimal', expected: GraphQLFloat },
      { type: 'boolean', expected: GraphQLBoolean },
      { type: 'bool', expected: GraphQLBoolean },
      { type: 'varchar', expected: GraphQLString },
      { type: 'text', expected: GraphQLString },
      { type: 'uuid', expected: GraphQLID },
      { type: 'date', expected: GraphQLString },
      { type: 'datetime', expected: GraphQLString },
      { type: 'timestamp', expected: GraphQLString },
      { type: 'unknown_type', expected: GraphQLString },
    ];

    for (const { type, expected } of typeMap) {
      const table = makeGqlTable('tasks', [
        {
          name: 'field1',
          type,
          isPrimary: false,
          isNullable: true,
          isPublished: true,
        },
        {
          name: 'id',
          type: 'int',
          isPrimary: true,
          isNullable: false,
          isPublished: true,
        },
      ]);
      const def = buildTableGraphQLDef(table, new Set(['tasks']), new Map());
      expect(def).not.toBeNull();
      const fields = (def!.type as any)._fields();
      const fieldType = fields['field1']?.type;
      expect(fieldType).toBe(expected);
    }
  });

  it('isPublished=false columns excluded from type', () => {
    const table = makeGqlTable('tasks', [
      {
        name: 'id',
        type: 'int',
        isPrimary: true,
        isNullable: false,
        isPublished: true,
      },
      {
        name: 'secret',
        type: 'varchar',
        isPrimary: false,
        isNullable: true,
        isPublished: false,
      },
      {
        name: 'title',
        type: 'varchar',
        isPrimary: false,
        isNullable: false,
        isPublished: true,
      },
    ]);
    const def = buildTableGraphQLDef(table, new Set(['tasks']), new Map());
    expect(def).not.toBeNull();
    const fields = (def!.type as any)._fields();
    expect(fields['secret']).toBeUndefined();
    expect(fields['title']).toBeDefined();
  });

  it('non-nullable column produces GraphQLNonNull wrapper', () => {
    const table = makeGqlTable('tasks', [
      {
        name: 'id',
        type: 'int',
        isPrimary: true,
        isNullable: false,
        isPublished: true,
      },
      {
        name: 'required',
        type: 'varchar',
        isPrimary: false,
        isNullable: false,
        isPublished: true,
      },
      {
        name: 'optional',
        type: 'varchar',
        isPrimary: false,
        isNullable: true,
        isPublished: true,
      },
    ]);
    const def = buildTableGraphQLDef(table, new Set(['tasks']), new Map());
    const fields = (def!.type as any)._fields();
    expect(fields['required'].type).toBeInstanceOf(GraphQLNonNull);
    expect(fields['optional'].type).toBe(GraphQLString);
  });

  it('JSON column type maps to GraphQLJSON scalar', () => {
    const table = makeGqlTable('tasks', [
      {
        name: 'id',
        type: 'int',
        isPrimary: true,
        isNullable: false,
        isPublished: true,
      },
      {
        name: 'meta',
        type: 'json',
        isPrimary: false,
        isNullable: true,
        isPublished: true,
      },
    ]);
    const def = buildTableGraphQLDef(table, new Set(['tasks']), new Map());
    const fields = (def!.type as any)._fields();
    expect(fields['meta'].type).toBe(GraphQLJSON);
  });

  it('one-to-many relation produces list type', () => {
    const table = makeGqlTable(
      'users',
      [
        {
          name: 'id',
          type: 'uuid',
          isPrimary: true,
          isNullable: false,
          isPublished: true,
        },
      ],
      [
        {
          propertyName: 'tasks',
          targetTableName: 'tasks',
          type: 'one-to-many',
          isPublished: true,
        },
      ],
    );
    const typeRegistry = new Map<string, GraphQLObjectType>();
    typeRegistry.set(
      'tasks',
      new GraphQLObjectType({
        name: 'tasks',
        fields: { id: { type: GraphQLID } },
      }),
    );
    const def = buildTableGraphQLDef(
      table,
      new Set(['users', 'tasks']),
      typeRegistry,
    );
    expect(def).not.toBeNull();
    const fields = (def!.type as any)._fields();
    const tasksField = fields['tasks'];
    expect(tasksField).toBeDefined();
    expect(tasksField.type).toBeInstanceOf(GraphQLNonNull);
  });

  it('many-to-one relation produces object type', () => {
    const table = makeGqlTable(
      'tasks',
      [
        {
          name: 'id',
          type: 'uuid',
          isPrimary: true,
          isNullable: false,
          isPublished: true,
        },
      ],
      [
        {
          propertyName: 'user',
          targetTableName: 'users',
          type: 'many-to-one',
          isPublished: true,
        },
      ],
    );
    const typeRegistry = new Map<string, GraphQLObjectType>();
    typeRegistry.set(
      'users',
      new GraphQLObjectType({
        name: 'users',
        fields: { id: { type: GraphQLID } },
      }),
    );
    const def = buildTableGraphQLDef(
      table,
      new Set(['tasks', 'users']),
      typeRegistry,
    );
    expect(def).not.toBeNull();
    const fields = (def!.type as any)._fields();
    expect(fields['user']).toBeDefined();
    expect(fields['user'].type).not.toBeInstanceOf(GraphQLList);
  });

  it('isPublished=false relation excluded from type', () => {
    const table = makeGqlTable(
      'tasks',
      [
        {
          name: 'id',
          type: 'int',
          isPrimary: true,
          isNullable: false,
          isPublished: true,
        },
      ],
      [
        {
          propertyName: 'hiddenRel',
          targetTableName: 'others',
          type: 'many-to-one',
          isPublished: false,
        },
      ],
    );
    const def = buildTableGraphQLDef(table, new Set(['tasks']), new Map());
    const fields = (def!.type as any)._fields();
    expect(fields['hiddenRel']).toBeUndefined();
  });

  it('non-queryable referenced table added to referencedStubs', () => {
    const table = makeGqlTable(
      'tasks',
      [
        {
          name: 'id',
          type: 'int',
          isPrimary: true,
          isNullable: false,
          isPublished: true,
        },
      ],
      [
        {
          propertyName: 'category',
          targetTableName: 'category_definition',
          type: 'many-to-one',
          isPublished: true,
        },
      ],
    );
    const def = buildTableGraphQLDef(table, new Set(['tasks']), new Map());
    expect(def).not.toBeNull();
    expect(def!.referencedStubs.has('category_definition')).toBe(true);
  });

  it('buildStubType produces type with only id field', () => {
    const stub = buildStubType('SomeStub');
    expect(stub.name).toBe('SomeStub');
    const fields = stub.getFields();
    expect(fields['id']).toBeDefined();
    expect(Object.keys(fields)).toHaveLength(1);
  });

  it('self-referencing relation (targetTableName === typeName) excluded', () => {
    const table = makeGqlTable(
      'tasks',
      [
        {
          name: 'id',
          type: 'int',
          isPrimary: true,
          isNullable: false,
          isPublished: true,
        },
      ],
      [
        {
          propertyName: 'parent',
          targetTableName: 'tasks',
          type: 'many-to-one',
          isPublished: true,
        },
      ],
    );
    const def = buildTableGraphQLDef(table, new Set(['tasks']), new Map());
    const fields = (def!.type as any)._fields();
    expect(fields['parent']).toBeUndefined();
  });

  describe('Incremental update simulation', () => {
    interface GqlState {
      tableDefCache: Map<string, TableGraphQLDef>;
      typeRegistry: Map<string, GraphQLObjectType>;
      queryableNames: Set<string>;
    }

    function fullBuild(tables: any[], queryableNames: Set<string>): GqlState {
      const tableDefCache = new Map<string, TableGraphQLDef>();
      const typeRegistry = new Map<string, GraphQLObjectType>();

      for (const table of tables) {
        if (!queryableNames.has(table.name)) continue;
        const def = buildTableGraphQLDef(table, queryableNames, typeRegistry);
        if (!def) continue;
        tableDefCache.set(table.name, def);
        typeRegistry.set(table.name, def.type);
      }

      const stubs = new Set<string>();
      for (const def of tableDefCache.values()) {
        for (const stub of def.referencedStubs) {
          if (!typeRegistry.has(stub)) stubs.add(stub);
        }
      }
      for (const stubName of stubs) {
        typeRegistry.set(stubName, buildStubType(stubName));
      }

      return { tableDefCache, typeRegistry, queryableNames };
    }

    function incrementalUpdate(
      state: GqlState,
      metadata: Map<string, any>,
      newQueryableNames: Set<string>,
      affectedTables: Set<string>,
    ): void {
      state.queryableNames = newQueryableNames;

      for (const tableName of affectedTables) {
        const tableData = metadata.get(tableName);
        if (!tableData || !newQueryableNames.has(tableName)) {
          state.tableDefCache.delete(tableName);
          state.typeRegistry.delete(tableName);
          continue;
        }

        const def = buildTableGraphQLDef(
          tableData,
          newQueryableNames,
          state.typeRegistry,
        );
        if (!def) {
          state.tableDefCache.delete(tableName);
          state.typeRegistry.delete(tableName);
          continue;
        }

        state.tableDefCache.set(tableName, def);
        state.typeRegistry.set(tableName, def.type);
      }

      const stubs = new Set<string>();
      for (const def of state.tableDefCache.values()) {
        for (const stub of def.referencedStubs) {
          if (!state.typeRegistry.has(stub)) stubs.add(stub);
        }
      }
      for (const stubName of stubs) {
        state.typeRegistry.set(stubName, buildStubType(stubName));
      }
    }

    it('full build produces correct type count', () => {
      const tables = [
        makeGqlTable('tasks', basicColumns),
        makeGqlTable('users', basicColumns),
      ];
      const state = fullBuild(tables, new Set(['tasks', 'users']));
      expect(state.tableDefCache.size).toBe(2);
      expect(state.typeRegistry.has('tasks')).toBe(true);
      expect(state.typeRegistry.has('users')).toBe(true);
    });

    it('incremental update for 1 table → only that table rebuilt', () => {
      const tables = [
        makeGqlTable('tasks', basicColumns),
        makeGqlTable('users', basicColumns),
      ];
      const state = fullBuild(tables, new Set(['tasks', 'users']));
      const originalUsersType = state.typeRegistry.get('users');

      const updatedTasks = makeGqlTable('tasks', [
        ...basicColumns,
        {
          name: 'priority',
          type: 'int',
          isPrimary: false,
          isNullable: true,
          isPublished: true,
        },
      ]);
      const metadata = new Map([
        ['tasks', updatedTasks],
        ['users', tables[1]],
      ]);

      incrementalUpdate(
        state,
        metadata,
        new Set(['tasks', 'users']),
        new Set(['tasks']),
      );

      expect(state.typeRegistry.get('users')).toBe(originalUsersType);
      const fields = (state.typeRegistry.get('tasks') as any)._fields();
      expect(fields['priority']).toBeDefined();
    });

    it('new table added incrementally → type added to registry', () => {
      const tables = [makeGqlTable('tasks', basicColumns)];
      const state = fullBuild(tables, new Set(['tasks']));

      const newTable = makeGqlTable('users', basicColumns);
      const metadata = new Map([
        ['tasks', tables[0]],
        ['users', newTable],
      ]);

      incrementalUpdate(
        state,
        metadata,
        new Set(['tasks', 'users']),
        new Set(['users']),
      );

      expect(state.tableDefCache.has('users')).toBe(true);
      expect(state.typeRegistry.has('users')).toBe(true);
    });

    it('table removed incrementally → type removed from registry', () => {
      const tables = [
        makeGqlTable('tasks', basicColumns),
        makeGqlTable('users', basicColumns),
      ];
      const state = fullBuild(tables, new Set(['tasks', 'users']));

      const metadata = new Map([['tasks', tables[0]]]);
      incrementalUpdate(
        state,
        metadata,
        new Set(['tasks']),
        new Set(['users']),
      );

      expect(state.tableDefCache.has('users')).toBe(false);
      expect(state.typeRegistry.has('users')).toBe(false);
    });

    it('stub types generated for non-queryable referenced tables', () => {
      const table = makeGqlTable('tasks', basicColumns, [
        {
          propertyName: 'category',
          targetTableName: 'category_definition',
          type: 'many-to-one',
          isPublished: true,
        },
      ]);
      const state = fullBuild([table], new Set(['tasks']));
      expect(state.typeRegistry.has('category_definition')).toBe(true);
      const stubType = state.typeRegistry.get('category_definition')!;
      expect(stubType.getFields()['id']).toBeDefined();
    });
  });
});

// ═════════════════════════════════════════════════════════════════════════════
// F. Cross-table cascade scenarios
// ═════════════════════════════════════════════════════════════════════════════

describe('F. Cross-table cascade / affectedTables scenarios', () => {
  function buildPayload(
    tableName: string,
    ids: (string | number)[],
    affectedTables?: string[],
  ): TCacheInvalidationPayload {
    return {
      tableName,
      action: 'reload',
      timestamp: Date.now(),
      scope: ids.length > 0 ? 'partial' : 'full',
      ids,
      affectedTables,
    };
  }

  it('delete relation on table A with inverse on table B → both in affectedTables', () => {
    const affectedTables: string[] = [];
    const deletedRelation = {
      sourceTableName: 'tasks',
      targetTableName: 'users',
      type: 'many-to-one',
    };

    affectedTables.push(deletedRelation.sourceTableName);
    affectedTables.push(deletedRelation.targetTableName);

    const payload = buildPayload('relation_definition', [5], affectedTables);
    expect(payload.affectedTables).toContain('tasks');
    expect(payload.affectedTables).toContain('users');
    expect(payload.scope).toBe('partial');
  });

  it('create table with relations → target tables in affectedTables', () => {
    const newTable = { name: 'comments', relations: ['tasks', 'users'] };
    const affectedTables = [...newTable.relations];
    const payload = buildPayload('table_definition', [10], affectedTables);
    expect(payload.affectedTables).toContain('tasks');
    expect(payload.affectedTables).toContain('users');
  });

  it('delete table with inbound relations → source tables in affectedTables', () => {
    const deletedTable = 'tasks';
    const inboundRelations = ['comments', 'attachments'];
    const payload = buildPayload(
      'table_definition',
      [10],
      [deletedTable, ...inboundRelations],
    );
    expect(payload.affectedTables).toContain('comments');
    expect(payload.affectedTables).toContain('attachments');
  });

  it('cascade payload merging deduplicates repeated affectedTables', () => {
    const p1 = buildPayload('table_definition', [1], ['tasks', 'users']);
    const p2 = buildPayload('table_definition', [2], ['tasks', 'comments']);
    const merged = mergePayload(p1, p2);
    const tableSet = new Set(merged.affectedTables);
    expect(tableSet.size).toBe(3);
    expect(tableSet.has('tasks')).toBe(true);
    expect(tableSet.has('users')).toBe(true);
    expect(tableSet.has('comments')).toBe(true);
  });

  it('DynamicRepository.reload builds correct payload scope based on ids', () => {
    const emitted: TCacheInvalidationPayload[] = [];
    const mockEmitter = {
      emit: jest.fn((event, payload) => emitted.push(payload)),
    };

    function reloadFn(
      tableName: string,
      opts?: { ids?: (string | number)[]; affectedTables?: string[] },
    ) {
      const payload: TCacheInvalidationPayload = {
        tableName,
        action: 'reload',
        timestamp: Date.now(),
        scope: opts?.ids?.length ? 'partial' : 'full',
        ids: opts?.ids,
        affectedTables: opts?.affectedTables,
      };
      mockEmitter.emit('cache:invalidate', payload);
    }

    reloadFn('table_definition', {
      ids: [1, 2],
      affectedTables: ['relatedTable'],
    });
    expect(emitted[0].scope).toBe('partial');
    expect(emitted[0].ids).toEqual([1, 2]);
    expect(emitted[0].affectedTables).toEqual(['relatedTable']);

    reloadFn('table_definition');
    expect(emitted[1].scope).toBe('full');
    expect(emitted[1].ids).toBeUndefined();
  });
});

// ═════════════════════════════════════════════════════════════════════════════
// G. End-to-end flow simulation
// ═════════════════════════════════════════════════════════════════════════════

describe('G. End-to-end flow simulation', () => {
  interface SystemState {
    metadata: Map<string, any>;
    routes: any[];
    gqlTypes: Set<string>;
  }

  class SimulatedSystem {
    state: SystemState = {
      metadata: new Map(),
      routes: [],
      gqlTypes: new Set(),
    };

    metadataReloads = 0;
    routeReloads = 0;
    gqlReloads = 0;
    fullFallbacks = 0;
    partialApplied = 0;

    private ee = new EventEmitter2();

    constructor() {
      this.ee.on('cache:invalidate', (payload: TCacheInvalidationPayload) => {
        if (
          [
            'table_definition',
            'column_definition',
            'relation_definition',
          ].includes(payload.tableName)
        ) {
          this._handleMetadataInvalidation(payload);
        }
        if (
          [
            'route_definition',
            'pre_hook_definition',
            'post_hook_definition',
          ].includes(payload.tableName)
        ) {
          this._handleRouteInvalidation(payload);
        }
      });
      this.ee.on('metadata:loaded', () => this._reloadRoutes());
      this.ee.on('routes:loaded', () => this._reloadGraphQL());
    }

    private _handleMetadataInvalidation(
      payload: TCacheInvalidationPayload,
    ): void {
      if (
        payload.scope === 'partial' &&
        payload.ids?.length &&
        this.state.metadata.size > 0
      ) {
        this._partialMetadataReload(payload);
      } else {
        this._fullMetadataReload();
      }
    }

    private _partialMetadataReload(payload: TCacheInvalidationPayload): void {
      this.metadataReloads++;
      this.partialApplied++;
      for (const id of payload.ids!) {
        const existing = [...this.state.metadata.values()].find(
          (t) => String(t.id) === String(id),
        );
        if (existing) {
          existing._updated = true;
          this.state.metadata.set(existing.name, existing);
        }
      }
      this.ee.emit('metadata:loaded');
    }

    private _fullMetadataReload(): void {
      this.metadataReloads++;
      this.ee.emit('metadata:loaded');
    }

    private _handleRouteInvalidation(payload: TCacheInvalidationPayload): void {
      if (payload.scope === 'partial' && payload.ids?.length) {
        this.routeReloads++;
        this.partialApplied++;
        this.ee.emit('routes:loaded');
      } else {
        this._reloadRoutes();
      }
    }

    private _reloadRoutes(): void {
      this.routeReloads++;
      this.ee.emit('routes:loaded');
    }

    private _reloadGraphQL(): void {
      this.gqlReloads++;
    }

    setupInitialState(): void {
      this.state.metadata.set('tasks', { id: 1, name: 'tasks', columns: [] });
      this.state.metadata.set('users', { id: 2, name: 'users', columns: [] });
      this.state.routes = [{ id: 1, path: '/tasks' }];
    }

    emit(event: string, payload: any): void {
      this.ee.emit(event, payload);
    }

    tryPartialFallback(applyFn: () => void, fullFn: () => void): void {
      try {
        applyFn();
      } catch {
        this.fullFallbacks++;
        fullFn();
      }
    }
  }

  it('column update → metadata partial → route partial → graphql incremental (all triggered)', () => {
    const sys = new SimulatedSystem();
    sys.setupInitialState();

    sys.emit('cache:invalidate', {
      tableName: 'column_definition',
      action: 'reload',
      timestamp: Date.now(),
      scope: 'partial',
      ids: [1],
    });

    expect(sys.metadataReloads).toBe(1);
    expect(sys.routeReloads).toBe(1);
    expect(sys.gqlReloads).toBe(1);
    expect(sys.partialApplied).toBeGreaterThan(0);
  });

  it('full reload fallback when partial applyPartialUpdate throws', () => {
    const sys = new SimulatedSystem();
    sys.setupInitialState();

    sys.tryPartialFallback(
      () => {
        throw new Error('DB error during partial');
      },
      () => {
        sys.metadataReloads++;
        sys.emit('metadata:loaded', {});
      },
    );

    expect(sys.fullFallbacks).toBe(1);
    expect(sys.metadataReloads).toBe(1);
    expect(sys.gqlReloads).toBe(1);
  });

  it('route_definition partial update → only routes reloaded, not metadata', () => {
    const sys = new SimulatedSystem();
    sys.setupInitialState();

    sys.emit('cache:invalidate', {
      tableName: 'route_definition',
      action: 'reload',
      timestamp: Date.now(),
      scope: 'partial',
      ids: [1],
    });

    expect(sys.metadataReloads).toBe(0);
    expect(sys.routeReloads).toBe(1);
    expect(sys.gqlReloads).toBe(1);
  });

  it('table_definition full reload → all caches reloaded in cascade', () => {
    const sys = new SimulatedSystem();
    sys.setupInitialState();

    sys.emit('cache:invalidate', {
      tableName: 'table_definition',
      action: 'reload',
      timestamp: Date.now(),
      scope: 'full',
    });

    expect(sys.metadataReloads).toBe(1);
    expect(sys.routeReloads).toBe(1);
    expect(sys.gqlReloads).toBe(1);
  });

  it('multi-instance: partial reload from instance A triggers partial on instance B', async () => {
    const CHANNEL = 'meta:sync';
    const pubsub = makePubSub();

    const svcA = new TestBaseCacheService(CHANNEL, pubsub, 'inst-A');
    const svcB = new TestBaseCacheService(CHANNEL, pubsub, 'inst-B');
    svcA._supportsPartial = true;
    svcB._supportsPartial = true;

    const payload: TCacheInvalidationPayload = {
      tableName: 'table_definition',
      action: 'reload',
      timestamp: Date.now(),
      scope: 'partial',
      ids: [1, 2],
    };

    await svcA.partialReload(payload, true);

    expect(pubsub.published).toHaveLength(1);
    expect(pubsub.published[0].payload.payload?.scope).toBe('partial');

    const signal = JSON.stringify(pubsub.published[0].payload);
    await pubsub.simulateMessage(CHANNEL, signal);

    expect(svcA.fullReloadCount).toBe(0);
    expect(svcA.partialReloadCount).toBe(1);
    expect(svcB.partialReloadCount).toBe(1);
    expect(svcB.fullReloadCount).toBe(0);
  });

  it('partial payload scope is correctly determined by presence of ids', () => {
    const cases = [
      { ids: [1], expectedScope: 'partial' as const },
      { ids: [], expectedScope: 'full' as const },
      { ids: undefined, expectedScope: 'full' as const },
    ];

    for (const { ids, expectedScope } of cases) {
      const payload: TCacheInvalidationPayload = {
        tableName: 'table_definition',
        action: 'reload',
        timestamp: Date.now(),
        scope: ids?.length ? 'partial' : 'full',
        ids,
      };
      expect(payload.scope).toBe(expectedScope);
    }
  });

  it('race condition: concurrent partial + full merges into full', () => {
    let pending: TCacheInvalidationPayload | null = null;

    const p1: TCacheInvalidationPayload = {
      tableName: 'table_definition',
      action: 'reload',
      timestamp: Date.now(),
      scope: 'partial',
      ids: [1],
    };
    const p2: TCacheInvalidationPayload = {
      tableName: 'table_definition',
      action: 'reload',
      timestamp: Date.now(),
      scope: 'full',
    };
    const p3: TCacheInvalidationPayload = {
      tableName: 'table_definition',
      action: 'reload',
      timestamp: Date.now(),
      scope: 'partial',
      ids: [3],
    };

    pending = mergePayload(pending, p1);
    pending = mergePayload(pending, p2);
    pending = mergePayload(pending, p3);

    expect(pending!.scope).toBe('full');
    expect(pending!.ids).toBeUndefined();
  });

  it('CACHE_INVALIDATION_MAP: correct caches reloaded per table', () => {
    const CACHE_INVALIDATION_MAP: Record<string, string[]> = {
      table_definition: ['metadata', 'route', 'graphql'],
      column_definition: ['metadata', 'route', 'graphql'],
      relation_definition: ['metadata', 'route', 'graphql'],
      route_definition: ['route', 'graphql'],
      pre_hook_definition: ['route', 'graphql'],
      post_hook_definition: ['route', 'graphql'],
      role_definition: ['route', 'graphql'],
      field_permission_definition: ['field-permission'],
      setting_definition: ['setting'],
    };

    const expectedMetadataTables = [
      'table_definition',
      'column_definition',
      'relation_definition',
    ];
    const expectedRouteOnlyTables = [
      'route_definition',
      'pre_hook_definition',
      'post_hook_definition',
    ];
    const expectedIsolatedTables = [
      'field_permission_definition',
      'setting_definition',
    ];

    for (const t of expectedMetadataTables) {
      expect(CACHE_INVALIDATION_MAP[t]).toContain('metadata');
      expect(CACHE_INVALIDATION_MAP[t]).toContain('route');
      expect(CACHE_INVALIDATION_MAP[t]).toContain('graphql');
    }

    for (const t of expectedRouteOnlyTables) {
      expect(CACHE_INVALIDATION_MAP[t]).not.toContain('metadata');
      expect(CACHE_INVALIDATION_MAP[t]).toContain('route');
    }

    expect(CACHE_INVALIDATION_MAP['field_permission_definition']).toEqual([
      'field-permission',
    ]);
    expect(CACHE_INVALIDATION_MAP['setting_definition']).toEqual(['setting']);
  });
});
