import { describe, expect, it } from 'vitest';
import { ObjectId } from 'mongodb';
import {
  BaseCacheService,
  RedisRuntimeCacheStore,
  RouteCacheService,
} from '../../src/engines/cache';
import { CACHE_IDENTIFIERS } from '../../src/shared/utils/cache-events.constants';

class MemoryRedis {
  data = new Map<string, string | Buffer>();

  async get(key: string) {
    const value = this.data.get(key);
    if (Buffer.isBuffer(value)) return value.toString();
    return value ?? null;
  }

  async getBuffer(key: string) {
    const value = this.data.get(key);
    if (value == null) return null;
    return Buffer.isBuffer(value) ? value : Buffer.from(value);
  }

  async set(key: string, value: string | Buffer, ...args: string[]) {
    if (args.includes('NX') && this.data.has(key)) return null;
    this.data.set(key, value);
    return 'OK';
  }

  async eval(_script: string, _keyCount: number, key: string, value: string) {
    if (this.data.get(key) === value) {
      this.data.delete(key);
      return 1;
    }
    return 0;
  }

  async scan(_cursor: string, _match: string, pattern: string) {
    const prefix = pattern.endsWith('*') ? pattern.slice(0, -1) : pattern;
    return [
      '0',
      [...this.data.keys()].filter((key) => key.startsWith(prefix)),
    ];
  }

  async del(...keys: string[]) {
    let count = 0;
    for (const key of keys) {
      if (this.data.delete(key)) count++;
    }
    return count;
  }
}

function createStore(redis = new MemoryRedis()) {
  return new RedisRuntimeCacheStore({
    redis: redis as any,
    envService: {
      get: (key: string) => {
        if (key === 'NODE_NAME') return 'shared-node';
        if (key === 'REDIS_RUNTIME_CACHE') return true;
        return undefined;
      },
    } as any,
  });
}

class SharedTestCache extends BaseCacheService<Map<string, Set<string>>> {
  loadCalls = 0;

  constructor(store: RedisRuntimeCacheStore) {
    super(
      {
        cacheIdentifier: CACHE_IDENTIFIERS.SETTING,
        colorCode: '',
        cacheName: 'SharedTestCache',
      },
      undefined,
      store,
    );
  }

  protected async loadFromDb(): Promise<any> {
    this.loadCalls++;
    return [['alpha', ['one', 'two']]];
  }

  protected transformData(rawData: Array<[string, string[]]>) {
    return new Map(rawData.map(([key, values]) => [key, new Set(values)]));
  }

  supportsPartialReload(): boolean {
    return true;
  }

  protected async applyPartialUpdate(): Promise<void> {
    this.cache.set('beta', new Set(['three']));
  }
}

describe('Redis runtime cache mode', () => {
  it('persists cache snapshots in Redis and does not expose local raw cache', async () => {
    const redis = new MemoryRedis();
    const store = createStore(redis);
    const cache = new SharedTestCache(store);

    await cache.reload(false);

    const snapshot = await store.getSnapshot<Map<string, Set<string>>>(
      CACHE_IDENTIFIERS.SETTING,
    );
    expect(snapshot?.data.get('alpha')).toEqual(new Set(['one', 'two']));
    expect(() => cache.getRawCache()).toThrow(/Redis-backed/);
    expect(redis.data.has('shared-node:runtime_cache:setting')).toBe(true);
  });

  it('round-trips structured cache values through JSON snapshots', async () => {
    const store = createStore();
    const timestamp = new Date('2026-04-29T00:00:00.000Z');

    await store.setSnapshot(CACHE_IDENTIFIERS.SETTING, {
      timestamp,
      nested: new Map([['roles', new Set(['admin', 'editor'])]]),
    });

    const snapshot = await store.getSnapshot<{
      timestamp: Date;
      nested: Map<string, Set<string>>;
    }>(CACHE_IDENTIFIERS.SETTING);

    expect(snapshot?.data.timestamp).toBeInstanceOf(Date);
    expect(snapshot?.data.timestamp.toISOString()).toBe(
      '2026-04-29T00:00:00.000Z',
    );
    expect(snapshot?.data.nested.get('roles')).toEqual(
      new Set(['admin', 'editor']),
    );
  });

  it('normalizes Mongo ObjectId values before writing shared snapshots', async () => {
    const store = createStore();
    const id = new ObjectId('69f22938280ef30e587a9ef4');

    await store.setSnapshot(CACHE_IDENTIFIERS.ROUTE, {
      routes: [{ _id: id, mainTable: { _id: id } }],
    });

    const snapshot = await store.getSnapshot<{
      routes: Array<{ _id: string; mainTable: { _id: string } }>;
    }>(CACHE_IDENTIFIERS.ROUTE);

    expect(snapshot?.data.routes[0]._id).toBe(id.toHexString());
    expect(snapshot?.data.routes[0].mainTable._id).toBe(id.toHexString());
  });

  it('serves another cache instance from the shared Redis snapshot', async () => {
    const redis = new MemoryRedis();
    const store = createStore(redis);
    const writer = new SharedTestCache(store);
    const reader = new SharedTestCache(store);

    await writer.reload(false);
    const data = await reader.getCacheAsync();

    expect(reader.loadCalls).toBe(0);
    expect(data.get('alpha')).toEqual(new Set(['one', 'two']));
  });

  it('writes partial reload results back to the shared snapshot', async () => {
    const store = createStore();
    const cache = new SharedTestCache(store);

    await cache.reload(false);
    await cache.partialReload(
      {
        table: 'setting_definition',
        action: 'reload',
        scope: 'partial',
        timestamp: Date.now(),
        ids: [1],
      },
      false,
    );

    const data = await cache.getCacheAsync();
    expect(data.get('alpha')).toEqual(new Set(['one', 'two']));
    expect(data.get('beta')).toEqual(new Set(['three']));
  });

  it('matches Redis-backed routes without the local route engine', async () => {
    const store = createStore();
    const routeCache = new RouteCacheService({
      queryBuilderService: {} as any,
      metadataCacheService: {} as any,
      eventEmitter: undefined as any,
      redisRuntimeCacheStore: store,
    });

    await store.setSnapshot(CACHE_IDENTIFIERS.ROUTE, {
      methods: ['GET'],
      routes: [
        {
          id: 1,
          path: '/posts/:id',
          availableMethods: [{ method: 'GET' }],
          mainTable: { name: 'posts' },
        },
        {
          id: 2,
          path: '/posts/archive',
          availableMethods: [{ method: 'GET' }],
          mainTable: { name: 'posts' },
        },
      ],
    });

    const dynamicMatch = await routeCache.matchRoute('GET', '/posts/123');
    const staticMatch = await routeCache.matchRoute('GET', '/posts/archive');

    expect(() => routeCache.getRouteEngine()).toThrow(/Redis-backed/);
    expect(dynamicMatch?.route.id).toBe(1);
    expect(dynamicMatch?.params).toEqual({ id: '123' });
    expect(staticMatch?.route.id).toBe(2);
  });

  it('matches Redis-backed routes from the lightweight route lookup index', async () => {
    const store = createStore();
    const routeCache = new RouteCacheService({
      queryBuilderService: {} as any,
      metadataCacheService: {} as any,
      eventEmitter: undefined as any,
      redisRuntimeCacheStore: store,
    });

    await store.setAux(CACHE_IDENTIFIERS.ROUTE, 'match-index', [
      {
        key: '1',
        path: '/posts/:id',
        methods: ['GET'],
        order: 0,
      },
      {
        key: '2',
        path: '/posts/archive',
        methods: ['GET'],
        order: 1,
      },
    ]);
    await store.setAux(CACHE_IDENTIFIERS.ROUTE, 'route:1', {
      id: 1,
      path: '/posts/:id',
      availableMethods: [{ method: 'GET' }],
    });
    await store.setAux(CACHE_IDENTIFIERS.ROUTE, 'route:2', {
      id: 2,
      path: '/posts/archive',
      availableMethods: [{ method: 'GET' }],
    });

    const dynamicMatch = await routeCache.matchRoute('GET', '/posts/123');
    const staticMatch = await routeCache.matchRoute('GET', '/posts/archive');

    expect(dynamicMatch?.route.id).toBe(1);
    expect(dynamicMatch?.params).toEqual({ id: '123' });
    expect(staticMatch?.route.id).toBe(2);
  });

  it('uses snake_case Redis key segments for runtime cache keys', async () => {
    const redis = new MemoryRedis();
    const store = createStore(redis);

    await store.setSnapshot(CACHE_IDENTIFIERS.COLUMN_RULE, { ok: true });
    await store.setAux(CACHE_IDENTIFIERS.FIELD_PERMISSION, 'match-index', {
      ok: true,
    });

    expect(redis.data.has('shared-node:runtime_cache:column_rule')).toBe(true);
    expect(
      redis.data.has(
        'shared-node:runtime_cache:field_permission:aux:match_index',
      ),
    ).toBe(true);
  });
});
