import { describe, expect, it, vi } from 'vitest';
import { ObjectId } from 'mongodb';
import {
  BaseCacheService,
  RedisRuntimeCacheStore,
  RouteCacheService,
} from '../../src/engines/cache';
import { CACHE_IDENTIFIERS } from '../../src/shared/utils/cache-events.constants';

class MemoryRedis {
  data = new Map<string, string | Buffer>();
  expiries = new Map<string, number>();
  scanPatterns: string[] = [];
  hashes = new Map<string, Map<string, Buffer>>();

  async hset(key: string, field: string, value: Buffer) {
    const hash = this.hashes.get(key) ?? new Map<string, Buffer>();
    hash.set(field, value);
    this.hashes.set(key, hash);
    return 1;
  }

  async hgetBuffer(key: string, field: string) { return this.hashes.get(key)?.get(field) ?? null; }
  async hgetallBuffer(key: string) { return Object.fromEntries(this.hashes.get(key) ?? []); }
  async pexpire(key: string, ttl: number) { this.expiries.set(key, ttl); return 1; }

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

  async set(key: string, value: string | Buffer, ...args: any[]) {
    if (args.includes('NX') && this.data.has(key)) return null;
    this.data.set(key, value);
    const pxIndex = args.indexOf('PX');
    if (pxIndex >= 0 && typeof args[pxIndex + 1] === 'number') {
      this.expiries.set(key, args[pxIndex + 1]);
    }
    return 'OK';
  }

  async eval(script: string, keyCount: number, ...args: any[]) {
    const keys = args.slice(0, keyCount);
    const values = args.slice(keyCount);
    if (this.data.get(keys[0]) !== values[0]) return 0;
    if (script.includes("redis.call('hset'")) {
      const count = values[2];
      const auxiliaryCount = values[3];
      for (const key of keys.slice(2 + count + auxiliaryCount)) {
        this.hashes.get(keys[1])?.delete(`aux:${key}`);
        await this.del(key);
      }
      for (let index = 0; index < count; index++) {
        await this.hset(keys[1], values[4 + index * 2], values[5 + index * 2]);
        await this.set(keys[2 + index], values[5 + index * 2], 'PX', values[1]);
      }
      for (let index = 0; index < auxiliaryCount; index++) {
        const key = keys[2 + count + index];
        const value = values[4 + count * 2 + index];
        await this.hset(keys[1], `aux:${key}`, value);
        await this.set(key, value, 'PX', values[1]);
      }
      await this.pexpire(keys[1], values[1]);
      return 1;
    }
    if (script.includes("redis.call('pexpire'")) return this.pexpire(keys[0], Number(values[1]));
    this.data.delete(keys[0]);
    return 1;
  }

  async scan(_cursor: string, _match: string, pattern: string) {
    this.scanPatterns.push(pattern);
    const prefix = pattern.endsWith('*') ? pattern.slice(0, -1) : pattern;
    return ['0', [...this.data.keys()].filter((key) => key.startsWith(prefix))];
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
    runtimeNamespaceLifecycleService: {
      getKeyTtlMs: () => 5000,
      touchKey: async (key: string) => {
        redis.expiries.set(key, 5000);
      },
      registerManagedKey: async () => {},
      unregisterManagedKey: async () => {},
    } as any,
  });
}

function createStoreWithLifecycle(redis = new MemoryRedis()) {
  return createStore(redis);
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
  it('rebuilds after lock contention instead of acknowledging an existing stale snapshot', async () => {
    const store = createStore();
    const cache = new SharedTestCache(store);
    await cache.reload();
    const acquire = vi.spyOn(store, 'acquireRefreshLock').mockResolvedValueOnce(null);
    await cache.reload();
    expect(cache.loadCalls).toBe(2);
    expect(acquire).toHaveBeenCalled();
  });

  it('does not expose staged snapshots when a reload chain fails', async () => {
    const store = createStore();
    await store.setSnapshot('metadata', { revision: 'old' });
    await expect((store as any).runWithPublication(async () => {
      await store.setSnapshot('metadata', { revision: 'candidate' });
      expect((await store.getSnapshot<any>('metadata'))?.data.revision).toBe('candidate');
      throw new Error('route build failed');
    })).rejects.toThrow('route build failed');
    expect((await store.getSnapshot<any>('metadata'))?.data.revision).toBe('old');
  });

  it('pins a complete committed snapshot set during receiver replay', async () => {
    const redis = new MemoryRedis();
    const writer = createStore(redis);
    const receiver = createStore(redis);
    await writer.setSnapshot('metadata', { revision: 'old' });
    await writer.setSnapshot('route', { revision: 'old' });
    await (writer as any).runWithPublication(async () => {
      await writer.setSnapshot('metadata', { revision: 'first' });
      await writer.setSnapshot('route', { revision: 'first' });
    });
    await (receiver as any).runWithCommittedSnapshots(async () => {
      expect((await receiver.getSnapshot<any>('metadata'))?.data.revision).toBe('first');
      await (writer as any).runWithPublication(async () => {
        await writer.setSnapshot('metadata', { revision: 'second' });
        await writer.setSnapshot('route', { revision: 'second' });
      });
      expect((await receiver.getSnapshot<any>('route'))?.data.revision).toBe('first');
    });
    expect((await receiver.getSnapshot<any>('route'))?.data.revision).toBe('second');
  });

  it('keeps committed auxiliary routes when a candidate publication fails', async () => {
    const redis = new MemoryRedis();
    const writer = createStore(redis);
    const reader = createStore(redis);
    await writer.runWithPublication(async () => {
      await writer.setSnapshot('route', { revision: 'old' });
      await writer.setAux('route', 'route:old', { revision: 'old' });
    });
    await expect(writer.runWithPublication(async () => {
      await writer.deleteAuxByPrefix('route', 'route:');
      expect(await writer.getAux('route', 'route:old')).toBeNull();
      expect(await reader.getAux('route', 'route:old')).toEqual({ revision: 'old' });
      throw new Error('candidate failed');
    })).rejects.toThrow('candidate failed');
    expect(await reader.getAux('route', 'route:old')).toEqual({ revision: 'old' });
    await writer.runWithPublication(async () => {
      await writer.deleteAuxByPrefix('route', 'route:');
      await writer.setAux('route', 'route:new', { revision: 'new' });
    });
    expect(await reader.getAux('route', 'route:old')).toBeNull();
    expect(await reader.getAux('route', 'route:new')).toEqual({ revision: 'new' });
  });

  it('pins auxiliary data with snapshots and keeps an empty pinned set immutable', async () => {
    const redis = new MemoryRedis();
    const writer = createStore(redis);
    const reader = createStore(redis);
    await reader.runWithCommittedSnapshots(async () => {
      await writer.setSnapshot('route', { revision: 'first' });
      expect(await reader.getSnapshot('route')).toBeNull();
    });
    await writer.runWithPublication(() => writer.setAux('route', 'index', { revision: 'first' }));
    await reader.runWithCommittedSnapshots(async () => {
      await writer.runWithPublication(() => writer.setAux('route', 'index', { revision: 'second' }));
      expect(await reader.getAux('route', 'index')).toEqual({ revision: 'first' });
    });
  });

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

  it('stores runtime snapshots with namespace lifecycle TTL when available', async () => {
    const redis = new MemoryRedis();
    const store = createStoreWithLifecycle(redis);

    await store.setSnapshot(CACHE_IDENTIFIERS.SETTING, { ok: true });

    expect(redis.expiries.get('shared-node:runtime_cache:setting')).toBe(5000);
  });

  it('renews runtime snapshot lifecycle TTL when reading a shared snapshot', async () => {
    const redis = new MemoryRedis();
    const store = createStoreWithLifecycle(redis);

    await store.setSnapshot(CACHE_IDENTIFIERS.SETTING, { ok: true });
    redis.expiries.set('shared-node:runtime_cache:setting', 100);
    await store.getSnapshot(CACHE_IDENTIFIERS.SETTING);

    expect(redis.expiries.get('shared-node:runtime_cache:setting')).toBe(5000);
  });

  it('stores and renews runtime aux keys with namespace lifecycle TTL', async () => {
    const redis = new MemoryRedis();
    const store = createStoreWithLifecycle(redis);

    await store.setAux(CACHE_IDENTIFIERS.ROUTE, 'match-index', [{ id: 1 }]);
    expect(
      redis.expiries.get('shared-node:runtime_cache:route:aux:match_index'),
    ).toBe(5000);

    redis.expiries.set('shared-node:runtime_cache:route:aux:match_index', 100);
    await store.getAux(CACHE_IDENTIFIERS.ROUTE, 'match-index');

    expect(
      redis.expiries.get('shared-node:runtime_cache:route:aux:match_index'),
    ).toBe(5000);
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
        table: 'enfyra_setting',
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
          availableMethods: [{ name: 'GET' }],
          mainTable: { name: 'posts' },
        },
        {
          id: 2,
          path: '/posts/archive',
          availableMethods: [{ name: 'GET' }],
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
      availableMethods: [{ name: 'GET' }],
    });
    await store.setAux(CACHE_IDENTIFIERS.ROUTE, 'route:2', {
      id: 2,
      path: '/posts/archive',
      availableMethods: [{ name: 'GET' }],
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

  it('escapes glob metacharacters when deleting aux keys by prefix', async () => {
    const redis = new MemoryRedis();
    const store = createStore(redis);

    await store.setAux(CACHE_IDENTIFIERS.FIELD_PERMISSION, 'route:1', {
      ok: true,
    });
    await store.setAux(CACHE_IDENTIFIERS.FIELD_PERMISSION, 'unrelated', {
      ok: true,
    });

    await store.deleteAuxByPrefix(CACHE_IDENTIFIERS.FIELD_PERMISSION, 'route:');

    expect(redis.scanPatterns).toEqual([
      'shared-node:runtime_cache:field_permission:aux:route:*',
    ]);
    expect(
      redis.data.has('shared-node:runtime_cache:field_permission:aux:route:1'),
    ).toBe(false);
    expect(
      redis.data.has('shared-node:runtime_cache:field_permission:aux:unrelated'),
    ).toBe(true);
  });

  it('treats wildcard characters in an aux prefix as literals', async () => {
    const redis = new MemoryRedis();
    const store = createStore(redis);

    await store.deleteAuxByPrefix(CACHE_IDENTIFIERS.FIELD_PERMISSION, '*');

    expect(redis.scanPatterns).toEqual([
      'shared-node:runtime_cache:field_permission:aux:\\**',
    ]);
  });
});
