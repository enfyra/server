import { describe, expect, it, beforeAll, afterAll } from 'vitest';
import Redis from 'ioredis';
import { RedisRuntimeCacheStore } from '../../../src/engines/cache/services/redis-runtime-cache-store.service';
import { MetadataCacheService } from '../../../src/engines/cache/services/metadata-cache.service';

/**
 * RED test: reproduces the production stale-metadata bug after an upgrade
 * with 2 instances sharing one Redis runtime cache namespace.
 *
 * Production symptom: `Schema mutation target revision mismatch` with a
 * FIXED hash pair (expected=f9b42f…, current=6680f1…) repeating — one
 * instance keeps building the expected revision from stale metadata while
 * the DB has already moved to the new revision.
 *
 * Root cause (code): when REDIS_RUNTIME_CACHE=true, the shared Redis
 * snapshot is the source of truth for every instance except the one that
 * wrote it. Two defects:
 *   1. `setLoadedMetadata(metadata, publish=false)` — the path used by
 *      first-run/upgrade (`metadataCacheService.reload(false)`) and by
 *      `clearMetadataCache`+warm — only sets `inMemoryCache`, it NEVER
 *      writes the new snapshot to Redis. So after an upgrade the leader
 *      works on new metadata in memory while Redis still holds the OLD
 *      snapshot that every other (and future) instance reads.
 *   2. `getMetadata()` returns a non-null `inMemoryCache` without ever
 *      comparing its version to the Redis snapshot version, so a stale
 *      in-memory copy is never refreshed.
 *
 * These use a real local Redis (port 6379) and a unique namespace so the
 * test is isolated and requirements-free.
 */
const NAMESPACE = `enfyra-test-${Date.now()}-${Math.floor(Math.random() * 1e6)}`;

function makeEnvService(overrides: Record<string, unknown> = {}) {
  return {
    get: (key: string) =>
      (overrides as Record<string, unknown>)[key] ??
      ({
        NODE_NAME: NAMESPACE,
        REDIS_RUNTIME_CACHE: true,
      } as Record<string, unknown>)[key],
  };
}

function makeLifecycleFake() {
  return {
    getKeyTtlMs: () => 30 * 60 * 1000,
    touchKey: async () => undefined,
  };
}

function makeMetadataService(store: RedisRuntimeCacheStore) {
  return new MetadataCacheService({
    databaseConfigService: { getDbType: () => 'mysql' } as any,
    lazyRef: {} as any,
    redisRuntimeCacheStore: store,
    eventEmitter: undefined,
  });
}

function makeMetadata(version: number, tableName: string) {
  return {
    tables: new Map([[tableName, { name: tableName }]]),
    tablesList: [{ name: tableName }],
    version,
    timestamp: new Date(),
  };
}

describe('MetadataCacheService shared Redis sync (stale-safe)', () => {
  let redis: Redis;
  let store: RedisRuntimeCacheStore;
  let storeUri: string;

  beforeAll(async () => {
    storeUri = process.env.REDIS_URI || 'redis://localhost:6379';
    redis = new Redis(storeUri, { maxRetriesPerRequest: 1 });
    await redis.ping();
    store = new RedisRuntimeCacheStore({
      redis,
      envService: makeEnvService() as any,
      runtimeNamespaceLifecycleService: makeLifecycleFake() as any,
    });
  });

  afterAll(async () => {
    if (redis) await redis.quit();
  });

  it('RED: upgrade path (reload(false)) must write the fresh snapshot to shared Redis', async () => {
    // Simulate: Redis already holds an OLD snapshot (pre-upgrade).
    await store.setSnapshot('metadata', makeMetadata(1, 'old_table'));

    // Instance A (leader) performs the upgrade/bootstrap warm:
    // metadataCacheService.reload(false) → setLoadedMetadata(metadata, false).
    const leader = makeMetadataService(store);
    await leader['setLoadedMetadata'](makeMetadata(2, 'new_table'), false);

    // The shared Redis snapshot must now reflect the NEW revision so that
    // every other instance booting afterward reads the upgraded schema.
    const snapshot = await store.getSnapshot<any>('metadata');
    expect(snapshot).not.toBeNull();
    expect(snapshot!.data.tablesList.map((t: any) => t.name)).toContain(
      'new_table',
    );
  });

  it('RED: getMetadata() must refresh a stale in-memory copy from Redis when versions diverge', async () => {
    // Instance A (leader) loads the NEW revision and persists it to Redis.
    const leader = makeMetadataService(store);
    await leader['setLoadedMetadata'](makeMetadata(50, 'new_table'), true);

    // Instance B (worker) already has an OLD in-memory cache loaded.
    const worker = makeMetadataService(store);
    worker['sharedCacheLoaded'] = true;
    worker['inMemoryCache'] = makeMetadata(1, 'old_table');

    // Instance B must NOT keep serving the stale version when Redis has moved on.
    const metadata = await worker.getMetadata();
    expect(metadata.tablesList.map((t: any) => t.name)).toContain('new_table');
  });
});