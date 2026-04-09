import { ConfigService } from '@nestjs/config';
import { EventEmitter2 } from '@nestjs/event-emitter';
import Redis from 'ioredis';
import { BaseCacheService } from '../../src/infrastructure/cache/services/base-cache.service';
import { RedisPubSubService } from '../../src/infrastructure/cache/services/redis-pubsub.service';
import { InstanceService } from '../../src/shared/services/instance.service';
import { CACHE_IDENTIFIERS } from '../../src/shared/utils/cache-events.constants';
import {
  PACKAGE_CACHE_SYNC_EVENT_KEY,
  ROUTE_CACHE_SYNC_EVENT_KEY,
  METADATA_CACHE_SYNC_EVENT_KEY,
  FLOW_CACHE_SYNC_EVENT_KEY,
  WEBSOCKET_CACHE_SYNC_EVENT_KEY,
  STORAGE_CONFIG_CACHE_SYNC_EVENT_KEY,
  AI_CONFIG_CACHE_SYNC_EVENT_KEY,
  OAUTH_CONFIG_CACHE_SYNC_EVENT_KEY,
  FOLDER_TREE_CACHE_SYNC_EVENT_KEY,
} from '../../src/shared/utils/constant';

function mockConfig(
  nodeName: string | undefined,
  redisUri = 'redis://127.0.0.1:6379',
): ConfigService {
  return {
    get: (key: string) => {
      if (key === 'NODE_NAME') return nodeName;
      if (key === 'REDIS_URI') return redisUri;
      return undefined;
    },
  } as ConfigService;
}

function createPubSubMatcher(nodeName: string | undefined): RedisPubSubService {
  return new RedisPubSubService(mockConfig(nodeName));
}

class TestPackageCache extends BaseCacheService<string[]> {
  reloadCallCount = 0;

  constructor(
    redisPubSub: RedisPubSubService | Record<string, unknown>,
    instance: InstanceService,
    emitter?: EventEmitter2,
  ) {
    super(
      {
        syncEventKey: PACKAGE_CACHE_SYNC_EVENT_KEY,
        cacheIdentifier: CACHE_IDENTIFIERS.PACKAGE,
        colorCode: '',
        cacheName: 'TestPackageCache',
      },
      redisPubSub as RedisPubSubService,
      instance,
      emitter,
    );
  }

  protected async loadFromDb(): Promise<string[]> {
    return ['stub-pkg'];
  }

  protected transformData(raw: string[]): string[] {
    return raw;
  }

  protected handleSyncData(): void {}

  async reload(): Promise<void> {
    this.reloadCallCount++;
    return super.reload();
  }
}

function websocketChannelMatches(channel: string, baseKey: string): boolean {
  return channel === baseKey || channel.startsWith(`${baseKey}:`);
}

const ALL_SYNC_BASE_KEYS = [
  PACKAGE_CACHE_SYNC_EVENT_KEY,
  ROUTE_CACHE_SYNC_EVENT_KEY,
  METADATA_CACHE_SYNC_EVENT_KEY,
  FLOW_CACHE_SYNC_EVENT_KEY,
  STORAGE_CONFIG_CACHE_SYNC_EVENT_KEY,
  AI_CONFIG_CACHE_SYNC_EVENT_KEY,
  OAUTH_CONFIG_CACHE_SYNC_EVENT_KEY,
  WEBSOCKET_CACHE_SYNC_EVENT_KEY,
  FOLDER_TREE_CACHE_SYNC_EVENT_KEY,
] as const;

describe('RedisPubSubService.isChannelForBase (multi-instance cohort)', () => {
  const clusterIds = [
    'my_enfyra',
    'prod-east',
    'tenant-a-1',
    'c9f2',
    'cluster:with:colons',
  ];

  it.each(clusterIds)(
    'NODE_NAME=%s: decorated channel matches base',
    (nodeName) => {
      const svc = createPubSubMatcher(nodeName);
      const key = 'enfyra:package-cache-sync';
      expect(svc.isChannelForBase(`${key}:${nodeName}`, key)).toBe(true);
      expect(svc.isChannelForBase(key, key)).toBe(true);
    },
  );

  it('NODE_NAME empty: only exact base matches', () => {
    const svc = createPubSubMatcher(undefined);
    const key = ROUTE_CACHE_SYNC_EVENT_KEY;
    expect(svc.isChannelForBase(key, key)).toBe(true);
    expect(svc.isChannelForBase(`${key}:anything`, key)).toBe(false);
  });

  it('NODE_NAME empty: empty string from config treated as unset', () => {
    const svc = new RedisPubSubService({
      get: (k: string) => {
        if (k === 'NODE_NAME') return '';
        if (k === 'REDIS_URI') return 'redis://x';
        return undefined;
      },
    } as ConfigService);
    const key = METADATA_CACHE_SYNC_EVENT_KEY;
    expect(svc.isChannelForBase(key, key)).toBe(true);
    expect(svc.isChannelForBase(`${key}:suffix`, key)).toBe(false);
  });

  it.each(ALL_SYNC_BASE_KEYS)(
    'production sync key %s matches decorated peer channel',
    (baseKey) => {
      const nodeName = 'peer-shared';
      const svc = createPubSubMatcher(nodeName);
      expect(svc.isChannelForBase(`${baseKey}:${nodeName}`, baseKey)).toBe(
        true,
      );
    },
  );

  it('rejects wrong cluster suffix (different NODE_NAME on message)', () => {
    const svc = createPubSubMatcher('cluster-a');
    const base = PACKAGE_CACHE_SYNC_EVENT_KEY;
    expect(svc.isChannelForBase(`${base}:cluster-b`, base)).toBe(false);
  });

  it('rejects unrelated channel prefix', () => {
    const svc = createPubSubMatcher('n1');
    expect(
      svc.isChannelForBase('other:channel:n1', PACKAGE_CACHE_SYNC_EVENT_KEY),
    ).toBe(false);
  });

  it('rejects substring trap: base must be full base, not partial', () => {
    const svc = createPubSubMatcher('x');
    expect(
      svc.isChannelForBase(
        'enfyra:route-cache-sync-extra:x',
        ROUTE_CACHE_SYNC_EVENT_KEY,
      ),
    ).toBe(false);
  });

  it('stable across repeated calls (lazy node name cache)', () => {
    const svc = createPubSubMatcher('lazy');
    const base = FLOW_CACHE_SYNC_EVENT_KEY;
    expect(svc.isChannelForBase(`${base}:lazy`, base)).toBe(true);
    expect(svc.isChannelForBase(`${base}:lazy`, base)).toBe(true);
  });

  describe('matrix', () => {
    const rows: Array<[string, string, string, boolean]> = [
      ['', 'enfyra:a', 'enfyra:a', true],
      ['', 'enfyra:a:b', 'enfyra:a', false],
      ['n', 'enfyra:a', 'enfyra:a', true],
      ['n', 'enfyra:a:n', 'enfyra:a', true],
      ['n', 'enfyra:a:n2', 'enfyra:a', false],
      ['long-name', 'k:long-name', 'k', true],
    ];
    it.each(rows)(
      'NODE_NAME=%j received=%j base=%j => %j',
      (nodeName, received, base, expected) => {
        const svc = createPubSubMatcher(nodeName || undefined);
        expect(svc.isChannelForBase(received, base)).toBe(expected);
      },
    );
  });
});

describe('WebSocket gateway channel pattern vs isChannelForBase', () => {
  it('decorated websocket channel matches both rules for same NODE_NAME', () => {
    const node = 'ws-cluster';
    const base = WEBSOCKET_CACHE_SYNC_EVENT_KEY;
    const decorated = `${base}:${node}`;
    const matcher = createPubSubMatcher(node);
    expect(matcher.isChannelForBase(decorated, base)).toBe(true);
    expect(websocketChannelMatches(decorated, base)).toBe(true);
  });

  it('startsWith allows future suffix variants; isChannelForBase is stricter', () => {
    const base = WEBSOCKET_CACHE_SYNC_EVENT_KEY;
    const weird = `${base}:node:extra`;
    const matcher = createPubSubMatcher('node');
    expect(websocketChannelMatches(weird, base)).toBe(true);
    expect(matcher.isChannelForBase(weird, base)).toBe(false);
  });
});

describe('BaseCacheService RELOAD_SIGNAL across instances (subscriber handler)', () => {
  let registeredHandler: (ch: string, msg: string) => Promise<void>;

  function buildAdapter(
    nodeName: string | undefined,
    instance: InstanceService,
  ) {
    const matcher = createPubSubMatcher(nodeName);
    const redisPubSub = {
      subscribeWithHandler: jest.fn(
        (_: string, handler: (c: string, m: string) => Promise<void>) => {
          registeredHandler = handler;
          return true;
        },
      ),
      publish: jest.fn().mockResolvedValue(undefined),
      isChannelForBase: matcher.isChannelForBase.bind(matcher),
    };
    const cache = new TestPackageCache(
      redisPubSub,
      instance,
      new EventEmitter2(),
    );
    return { cache, redisPubSub };
  }

  it('peer reload: decorated channel triggers reload on other instance', async () => {
    const peerA = new InstanceService();
    const peerB = new InstanceService();
    const node = 'shared-01';
    const { cache } = buildAdapter(node, peerB);
    expect(registeredHandler).toBeDefined();

    const msg = JSON.stringify({
      instanceId: peerA.getInstanceId(),
      type: 'RELOAD_SIGNAL',
      timestamp: Date.now(),
    });
    await registeredHandler(`${PACKAGE_CACHE_SYNC_EVENT_KEY}:${node}`, msg);
    expect(cache.reloadCallCount).toBe(1);
  });

  it('same instance id echo: no reload', async () => {
    const self = new InstanceService();
    const node = 'shared-02';
    const { cache } = buildAdapter(node, self);
    const msg = JSON.stringify({
      instanceId: self.getInstanceId(),
      type: 'RELOAD_SIGNAL',
    });
    await registeredHandler(`${PACKAGE_CACHE_SYNC_EVENT_KEY}:${node}`, msg);
    expect(cache.reloadCallCount).toBe(0);
  });

  it('NODE_NAME unset: plain base channel triggers peer reload', async () => {
    const peerA = new InstanceService();
    const peerB = new InstanceService();
    const { cache } = buildAdapter(undefined, peerB);
    const msg = JSON.stringify({
      instanceId: peerA.getInstanceId(),
      type: 'RELOAD_SIGNAL',
    });
    await registeredHandler(PACKAGE_CACHE_SYNC_EVENT_KEY, msg);
    expect(cache.reloadCallCount).toBe(1);
  });

  it('NODE_NAME set but handler sees wrong channel: no reload', async () => {
    const peerA = new InstanceService();
    const peerB = new InstanceService();
    const { cache } = buildAdapter('correct-node', peerB);
    const msg = JSON.stringify({
      instanceId: peerA.getInstanceId(),
      type: 'RELOAD_SIGNAL',
    });
    await registeredHandler(`${PACKAGE_CACHE_SYNC_EVENT_KEY}:wrong-node`, msg);
    expect(cache.reloadCallCount).toBe(0);
  });

  it('malformed JSON: reload not counted (handler catches)', async () => {
    const peerB = new InstanceService();
    const node = 'shared-03';
    const { cache } = buildAdapter(node, peerB);
    const before = cache.reloadCallCount;
    await registeredHandler(
      `${PACKAGE_CACHE_SYNC_EVENT_KEY}:${node}`,
      'not-json',
    );
    expect(cache.reloadCallCount).toBe(before);
  });

  it('peer JSON sync triggers reload (type field not filtered)', async () => {
    const peerA = new InstanceService();
    const peerB = new InstanceService();
    const node = 'shared-04';
    const { cache } = buildAdapter(node, peerB);
    const msg = JSON.stringify({
      instanceId: peerA.getInstanceId(),
      type: 'OTHER',
    });
    await registeredHandler(`${PACKAGE_CACHE_SYNC_EVENT_KEY}:${node}`, msg);
    expect(cache.reloadCallCount).toBe(1);
  });

  it('concurrent peer signals: each handler entry runs reload(); coalesced work inside BaseCacheService', async () => {
    const peerA = new InstanceService();
    const peerB = new InstanceService();
    const node = 'shared-05';
    const { cache } = buildAdapter(node, peerB);
    const msg = JSON.stringify({
      instanceId: peerA.getInstanceId(),
      type: 'RELOAD_SIGNAL',
    });
    const ch = `${PACKAGE_CACHE_SYNC_EVENT_KEY}:${node}`;
    const n = 40;
    await Promise.all(
      Array.from({ length: n }, () => registeredHandler(ch, msg)),
    );
    expect(cache.reloadCallCount).toBe(n);
  });
});

describe('Redis decorated channel round-trip (integration)', () => {
  const redisUri = process.env.REDIS_URI || 'redis://localhost:6379';
  let redis: Redis | null = null;

  beforeAll(async () => {
    try {
      const r = new Redis(redisUri, {
        lazyConnect: true,
        maxRetriesPerRequest: 1,
      });
      await r.connect();
      await r.ping();
      redis = r;
    } catch {
      redis = null;
    }
  });

  afterAll(async () => {
    if (redis) redis.disconnect();
  });

  it('publish base+NODE_NAME delivers to subscriber on same decorated channel', async () => {
    if (!redis) return;
    const runId = `mi-${Date.now()}`;
    const base = `enfyra:test-sync:${runId}`;
    const nodeName = `cluster-${runId}`;
    const decorated = `${base}:${nodeName}`;
    const payload = JSON.stringify({ ok: true, runId });

    const sub = redis!.duplicate();
    const received: string[] = [];
    await sub.subscribe(decorated);
    const done = new Promise<void>((resolve, reject) => {
      const t = setTimeout(() => reject(new Error('subscribe timeout')), 8000);
      sub.on('message', (ch, msg) => {
        if (ch === decorated) {
          received.push(msg);
          clearTimeout(t);
          resolve();
        }
      });
    });
    await redis!.publish(decorated, payload);
    await done;
    await sub.unsubscribe(decorated);
    sub.disconnect();
    expect(received).toEqual([payload]);
  });

  it('two ioredis clients mimic two pods: same NODE_NAME suffix see same bus', async () => {
    if (!redis) return;
    const runId = `bus-${Date.now()}`;
    const base = `enfyra:mimic:${runId}`;
    const nodeName = 'same-for-all-pods';
    const channel = `${base}:${nodeName}`;
    const subB = redis!.duplicate();
    const got: string[] = [];
    await subB.subscribe(channel);
    const waitOne = new Promise<void>((resolve, reject) => {
      const t = setTimeout(() => reject(new Error('timeout')), 8000);
      subB.on('message', (_ch, m) => {
        got.push(m);
        clearTimeout(t);
        resolve();
      });
    });
    await redis!.publish(channel, 'ping');
    await waitOne;
    await subB.unsubscribe(channel);
    subB.disconnect();
    expect(got).toEqual(['ping']);
  });
});

describe('Contract: decorateChannel equals base + ":" + NODE_NAME', () => {
  it.each([
    ['a', 'enfyra:k', 'enfyra:k:a'],
    ['z-9', 'x:y', 'x:y:z-9'],
  ])('NODE_NAME %s + base %s', (node, base, expected) => {
    const svc = createPubSubMatcher(node);
    expect(svc.isChannelForBase(expected, base)).toBe(true);
  });
});

describe('Randomized isChannelForBase guard', () => {
  it('100 random bases with NODE_NAME never false-positive wrong suffix', () => {
    const node = 'rand-cluster';
    const svc = createPubSubMatcher(node);
    for (let i = 0; i < 100; i++) {
      const base = `enfyra:rnd:${i}:${Math.random().toString(36).slice(2)}`;
      const good = `${base}:${node}`;
      const bad = `${base}:${node}x`;
      expect(svc.isChannelForBase(good, base)).toBe(true);
      expect(svc.isChannelForBase(bad, base)).toBe(false);
    }
  });
});
