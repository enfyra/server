/**
 * Tests for cache pub/sub reload flow.
 * Uses inline implementation to avoid importing the full module graph (isolated-vm crashes Jest).
 */

import { EventEmitter2 } from '@nestjs/event-emitter';

// ─── Inline BaseCacheService (mirrors production logic) ──────────

type MessageHandler = (channel: string, message: string) => void;

interface MockPubSub {
  handlers: Map<string, MessageHandler[]>;
  published: Array<{ channel: string; payload: any }>;
  subscribeWithHandler: jest.Mock;
  publish: jest.Mock;
  isChannelForBase: jest.Mock;
  simulateMessage(channel: string, message: string): Promise<any>;
}

function createMockPubSub(): MockPubSub {
  const handlers: Map<string, MessageHandler[]> = new Map();
  const published: Array<{ channel: string; payload: any }> = [];

  return {
    handlers,
    published,
    subscribeWithHandler: jest.fn(
      (channel: string, handler: MessageHandler) => {
        if (!handlers.has(channel)) handlers.set(channel, []);
        handlers.get(channel)!.push(handler);
      },
    ),
    publish: jest.fn(async (channel: string, message: string) => {
      published.push({ channel, payload: JSON.parse(message) });
    }),
    isChannelForBase: jest.fn(
      (received: string, base: string) => received === base,
    ),
    simulateMessage(channel: string, message: string) {
      const list = handlers.get(channel) || [];
      return Promise.all(list.map((h) => h(channel, message)));
    },
  };
}

interface CacheConfig {
  syncEventKey: string;
  cacheName: string;
}

class TestBaseCacheService {
  cache: string[] = [];
  cacheLoaded = false;
  isLoading = false;
  loadingPromise: Promise<void> | null = null;
  loadCount = 0;
  loadDelay = 0;
  loadData: string[] = ['a', 'b', 'c'];
  loadError: Error | null = null;
  publishedCount = 0;

  constructor(
    private readonly config: CacheConfig,
    private readonly pubsub: MockPubSub,
    private readonly instanceId: string,
    private readonly eventEmitter?: EventEmitter2,
  ) {
    this.setupSubscription();
  }

  private setupSubscription(): void {
    const handler = async (channel: string, message: string) => {
      if (this.pubsub.isChannelForBase(channel, this.config.syncEventKey)) {
        await this.handleIncomingMessage(message);
      }
    };
    this.pubsub.subscribeWithHandler(this.config.syncEventKey, handler);
  }

  private async handleIncomingMessage(message: string): Promise<void> {
    try {
      const payload = JSON.parse(message);
      if (payload.instanceId === this.instanceId) {
        return;
      }
      if (payload.type === 'RELOAD_SIGNAL') {
        await this.reload(false);
      }
    } catch {}
  }

  async reload(publish = true): Promise<void> {
    if (this.isLoading && this.loadingPromise) {
      return this.loadingPromise;
    }

    this.isLoading = true;
    this.loadingPromise = (async () => {
      try {
        this.loadCount++;
        if (this.loadDelay > 0) {
          await new Promise((r) => setTimeout(r, this.loadDelay));
        }
        if (this.loadError) throw this.loadError;

        this.cache = [...this.loadData];
        this.cacheLoaded = true;

        this.eventEmitter?.emit(`${this.config.cacheName}:loaded`);

        if (publish) {
          await this.publishReloadSignal();
        }
      } catch (error) {
        throw error;
      } finally {
        this.isLoading = false;
        this.loadingPromise = null;
      }
    })();

    return this.loadingPromise;
  }

  private async publishReloadSignal(): Promise<void> {
    this.publishedCount++;
    await this.pubsub.publish(
      this.config.syncEventKey,
      JSON.stringify({
        instanceId: this.instanceId,
        type: 'RELOAD_SIGNAL',
        timestamp: Date.now(),
      }),
    );
  }
}

function makeSignal(instanceId: string) {
  return JSON.stringify({
    instanceId,
    type: 'RELOAD_SIGNAL',
    timestamp: Date.now(),
  });
}

// ─── Tests ───────────────────────────────────────────────────────

describe('Cache PubSub Reload Flow', () => {
  const CHANNEL = 'test:cache:sync';
  const config: CacheConfig = { syncEventKey: CHANNEL, cacheName: 'TestCache' };

  // ════════════════════════════════════════════════════════════════
  // BASIC RELOAD
  // ════════════════════════════════════════════════════════════════

  describe('Basic reload', () => {
    it('reload(true) loads data and publishes signal', async () => {
      const pubsub = createMockPubSub();
      const c = new TestBaseCacheService(config, pubsub, 'inst-1');
      await c.reload(true);

      expect(c.cache).toEqual(['a', 'b', 'c']);
      expect(c.cacheLoaded).toBe(true);
      expect(pubsub.published).toHaveLength(1);
      expect(pubsub.published[0].payload.instanceId).toBe('inst-1');
    });

    it('reload(false) loads data but does NOT publish', async () => {
      const pubsub = createMockPubSub();
      const c = new TestBaseCacheService(config, pubsub, 'inst-1');
      await c.reload(false);

      expect(c.cache).toEqual(['a', 'b', 'c']);
      expect(c.cacheLoaded).toBe(true);
      expect(pubsub.published).toHaveLength(0);
    });

    it('reload() defaults to publish=true', async () => {
      const pubsub = createMockPubSub();
      const c = new TestBaseCacheService(config, pubsub, 'inst-1');
      await c.reload();

      expect(pubsub.published).toHaveLength(1);
    });

    it('emits loaded event on both publish and no-publish', async () => {
      const pubsub = createMockPubSub();
      const ee = new EventEmitter2();
      const events: string[] = [];
      ee.on('TestCache:loaded', () => events.push('loaded'));

      const c = new TestBaseCacheService(config, pubsub, 'inst-1', ee);
      await c.reload(true);
      await c.reload(false);

      expect(events).toEqual(['loaded', 'loaded']);
    });
  });

  // ════════════════════════════════════════════════════════════════
  // REDIS SIGNAL HANDLING
  // ════════════════════════════════════════════════════════════════

  describe('Redis signal handling', () => {
    it('ignores signal from same instance', async () => {
      const pubsub = createMockPubSub();
      const c = new TestBaseCacheService(config, pubsub, 'inst-1');
      await pubsub.simulateMessage(CHANNEL, makeSignal('inst-1'));

      expect(c.cacheLoaded).toBe(false);
      expect(c.loadCount).toBe(0);
    });

    it('reloads on signal from different instance', async () => {
      const pubsub = createMockPubSub();
      const c = new TestBaseCacheService(config, pubsub, 'inst-1');
      await pubsub.simulateMessage(CHANNEL, makeSignal('inst-2'));

      expect(c.cacheLoaded).toBe(true);
      expect(c.cache).toEqual(['a', 'b', 'c']);
    });

    it('does NOT re-publish when handling remote signal', async () => {
      const pubsub = createMockPubSub();
      const c = new TestBaseCacheService(config, pubsub, 'inst-1');
      await pubsub.simulateMessage(CHANNEL, makeSignal('inst-2'));

      expect(pubsub.published).toHaveLength(0);
      expect(c.publishedCount).toBe(0);
    });

    it('handles malformed JSON gracefully', async () => {
      const pubsub = createMockPubSub();
      const c = new TestBaseCacheService(config, pubsub, 'inst-1');
      await pubsub.simulateMessage(CHANNEL, 'not json');

      expect(c.cacheLoaded).toBe(false);
    });
  });

  // ════════════════════════════════════════════════════════════════
  // MULTI-INSTANCE NO LOOP
  // ════════════════════════════════════════════════════════════════

  describe('Multi-instance no loop', () => {
    it('2 instances: inst-1 reload → inst-2 syncs, no publish back', async () => {
      const pubsub = createMockPubSub();
      const c1 = new TestBaseCacheService(config, pubsub, 'inst-1');
      const c2 = new TestBaseCacheService(config, pubsub, 'inst-2');

      await c1.reload(true);
      expect(pubsub.published).toHaveLength(1);

      const signal = JSON.stringify(pubsub.published[0].payload);
      await pubsub.simulateMessage(CHANNEL, signal);

      expect(c1.loadCount).toBe(1);
      expect(c2.loadCount).toBe(1);
      expect(pubsub.published).toHaveLength(1);
    });

    it('3 instances: only originator publishes', async () => {
      const pubsub = createMockPubSub();
      const c1 = new TestBaseCacheService(config, pubsub, 'inst-1');
      const c2 = new TestBaseCacheService(config, pubsub, 'inst-2');
      const c3 = new TestBaseCacheService(config, pubsub, 'inst-3');

      await c1.reload(true);
      const signal = JSON.stringify(pubsub.published[0].payload);
      await pubsub.simulateMessage(CHANNEL, signal);

      expect(c1.loadCount).toBe(1);
      expect(c2.loadCount).toBe(1);
      expect(c3.loadCount).toBe(1);
      expect(pubsub.published).toHaveLength(1);
    });

    it('10 instances: still only 1 publish', async () => {
      const pubsub = createMockPubSub();
      const instances = Array.from(
        { length: 10 },
        (_, i) => new TestBaseCacheService(config, pubsub, `inst-${i}`),
      );

      await instances[0].reload(true);
      const signal = JSON.stringify(pubsub.published[0].payload);
      await pubsub.simulateMessage(CHANNEL, signal);

      for (const inst of instances) {
        expect(inst.loadCount).toBe(1);
      }
      expect(pubsub.published).toHaveLength(1);
    });
  });

  // ════════════════════════════════════════════════════════════════
  // CONCURRENT RELOAD GUARD
  // ════════════════════════════════════════════════════════════════

  describe('Concurrent reload guard', () => {
    it('concurrent reload() calls share same promise', async () => {
      const pubsub = createMockPubSub();
      const c = new TestBaseCacheService(config, pubsub, 'inst-1');
      c.loadDelay = 50;

      const p1 = c.reload();
      const p2 = c.reload();
      await Promise.all([p1, p2]);

      expect(c.loadCount).toBe(1);
    });

    it('reload after previous completes triggers new load', async () => {
      const pubsub = createMockPubSub();
      const c = new TestBaseCacheService(config, pubsub, 'inst-1');

      await c.reload(false);
      await c.reload(false);

      expect(c.loadCount).toBe(2);
    });

    it('concurrent remote signals deduplicate', async () => {
      const pubsub = createMockPubSub();
      const c = new TestBaseCacheService(config, pubsub, 'inst-1');
      c.loadDelay = 50;

      const p1 = pubsub.simulateMessage(CHANNEL, makeSignal('inst-2'));
      const p2 = pubsub.simulateMessage(CHANNEL, makeSignal('inst-3'));
      await Promise.all([p1, p2]);

      expect(c.loadCount).toBe(1);
      expect(pubsub.published).toHaveLength(0);
    });
  });

  // ════════════════════════════════════════════════════════════════
  // ERROR HANDLING
  // ════════════════════════════════════════════════════════════════

  describe('Error handling', () => {
    it('reload throws on loadFromDb error', async () => {
      const pubsub = createMockPubSub();
      const c = new TestBaseCacheService(config, pubsub, 'inst-1');
      c.loadError = new Error('DB down');

      await expect(c.reload()).rejects.toThrow('DB down');
      expect(c.cacheLoaded).toBe(false);
    });

    it('error clears isLoading flag so next reload works', async () => {
      const pubsub = createMockPubSub();
      const c = new TestBaseCacheService(config, pubsub, 'inst-1');
      c.loadError = new Error('DB down');
      await c.reload().catch(() => {});

      c.loadError = null;
      await c.reload(false);

      expect(c.cacheLoaded).toBe(true);
      expect(c.loadCount).toBe(2);
    });

    it('error on reload does NOT publish', async () => {
      const pubsub = createMockPubSub();
      const c = new TestBaseCacheService(config, pubsub, 'inst-1');
      c.loadError = new Error('DB down');
      await c.reload().catch(() => {});

      expect(pubsub.published).toHaveLength(0);
    });

    it('remote signal with loadFromDb error is handled gracefully', async () => {
      const pubsub = createMockPubSub();
      const c = new TestBaseCacheService(config, pubsub, 'inst-1');
      c.loadError = new Error('DB down');

      await pubsub.simulateMessage(CHANNEL, makeSignal('inst-2'));
      expect(c.cacheLoaded).toBe(false);
    });
  });

  // ════════════════════════════════════════════════════════════════
  // CASCADE PATTERN (boot chain)
  // ════════════════════════════════════════════════════════════════

  describe('Cascade pattern (boot chain)', () => {
    it('metadata → route → graphql: cascade uses reload(false)', async () => {
      const pubsub = createMockPubSub();
      const ee = new EventEmitter2();

      const metaConfig = { syncEventKey: 'meta:sync', cacheName: 'Meta' };
      const routeConfig = { syncEventKey: 'route:sync', cacheName: 'Route' };

      const meta = new TestBaseCacheService(metaConfig, pubsub, 'inst-1', ee);
      const route = new TestBaseCacheService(routeConfig, pubsub, 'inst-1', ee);

      let graphqlReloads = 0;
      ee.on('Meta:loaded', () => route.reload(false));
      ee.on('Route:loaded', () => graphqlReloads++);

      await meta.reload(true);

      expect(meta.loadCount).toBe(1);
      expect(route.loadCount).toBe(1);
      expect(graphqlReloads).toBe(1);
      expect(pubsub.published).toHaveLength(1);
      expect(pubsub.published[0].channel).toBe('meta:sync');
    });

    it('2 instances boot: no cross-publish from cascade', async () => {
      const pubsub = createMockPubSub();
      const ee1 = new EventEmitter2();
      const ee2 = new EventEmitter2();

      const metaConfig = { syncEventKey: 'meta:sync', cacheName: 'Meta' };
      const routeConfig = { syncEventKey: 'route:sync', cacheName: 'Route' };

      const inst1Meta = new TestBaseCacheService(
        metaConfig,
        pubsub,
        'inst-1',
        ee1,
      );
      const inst1Route = new TestBaseCacheService(
        routeConfig,
        pubsub,
        'inst-1',
        ee1,
      );
      const inst2Meta = new TestBaseCacheService(
        metaConfig,
        pubsub,
        'inst-2',
        ee2,
      );
      const inst2Route = new TestBaseCacheService(
        routeConfig,
        pubsub,
        'inst-2',
        ee2,
      );

      let graphql1 = 0,
        graphql2 = 0;
      ee1.on('Meta:loaded', () => inst1Route.reload(false));
      ee1.on('Route:loaded', () => graphql1++);
      ee2.on('Meta:loaded', () => inst2Route.reload(false));
      ee2.on('Route:loaded', () => graphql2++);

      await inst1Meta.reload(true);
      expect(pubsub.published).toHaveLength(1);

      const signal = JSON.stringify(pubsub.published[0].payload);
      await pubsub.simulateMessage('meta:sync', signal);

      expect(inst1Meta.loadCount).toBe(1);
      expect(inst1Route.loadCount).toBe(1);
      expect(inst2Meta.loadCount).toBe(1);
      expect(inst2Route.loadCount).toBe(1);
      expect(graphql1).toBe(1);
      expect(graphql2).toBe(1);
      expect(pubsub.published).toHaveLength(1);
    });
  });

  // ════════════════════════════════════════════════════════════════
  // INVALIDATION FLOW
  // ════════════════════════════════════════════════════════════════

  describe('Invalidation flow', () => {
    it('invalidation: reload(true) → other instance syncs without publish', async () => {
      const pubsub = createMockPubSub();
      const c1 = new TestBaseCacheService(config, pubsub, 'inst-1');
      const c2 = new TestBaseCacheService(config, pubsub, 'inst-2');

      c1.loadData = ['updated'];
      await c1.reload(true);

      c2.loadData = ['updated'];
      const signal = JSON.stringify(pubsub.published[0].payload);
      await pubsub.simulateMessage(CHANNEL, signal);

      expect(c1.cache).toEqual(['updated']);
      expect(c2.cache).toEqual(['updated']);
      expect(pubsub.published).toHaveLength(1);
    });

    it('rapid invalidations: each publishes independently', async () => {
      const pubsub = createMockPubSub();
      const c = new TestBaseCacheService(config, pubsub, 'inst-1');

      c.loadData = ['v1'];
      await c.reload(true);
      c.loadData = ['v2'];
      await c.reload(true);
      c.loadData = ['v3'];
      await c.reload(true);

      expect(c.cache).toEqual(['v3']);
      expect(pubsub.published).toHaveLength(3);
    });

    it('invalidation with cascade: route reload(true) + graphql, remote syncs cleanly', async () => {
      const pubsub = createMockPubSub();
      const ee1 = new EventEmitter2();
      const ee2 = new EventEmitter2();

      const routeConfig = { syncEventKey: 'route:sync', cacheName: 'Route' };
      const inst1Route = new TestBaseCacheService(
        routeConfig,
        pubsub,
        'inst-1',
        ee1,
      );
      const _inst2Route = new TestBaseCacheService(
        routeConfig,
        pubsub,
        'inst-2',
        ee2,
      );

      let gql1 = 0,
        gql2 = 0;
      ee1.on('Route:loaded', () => gql1++);
      ee2.on('Route:loaded', () => gql2++);

      await inst1Route.reload(true);
      expect(pubsub.published).toHaveLength(1);

      const signal = JSON.stringify(pubsub.published[0].payload);
      await pubsub.simulateMessage('route:sync', signal);

      expect(gql1).toBe(1);
      expect(gql2).toBe(1);
      expect(pubsub.published).toHaveLength(1);
    });
  });

  // ════════════════════════════════════════════════════════════════
  // RACE CONDITIONS
  // ════════════════════════════════════════════════════════════════

  describe('Race conditions', () => {
    it('local reload + remote signal arriving simultaneously', async () => {
      const pubsub = createMockPubSub();
      const c = new TestBaseCacheService(config, pubsub, 'inst-1');
      c.loadDelay = 30;

      const localReload = c.reload(true);
      const remoteSignal = pubsub.simulateMessage(
        CHANNEL,
        makeSignal('inst-2'),
      );
      await Promise.all([localReload, remoteSignal]);

      expect(c.loadCount).toBe(1);
      expect(pubsub.published).toHaveLength(1);
    });

    it('remote signal during ongoing remote signal', async () => {
      const pubsub = createMockPubSub();
      const c = new TestBaseCacheService(config, pubsub, 'inst-1');
      c.loadDelay = 30;

      const s1 = pubsub.simulateMessage(CHANNEL, makeSignal('inst-2'));
      const s2 = pubsub.simulateMessage(CHANNEL, makeSignal('inst-3'));
      await Promise.all([s1, s2]);

      expect(c.loadCount).toBe(1);
      expect(pubsub.published).toHaveLength(0);
    });

    it('publish signal arrives back to sender (self-loop check)', async () => {
      const pubsub = createMockPubSub();
      const c = new TestBaseCacheService(config, pubsub, 'inst-1');

      await c.reload(true);
      const signal = JSON.stringify(pubsub.published[0].payload);
      await pubsub.simulateMessage(CHANNEL, signal);

      expect(c.loadCount).toBe(1);
    });
  });
});
