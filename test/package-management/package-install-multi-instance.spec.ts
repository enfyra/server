import { EventEmitter2 } from 'eventemitter2';
import Redis from 'ioredis';
import { CACHE_EVENTS } from '../../src/shared/utils/cache-events.constants';

const REDIS_URI = process.env.REDIS_URI || 'redis://localhost:6379';
const RUN_ID = `t${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;

function sleep(ms: number) {
  return new Promise<void>((r) => setTimeout(r, ms));
}

async function redisAcquire(
  redis: Redis,
  key: string,
  value: string,
  ttlMs: number,
): Promise<boolean> {
  const result = await redis.set(key, value, 'PX', ttlMs, 'NX');
  return result === 'OK';
}

async function redisRelease(
  redis: Redis,
  key: string,
  value: string,
): Promise<boolean> {
  const lua = `
      if redis.call("get", KEYS[1]) == ARGV[1] then
        return redis.call("del", KEYS[1])
      else
        return 0
      end`;
  const deleted = await redis.eval(lua, 1, key, value);
  return deleted === 1;
}

type InstallBatchFn = (
  pkgs: Array<{ name: string; version: string }>,
) => Promise<void>;

async function simulateEnsurePackagesInstalled(params: {
  redis: Redis;
  lockKey: string;
  holderId: string;
  lockTtlMs: number;
  lockPollMs: number;
  lockWaitMs: number;
  getExpectedPackageNames: () => string[];
  isPackageInstalled: (name: string) => boolean;
  markPackagesInstalled: (names: string[]) => void;
  onInstallBatch: InstallBatchFn;
}): Promise<
  | 'skipped_no_missing'
  | 'skipped_lock_timeout'
  | 'skipped_other_instance'
  | 'installed'
> {
  const expected = params.getExpectedPackageNames();
  const missing = expected.filter((n) => !params.isPackageInstalled(n));
  if (missing.length === 0) return 'skipped_no_missing';

  const deadline = Date.now() + params.lockWaitMs;
  let locked = false;
  while (Date.now() < deadline) {
    locked = await redisAcquire(
      params.redis,
      params.lockKey,
      params.holderId,
      params.lockTtlMs,
    );
    if (locked) break;
    await sleep(params.lockPollMs);
  }
  if (!locked) return 'skipped_lock_timeout';

  try {
    const still = params
      .getExpectedPackageNames()
      .filter((n) => !params.isPackageInstalled(n));
    if (still.length === 0) {
      return 'skipped_other_instance';
    }
    await params.onInstallBatch(
      still.map((name) => ({ name, version: 'latest' })),
    );
    params.markPackagesInstalled(still);
    return 'installed';
  } finally {
    await redisRelease(params.redis, params.lockKey, params.holderId);
  }
}

function wireSystemReadyGate(eventEmitter: EventEmitter2) {
  const BOOT_EVENTS = [
    CACHE_EVENTS.METADATA_LOADED,
    CACHE_EVENTS.ROUTE_LOADED,
    CACHE_EVENTS.PACKAGE_LOADED,
    CACHE_EVENTS.STORAGE_LOADED,
    CACHE_EVENTS.AI_CONFIG_LOADED,
    CACHE_EVENTS.OAUTH_CONFIG_LOADED,
    CACHE_EVENTS.WEBSOCKET_LOADED,
    CACHE_EVENTS.FLOW_LOADED,
    CACHE_EVENTS.GRAPHQL_LOADED,
  ];
  const received = new Set<string>();
  let systemReadyCount = 0;
  let gateDone = false;
  const systemReadyPromise = new Promise<void>((resolve) => {
    for (const ev of BOOT_EVENTS) {
      eventEmitter.on(ev, () => {
        received.add(ev);
        if (!gateDone && received.size === BOOT_EVENTS.length) {
          gateDone = true;
          eventEmitter.emit(CACHE_EVENTS.SYSTEM_READY);
          systemReadyCount++;
          resolve();
        }
      });
    }
  });
  return {
    BOOT_EVENTS,
    received,
    systemReadyPromise,
    getSystemReadyCount: () => systemReadyCount,
  };
}

describe('Package install + multi-instance (Redis lock)', () => {
  let redis: Redis;

  beforeAll(async () => {
    redis = new Redis(REDIS_URI, {
      maxRetriesPerRequest: 2,
      lazyConnect: true,
    });
    await redis.connect();
    await redis.ping();
  });

  afterAll(async () => {
    let cursor = '0';
    do {
      const [next, keys] = await redis.scan(
        cursor,
        'MATCH',
        `test:${RUN_ID}:*`,
        'COUNT',
        200,
      );
      if (keys.length) await redis.del(...keys);
      cursor = next;
    } while (cursor !== '0');
    redis.disconnect();
  });

  it('grants at most one concurrent holder per lock key (many contenders)', async () => {
    const key = `test:${RUN_ID}:lock:one-holder`;
    const n = 60;
    const attempts = await Promise.all(
      Array.from({ length: n }, (_, i) =>
        redisAcquire(redis, key, `pid-${i}`, 30_000),
      ),
    );
    const winners = attempts.filter(Boolean).length;
    expect(winners).toBe(1);
    const ownerIdx = attempts.findIndex((x) => x);
    for (let i = 0; i < n; i++) {
      if (i === ownerIdx) continue;
      const second = await redisAcquire(redis, key, `pid-${i}`, 30_000);
      expect(second).toBe(false);
    }
    await redisRelease(redis, key, `pid-${ownerIdx}`);
    const next = await redisAcquire(redis, key, 'next-owner', 30_000);
    expect(next).toBe(true);
    await redisRelease(redis, key, 'next-owner');
  });

  it('after TTL expires, a new holder can acquire without explicit release', async () => {
    const key = `test:${RUN_ID}:lock:ttl`;
    const first = await redisAcquire(redis, key, 'ghost', 80);
    expect(first).toBe(true);
    await sleep(150);
    const second = await redisAcquire(redis, key, 'recovery', 30_000);
    expect(second).toBe(true);
    await redisRelease(redis, key, 'recovery');
  });

  it('release is token-safe: wrong holder cannot delete lock', async () => {
    const key = `test:${RUN_ID}:lock:token`;
    await redisAcquire(redis, key, 'alice', 30_000);
    const badRelease = await redisRelease(redis, key, 'bob');
    expect(badRelease).toBe(false);
    const stillThere = await redis.get(key);
    expect(stillThere).toBe('alice');
    const ok = await redisRelease(redis, key, 'alice');
    expect(ok).toBe(true);
  });

  it('many concurrent ensurePackages flows: installBatch runs once; others skip as other_instance', async () => {
    const lockKey = `test:${RUN_ID}:pkg-install:shared-host`;
    const pkgNames = ['pkg-a', 'pkg-b', 'pkg-c'];
    const installed = new Set<string>();
    let installBatchCalls = 0;

    const onInstallBatch: InstallBatchFn = async (pkgs) => {
      installBatchCalls++;
      expect(pkgs.length).toBeGreaterThan(0);
      await sleep(30);
      for (const p of pkgs) installed.add(p.name);
    };

    const workers = 48;
    const results = await Promise.all(
      Array.from({ length: workers }, (_, w) =>
        simulateEnsurePackagesInstalled({
          redis,
          lockKey,
          holderId: `worker-${w}`,
          lockTtlMs: 60_000,
          lockPollMs: 15,
          lockWaitMs: 120_000,
          getExpectedPackageNames: () => pkgNames,
          isPackageInstalled: (name) => installed.has(name),
          markPackagesInstalled: () => {},
          onInstallBatch,
        }),
      ),
    );

    expect(installBatchCalls).toBe(1);
    for (const n of pkgNames) expect(installed.has(n)).toBe(true);
    const installedCount = results.filter((r) => r === 'installed').length;
    const otherCount = results.filter(
      (r) => r === 'skipped_other_instance',
    ).length;
    expect(installedCount).toBeGreaterThanOrEqual(1);
    expect(
      installedCount +
        otherCount +
        results.filter((r) => r === 'skipped_no_missing').length,
    ).toBe(workers);
  });

  it('staged startups: repeated waves keep installBatch at one per distinct missing set', async () => {
    const lockKey = `test:${RUN_ID}:pkg-install:waves`;
    const installed = new Set<string>();
    let installBatchCalls = 0;
    const onInstallBatch: InstallBatchFn = async (pkgs) => {
      installBatchCalls++;
      await sleep(20);
      for (const p of pkgs) installed.add(p.name);
    };

    const waves = 12;
    const parallelPerWave = 25;

    for (let w = 0; w < waves; w++) {
      const wavePkgs = [`wave${w}-x`, `wave${w}-y`];
      await Promise.all(
        Array.from({ length: parallelPerWave }, (_, i) =>
          simulateEnsurePackagesInstalled({
            redis,
            lockKey,
            holderId: `wave${w}-p${i}`,
            lockTtlMs: 120_000,
            lockPollMs: 10,
            lockWaitMs: 180_000,
            getExpectedPackageNames: () => wavePkgs,
            isPackageInstalled: (name) => installed.has(name),
            markPackagesInstalled: () => {},
            onInstallBatch,
          }),
        ),
      );
      for (const n of wavePkgs) expect(installed.has(n)).toBe(true);
    }

    expect(installBatchCalls).toBe(waves);
  });

  it('different lock host keys allow parallel install batches (different data centers)', async () => {
    const installed = new Set<string>();
    let installBatchCalls = 0;
    const onInstallBatch: InstallBatchFn = async (pkgs) => {
      installBatchCalls++;
      await sleep(25);
      for (const p of pkgs) installed.add(p.name);
    };

    await Promise.all([
      simulateEnsurePackagesInstalled({
        redis,
        lockKey: `test:${RUN_ID}:pkg-install:host-A`,
        holderId: 'a1',
        lockTtlMs: 60_000,
        lockPollMs: 5,
        lockWaitMs: 60_000,
        getExpectedPackageNames: () => ['only-a'],
        isPackageInstalled: (name) => installed.has(name),
        markPackagesInstalled: () => {},
        onInstallBatch,
      }),
      simulateEnsurePackagesInstalled({
        redis,
        lockKey: `test:${RUN_ID}:pkg-install:host-B`,
        holderId: 'b1',
        lockTtlMs: 60_000,
        lockPollMs: 5,
        lockWaitMs: 60_000,
        getExpectedPackageNames: () => ['only-b'],
        isPackageInstalled: (name) => installed.has(name),
        markPackagesInstalled: () => {},
        onInstallBatch,
      }),
    ]);

    expect(installBatchCalls).toBe(2);
    expect(installed.has('only-a') && installed.has('only-b')).toBe(true);
  });
});

describe('Cold start SYSTEM_READY gate (boot dependency closure)', () => {
  it('emits SYSTEM_READY exactly once after all BOOT_EVENTS received; order irrelevant', async () => {
    const emitter = new EventEmitter2();
    const { BOOT_EVENTS, systemReadyPromise, getSystemReadyCount } =
      wireSystemReadyGate(emitter);

    const shuffled = [...BOOT_EVENTS];
    for (let i = shuffled.length - 1; i > 0; i--) {
      const j = Math.floor(Math.random() * (i + 1));
      [shuffled[i], shuffled[j]] = [shuffled[j], shuffled[i]];
    }

    const spam = [...shuffled, ...shuffled.slice(0, 3)];
    for (const ev of spam) {
      emitter.emit(ev);
    }

    await systemReadyPromise;
    expect(getSystemReadyCount()).toBe(1);
  });

  it('does not resolve early when one event is missing', async () => {
    const emitter = new EventEmitter2();
    const { BOOT_EVENTS, systemReadyPromise } = wireSystemReadyGate(emitter);

    let settled = false;
    systemReadyPromise.then(() => {
      settled = true;
    });

    for (const ev of BOOT_EVENTS.slice(0, BOOT_EVENTS.length - 2)) {
      emitter.emit(ev);
    }
    await sleep(80);
    expect(settled).toBe(false);

    emitter.emit(BOOT_EVENTS[BOOT_EVENTS.length - 2]);
    emitter.emit(BOOT_EVENTS[BOOT_EVENTS.length - 1]);
    await systemReadyPromise;
    expect(settled).toBe(true);
  });
});

describe('Startup latency model (serialized install vs uncontended ideal)', () => {
  it('documents wall-clock when N workers serialize on one install critical section', async () => {
    const fakeInstallMs = 40;
    let concurrentInsideInstall = 0;
    let maxConcurrentInsideInstall = 0;
    const mutex = { locked: false };

    async function withFakeInstallLock(run: () => Promise<void>) {
      while (mutex.locked) await sleep(2);
      mutex.locked = true;
      concurrentInsideInstall++;
      maxConcurrentInsideInstall = Math.max(
        maxConcurrentInsideInstall,
        concurrentInsideInstall,
      );
      try {
        await sleep(fakeInstallMs);
        await run();
      } finally {
        concurrentInsideInstall--;
        mutex.locked = false;
      }
    }

    const n = 30;
    const t0 = Date.now();
    await Promise.all(
      Array.from({ length: n }, async (_, i) => {
        await withFakeInstallLock(async () => {
          void i;
        });
      }),
    );
    const wall = Date.now() - t0;
    expect(maxConcurrentInsideInstall).toBe(1);
    expect(wall).toBeGreaterThanOrEqual(fakeInstallMs * n - 50);
  });
});
