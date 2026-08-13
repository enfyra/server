import { randomUUID } from 'node:crypto';
import Redis from 'ioredis';
import {
  RedisCacheService,
  RuntimeNamespaceLifecycleService,
} from '../../src/engines/cache';

const redisUri = process.env.MATRIX_REDIS_URI || 'redis://127.0.0.1:6379/13';
const nodeName = `e2e-cache-lock-${randomUUID()}`;
const lifecycleTtlMs = 1000;
const explicitTtlMs = 400;

async function clearNamespace(redis: Redis): Promise<void> {
  let cursor = '0';
  do {
    const [nextCursor, keys] = await redis.scan(
      cursor,
      'MATCH',
      `${nodeName}:*`,
      'COUNT',
      100,
    );
    if (keys.length > 0) await redis.del(...keys);
    cursor = nextCursor;
  } while (cursor !== '0');
}

async function main(): Promise<void> {
  const redis = new Redis(redisUri);
  const envService = {
    get(key: string) {
      switch (key) {
        case 'NODE_NAME':
          return nodeName;
        case 'NODE_ENV':
          return 'test';
        case 'REDIS_NAMESPACE_KEY_TTL_MS':
          return lifecycleTtlMs;
        case 'REDIS_NAMESPACE_LEASE_TTL_MS':
          return 200;
        case 'REDIS_NAMESPACE_RENEW_INTERVAL_MS':
          return 50;
        default:
          return undefined;
      }
    },
  } as any;
  const lifecycle = new RuntimeNamespaceLifecycleService({
    redis,
    envService,
    instanceService: { getInstanceId: () => 'e2e' } as any,
  });
  const cache = new RedisCacheService({
    redis,
    envService,
    runtimeNamespaceLifecycleService: lifecycle,
    policy: {
      keyPrefix: 'user_cache:',
      requireNamespace: true,
      quota: { limitBytes: 1024 * 1024, maxValueBytes: 1024 * 1024 },
      clearAllMode: 'prefix',
    },
  });
  const lockKey = 'quota-renewal';
  const redisLockKey = `${nodeName}:user_cache_lock:${lockKey}`;

  try {
    await clearNamespace(redis);
    if (!(await cache.acquire(lockKey, 'owner-token', explicitTtlMs))) {
      throw new Error('Expected explicit lock acquisition to succeed');
    }
    if ((await redis.get(redisLockKey)) !== 'owner-token') {
      throw new Error('Expected lock in dedicated user_cache_lock namespace');
    }
    if (await redis.exists(`${nodeName}:user_cache:${lockKey}`)) {
      throw new Error('Lock must not be stored as user cache data');
    }
    const initialTtl = await redis.pttl(redisLockKey);
    if (initialTtl <= 0 || initialTtl > explicitTtlMs) {
      throw new Error(`Unexpected explicit lock TTL: ${initialTtl}`);
    }

    await new Promise((resolve) => setTimeout(resolve, 100));
    await lifecycle.renewCurrentNamespaceKeys();
    const renewedTtl = await redis.pttl(redisLockKey);
    if (renewedTtl <= 0 || renewedTtl >= initialTtl || renewedTtl >= lifecycleTtlMs) {
      throw new Error(`Lifecycle renewal overwrote explicit lock TTL: ${renewedTtl}`);
    }

    await new Promise((resolve) => setTimeout(resolve, explicitTtlMs));
    if (await redis.exists(redisLockKey)) {
      throw new Error('Explicit lock did not expire after its requested TTL');
    }

    if (!(await cache.acquire('lifecycle-lock', 'owner-token', 0))) {
      throw new Error('Expected zero-TTL lock acquisition to succeed');
    }
    const zeroTtl = await redis.pttl(`${nodeName}:user_cache_lock:lifecycle-lock`);
    if (zeroTtl <= 0 || zeroTtl > lifecycleTtlMs) {
      throw new Error(`Unexpected lifecycle lock TTL: ${zeroTtl}`);
    }
  } finally {
    await lifecycle.onDestroy();
    await clearNamespace(redis);
    await redis.quit();
  }
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
