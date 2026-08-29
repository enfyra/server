import { randomUUID } from 'node:crypto';
import { Queue, Worker } from 'bullmq';
import Redis from 'ioredis';
import {
  RedisCacheService,
  RuntimeNamespaceLifecycleService,
} from '../../src/engines/cache';
import { FlowSchedulerService } from '../../src/modules/flow/services/flow-scheduler.service';
import { SYSTEM_QUEUES } from '../../src/shared/utils/constant';

const redisUri = process.env.MATRIX_REDIS_URI || 'redis://127.0.0.1:6379/13';
const nodeName = `e2e-bullmq-lifecycle-${randomUUID()}`;
const lifecycleTtlMs = 500;
const schedulerId = 'flow-schedule-9001';

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

async function waitForExecutions(
  getCount: () => number,
  expected: number,
  timeoutMs: number,
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (getCount() >= expected) return;
    await new Promise((resolve) => setTimeout(resolve, 50));
  }
  throw new Error(
    `Expected at least ${expected} scheduled executions, received ${getCount()}`,
  );
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
  const connection = { url: redisUri, maxRetriesPerRequest: null };
  const flowQueue = new Queue(SYSTEM_QUEUES.FLOW_EXECUTION, {
    prefix: nodeName,
    connection,
  });
  let executionCount = 0;
  const createWorker = () =>
    new Worker(
      SYSTEM_QUEUES.FLOW_EXECUTION,
      async () => {
        executionCount++;
        return { ok: true };
      },
      { prefix: nodeName, connection, concurrency: 1 },
    );
  let worker: Worker | undefined = createWorker();
  const lifecycle = new RuntimeNamespaceLifecycleService({
    redis,
    envService,
    instanceService: { getInstanceId: () => 'e2e' } as any,
  });
  const cacheService = new RedisCacheService({
    redis,
    envService,
    runtimeNamespaceLifecycleService: lifecycle,
    policy: { keyPrefix: '', clearAllMode: 'namespace' },
  });
  const scheduler = new FlowSchedulerService({
    flowQueue,
    cacheService,
    runtimeRegistryService: {
      requireActiveData: () => [
        {
          id: 9000,
          name: 'lifecycle-e2e',
          isEnabled: true,
          triggers: [
            {
              id: 9001,
              type: 'schedule',
              isEnabled: true,
              config: { cron: '*/1 * * * * *', timezone: 'UTC' },
            },
          ],
        },
      ],
    } as any,
  });

  try {
    await clearNamespace(redis);
    await lifecycle.init();
    await cacheService.set('managed-e2e-value', { ok: true });
    await scheduler.init();

    await waitForExecutions(() => executionCount, 2, 5000);
    await new Promise((resolve) => setTimeout(resolve, lifecycleTtlMs * 2));
    await waitForExecutions(() => executionCount, 3, 3000);

    const liveScheduler = await flowQueue.getJobScheduler(schedulerId);
    if (liveScheduler?.key !== schedulerId) {
      throw new Error('BullMQ scheduler disappeared during lifecycle renewal');
    }
    if ((await cacheService.get('managed-e2e-value'))?.ok !== true) {
      throw new Error('Application-managed cache value was not renewed');
    }

    let cursor = '0';
    const repeatKeys: string[] = [];
    do {
      const [nextCursor, keys] = await redis.scan(
        cursor,
        'MATCH',
        `${nodeName}:${SYSTEM_QUEUES.FLOW_EXECUTION}:repeat*`,
        'COUNT',
        100,
      );
      repeatKeys.push(...keys);
      cursor = nextCursor;
    } while (cursor !== '0');
    if (repeatKeys.length === 0) {
      throw new Error('Expected BullMQ repeat keys to remain present');
    }
    for (const key of repeatKeys) {
      const ttl = await redis.pttl(key);
      if (ttl !== -1) {
        throw new Error(`BullMQ key received an application TTL: ${ttl}`);
      }
    }

    await worker.close();
    worker = undefined;
    const orphanedIteration = (await flowQueue.getDelayed(0, -1)).find(
      (job) => job.repeatJobKey === schedulerId,
    );
    if (!orphanedIteration) {
      throw new Error('Expected a delayed scheduler iteration before recovery');
    }
    const repeatKey = flowQueue.keys.repeat;
    await redis.zrem(repeatKey, schedulerId);
    await redis.del(`${repeatKey}:${schedulerId}`);
    if (await flowQueue.getJobScheduler(schedulerId)) {
      throw new Error('Failed to create an orphaned scheduler iteration');
    }

    const recoveryScheduler = new FlowSchedulerService({
      flowQueue,
      cacheService,
      runtimeRegistryService: {
        requireActiveData: () => [
          {
            id: 9000,
            name: 'lifecycle-e2e',
            isEnabled: true,
            triggers: [
              {
                id: 9001,
                type: 'schedule',
                isEnabled: true,
                config: { cron: '*/1 * * * * *', timezone: 'UTC' },
              },
            ],
          },
        ],
      } as any,
    });
    await recoveryScheduler.init();
    if ((await flowQueue.getJobScheduler(schedulerId))?.key !== schedulerId) {
      throw new Error('Flow scheduler did not recover its orphaned iteration');
    }

    worker = createWorker();
    await waitForExecutions(() => executionCount, 4, 4000);
  } finally {
    await flowQueue.removeJobScheduler(schedulerId).catch(() => undefined);
    await worker?.close();
    await flowQueue.close();
    await lifecycle.onDestroy();
    await clearNamespace(redis);
    await redis.quit();
  }
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
