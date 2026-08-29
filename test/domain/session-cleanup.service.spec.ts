import { describe, expect, it, vi } from 'vitest';

const workerClose = vi.fn(async () => undefined);

vi.mock('bullmq', () => ({
  Worker: class {
    close = workerClose;
  },
}));

import { SessionCleanupService } from '../../src/domain/auth/services/session-cleanup.service';

type SchedulerQueue = {
  upsertJobScheduler: ReturnType<typeof vi.fn>;
  getJobScheduler: ReturnType<typeof vi.fn>;
  getDelayed: ReturnType<typeof vi.fn>;
};

function createQueue(): SchedulerQueue {
  return {
    upsertJobScheduler: vi.fn(async () => undefined),
    getJobScheduler: vi.fn(async () => undefined),
    getDelayed: vi.fn(async () => []),
  };
}

function createSharedLock() {
  let owner: string | undefined;

  return {
    acquire: vi.fn(async (_key: string, token: string) => {
      if (owner) return false;
      owner = token;
      return true;
    }),
    release: vi.fn(async (_key: string, token: string) => {
      if (owner !== token) return false;
      owner = undefined;
      return true;
    }),
  };
}

function createService(deps: {
  cleanupQueue: SchedulerQueue;
  cacheService: ReturnType<typeof createSharedLock>;
}) {
  const service = new SessionCleanupService({
    queryBuilderService: {} as any,
    cleanupQueue: deps.cleanupQueue as any,
    envService: {
      get: vi.fn((key: string) =>
        key === 'NODE_NAME' ? 'admin' : 'redis://example.test',
      ),
    } as any,
    cacheService: deps.cacheService as any,
  } as any);

  return { service };
}

describe('SessionCleanupService', () => {
  it('registers the shared scheduler once across two blue-green deployments with two workers each', async () => {
    const cacheService = createSharedLock();
    const queues = [createQueue(), createQueue(), createQueue(), createQueue()];
    const instances = queues.map((cleanupQueue) =>
      createService({ cleanupQueue, cacheService }),
    );

    await Promise.all(instances.map(({ service }) => service.init()));

    expect(
      queues.reduce(
        (count, queue) => count + queue.upsertJobScheduler.mock.calls.length,
        0,
      ),
    ).toBe(1);
  });

  it('does not register again when another worker starts after the lock holder has finished', async () => {
    const cacheService = createSharedLock();
    const cleanupQueue = createQueue();
    cleanupQueue.getJobScheduler
      .mockResolvedValueOnce(undefined)
      .mockResolvedValueOnce({
        key: 'session-cleanup-daily',
        name: 'cleanup-expired-sessions',
        pattern: '0 2 * * *',
      });
    const first = createService({ cleanupQueue, cacheService });
    const second = createService({ cleanupQueue, cacheService });

    await first.service.init();
    await second.service.init();

    expect(cleanupQueue.upsertJobScheduler).toHaveBeenCalledOnce();
  });

  it('treats the verified BullMQ duplicate scheduler iteration as idempotent', async () => {
    const cacheService = createSharedLock();
    const cleanupQueue = createQueue();
    cleanupQueue.upsertJobScheduler.mockRejectedValueOnce(
      new Error(
        'Cannot create job scheduler iteration - job ID already exists',
      ),
    );
    cleanupQueue.getJobScheduler
      .mockResolvedValueOnce(undefined)
      .mockResolvedValueOnce({
        key: 'session-cleanup-daily',
        name: 'cleanup-expired-sessions',
        pattern: '0 2 * * *',
      });
    const { service } = createService({ cleanupQueue, cacheService });

    await expect(service.init()).resolves.toBeUndefined();
  });

  it('recreates a scheduler when a prior scheduler record is missing but its delayed iteration remains', async () => {
    const cacheService = createSharedLock();
    const cleanupQueue = createQueue();
    const error = new Error(
      'Cannot create job scheduler iteration - job ID already exists',
    );
    cleanupQueue.upsertJobScheduler.mockRejectedValueOnce(error);
    const remove = vi.fn(async () => undefined);
    cleanupQueue.getDelayed.mockResolvedValueOnce([
      {
        name: 'cleanup-expired-sessions',
        repeatJobKey: 'session-cleanup-daily',
        remove,
      },
    ]);
    const { service } = createService({ cleanupQueue, cacheService });

    await expect(service.init()).resolves.toBeUndefined();
    expect(remove).toHaveBeenCalledOnce();
    expect(cleanupQueue.upsertJobScheduler).toHaveBeenCalledTimes(2);
  });

  it('fails boot for non-idempotent scheduler errors', async () => {
    const cacheService = createSharedLock();
    const cleanupQueue = createQueue();
    cleanupQueue.upsertJobScheduler.mockRejectedValueOnce(
      new Error('Redis connection lost'),
    );
    const { service } = createService({ cleanupQueue, cacheService });

    await expect(service.init()).rejects.toThrow('Redis connection lost');
  });

  it('fails boot when the distributed lock cannot be acquired because Redis failed', async () => {
    const cacheService = createSharedLock();
    cacheService.acquire.mockRejectedValueOnce(new Error('Redis unavailable'));
    const cleanupQueue = createQueue();
    const { service } = createService({ cleanupQueue, cacheService });

    await expect(service.init()).rejects.toThrow('Redis unavailable');
    expect(cleanupQueue.upsertJobScheduler).not.toHaveBeenCalled();
  });
});
