import { spawn } from 'node:child_process';
import { once } from 'node:events';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { Queue } from 'bullmq';
import Redis from 'ioredis';
import { expect, it, vi } from 'vitest';
import { FileCleanupService } from '../../src/modules/file-management/services/file-cleanup.service';
import { SYSTEM_QUEUES } from '../../src/shared/utils/constant';

it('retries failed object deletion through the real cleanup queue and worker', async () => {
  const directory = await mkdtemp(join(tmpdir(), 'enfyra-cleanup-contract-'));
  const socket = join(directory, 'redis.sock');
  const child = spawn(
    'redis-server',
    ['--port', '0', '--unixsocket', socket, '--save', '', '--appendonly', 'no'],
    { stdio: 'ignore' },
  );
  const connection = { path: socket, maxRetriesPerRequest: null };
  const redis = new Redis({
    ...connection,
    lazyConnect: true,
    retryStrategy: () => null,
  });
  redis.on('error', () => {});
  let queue: Queue | undefined;
  let service: FileCleanupService | undefined;
  try {
    for (let attempt = 0; attempt < 40; attempt++) {
      try {
        await redis.connect();
        break;
      } catch {
        await new Promise((resolve) => setTimeout(resolve, 25));
      }
    }
    expect(await redis.ping()).toBe('PONG');
    queue = new Queue(SYSTEM_QUEUES.FILE_CLEANUP, {
      prefix: 'synthetic-cleanup',
      connection,
    });
    const remove = vi
      .fn()
      .mockRejectedValueOnce(new Error('synthetic storage unavailable'))
      .mockResolvedValue(undefined);
    service = new FileCleanupService({
      fileCleanupQueue: queue,
      queryBuilderService: {
        transaction: async (work: () => Promise<unknown>) => work(),
        find: async () => ({ data: [] }),
        getPkField: () => 'id',
      },
      runtimeRegistryService: {
        getStorageConfigById: () => ({ id: 'fixture', isEnabled: true }),
      },
      storageFactoryService: {
        getStorageServiceByConfig: () => ({ delete: remove }),
      },
      envService: { get: () => 'synthetic-cleanup' },
    } as any);
    await service.init();
    await service.enqueue('synthetic/object', 'fixture');
    const deadline = Date.now() + 6000;
    let completed = false;
    while (Date.now() < deadline) {
      const jobs = await queue.getJobs(['completed']);
      if (jobs.length === 1) {
        expect(jobs[0].attemptsMade).toBe(2);
        completed = true;
        break;
      }
      await new Promise((resolve) => setTimeout(resolve, 25));
    }
    expect(completed).toBe(true);
    expect(remove).toHaveBeenCalledTimes(2);
  } finally {
    await service?.onDestroy();
    await queue?.close();
    redis.disconnect();
    const exited = once(child, 'exit');
    child.kill('SIGTERM');
    await exited;
    await rm(directory, { recursive: true, force: true });
  }
}, 10000);
