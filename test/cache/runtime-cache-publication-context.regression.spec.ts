import { spawn } from 'node:child_process';
import { once } from 'node:events';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { IsolatedExecutorService } from '@enfyra/kernel';
import { EventEmitter2 } from 'eventemitter2';
import Redis from 'ioredis';
import { expect, it } from 'vitest';
import { CacheOrchestratorService } from '../../src/engines/cache/services/cache-orchestrator.service';
import { RedisRuntimeCacheStore } from '../../src/engines/cache/services/redis-runtime-cache-store.service';
import { CACHE_EVENTS } from '../../src/shared/utils/cache-events.constants';

it.each([false, true])('publishes a handler patch received through a boot-created executor (critical=%s)', async (critical) => {
  const directory = await mkdtemp(join(tmpdir(), 'enfyra-publication-context-'));
  const socket = join(directory, 'redis.sock');
  const redisProcess = spawn('redis-server', [
    '--port', '0', '--unixsocket', socket, '--save', '', '--appendonly', 'no',
  ], { stdio: 'ignore' });
  const redis = new Redis({ path: socket, lazyConnect: true, retryStrategy: () => null, maxRetriesPerRequest: 0 });
  redis.on('error', () => {});
  let executor: IsolatedExecutorService | undefined;
  let orchestrator: CacheOrchestratorService | undefined;
  try {
    for (let attempt = 0; attempt < 40; attempt++) {
      try { await redis.connect(); break; } catch { await new Promise(resolve => setTimeout(resolve, 25)); }
    }
    expect(await redis.ping()).toBe('PONG');
    const store = new RedisRuntimeCacheStore({
      redis,
      envService: { get: (key: string) => key === 'NODE_NAME' ? 'publication-context-test' : key === 'REDIS_RUNTIME_CACHE' ? true : undefined },
      runtimeNamespaceLifecycleService: {
        getKeyTtlMs: () => 5000,
        registerManagedKey: async () => {},
        unregisterManagedKey: async () => {},
        touchKey: async () => {},
      },
    } as any);
    const eventEmitter = new EventEmitter2();
    const statuses: string[] = [];
    const reloadRoute = async () => store.runWithPublication(async () => {
      await store.deleteAuxByPrefix('route', 'route:');
      await store.setAux('route', 'route:1', { revision: 'patched' });
      await store.setSnapshot('route', { revision: 'patched' });
    });
    orchestrator = new CacheOrchestratorService({
      redisRuntimeCacheStore: store,
      eventEmitter,
      instanceService: { getInstanceId: () => 'publication-context-test' },
      redisPubSubService: { publish: async () => {} },
      routeCacheService: {
        isLoaded: () => true,
        supportsPartialReload: () => true,
        partialReload: reloadRoute,
        reload: reloadRoute,
      },
      dynamicWebSocketGateway: {
        emitToNamespace: (_path: string, _event: string, data: { status: string }) => statuses.push(data.status),
      },
    } as any);
    await store.runWithPublication(async () => {
      await store.setSnapshot('route', { revision: 'boot' });
      executor = new IsolatedExecutorService({
        packageCacheService: { getPackages: async () => [] },
        packageCdnLoaderService: { getPackageSources: () => [] },
        tuning: { maxConcurrentWorkers: 1, isolatesPerWorker: 1, isolateMemoryLimitMb: 64 },
      });
    });
    const patched = executor!.run('return await $ctx.$repos.main.update({ id: 1 });', {
      $repos: {
        main: {
          update: async () => {
            await eventEmitter.emitAsync(CACHE_EVENTS.INVALIDATE, {
              table: 'enfyra_route_handler',
              scope: 'partial',
              action: 'update',
              ids: [1],
              timestamp: Date.now(),
              critical,
            });
            return { id: 1 };
          },
        },
      },
    }, 3000);
    await expect(patched).resolves.toEqual({ id: 1 });
    expect((await store.getSnapshot<{ revision: string }>('route'))?.data.revision).toBe('patched');
    expect(await store.getAux('route', 'route:1')).toEqual({ revision: 'patched' });
    expect(statuses).toEqual(['pending', 'done']);
  } finally {
    orchestrator?.onDestroy();
    await executor?.onDestroy();
    redis.disconnect();
    const exited = once(redisProcess, 'exit');
    redisProcess.kill('SIGTERM');
    await exited;
    await rm(directory, { recursive: true, force: true });
  }
}, 10000);
