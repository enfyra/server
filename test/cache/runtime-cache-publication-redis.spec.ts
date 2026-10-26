import { spawn } from 'node:child_process';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { once } from 'node:events';
import Redis from 'ioredis';
import { expect, it } from 'vitest';
import { RedisRuntimeCacheStore } from '../../src/engines/cache/services/redis-runtime-cache-store.service';

it('atomically publishes a fenced snapshot set through real Redis', async () => {
  const directory = await mkdtemp(join(tmpdir(), 'enfyra-cache-contract-'));
  const socket = join(directory, 'redis.sock');
  const process = spawn('redis-server', ['--port', '0', '--unixsocket', socket, '--save', '', '--appendonly', 'no'], { stdio: 'ignore' });
  const redis = new Redis({ path: socket, lazyConnect: true, retryStrategy: () => null, maxRetriesPerRequest: 0 });
  redis.on('error', () => {});
  try {
    for (let attempt = 0; attempt < 40; attempt++) {
      try { await redis.connect(); break; } catch { await new Promise(resolve => setTimeout(resolve, 25)); }
    }
    expect(await redis.ping()).toBe('PONG');
    const store = new RedisRuntimeCacheStore({ redis, envService: { get: (key: string) => key === 'NODE_NAME' ? 'synthetic' : key === 'REDIS_RUNTIME_CACHE' ? true : undefined }, runtimeNamespaceLifecycleService: { getKeyTtlMs: () => 5000, registerManagedKey: async () => {}, unregisterManagedKey: async () => {}, touchKey: async () => {} } } as any);
    await store.setSnapshot('metadata', { revision: 'old' });
    await store.setSnapshot('route', { revision: 'old' });
    await store.runWithPublication(async () => {
      await store.setSnapshot('metadata', { revision: 'new' });
      await store.setSnapshot('route', { revision: 'new' });
      await store.setAux('route', 'index', { revision: 'new' });
      expect(await redis.hget('synthetic:runtime_cache:committed', 'metadata')).not.toBeNull();
    });
    expect((await store.getSnapshot<any>('metadata'))?.data.revision).toBe('new');
    expect((await store.getSnapshot<any>('route'))?.data.revision).toBe('new');
    expect(await store.getAux('route', 'index')).toEqual({ revision: 'new' });
    await expect(store.runWithPublication(async () => {
      await store.setSnapshot('metadata', { revision: 'stale' });
      await redis.del('synthetic:runtime_cache:publication:lock');
    })).rejects.toThrow('lease lost');
    expect((await store.getSnapshot<any>('metadata'))?.data.revision).toBe('new');
    await store.runWithPublication(() => store.setAux('route', 'route:old', { revision: 'old' }));
    await expect(store.runWithPublication(async () => {
      await store.deleteAuxByPrefix('route', 'route:');
      await store.setAux('route', 'route:new', { revision: 'new' });
      throw new Error('failed candidate');
    })).rejects.toThrow('failed candidate');
    expect(await store.getAux('route', 'route:old')).toEqual({ revision: 'old' });
    const receiver = new RedisRuntimeCacheStore({ redis, envService: { get: (key: string) => key === 'NODE_NAME' ? 'synthetic' : key === 'REDIS_RUNTIME_CACHE' ? true : undefined }, runtimeNamespaceLifecycleService: { getKeyTtlMs: () => 5000, touchKey: async () => {}, registerManagedKey: async () => {}, unregisterManagedKey: async () => {} } } as any);
    await receiver.runWithCommittedSnapshots(async () => {
      await store.runWithPublication(async () => {
        await store.deleteAuxByPrefix('route', 'route:');
        await store.setAux('route', 'route:new', { revision: 'new' });
      });
      expect(await receiver.getAux('route', 'route:old')).toEqual({ revision: 'old' });
      expect(await receiver.getAux('route', 'route:new')).toBeNull();
    });
    expect(await store.getAux('route', 'route:old')).toBeNull();
    expect(await store.getAux('route', 'route:new')).toEqual({ revision: 'new' });
  } finally {
    redis.disconnect();
    const exited = once(process, 'exit');
    process.kill('SIGTERM');
    await exited;
    await rm(directory, { recursive: true, force: true });
  }
}, 10000);
