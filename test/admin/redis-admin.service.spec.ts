import { describe, expect, it } from 'vitest';
import { RedisAdminService } from '../../src/modules/admin';

class FakePipeline {
  private readonly ops: Array<() => void> = [];

  constructor(private readonly redis: FakeRedis) {}

  del(key: string) {
    this.ops.push(() => this.redis.delSync(key));
    return this;
  }

  set(key: string, value: string) {
    this.ops.push(() => this.redis.setSync(key, value));
    return this;
  }

  hset(key: string, field: string, value: string) {
    this.ops.push(() => this.redis.hsetSync(key, field, value));
    return this;
  }

  rpush(key: string, value: string) {
    this.ops.push(() => this.redis.rpushSync(key, value));
    return this;
  }

  sadd(key: string, value: string) {
    this.ops.push(() => this.redis.saddSync(key, value));
    return this;
  }

  zadd(key: string, score: number, value: string) {
    this.ops.push(() => this.redis.zaddSync(key, score, value));
    return this;
  }

  expire(key: string, ttl: number) {
    this.ops.push(() => this.redis.expireSync(key, ttl));
    return this;
  }

  async exec() {
    for (const op of this.ops) op();
    return [];
  }
}

class FakeRedis {
  values = new Map<string, { type: string; value: any; ttl: number }>();

  pipeline() {
    return new FakePipeline(this);
  }

  async info(section?: string) {
    if (section === 'keyspace') return '# Keyspace\r\ndb0:keys=2,expires=1\r\n';
    return [
      '# Server',
      'redis_version:7.2.0',
      'redis_mode:standalone',
      'os:Darwin 24.0 arm64',
      'arch_bits:64',
      'process_id:123',
      'tcp_port:6379',
      'configured_hz:10',
      'uptime_in_seconds:99',
      '# Clients',
      'connected_clients:3',
      '# Memory',
      'used_memory:1048576',
      'used_memory_human:1.00M',
      'maxmemory:2097152',
      'maxmemory_human:2.00M',
      'total_system_memory:17179869184',
      'total_system_memory_human:16.00G',
      'mem_allocator:libc',
      'mem_fragmentation_ratio:1.25',
      '# CPU',
      'used_cpu_sys:1.5',
      'used_cpu_user:2.5',
      'used_cpu_sys_children:0.1',
      'used_cpu_user_children:0.2',
      '# Replication',
      'role:master',
      '',
    ].join('\r\n');
  }

  async dbsize() {
    return this.values.size;
  }

  async scan(cursor: string, ...args: any[]) {
    const matchIndex = args.findIndex((item) => item === 'MATCH');
    const pattern = matchIndex >= 0 ? String(args[matchIndex + 1]) : '*';
    const regex = new RegExp(
      `^${pattern
        .replace(/[.+^${}()|[\]\\]/g, '\\$&')
        .replace(/\*/g, '.*')}$`,
    );
    return [
      '0',
      cursor === '0' ? [...this.values.keys()].filter((key) => regex.test(key)) : [],
    ];
  }

  async type(key: string) {
    return this.values.get(key)?.type ?? 'none';
  }

  async ttl(key: string) {
    return this.values.get(key)?.ttl ?? -2;
  }

  async strlen(key: string) {
    return String(this.values.get(key)?.value ?? '').length;
  }

  async hlen(key: string) {
    return Object.keys(this.values.get(key)?.value ?? {}).length;
  }

  async llen(key: string) {
    return (this.values.get(key)?.value ?? []).length;
  }

  async scard(key: string) {
    return (this.values.get(key)?.value ?? new Set()).size;
  }

  async zcard(key: string) {
    return (this.values.get(key)?.value ?? []).length;
  }

  async xlen() {
    return 0;
  }

  async get(key: string) {
    return this.values.get(key)?.value ?? null;
  }

  async exists(key: string) {
    return this.values.has(key) ? 1 : 0;
  }

  async getBuffer(key: string) {
    const value = this.values.get(key)?.value;
    if (Buffer.isBuffer(value)) return value;
    return value == null ? null : Buffer.from(String(value));
  }

  async hscan(key: string) {
    const value = this.values.get(key)?.value ?? {};
    return ['0', Object.entries(value).flatMap(([field, item]) => [field, item])];
  }

  async lrange(key: string, start: number, end: number) {
    return (this.values.get(key)?.value ?? []).slice(start, end + 1);
  }

  async sscan(key: string) {
    return ['0', [...(this.values.get(key)?.value ?? new Set())]];
  }

  async zrange(key: string) {
    return (this.values.get(key)?.value ?? []).flatMap((item: any) => [
      item.value,
      String(item.score),
    ]);
  }

  async xrange() {
    return [];
  }

  async call(command: string, subcommand: string, key: string) {
    if (command === 'MEMORY' && subcommand === 'USAGE') {
      return this.values.has(key) ? 64 : null;
    }
    if (command === 'OBJECT' && subcommand === 'ENCODING') {
      return this.values.has(key) ? 'raw' : null;
    }
    return null;
  }

  async del(key: string) {
    return this.delSync(key);
  }

  async expire(key: string, ttl: number) {
    this.expireSync(key, ttl);
    return 1;
  }

  async persist(key: string) {
    const value = this.values.get(key);
    if (value) value.ttl = -1;
    return value ? 1 : 0;
  }

  delSync(key: string) {
    const existed = this.values.delete(key);
    return existed ? 1 : 0;
  }

  setSync(key: string, value: string) {
    this.values.set(key, { type: 'string', value, ttl: -1 });
  }

  hsetSync(key: string, field: string, value: string) {
    const current = this.values.get(key)?.value ?? {};
    current[field] = value;
    this.values.set(key, { type: 'hash', value: current, ttl: -1 });
  }

  rpushSync(key: string, value: string) {
    const current = this.values.get(key)?.value ?? [];
    current.push(value);
    this.values.set(key, { type: 'list', value: current, ttl: -1 });
  }

  saddSync(key: string, value: string) {
    const current = this.values.get(key)?.value ?? new Set<string>();
    current.add(value);
    this.values.set(key, { type: 'set', value: current, ttl: -1 });
  }

  zaddSync(key: string, score: number, value: string) {
    const current = this.values.get(key)?.value ?? [];
    current.push({ score, value });
    this.values.set(key, { type: 'zset', value: current, ttl: -1 });
  }

  expireSync(key: string, ttl: number) {
    const value = this.values.get(key);
    if (value) value.ttl = ttl;
  }
}

function makeService(redis = new FakeRedis()) {
  const userCacheService = {
    async set(key: string, value: any, ttlMs: number) {
      redis.values.set(`app-a:user_cache:${key}`, {
        type: 'string',
        value: typeof value === 'string' ? value : JSON.stringify(value),
        ttl: ttlMs > 0 ? ttlMs / 1000 : -1,
      });
    },
    async deleteKey(key: string) {
      await redis.del(`app-a:user_cache:${key}`);
    },
  };
  return {
    redis,
    service: new RedisAdminService({
      redis: redis as any,
      envService: {
        get: (key: string) => (key === 'NODE_NAME' ? 'app-a' : 0),
      } as any,
      userCacheService: userCacheService as any,
    }),
  };
}

describe('RedisAdminService', () => {
  it('returns Redis server and hardware details in overview', async () => {
    const { redis, service } = makeService();
    redis.setSync('app-a:user:key', 'value');

    const overview = await service.getOverview();

    expect(overview.server).toEqual(
      expect.objectContaining({
        redisVersion: '7.2.0',
        mode: 'standalone',
        role: 'master',
        os: 'Darwin 24.0 arm64',
        archBits: 64,
        tcpPort: 6379,
        usedMemoryBytes: 1048576,
        maxMemoryBytes: 2097152,
        totalSystemMemoryBytes: 17179869184,
        allocator: 'libc',
        connectedClients: 3,
        usedCpuSys: 1.5,
      }),
    );
    expect(overview.userCache).toEqual(
      expect.objectContaining({
        usedBytes: 0,
        limitBytes: 0,
        maxValueBytes: 0,
        remainingBytes: null,
        evictionPolicy: 'disabled',
      }),
    );
    expect(overview.keyCount).toBe(1);
    expect(overview.groups[0]).toEqual(
      expect.objectContaining({
        name: 'current namespace',
        system: false,
        systemKind: undefined,
      }),
    );
    expect(overview.topKeys[0]).toEqual(
      expect.objectContaining({ key: 'user:key', modifiable: true }),
    );
  });

  it('allows modifying normal keys', async () => {
    const { redis, service } = makeService();

    const detail = await service.setKey({
      key: 'user:feature-flag',
      type: 'string',
      value: { enabled: true },
      ttlSeconds: 60,
    });

    expect(detail).toEqual(
      expect.objectContaining({
        key: 'user:feature-flag',
        type: 'string',
        ttlSeconds: 60,
        modifiable: true,
        systemKind: 'user_cache',
        value: '{"enabled":true}',
      }),
    );
    expect(redis.values.has('app-a:user_cache:user:feature-flag')).toBe(true);
  });

  it('searches editable keys through the $cache user_cache namespace', async () => {
    const { redis, service } = makeService();
    redis.setSync('app-a:user_cache:feature', 'enabled');

    const listed = await service.listKeys({ pattern: 'feature' });
    const detail = await service.getKey('feature');

    expect(listed.keys[0]).toEqual(
      expect.objectContaining({
        key: 'feature',
        systemKind: 'user_cache',
        modifiable: true,
      }),
    );
    expect(detail.value).toBe('enabled');
  });

  it('marks system keys and blocks mutation', async () => {
    const { redis, service } = makeService();
    redis.setSync('app-a:runtime_cache:metadata', 'snapshot');

    const detail = await service.getKey('app-a:runtime_cache:metadata');

    expect(detail).toEqual(
      expect.objectContaining({
        key: 'runtime_cache:metadata',
        isSystem: true,
        modifiable: false,
        reason: 'runtime cache snapshot',
      }),
    );
    await expect(service.deleteKey('app-a:runtime_cache:metadata')).rejects.toMatchObject({
      statusCode: 403,
      errorCode: 'AUTHORIZATION_ERROR',
    });
  });

  it('formats current BullMQ queue keys for admin display', async () => {
    const { redis, service } = makeService();
    redis.setSync('app-a:sys_session-cleanup:events', '1');

    const detail = await service.getKey('sys_session-cleanup:events');

    expect(detail).toEqual(
      expect.objectContaining({
        key: 'sys_session-cleanup:events',
        isSystem: true,
        modifiable: false,
        systemKind: 'bullmq',
        reason: 'BullMQ system queue sys_session-cleanup',
      }),
    );
  });

  it('does not list external namespaces', async () => {
    const { redis, service } = makeService();
    redis.setSync('app-a:runtime_cache:metadata', 'snapshot');
    redis.setSync('enfyra_bench_1:sys_session-cleanup:events', '1');

    const current = await service.listKeys({});
    const overview = await service.getOverview();

    expect(current.keys.map((item) => item.key)).toEqual(['runtime_cache:metadata']);
    expect(overview.groups[0]).toEqual(
      expect.objectContaining({
        name: 'runtime cache',
        system: true,
        systemKind: 'runtime_cache',
      }),
    );
    expect(current.keys).not.toEqual(
      expect.arrayContaining([expect.objectContaining({ namespace: 'enfyra_bench_1' })]),
    );
  });

  it('auto-scopes searches and reads to the current NODE_NAME', async () => {
    const { redis, service } = makeService();
    redis.setSync('app-a:runtime_cache:metadata', 'snapshot');
    redis.setSync('external:runtime_cache:metadata', 'snapshot');

    const result = await service.listKeys({
      pattern: 'runtime_cache:*',
    });
    const detail = await service.getKey('runtime_cache:metadata');

    expect(result.keys.map((item) => item.key)).toEqual(['runtime_cache:metadata']);
    expect(detail).toEqual(
      expect.objectContaining({
        key: 'runtime_cache:metadata',
        namespaceScope: 'current',
      }),
    );
  });

  it('hides current BullMQ namespace while preserving read round trips', async () => {
    const { redis, service } = makeService();
    redis.setSync('app-a:sys_ws-event:meta', '1');

    const listed = await service.listKeys({
      pattern: 'sys_ws-event:*',
    });
    const detail = await service.getKey('sys_ws-event:meta');

    expect(listed.keys[0]).toEqual(
      expect.objectContaining({
        key: 'sys_ws-event:meta',
        isSystem: true,
        systemKind: 'bullmq',
        reason: 'BullMQ system queue sys_ws-event',
        namespaceScope: 'current',
      }),
    );
    expect(detail.key).toBe('sys_ws-event:meta');
    expect(detail.value).toBe('1');
  });

  it('does not dump binary string values into admin details', async () => {
    const { redis, service } = makeService();
    redis.values.set('app-a:runtime_cache:metadata', {
      type: 'string',
      value: Buffer.from([0, 1, 2, 3, 4]),
      ttl: -1,
    });

    const detail = await service.getKey('runtime_cache:metadata');

    expect(detail.value).toBe('[binary value, 5 bytes]');
  });
});
