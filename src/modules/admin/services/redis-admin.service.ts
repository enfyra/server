import type { Redis } from 'ioredis';
import { EnvService } from '../../../shared/services';
import { UserCacheService } from '../../../engines/cache';
import { AuthorizationException } from '../../../domain/exceptions';
import {
  BOOTSTRAP_SCRIPT_EXECUTION_LOCK_KEY,
  PROVISION_LOCK_KEY,
  SAGA_ORPHAN_RECOVERY_LOCK_KEY,
  SYSTEM_QUEUES,
} from '../../../shared/utils/constant';
import type {
  RedisAdminKeyDetail,
  RedisAdminKeySummary,
  RedisAdminNamespaceScope,
  RedisAdminOverview,
  RedisAdminSystemMark,
  RedisAdminValueType,
} from '../types';

const DEFAULT_SCAN_COUNT = 10;
const MAX_SCAN_COUNT = 500;
const DEFAULT_VALUE_LIMIT = 100;
const MAX_VALUE_LIMIT = 500;
const OVERVIEW_SCAN_COUNT = 500;
const OVERVIEW_SCAN_LIMIT = 10000;

export class RedisAdminService {
  private readonly redis: Redis;
  private readonly nodeName: string;
  private readonly userCacheLimitBytes: number;
  private readonly userCacheMaxValueBytes: number;
  private readonly userCacheService: UserCacheService;

  constructor(deps: {
    redis: Redis;
    envService: EnvService;
    userCacheService: UserCacheService;
  }) {
    this.redis = deps.redis;
    this.userCacheService = deps.userCacheService;
    this.nodeName = deps.envService.get('NODE_NAME') || 'enfyra';
    this.userCacheLimitBytes =
      Number(deps.envService.get('REDIS_USER_CACHE_LIMIT_MB') || 0) *
      1024 *
      1024;
    this.userCacheMaxValueBytes = Number(
      deps.envService.get('REDIS_USER_CACHE_MAX_VALUE_BYTES') || 0,
    );
  }

  async getOverview(): Promise<RedisAdminOverview> {
    const [info, keyspace, keys] = await Promise.all([
      this.readInfo(),
      this.readInfo('keyspace'),
      this.scanForOverview(),
    ]);
    const groups = new Map<
      string,
      {
        name: string;
        count: number;
        memoryBytes: number;
        system: boolean;
        systemKind?: NonNullable<RedisAdminSystemMark['systemKind']>;
        namespace?: string;
        scope: RedisAdminNamespaceScope;
      }
    >();
    const allSummaries = await Promise.all(
      keys.keys
        .filter((key) => this.isReadableKey(key))
        .map((key) => this.describeKey(key)),
    );
    const summaries = allSummaries;

    for (const summary of summaries) {
      const group = this.groupKey(summary);
      const current =
        groups.get(group.name) ??
        {
          ...group,
          count: 0,
          memoryBytes: 0,
          system: summary.isSystem,
          systemKind: summary.systemKind,
        };
      current.count += 1;
      current.memoryBytes += summary.memoryBytes ?? 0;
      current.system = current.system || summary.isSystem;
      current.systemKind = current.systemKind ?? summary.systemKind;
      groups.set(group.name, current);
    }

    return {
      connected: true,
      keyCount: summaries.length,
      scanned: keys.scanned,
      scanComplete: keys.complete,
      server: this.parseServerInfo(info),
      keyspace: this.parseInfoSection(keyspace),
      userCache: await this.getUserCacheQuota(),
      groups: [...groups.values()].sort((a, b) => b.count - a.count),
      topKeys: summaries
        .filter((item) => item.memoryBytes != null)
        .sort((a, b) => (b.memoryBytes ?? 0) - (a.memoryBytes ?? 0))
        .slice(0, 20),
    };
  }

  async listKeys(options: {
    cursor?: string;
    pattern?: string;
    count?: number;
  }): Promise<{
    cursor: string;
    count: number;
    keys: RedisAdminKeySummary[];
  }> {
    const count = this.clampCount(
      options.count,
      DEFAULT_SCAN_COUNT,
      MAX_SCAN_COUNT,
    );
    const pattern = this.effectiveListPattern(options.pattern);
    const [cursor, keys] = await this.redis.scan(
      options.cursor || '0',
      'MATCH',
      pattern,
      'COUNT',
      count,
    );

    const summaries = await Promise.all(keys.map((key) => this.describeKey(key)));
    const readable = summaries.filter((key) => key.namespaceScope === 'current');
    return {
      cursor,
      count: readable.length,
      keys: readable,
    };
  }

  async getKey(
    key: string,
    options?: { limit?: number },
  ): Promise<RedisAdminKeyDetail> {
    const redisKey = this.resolveKey(key);
    this.assertValidKey(redisKey);
    const summary = await this.describeKey(redisKey);
    const limit = this.clampCount(
      options?.limit,
      DEFAULT_VALUE_LIMIT,
      MAX_VALUE_LIMIT,
    );
    const [value, encoding] = await Promise.all([
      this.readKeyValue(redisKey, summary.type, limit),
      this.objectEncoding(redisKey),
    ]);

    return {
      ...summary,
      encoding,
      value: value.value,
      truncated: value.truncated,
    };
  }

  async setKey(input: {
    key: string;
    type?: RedisAdminValueType;
    value: any;
    ttlSeconds?: number | null;
  }): Promise<RedisAdminKeyDetail> {
    const redisKey = this.resolveKey(input.key);
    this.assertCanModify(redisKey);
    const type = input.type || 'string';
    if (type !== 'string') {
      throw new Error('Redis Admin editable keys use $cache storage and support string values only');
    }
    const ttlMs = input.ttlSeconds == null ? 0 : Number(input.ttlSeconds) * 1000;
    if (input.ttlSeconds != null) {
      this.assertPositiveTtlSeconds(input.ttlSeconds);
    }
    await this.userCacheService.set(this.publicKey(redisKey), input.value, ttlMs);
    return this.getKey(input.key);
  }

  async deleteKey(key: string): Promise<{ deleted: number }> {
    const redisKey = this.resolveKey(key);
    this.assertCanModify(redisKey);
    const existed = await this.redis.exists(redisKey);
    await this.userCacheService.deleteKey(this.publicKey(redisKey));
    return { deleted: existed ? 1 : 0 };
  }

  async expireKey(
    key: string,
    ttlSeconds: number | null,
  ): Promise<RedisAdminKeySummary> {
    const redisKey = this.resolveKey(key);
    this.assertCanModify(redisKey);
    if (ttlSeconds == null) {
      await this.redis.persist(redisKey);
    } else {
      this.assertPositiveTtlSeconds(ttlSeconds);
      await this.redis.expire(redisKey, Number(ttlSeconds));
    }
    return this.describeKey(redisKey);
  }

  markSystemKey(key: string): RedisAdminSystemMark {
    const systemPrefixes = [
      {
        prefix: `${this.nodeName}:runtime_cache:`,
        reason: 'runtime cache snapshot',
        systemKind: 'runtime_cache' as const,
      },
      {
        prefix: `${this.nodeName}:socket.io:`,
        reason: 'Socket.IO Redis adapter',
        systemKind: 'socket_io' as const,
      },
      {
        prefix: `${this.nodeName}:runtime-monitor:`,
        reason: 'runtime monitor telemetry',
        systemKind: 'runtime_monitor' as const,
      },
      {
        prefix: `${this.nodeName}:cluster-telemetry:`,
        reason: 'runtime monitor telemetry',
        systemKind: 'runtime_monitor' as const,
      },
    ];
    if (key.startsWith(`${this.nodeName}:user_cache_meta:`)) {
      return {
        isSystem: true,
        modifiable: false,
        systemKind: 'user_cache',
        reason: '$cache quota metadata',
      };
    }
    if (key.startsWith(`${this.nodeName}:user_cache:`)) {
      return {
        isSystem: false,
        modifiable: true,
        systemKind: 'user_cache',
        reason: '$cache user data',
      };
    }
    for (const item of systemPrefixes) {
      if (key.startsWith(item.prefix)) {
        return {
          isSystem: true,
          modifiable: false,
          systemKind: item.systemKind,
          reason: item.reason,
        };
      }
    }
    const queue = this.parseSystemQueueKey(key);
    if (queue) {
      return {
        isSystem: true,
        modifiable: false,
        systemKind: 'bullmq',
        reason: `BullMQ system queue ${queue.queueName}`,
      };
    }
    if (
      [
        BOOTSTRAP_SCRIPT_EXECUTION_LOCK_KEY,
        PROVISION_LOCK_KEY,
        SAGA_ORPHAN_RECOVERY_LOCK_KEY,
      ].includes(key)
    ) {
      return {
        isSystem: true,
        modifiable: false,
        systemKind: 'system_lock',
        reason: 'system lock',
      };
    }
    return { isSystem: false, modifiable: true };
  }

  private async describeKey(key: string): Promise<RedisAdminKeySummary> {
    const [type, ttlSeconds, size, memoryBytes] = await Promise.all([
      this.redis.type(key) as Promise<RedisAdminValueType>,
      this.redis.ttl(key),
      this.readSize(key),
      this.memoryUsage(key),
    ]);
    return {
      key: this.publicKey(key),
      ...this.namespaceForKey(key),
      type,
      ttlSeconds,
      size,
      memoryBytes,
      ...this.markSystemKey(key),
    };
  }

  private async readKeyValue(
    key: string,
    type: RedisAdminValueType,
    limit: number,
  ): Promise<{ value: any; truncated: boolean }> {
    switch (type) {
      case 'string': {
        const value = await this.readStringValue(key);
        return { value, truncated: false };
      }
      case 'hash': {
        const rows = await this.scanHash(key, limit);
        return rows;
      }
      case 'list': {
        const [items, size] = await Promise.all([
          this.redis.lrange(key, 0, limit - 1),
          this.redis.llen(key),
        ]);
        return { value: items, truncated: size > items.length };
      }
      case 'set':
        return this.scanSet(key, limit);
      case 'zset': {
        const [items, size] = await Promise.all([
          this.redis.zrange(key, 0, limit - 1, 'WITHSCORES'),
          this.redis.zcard(key),
        ]);
        const value = [];
        for (let i = 0; i < items.length; i += 2) {
          value.push({ value: items[i], score: Number(items[i + 1]) });
        }
        return { value, truncated: size > value.length };
      }
      case 'stream': {
        const items = await this.redis.xrange(
          key,
          '-',
          '+',
          'COUNT',
          limit,
        );
        const size = await this.redis.xlen(key);
        return {
          value: items.map(([id, fields]) => ({ id, fields })),
          truncated: size > items.length,
        };
      }
      case 'none':
        return { value: null, truncated: false };
      default:
        return { value: null, truncated: false };
    }
  }

  private async scanHash(
    key: string,
    limit: number,
  ): Promise<{ value: Record<string, string>; truncated: boolean }> {
    let cursor = '0';
    const value: Record<string, string> = {};
    do {
      const [nextCursor, rows] = await this.redis.hscan(
        key,
        cursor,
        'COUNT',
        Math.min(limit, 100),
      );
      cursor = nextCursor;
      for (let i = 0; i < rows.length && Object.keys(value).length < limit; i += 2) {
        value[rows[i]] = rows[i + 1];
      }
    } while (cursor !== '0' && Object.keys(value).length < limit);
    const size = await this.redis.hlen(key);
    return { value, truncated: size > Object.keys(value).length };
  }

  private async scanSet(
    key: string,
    limit: number,
  ): Promise<{ value: string[]; truncated: boolean }> {
    let cursor = '0';
    const value: string[] = [];
    do {
      const [nextCursor, rows] = await this.redis.sscan(
        key,
        cursor,
        'COUNT',
        Math.min(limit, 100),
      );
      cursor = nextCursor;
      for (const row of rows) {
        if (value.length >= limit) break;
        value.push(row);
      }
    } while (cursor !== '0' && value.length < limit);
    const size = await this.redis.scard(key);
    return { value, truncated: size > value.length };
  }

  private async readSize(key: string): Promise<number | undefined> {
    const type = (await this.redis.type(key)) as RedisAdminValueType;
    switch (type) {
      case 'string':
        return this.redis.strlen(key);
      case 'hash':
        return this.redis.hlen(key);
      case 'list':
        return this.redis.llen(key);
      case 'set':
        return this.redis.scard(key);
      case 'zset':
        return this.redis.zcard(key);
      case 'stream':
        return this.redis.xlen(key);
      default:
        return undefined;
    }
  }

  private async memoryUsage(key: string): Promise<number | null> {
    try {
      const value = await this.redis.call('MEMORY', 'USAGE', key);
      if (value == null) return null;
      const bytes = typeof value === 'number' ? value : Number(value);
      return Number.isFinite(bytes) ? bytes : null;
    } catch {
      return null;
    }
  }

  private async objectEncoding(key: string): Promise<string | null> {
    try {
      const value = await this.redis.call('OBJECT', 'ENCODING', key);
      return typeof value === 'string' ? value : null;
    } catch {
      return null;
    }
  }

  private async readInfo(section?: string): Promise<string> {
    try {
      return section ? await this.redis.info(section) : await this.redis.info();
    } catch {
      return '';
    }
  }

  private parseInfoSection(info: string): Record<string, string> {
    const out: Record<string, string> = {};
    for (const line of info.split('\r\n')) {
      if (!line || line.startsWith('#')) continue;
      const idx = line.indexOf(':');
      if (idx <= 0) continue;
      out[line.slice(0, idx)] = line.slice(idx + 1);
    }
    return out;
  }

  private parseServerInfo(info: string): RedisAdminOverview['server'] {
    const parsed = this.parseInfoSection(info);
    return {
      redisVersion: parsed.redis_version,
      mode: parsed.redis_mode,
      role: parsed.role,
      os: parsed.os,
      archBits: parsed.arch_bits ? Number(parsed.arch_bits) : undefined,
      processId: parsed.process_id ? Number(parsed.process_id) : undefined,
      tcpPort: parsed.tcp_port ? Number(parsed.tcp_port) : undefined,
      configuredHz: parsed.configured_hz
        ? Number(parsed.configured_hz)
        : undefined,
      uptimeSeconds: parsed.uptime_in_seconds
        ? Number(parsed.uptime_in_seconds)
        : undefined,
      usedMemoryHuman: parsed.used_memory_human,
      usedMemoryBytes: parsed.used_memory ? Number(parsed.used_memory) : undefined,
      maxMemoryHuman: parsed.maxmemory_human,
      maxMemoryBytes: parsed.maxmemory ? Number(parsed.maxmemory) : undefined,
      totalSystemMemoryHuman: parsed.total_system_memory_human,
      totalSystemMemoryBytes: parsed.total_system_memory
        ? Number(parsed.total_system_memory)
        : undefined,
      allocator: parsed.mem_allocator,
      memFragmentationRatio: parsed.mem_fragmentation_ratio
        ? Number(parsed.mem_fragmentation_ratio)
        : undefined,
      connectedClients: parsed.connected_clients
        ? Number(parsed.connected_clients)
        : undefined,
      usedCpuSys: parsed.used_cpu_sys ? Number(parsed.used_cpu_sys) : undefined,
      usedCpuUser: parsed.used_cpu_user
        ? Number(parsed.used_cpu_user)
        : undefined,
      usedCpuSysChildren: parsed.used_cpu_sys_children
        ? Number(parsed.used_cpu_sys_children)
        : undefined,
      usedCpuUserChildren: parsed.used_cpu_user_children
        ? Number(parsed.used_cpu_user_children)
        : undefined,
    };
  }

  private async scanForOverview(): Promise<{
    keys: string[];
    scanned: number;
    complete: boolean;
  }> {
    let cursor = '0';
    const keys: string[] = [];
    do {
      const [nextCursor, rows] = await this.redis.scan(
        cursor,
        'COUNT',
        OVERVIEW_SCAN_COUNT,
      );
      cursor = nextCursor;
      keys.push(...rows);
    } while (cursor !== '0' && keys.length < OVERVIEW_SCAN_LIMIT);
    return {
      keys: keys.slice(0, OVERVIEW_SCAN_LIMIT),
      scanned: keys.length,
      complete: cursor === '0',
    };
  }

  private groupKey(summary: RedisAdminKeySummary): {
    name: string;
    systemKind?: NonNullable<RedisAdminSystemMark['systemKind']>;
    namespace?: string;
    scope: RedisAdminNamespaceScope;
  } {
    if (summary.namespaceScope === 'current') {
      return {
        name: summary.systemKind
          ? this.systemKindLabel(summary.systemKind)
          : 'current namespace',
        systemKind: summary.systemKind,
        scope: summary.namespaceScope,
      };
    }
    if (summary.reason) {
      return {
        name: summary.reason,
        systemKind: summary.systemKind,
        namespace: summary.namespace,
        scope: summary.namespaceScope,
      };
    }
    return {
      name: summary.namespace ?? 'global',
      namespace: summary.namespace,
      scope: summary.namespaceScope,
    };
  }

  private async getUserCacheQuota(): Promise<RedisAdminOverview['userCache']> {
    const usedBytes = Math.max(
      0,
      Number((await this.redis.get(`${this.nodeName}:user_cache_meta:total_bytes`)) ?? 0),
    );
    return {
      usedBytes,
      limitBytes: this.userCacheLimitBytes,
      maxValueBytes: this.userCacheMaxValueBytes,
      remainingBytes:
        this.userCacheLimitBytes > 0
          ? Math.max(0, this.userCacheLimitBytes - usedBytes)
          : null,
      evictionPolicy: this.userCacheLimitBytes > 0 ? 'lru' : 'disabled',
    };
  }

  private effectiveListPattern(
    patternInput: string | undefined,
  ): string {
    const pattern = patternInput?.trim() || '*';
    if (pattern.startsWith(`${this.nodeName}:`)) return pattern;
    if (this.isSystemPublicKey(pattern) || pattern === '*') {
      return `${this.nodeName}:${pattern}`;
    }
    if (pattern.startsWith('user_cache:') || pattern.startsWith('user_cache_meta:')) {
      return `${this.nodeName}:${pattern}`;
    }
    if (!pattern.includes(':')) {
      return `${this.nodeName}:user_cache:${pattern}`;
    }
    return `${this.nodeName}:${pattern}`;
  }

  private namespaceForKey(key: string): {
    namespace?: string;
    namespaceScope: RedisAdminNamespaceScope;
  } {
    const queue = this.parseSystemQueueKey(key);
    const namespace = queue?.nodeName || this.parseNamespace(key);
    if (!namespace) return { namespaceScope: 'global' };
    if (namespace === this.nodeName) return { namespaceScope: 'current' };
    return { namespaceScope: 'global' };
  }

  private parseNamespace(key: string): string | undefined {
    if (
      [
        BOOTSTRAP_SCRIPT_EXECUTION_LOCK_KEY,
        PROVISION_LOCK_KEY,
        SAGA_ORPHAN_RECOVERY_LOCK_KEY,
      ].includes(key)
    ) {
      return undefined;
    }
    const index = key.indexOf(':');
    if (index <= 0) return undefined;
    return key.slice(0, index);
  }

  private resolveKey(key: string): string {
    this.assertValidKey(key);
    if (key.startsWith(`${this.nodeName}:`)) return key;
    if (this.isSystemPublicKey(key)) return `${this.nodeName}:${key}`;
    if (key.startsWith('user_cache:') || key.startsWith('user_cache_meta:')) {
      return `${this.nodeName}:${key}`;
    }
    if (
      [
        BOOTSTRAP_SCRIPT_EXECUTION_LOCK_KEY,
        PROVISION_LOCK_KEY,
        SAGA_ORPHAN_RECOVERY_LOCK_KEY,
      ].includes(key)
    ) {
      return key;
    }
    return `${this.nodeName}:user_cache:${key}`;
  }

  private publicKey(key: string): string {
    const prefix = `${this.nodeName}:`;
    const withoutNode = key.startsWith(prefix) ? key.slice(prefix.length) : key;
    const userPrefix = 'user_cache:';
    return withoutNode.startsWith(userPrefix)
      ? withoutNode.slice(userPrefix.length)
      : withoutNode;
  }

  private isSystemPublicKey(key: string): boolean {
    const firstPart = key.split(':')[0];
    return (
      key.startsWith('runtime_cache:') ||
      key.startsWith('socket.io:') ||
      key.startsWith('runtime-monitor:') ||
      key.startsWith('cluster-telemetry:') ||
      Object.values(SYSTEM_QUEUES).includes(firstPart as any)
    );
  }

  private assertPositiveTtlSeconds(value: any): void {
    const ttl = Number(value);
    if (!Number.isInteger(ttl) || ttl <= 0) {
      throw new Error('ttlSeconds must be a positive integer or null');
    }
  }

  private isReadableKey(key: string): boolean {
    return (
      key.startsWith(`${this.nodeName}:`) ||
      [
        BOOTSTRAP_SCRIPT_EXECUTION_LOCK_KEY,
        PROVISION_LOCK_KEY,
        SAGA_ORPHAN_RECOVERY_LOCK_KEY,
      ].includes(key)
    );
  }

  private systemKindLabel(kind: NonNullable<RedisAdminSystemMark['systemKind']>) {
    switch (kind) {
      case 'runtime_cache':
        return 'runtime cache';
      case 'user_cache':
        return 'user cache';
      case 'bullmq':
        return 'BullMQ';
      case 'socket_io':
        return 'Socket.IO';
      case 'runtime_monitor':
        return 'runtime monitor';
      case 'system_lock':
        return 'system lock';
    }
  }

  private async getBuffer(key: string): Promise<Buffer | null> {
    const redisWithBuffer = this.redis as Redis & {
      getBuffer?: (key: string) => Promise<Buffer | null>;
    };
    if (typeof redisWithBuffer.getBuffer === 'function') {
      return redisWithBuffer.getBuffer(key);
    }
    const raw = await this.redis.get(key);
    return raw == null ? null : Buffer.from(raw);
  }

  private async readStringValue(key: string): Promise<string | null> {
    const raw = await this.getBuffer(key);
    if (!raw) return null;
    const text = raw.toString('utf8');
    if (this.isLikelyText(text)) return text;
    return `[binary value, ${raw.length} bytes]`;
  }

  private isLikelyText(value: string): boolean {
    if (value.includes('\uFFFD')) return false;
    return !/[\u0000-\u0008\u000B\u000C\u000E-\u001F]/.test(value);
  }

  private parseSystemQueueKey(
    key: string,
  ): { nodeName: string; queueName: string; suffix: string[] } | null {
    const parts = key.split(':');
    for (let i = 0; i < parts.length; i++) {
      const part = parts[i];
      if (!Object.values(SYSTEM_QUEUES).includes(part as any)) continue;
      const nodeName = parts.slice(0, i).filter(Boolean).join(':');
      return {
        nodeName: nodeName || 'default',
        queueName: part,
        suffix: parts.slice(i + 1).filter((item) => item.length > 0),
      };
    }
    return null;
  }

  private clampCount(value: any, fallback: number, max: number): number {
    const count = Number(value ?? fallback);
    if (!Number.isFinite(count)) return fallback;
    return Math.max(1, Math.min(max, Math.trunc(count)));
  }

  private assertCanModify(key: string): void {
    this.assertValidKey(key);
    const mark = this.markSystemKey(key);
    if (!mark.modifiable) {
      throw new AuthorizationException(
        `Redis key is system-managed and cannot be modified: ${mark.reason}`,
        { key, reason: mark.reason },
      );
    }
  }

  private assertValidKey(key: string): void {
    if (!key || typeof key !== 'string') {
      throw new Error('key is required');
    }
  }

}
