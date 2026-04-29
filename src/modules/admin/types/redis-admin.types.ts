export type RedisAdminValueType =
  | 'string'
  | 'hash'
  | 'list'
  | 'set'
  | 'zset'
  | 'stream'
  | 'none'
  | 'unknown';

export interface RedisAdminSystemMark {
  isSystem: boolean;
  modifiable: boolean;
  systemKind?: RedisAdminSystemKind;
  reason?: string;
}

export type RedisAdminSystemKind =
  | 'runtime_cache'
  | 'user_cache'
  | 'bullmq'
  | 'socket_io'
  | 'runtime_monitor'
  | 'system_lock';

export type RedisAdminNamespaceScope = 'current' | 'global';

export interface RedisAdminKeySummary extends RedisAdminSystemMark {
  key: string;
  namespace?: string;
  namespaceScope: RedisAdminNamespaceScope;
  type: RedisAdminValueType;
  ttlSeconds: number;
  size?: number;
  memoryBytes?: number | null;
}

export interface RedisAdminOverview {
  connected: boolean;
  keyCount: number;
  scanned: number;
  scanComplete: boolean;
  server: {
    redisVersion?: string;
    mode?: string;
    role?: string;
    os?: string;
    archBits?: number;
    processId?: number;
    tcpPort?: number;
    configuredHz?: number;
    uptimeSeconds?: number;
    usedMemoryHuman?: string;
    usedMemoryBytes?: number;
    maxMemoryHuman?: string;
    maxMemoryBytes?: number;
    totalSystemMemoryHuman?: string;
    totalSystemMemoryBytes?: number;
    allocator?: string;
    memFragmentationRatio?: number;
    connectedClients?: number;
    usedCpuSys?: number;
    usedCpuUser?: number;
    usedCpuSysChildren?: number;
    usedCpuUserChildren?: number;
  };
  keyspace: Record<string, string>;
  userCache: {
    usedBytes: number;
    limitBytes: number;
    maxValueBytes: number;
    remainingBytes: number | null;
    evictionPolicy: 'lru' | 'disabled';
  };
  groups: Array<{
    name: string;
    count: number;
    memoryBytes: number;
    system: boolean;
    systemKind?: RedisAdminSystemKind;
    namespace?: string;
    scope: RedisAdminNamespaceScope;
  }>;
  topKeys: RedisAdminKeySummary[];
}

export interface RedisAdminKeyDetail extends RedisAdminKeySummary {
  encoding?: string | null;
  value: any;
  truncated?: boolean;
}
