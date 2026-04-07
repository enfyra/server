import {
  Injectable,
  Logger,
  OnApplicationBootstrap,
  OnModuleDestroy,
  Optional,
} from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { RedisService } from '@liaoliaots/nestjs-redis';
import { EventEmitter2 } from '@nestjs/event-emitter';
import { createHash } from 'crypto';
import { Redis } from 'ioredis';
import { InstanceService } from '../../../shared/services/instance.service';
import { KnexService } from '../knex.service';
import { ReplicationManager } from './replication-manager.service';
import { parseDatabaseUri } from '../utils/uri-parser';
import { computeCoordinatedPoolMax } from '../utils/sql-pool-coordination.util';
import { CACHE_EVENTS } from '../../../shared/utils/cache-events.constants';
import {
  SQL_COORD_HEARTBEAT_MS,
  SQL_COORD_STALE_MS,
  SQL_COORD_RECONCILE_INTERVAL_MS,
  SQL_COORD_RESERVE_MIN,
  SQL_COORD_RESERVE_RATIO,
  SQL_COORD_HINT_TTL_S,
  SQL_COORD_PRESSURE_CHECK_MS,
  SQL_COORD_PRESSURE_PENDING_THRESHOLD,
  SQL_COORD_PRESSURE_TICKS_THRESHOLD,
  SQL_COORD_PRESSURE_COOLDOWN_MS,
} from '../../../shared/utils/auto-scaling.constants';

interface PoolHint {
  serverMax: number;
  target: number;
  instanceCount: number;
  ts: number;
}

@Injectable()
export class SqlPoolClusterCoordinatorService
  implements OnApplicationBootstrap, OnModuleDestroy
{
  private readonly logger = new Logger(SqlPoolClusterCoordinatorService.name);
  private redis: Redis | null = null;
  private readonly zsetKey: string;
  private readonly hintKey: string;
  private readonly instanceId: string;
  private heartbeatTimer?: ReturnType<typeof setInterval>;
  private reconcileTimer?: ReturnType<typeof setInterval>;
  private pressureTimer?: ReturnType<typeof setInterval>;
  private lastAppliedTarget = -1;
  private pressureTickCount = 0;
  private lastPressureReconcileAt = 0;
  private lastReconcileServerMax: number | null = null;

  constructor(
    private readonly redisService: RedisService,
    private readonly configService: ConfigService,
    private readonly instanceService: InstanceService,
    private readonly knexService: KnexService,
    private readonly eventEmitter: EventEmitter2,
    @Optional() private readonly replicationManager?: ReplicationManager,
  ) {
    const serverHash = this.resolveDbServerHash();
    this.zsetKey = `enfyra:coord:sql:pool:${serverHash}:instances`;
    this.hintKey = `enfyra:coord:sql:pool:${serverHash}:hint`;
    this.instanceId = this.instanceService.getInstanceId();
  }

  private resolveDbServerHash(): string {
    const dbUri = this.configService.get<string>('DB_URI');
    let host: string;
    let port: number;
    if (dbUri) {
      const parsed = parseDatabaseUri(dbUri);
      host = parsed.host;
      port = parsed.port;
    } else {
      const dbType = this.configService.get<string>('DB_TYPE') || 'mysql';
      host = this.configService.get<string>('DB_HOST') || 'localhost';
      port = this.configService.get<number>('DB_PORT') || (dbType === 'postgres' ? 5432 : 3306);
    }
    return createHash('sha256').update(`${host}:${port}`).digest('hex').slice(0, 12);
  }

  onApplicationBootstrap(): void {
    const dbType = this.configService.get<string>('DB_TYPE') || 'mysql';
    if (dbType === 'mongodb') {
      return;
    }
    this.redis = this.redisService.getOrNil();
    if (!this.redis) {
      this.logger.warn('Redis unavailable; SQL pool cluster coordination skipped');
      return;
    }

    void this.heartbeatOnce();
    this.heartbeatTimer = setInterval(() => void this.heartbeatOnce(), SQL_COORD_HEARTBEAT_MS);

    void this.tryApplyPoolHint();

    this.startPressureMonitor();

    this.eventEmitter.once(CACHE_EVENTS.SYSTEM_READY, () => {
      void this.reconcilePool();
      this.reconcileTimer = setInterval(() => void this.reconcilePool(), SQL_COORD_RECONCILE_INTERVAL_MS);
    });
  }

  onModuleDestroy(): void {
    if (this.heartbeatTimer) {
      clearInterval(this.heartbeatTimer);
    }
    if (this.reconcileTimer) {
      clearInterval(this.reconcileTimer);
    }
    if (this.pressureTimer) {
      clearInterval(this.pressureTimer);
    }
    if (this.redis) {
      void this.redis.zrem(this.zsetKey, this.instanceId).catch(() => {});
    }
  }

  // ─── Layer 1: Redis Pool Hint ──────────────────────────────────

  private async tryApplyPoolHint(): Promise<void> {
    if (!this.redis) {
      return;
    }
    try {
      const raw = await this.redis.get(this.hintKey);
      if (!raw) {
        this.logger.debug('No pool hint found; using bootstrap pool');
        return;
      }
      const hint: PoolHint = JSON.parse(raw);
      const age = Date.now() - hint.ts;
      if (age > SQL_COORD_HINT_TTL_S * 1000) {
        this.logger.debug('Pool hint expired; using bootstrap pool');
        return;
      }

      const activeCount = await this.countActiveInstances();
      const reserve = Math.max(SQL_COORD_RESERVE_MIN, Math.floor(hint.serverMax * SQL_COORD_RESERVE_RATIO));
      const target = computeCoordinatedPoolMax({
        serverMaxConnections: hint.serverMax,
        activeInstanceCount: activeCount,
        reserveConnections: reserve,
      });

      this.applyTarget(target);
      this.logger.log(
        `Pool hint applied: target=${target} (from hint.serverMax=${hint.serverMax}, instances=${activeCount}, age=${Math.round(age / 1000)}s)`,
      );
    } catch (e) {
      this.logger.warn(`Failed to apply pool hint: ${(e as Error).message}`);
    }
  }

  private async savePoolHint(serverMax: number, target: number, instanceCount: number): Promise<void> {
    if (!this.redis) {
      return;
    }
    try {
      const hint: PoolHint = { serverMax, target, instanceCount, ts: Date.now() };
      await this.redis.set(this.hintKey, JSON.stringify(hint), 'EX', SQL_COORD_HINT_TTL_S);
    } catch (e) {
      this.logger.warn(`Failed to save pool hint: ${(e as Error).message}`);
    }
  }

  // ─── Layer 2: Real Connection Awareness ────────────────────────

  private async fetchActiveConnectionCount(): Promise<number | null> {
    const dbType = this.configService.get<string>('DB_TYPE') || 'mysql';
    try {
      if (dbType === 'postgres') {
        const r = await this.knexService.raw(
          `SELECT count(*)::int AS v FROM pg_stat_activity WHERE pid != pg_backend_pid()`,
        );
        const row = (r as any).rows?.[0] ?? (Array.isArray(r) ? r[0] : null);
        const v = row?.v;
        const n = typeof v === 'number' ? v : parseInt(String(v), 10);
        return Number.isFinite(n) ? n : null;
      }
      if (dbType === 'mysql' || dbType === 'mariadb') {
        const r = await this.knexService.raw(`SHOW STATUS LIKE 'Threads_connected'`);
        const rows = (r as any)[0] ?? (r as any).rows;
        const row = Array.isArray(rows) ? rows[0] : rows;
        const val = row?.Value ?? row?.value;
        const n = parseInt(String(val), 10);
        return Number.isFinite(n) ? n : null;
      }
    } catch (e) {
      this.logger.warn(`Could not read active connections: ${(e as Error).message}`);
    }
    return null;
  }

  // ─── Layer 3: Reactive Backpressure ────────────────────────────

  private startPressureMonitor(): void {
    this.pressureTimer = setInterval(() => {
      this.checkPressure();
    }, SQL_COORD_PRESSURE_CHECK_MS);
  }

  private checkPressure(): void {
    const stats = this.knexService.getPoolStats();
    const hasPressure = stats.pending >= SQL_COORD_PRESSURE_PENDING_THRESHOLD;

    if (hasPressure) {
      this.pressureTickCount++;
    } else {
      this.pressureTickCount = Math.max(0, this.pressureTickCount - 1);
    }

    if (this.pressureTickCount >= SQL_COORD_PRESSURE_TICKS_THRESHOLD) {
      const now = Date.now();
      if (now - this.lastPressureReconcileAt >= SQL_COORD_PRESSURE_COOLDOWN_MS) {
        this.lastPressureReconcileAt = now;
        this.pressureTickCount = 0;
        this.logger.warn(
          `Pool pressure detected (pending=${stats.pending}, used=${stats.used}, max=${stats.max}); triggering early reconcile`,
        );
        void this.reconcilePool();
      }
    }
  }

  private async heartbeatOnce(): Promise<void> {
    if (!this.redis) {
      return;
    }
    try {
      await this.redis.zadd(this.zsetKey, Date.now(), this.instanceId);
    } catch (e) {
      this.logger.warn(`Pool coordination heartbeat failed: ${(e as Error).message}`);
    }
  }

  private async countActiveInstances(): Promise<number> {
    if (!this.redis) {
      return 1;
    }
    const now = Date.now();
    await this.redis.zremrangebyscore(this.zsetKey, '-inf', now - SQL_COORD_STALE_MS);
    const n = await this.redis.zcard(this.zsetKey);
    return Math.max(1, n);
  }

  private async fetchServerMaxConnections(): Promise<number | null> {
    const dbType = this.configService.get<string>('DB_TYPE') || 'mysql';
    try {
      if (dbType === 'postgres') {
        const r = await this.knexService.raw(
          `SELECT setting::int AS v FROM pg_settings WHERE name = 'max_connections'`,
        );
        const row = (r as any).rows?.[0] ?? (Array.isArray(r) ? r[0] : null);
        const v = row?.v;
        const n = typeof v === 'number' ? v : parseInt(String(v), 10);
        return Number.isFinite(n) ? n : null;
      }
      if (dbType === 'mysql' || dbType === 'mariadb') {
        const r = await this.knexService.raw(`SHOW VARIABLES LIKE 'max_connections'`);
        const rows = (r as any)[0] ?? (r as any).rows;
        const row = Array.isArray(rows) ? rows[0] : rows;
        const val = row?.Value ?? row?.value;
        const n = parseInt(String(val), 10);
        return Number.isFinite(n) ? n : null;
      }
    } catch (e) {
      this.logger.warn(`Could not read server max_connections: ${(e as Error).message}`);
    }
    return null;
  }

  private async reconcilePool(): Promise<void> {
    if (!this.redis) {
      return;
    }
    const dbType = this.configService.get<string>('DB_TYPE') || 'mysql';
    if (dbType === 'mongodb') {
      return;
    }
    try {
      const serverMax = await this.fetchServerMaxConnections();
      if (serverMax == null) {
        return;
      }
      this.lastReconcileServerMax = serverMax;

      const activeCount = await this.countActiveInstances();
      const reserve = Math.max(SQL_COORD_RESERVE_MIN, Math.floor(serverMax * SQL_COORD_RESERVE_RATIO));

      const activeConnections = await this.fetchActiveConnectionCount();
      const ownPoolStats = this.knexService.getPoolStats();
      const ownUsed = ownPoolStats.used;

      let target = computeCoordinatedPoolMax({
        serverMaxConnections: serverMax,
        activeInstanceCount: activeCount,
        reserveConnections: reserve,
        externalConnectionsUsed: activeConnections ?? undefined,
        ownConnectionsUsed: ownUsed,
      });

      if (this.replicationManager) {
        const replicaTotal = this.replicationManager.getReplicaStats().total;
        const minPerProcess = replicaTotal === 0 ? 1 : 1 + replicaTotal;
        target = Math.max(minPerProcess, target);
      }

      if (target === this.lastAppliedTarget) {
        return;
      }

      this.applyTarget(target);

      void this.savePoolHint(serverMax, target, activeCount);

      this.logger.log(
        `Pool reconciled: target=${target} (serverMax=${serverMax}, instances=${activeCount}, activeConns=${activeConnections ?? '?'}, ownUsed=${ownUsed})`,
      );
    } catch (e) {
      this.logger.warn(`Pool coordination reconcile failed: ${(e as Error).message}`);
    }
  }

  private applyTarget(target: number): void {
    this.lastAppliedTarget = target;
    if (this.replicationManager && this.knexService.coordinatesPoolViaReplication()) {
      this.replicationManager.applyCoordinatedTotalPoolMax(target);
    } else {
      this.knexService.applyCoordinatedPoolMax(target);
    }
  }
}
