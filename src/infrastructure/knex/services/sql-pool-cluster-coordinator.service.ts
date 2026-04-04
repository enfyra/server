import {
  Injectable,
  Logger,
  OnApplicationBootstrap,
  OnModuleDestroy,
  Optional,
} from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { RedisService } from '@liaoliaots/nestjs-redis';
import { Redis } from 'ioredis';
import { InstanceService } from '../../../shared/services/instance.service';
import { KnexService } from '../knex.service';
import { ReplicationManager } from './replication-manager.service';
import { computeCoordinatedPoolMax } from '../utils/sql-pool-coordination.util';
import {
  SQL_COORD_HEARTBEAT_MS,
  SQL_COORD_STALE_MS,
  SQL_COORD_FIRST_RECONCILE_BASE_MS,
  SQL_COORD_FIRST_RECONCILE_JITTER_MS,
  SQL_COORD_RECONCILE_INTERVAL_MS,
  SQL_COORD_RESERVE_MIN,
  SQL_COORD_RESERVE_RATIO,
} from '../../../shared/utils/auto-scaling.constants';

const ZSET_KEY_SUFFIX = 'coord:sql:pool:instances';

@Injectable()
export class SqlPoolClusterCoordinatorService
  implements OnApplicationBootstrap, OnModuleDestroy
{
  private readonly logger = new Logger(SqlPoolClusterCoordinatorService.name);
  private redis: Redis | null = null;
  private readonly zsetKey: string;
  private readonly instanceId: string;
  private heartbeatTimer?: ReturnType<typeof setInterval>;
  private reconcileTimer?: ReturnType<typeof setInterval>;
  private firstReconcileTimer?: ReturnType<typeof setTimeout>;
  private lastAppliedTarget = -1;

  constructor(
    private readonly redisService: RedisService,
    private readonly configService: ConfigService,
    private readonly instanceService: InstanceService,
    private readonly knexService: KnexService,
    @Optional() private readonly replicationManager?: ReplicationManager,
  ) {
    const nodeName = this.configService.get<string>('NODE_NAME') || 'enfyra';
    this.zsetKey = `${nodeName}:${ZSET_KEY_SUFFIX}`;
    this.instanceId = this.instanceService.getInstanceId();
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
    const jitter = Math.floor(Math.random() * SQL_COORD_FIRST_RECONCILE_JITTER_MS);
    this.firstReconcileTimer = setTimeout(() => {
      void this.reconcilePool();
      this.reconcileTimer = setInterval(() => void this.reconcilePool(), SQL_COORD_RECONCILE_INTERVAL_MS);
    }, SQL_COORD_FIRST_RECONCILE_BASE_MS + jitter);
  }

  onModuleDestroy(): void {
    if (this.heartbeatTimer) {
      clearInterval(this.heartbeatTimer);
    }
    if (this.reconcileTimer) {
      clearInterval(this.reconcileTimer);
    }
    if (this.firstReconcileTimer) {
      clearTimeout(this.firstReconcileTimer);
    }
    if (this.redis) {
      void this.redis.zrem(this.zsetKey, this.instanceId).catch(() => {});
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
      const activeCount = await this.countActiveInstances();
      const reserve = Math.max(SQL_COORD_RESERVE_MIN, Math.floor(serverMax * SQL_COORD_RESERVE_RATIO));
      let target = computeCoordinatedPoolMax({
        serverMaxConnections: serverMax,
        activeInstanceCount: activeCount,
        reserveConnections: reserve,
      });
      if (this.replicationManager) {
        const replicaTotal = this.replicationManager.getReplicaStats().total;
        const minPerProcess = replicaTotal === 0 ? 1 : 1 + replicaTotal;
        target = Math.max(minPerProcess, target);
      }
      if (target === this.lastAppliedTarget) {
        return;
      }
      this.lastAppliedTarget = target;
      if (this.replicationManager && this.knexService.coordinatesPoolViaReplication()) {
        this.replicationManager.applyCoordinatedTotalPoolMax(target);
      } else {
        this.knexService.applyCoordinatedPoolMax(target);
      }
    } catch (e) {
      this.logger.warn(`Pool coordination reconcile failed: ${(e as Error).message}`);
    }
  }
}
