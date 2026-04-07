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
} from '../../../shared/utils/auto-scaling.constants';

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
  private lastAppliedTarget = -1;

  constructor(
    private readonly redisService: RedisService,
    private readonly configService: ConfigService,
    private readonly instanceService: InstanceService,
    private readonly knexService: KnexService,
    private readonly eventEmitter: EventEmitter2,
    @Optional() private readonly replicationManager?: ReplicationManager,
  ) {
    this.zsetKey = `enfyra:coord:sql:pool:${this.resolveDbServerHash()}:instances`;
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
