import { Logger } from '../../../shared/logger';
import { EventEmitter2 } from 'eventemitter2';
import { createHash } from 'crypto';
import { Redis } from 'ioredis';
import { DatabaseConfigService } from '../../../shared/services/database-config.service';
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

export class SqlPoolClusterCoordinatorService {
  private readonly logger = new Logger(SqlPoolClusterCoordinatorService.name);
  private redis: Redis | null = null;
  private readonly zsetKey: string;
  private readonly instanceId: string;
  private heartbeatTimer?: ReturnType<typeof setInterval>;
  private reconcileTimer?: ReturnType<typeof setInterval>;
  private lastAppliedTarget = -1;
  private readonly _redis: Redis;
  private readonly databaseConfigService: DatabaseConfigService;
  private readonly instanceService: InstanceService;
  private readonly knexService: KnexService;
  private readonly eventEmitter: EventEmitter2;
  private readonly replicationManager?: ReplicationManager;

  constructor(deps: {
    redis: Redis;
    databaseConfigService: DatabaseConfigService;
    instanceService: InstanceService;
    knexService: KnexService;
    eventEmitter: EventEmitter2;
    replicationManager?: ReplicationManager;
  }) {
    this._redis = deps.redis;
    this.databaseConfigService = deps.databaseConfigService;
    this.instanceService = deps.instanceService;
    this.knexService = deps.knexService;
    this.eventEmitter = deps.eventEmitter;
    this.replicationManager = deps.replicationManager;
    this.zsetKey = `enfyra:coord:sql:pool:${this.resolveDbServerHash()}:instances`;
    this.instanceId = this.instanceService.getInstanceId();
  }

  private resolveDbServerHash(): string {
    const dbUri = (this as any).configService?.get?.('DB_URI') || (global as any).__env?.DB_URI;
    let host: string;
    let port: number;
    if (dbUri) {
      const parsed = parseDatabaseUri(dbUri);
      host = parsed.host;
      port = parsed.port;
    } else {
      host = 'localhost';
      port = this.databaseConfigService.isPostgres() ? 5432 : 3306;
    }
    return createHash('sha256')
      .update(`${host}:${port}`)
      .digest('hex')
      .slice(0, 12);
  }

  async init(): Promise<void> {
    if (this.databaseConfigService.isMongoDb()) {
      return;
    }
    this.redis = this._redis;
    if (!this.redis) {
      this.logger.warn(
        'Redis unavailable; SQL pool cluster coordination skipped',
      );
      return;
    }
    void this.heartbeatOnce();
    this.heartbeatTimer = setInterval(
      () => void this.heartbeatOnce(),
      SQL_COORD_HEARTBEAT_MS,
    );
    this.eventEmitter.once(CACHE_EVENTS.SYSTEM_READY, () => {
      void this.reconcilePool();
      this.reconcileTimer = setInterval(
        () => void this.reconcilePool(),
        SQL_COORD_RECONCILE_INTERVAL_MS,
      );
    });
  }

  async onDestroy(): Promise<void> {
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
      this.logger.warn(
        `Pool coordination heartbeat failed: ${(e as Error).message}`,
      );
    }
  }

  private async countActiveInstances(): Promise<number> {
    if (!this.redis) {
      return 1;
    }
    const now = Date.now();
    await this.redis.zremrangebyscore(
      this.zsetKey,
      '-inf',
      now - SQL_COORD_STALE_MS,
    );
    const n = await this.redis.zcard(this.zsetKey);
    return Math.max(1, n);
  }

  private async fetchServerMaxConnections(): Promise<number | null> {
    try {
      if (this.databaseConfigService.isPostgres()) {
        const r = await this.knexService.raw(
          `SELECT setting::int AS v FROM pg_settings WHERE name = 'max_connections'`,
        );
        const row = (r as any).rows?.[0] ?? (Array.isArray(r) ? r[0] : null);
        const v = row?.v;
        const n = typeof v === 'number' ? v : parseInt(String(v), 10);
        return Number.isFinite(n) ? n : null;
      }
      if (this.databaseConfigService.isMySql()) {
        const r = await this.knexService.raw(
          `SHOW VARIABLES LIKE 'max_connections'`,
        );
        const rows = (r as any)[0] ?? (r as any).rows;
        const row = Array.isArray(rows) ? rows[0] : rows;
        const val = row?.Value ?? row?.value;
        const n = parseInt(String(val), 10);
        return Number.isFinite(n) ? n : null;
      }
    } catch (e) {
      this.logger.warn(
        `Could not read server max_connections: ${(e as Error).message}`,
      );
    }
    return null;
  }

  private async reconcilePool(): Promise<void> {
    if (!this.redis) {
      return;
    }
    if (this.databaseConfigService.isMongoDb()) {
      return;
    }
    try {
      const serverMax = await this.fetchServerMaxConnections();
      if (serverMax == null) {
        return;
      }
      const activeCount = await this.countActiveInstances();
      const reserve = Math.max(
        SQL_COORD_RESERVE_MIN,
        Math.floor(serverMax * SQL_COORD_RESERVE_RATIO),
      );
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
      if (
        this.replicationManager &&
        this.knexService.coordinatesPoolViaReplication()
      ) {
        this.replicationManager.applyCoordinatedTotalPoolMax(target);
      } else {
        this.knexService.applyCoordinatedPoolMax(target);
      }
    } catch (e) {
      this.logger.warn(
        `Pool coordination reconcile failed: ${(e as Error).message}`,
      );
    }
  }
}
