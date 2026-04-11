import {
  Injectable,
  Logger,
  OnModuleInit,
  OnModuleDestroy,
} from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { Knex, knex } from 'knex';
import { parseDatabaseUri } from '../../knex/utils/uri-parser';
import {
  SQL_ACQUIRE_TIMEOUT_MS,
  SQL_BOOTSTRAP_POOL_MAX_TOTAL,
  SQL_BOOTSTRAP_POOL_MIN,
} from '../../../shared/utils/auto-scaling.constants';
import { splitSqlPoolAcrossReplication } from '../utils/sql-pool-coordination.util';
import { DatabaseConfigService } from '../../../shared/services/database-config.service';
export interface ReplicaNode {
  knex: Knex;
  uri: string;
  isHealthy: boolean;
  errorCount: number;
  lastErrorTime?: number;
  connectionCount: number;
}
@Injectable()
export class ReplicationManager implements OnModuleInit, OnModuleDestroy {
  private masterKnex: Knex;
  private replicaNodes: ReplicaNode[] = [];
  private currentReplicaIndex = 0;
  private readonly logger = new Logger(ReplicationManager.name);
  private readonly dbType: string;
  private healthCheckInterval?: NodeJS.Timeout;
  constructor(
    private readonly configService: ConfigService,
    private readonly databaseConfig: DatabaseConfigService,
  ) {
    this.dbType = this.databaseConfig.getDbType();
  }
  async onModuleInit() {
    const DB_URI = this.configService.get<string>('DB_URI');
    if (!DB_URI) {
      throw new Error('DB_URI is required');
    }
    const replicaUris = this.configService.get<string>('DB_REPLICA_URIS');
    const replicaCount = replicaUris
      ? replicaUris
          .split(',')
          .map((uri) => uri.trim())
          .filter(Boolean).length
      : 0;
    const split = splitSqlPoolAcrossReplication({
      totalMax: SQL_BOOTSTRAP_POOL_MAX_TOTAL,
      totalMin: SQL_BOOTSTRAP_POOL_MIN,
      replicaCount,
    });
    const masterPoolMin = split.masterMin;
    const masterPoolMax = split.masterMax;
    const replicaPoolMin = split.replicaMin;
    const replicaPoolMax = split.replicaMax;
    const masterConfig = parseDatabaseUri(DB_URI);
    this.masterKnex = this.createKnexInstance(
      masterConfig,
      masterPoolMin,
      masterPoolMax,
    );
    try {
      await this.masterKnex.raw('SELECT 1');
    } catch (error) {
      this.logger.error('Failed to establish master connection:', error);
      throw error;
    }
    if (replicaUris) {
      const uris = replicaUris
        .split(',')
        .map((uri) => uri.trim())
        .filter(Boolean);
      for (const uri of uris) {
        try {
          const replicaConfig = parseDatabaseUri(uri);
          const replicaKnex = this.createKnexInstance(
            replicaConfig,
            replicaPoolMin,
            replicaPoolMax,
          );
          await replicaKnex.raw('SELECT 1');
          this.replicaNodes.push({
            knex: replicaKnex,
            uri,
            isHealthy: true,
            errorCount: 0,
            connectionCount: 0,
          });
        } catch (error) {}
      }
      if (this.replicaNodes.length > 0) {
        this.startHealthCheck();
      }
    }
  }
  private createKnexInstance(
    config: {
      host: string;
      port: number;
      user: string;
      password: string;
      database: string;
    },
    poolMinSize: number,
    poolMaxSize: number,
  ): Knex {
    return knex({
      client: this.dbType === 'postgres' ? 'pg' : 'mysql2',
      connection: {
        host: config.host,
        port: config.port,
        user: config.user,
        password: config.password,
        database: config.database,
        typeCast: (field: any, next: any) => {
          if (
            field.type === 'DATE' ||
            field.type === 'DATETIME' ||
            field.type === 'TIMESTAMP'
          ) {
            return field.string();
          }
          return next();
        },
      },
      pool: {
        min: poolMinSize,
        max: poolMaxSize,
      },
      acquireConnectionTimeout: SQL_ACQUIRE_TIMEOUT_MS,
      debug: false,
    });
  }
  applyCoordinatedTotalPoolMax(totalMax: number): void {
    const replicaCount = this.replicaNodes.length;
    const minTotal = replicaCount === 0 ? 1 : 1 + replicaCount;
    const total = Math.max(minTotal, Math.max(1, Math.trunc(totalMax)));
    const coordinatedMin = Math.min(2, total);
    const split = splitSqlPoolAcrossReplication({
      totalMax: total,
      totalMin: coordinatedMin,
      replicaCount,
    });
    if (replicaCount === 0) {
      this.applyPoolLimits(this.masterKnex, split.masterMin, split.masterMax);
      this.logger.log(
        `SQL pool coordinated (replication): totalMax=${total} masterOnly`,
      );
      return;
    }
    this.applyPoolLimits(this.masterKnex, split.masterMin, split.masterMax);
    for (const node of this.replicaNodes) {
      this.applyPoolLimits(node.knex, split.replicaMin, split.replicaMax);
    }
    this.logger.log(
      `SQL pool coordinated (replication): totalMax=${total} master=${split.masterMax} replicaEach=${split.replicaMax}`,
    );
  }

  private applyPoolLimits(instance: Knex, min: number, max: number): void {
    const pool = instance.client.pool;
    const used = typeof pool?.numUsed === 'function' ? pool.numUsed() : '?';
    const free = typeof pool?.numFree === 'function' ? pool.numFree() : '?';
    const pending =
      typeof pool?.numPendingAcquires === 'function'
        ? pool.numPendingAcquires()
        : '?';
    this.logger.debug(
      `Pool before resize: used=${used} free=${free} pending=${pending}`,
    );
    const p = pool as { min: number; max: number };
    const M = Math.max(1, max);
    const m = Math.max(0, Math.min(min, M));
    p.min = m;
    p.max = M;
  }

  getMasterKnex(): Knex {
    return this.masterKnex;
  }
  getReplicaKnex(): Knex {
    const readFromMaster =
      this.configService.get<string>('DB_READ_FROM_MASTER') === 'true';
    const healthyReplicas = this.replicaNodes.filter((node) => node.isHealthy);
    if (healthyReplicas.length === 0) {
      return this.masterKnex;
    }
    if (readFromMaster) {
      const totalNodes = 1 + healthyReplicas.length;
      const currentIndex = this.currentReplicaIndex % totalNodes;
      this.currentReplicaIndex = (this.currentReplicaIndex + 1) % totalNodes;
      if (currentIndex === 0) {
        return this.masterKnex;
      }
      const replicaIndex = currentIndex - 1;
      if (replicaIndex >= healthyReplicas.length) {
        return this.masterKnex;
      }
      const selectedNode = healthyReplicas[replicaIndex];
      selectedNode.connectionCount++;
      return selectedNode.knex;
    }
    const selectedNode =
      healthyReplicas[this.currentReplicaIndex % healthyReplicas.length];
    this.currentReplicaIndex =
      (this.currentReplicaIndex + 1) % healthyReplicas.length;
    selectedNode.connectionCount++;
    return selectedNode.knex;
  }
  private startHealthCheck() {
    const interval = parseInt(
      this.configService.get<string>('DB_REPLICA_HEALTH_CHECK_INTERVAL') ||
        '30000',
    );
    this.healthCheckInterval = setInterval(async () => {
      for (const node of this.replicaNodes) {
        try {
          await node.knex.raw('SELECT 1');
          if (!node.isHealthy) {
            node.isHealthy = true;
            node.errorCount = 0;
          }
        } catch (error) {
          node.errorCount++;
          node.lastErrorTime = Date.now();
          if (node.isHealthy) {
            node.isHealthy = false;
          }
        }
      }
    }, interval);
  }
  async onModuleDestroy() {
    if (this.healthCheckInterval) {
      clearInterval(this.healthCheckInterval);
    }
    for (const node of this.replicaNodes) {
      try {
        await node.knex.destroy();
      } catch (error) {
        this.logger.error(`Error destroying replica: ${node.uri}`, error);
      }
    }
    if (this.masterKnex) {
      await this.masterKnex.destroy();
    }
  }
  getReplicaStats() {
    return {
      total: this.replicaNodes.length,
      healthy: this.replicaNodes.filter((n) => n.isHealthy).length,
      unhealthy: this.replicaNodes.filter((n) => !n.isHealthy).length,
      replicas: this.replicaNodes.map((node) => ({
        uri: node.uri.replace(/:[^:@]+@/, ':****@'),
        isHealthy: node.isHealthy,
        errorCount: node.errorCount,
        connectionCount: node.connectionCount,
      })),
    };
  }
}
