import { Injectable, Logger, OnModuleInit, OnModuleDestroy } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { Knex, knex } from 'knex';
import { parseDatabaseUri } from '../../knex/utils/uri-parser';

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

  constructor(private readonly configService: ConfigService) {
    this.dbType = this.configService.get<string>('DB_TYPE') || 'mysql';
  }

  async onModuleInit() {
    const DB_URI = this.configService.get<string>('DB_URI');
    if (!DB_URI) {
      throw new Error('DB_URI is required');
    }

    const totalPoolMinSize = parseInt(this.configService.get<string>('DB_POOL_MIN_SIZE') || '2');
    const totalPoolMaxSize = parseInt(this.configService.get<string>('DB_POOL_MAX_SIZE') || '10');
    
    const replicaUris = this.configService.get<string>('DB_REPLICA_URIS');
    const replicaCount = replicaUris ? replicaUris.split(',').map(uri => uri.trim()).filter(Boolean).length : 0;
    const totalNodes = 1 + replicaCount;
    
    const masterRatio = parseFloat(this.configService.get<string>('DB_POOL_MASTER_RATIO') || '0.6');
    const replicaRatio = (1 - masterRatio) / Math.max(replicaCount, 1);
    
    const masterPoolMin = Math.max(1, Math.floor(totalPoolMinSize * masterRatio));
    const masterPoolMax = Math.max(1, Math.floor(totalPoolMaxSize * masterRatio));
    const replicaPoolMin = replicaCount > 0 ? Math.max(1, Math.floor(totalPoolMinSize * replicaRatio)) : totalPoolMinSize;
    const replicaPoolMax = replicaCount > 0 ? Math.max(1, Math.floor(totalPoolMaxSize * replicaRatio)) : totalPoolMaxSize;
    
    this.logger.log(`📊 Pool distribution: Master (${masterPoolMin}-${masterPoolMax}), Replicas (${replicaPoolMin}-${replicaPoolMax} each, ${replicaCount} replica(s))`);

    const masterConfig = parseDatabaseUri(DB_URI);
    this.masterKnex = this.createKnexInstance(masterConfig, masterPoolMin, masterPoolMax);
    
    try {
      await this.masterKnex.raw('SELECT 1');
      this.logger.log(`✅ Master connection established: ${DB_URI.replace(/:[^:@]+@/, ':****@')}`);
    } catch (error) {
      this.logger.error('Failed to establish master connection:', error);
      throw error;
    }

    // Initialize replica connections
    if (replicaUris) {
      const uris = replicaUris.split(',').map(uri => uri.trim()).filter(Boolean);
      
      for (const uri of uris) {
        try {
          const replicaConfig = parseDatabaseUri(uri);
          const replicaKnex = this.createKnexInstance(replicaConfig, replicaPoolMin, replicaPoolMax);
          
          await replicaKnex.raw('SELECT 1');
          
          this.replicaNodes.push({
            knex: replicaKnex,
            uri,
            isHealthy: true,
            errorCount: 0,
            connectionCount: 0,
          });
          
          this.logger.log(`✅ Replica connection established: ${uri.replace(/:[^:@]+@/, ':****@')}`);
        } catch (error) {
          this.logger.warn(`Failed to connect to replica ${uri.replace(/:[^:@]+@/, ':****@')}:`, error);
        }
      }

      if (this.replicaNodes.length > 0) {
        this.logger.log(`📊 Replication enabled: ${this.replicaNodes.length} replica(s) active`);
        this.startHealthCheck();
      } else {
        this.logger.warn('No healthy replicas available, falling back to master only');
      }
    } else {
      this.logger.log('No replicas configured, using master only');
    }
  }

  private createKnexInstance(
    config: { host: string; port: number; user: string; password: string; database: string },
    poolMinSize: number,
    poolMaxSize: number
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
          if (field.type === 'DATE' || field.type === 'DATETIME' || field.type === 'TIMESTAMP') {
            return field.string();
          }
          return next();
        },
      },
      pool: {
        min: poolMinSize,
        max: poolMaxSize,
      },
      acquireConnectionTimeout: parseInt(this.configService.get<string>('DB_ACQUIRE_TIMEOUT') || '10000'),
      debug: false,
    });
  }

  getMasterKnex(): Knex {
    return this.masterKnex;
  }

  getReplicaKnex(): Knex {
    const readFromMaster = this.configService.get<string>('DB_READ_FROM_MASTER') === 'true';
    const healthyReplicas = this.replicaNodes.filter(node => node.isHealthy);
    
    if (readFromMaster) {
      const totalNodes = 1 + healthyReplicas.length;
      const currentIndex = this.currentReplicaIndex % totalNodes;
      this.currentReplicaIndex = (this.currentReplicaIndex + 1) % totalNodes;
      
      if (currentIndex === 0) {
        this.logger.debug('DB_READ_FROM_MASTER enabled, using master for read (round-robin)');
        return this.masterKnex;
      }
      
      const replicaIndex = currentIndex - 1;
      const selectedNode = healthyReplicas[replicaIndex];
      selectedNode.connectionCount++;
      this.logger.debug(`DB_READ_FROM_MASTER enabled, using replica: ${selectedNode.uri.replace(/:[^:@]+@/, ':****@')} (round-robin)`);
      return selectedNode.knex;
    }
    
    if (healthyReplicas.length === 0) {
      this.logger.debug('No healthy replicas available, using master for read');
      return this.masterKnex;
    }

    const selectedNode = healthyReplicas[this.currentReplicaIndex % healthyReplicas.length];
    this.currentReplicaIndex = (this.currentReplicaIndex + 1) % healthyReplicas.length;
    selectedNode.connectionCount++;
    this.logger.debug(`Using replica: ${selectedNode.uri.replace(/:[^:@]+@/, ':****@')} (round-robin)`);
    
    return selectedNode.knex;
  }

  private startHealthCheck() {
    const interval = parseInt(this.configService.get<string>('DB_REPLICA_HEALTH_CHECK_INTERVAL') || '30000');
    
    this.healthCheckInterval = setInterval(async () => {
      for (const node of this.replicaNodes) {
        try {
          await node.knex.raw('SELECT 1');
          if (!node.isHealthy) {
            this.logger.log(`✅ Replica recovered: ${node.uri.replace(/:[^:@]+@/, ':****@')}`);
            node.isHealthy = true;
            node.errorCount = 0;
          }
        } catch (error) {
          node.errorCount++;
          node.lastErrorTime = Date.now();
          
          if (node.isHealthy) {
            this.logger.warn(`❌ Replica unhealthy: ${node.uri.replace(/:[^:@]+@/, ':****@')} (error count: ${node.errorCount})`);
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

    this.logger.log('🔌 Destroying replication connections...');
    
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

    this.logger.log('Replication connections destroyed');
  }

  getReplicaStats() {
    return {
      total: this.replicaNodes.length,
      healthy: this.replicaNodes.filter(n => n.isHealthy).length,
      unhealthy: this.replicaNodes.filter(n => !n.isHealthy).length,
      replicas: this.replicaNodes.map(node => ({
        uri: node.uri.replace(/:[^:@]+@/, ':****@'),
        isHealthy: node.isHealthy,
        errorCount: node.errorCount,
        connectionCount: node.connectionCount,
      })),
    };
  }
}

