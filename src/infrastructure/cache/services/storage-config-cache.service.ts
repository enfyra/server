import { Injectable, Logger, OnModuleInit, OnApplicationBootstrap } from '@nestjs/common';
import { QueryBuilderService } from '../../query-builder/query-builder.service';
import { RedisPubSubService } from './redis-pubsub.service';
import { CacheService } from './cache.service';
import { MetadataCacheService } from './metadata-cache.service';
import { InstanceService } from '../../../shared/services/instance.service';
import {
  STORAGE_CONFIG_CACHE_SYNC_EVENT_KEY,
  STORAGE_CONFIG_RELOAD_LOCK_KEY,
  REDIS_TTL,
} from '../../../shared/utils/constant';

@Injectable()
export class StorageConfigCacheService implements OnModuleInit, OnApplicationBootstrap {
  private readonly logger = new Logger(StorageConfigCacheService.name);
  private storageConfigsCache: Map<string | number, any> = new Map();
  private cacheLoaded = false;
  private messageHandler: ((channel: string, message: string) => void) | null = null;

  constructor(
    private readonly queryBuilder: QueryBuilderService,
    private readonly redisPubSubService: RedisPubSubService,
    private readonly cacheService: CacheService,
    private readonly metadataCacheService: MetadataCacheService,
    private readonly instanceService: InstanceService,
  ) {}

  async onModuleInit() {
    this.subscribe();
  }

  async onApplicationBootstrap() {
    await this.metadataCacheService.getMetadata();

    await this.reload();
  }

  private subscribe() {
    const sub = this.redisPubSubService.sub;
    if (!sub) {
      this.logger.warn('Redis subscription not available for storage config cache sync');
      return;
    }

    if (this.messageHandler) {
      return;
    }

    this.messageHandler = (channel: string, message: string) => {
      if (channel === STORAGE_CONFIG_CACHE_SYNC_EVENT_KEY) {
        try {
          const payload = JSON.parse(message);
          const myInstanceId = this.instanceService.getInstanceId();

          if (payload.instanceId === myInstanceId) {
            return;
          }

          this.logger.log(`Received storage config cache sync from instance ${payload.instanceId.slice(0, 8)}...`);

          this.storageConfigsCache = new Map();
          for (const [id, config] of payload.configs) {
            this.storageConfigsCache.set(id, config);
            if (typeof id === 'number') {
              this.storageConfigsCache.set(String(id), config);
            } else if (typeof id === 'string' && !isNaN(Number(id))) {
              this.storageConfigsCache.set(Number(id), config);
            }
          }
          this.cacheLoaded = true;
          this.logger.log(`Storage config cache synced: ${payload.configs.length} configs`);
        } catch (error) {
          this.logger.error('Failed to parse storage config cache sync message:', error);
        }
      }
    };

    this.redisPubSubService.subscribeWithHandler(
      STORAGE_CONFIG_CACHE_SYNC_EVENT_KEY,
      this.messageHandler
    );
  }

  async getStorageConfigById(id: number | string): Promise<any | null> {
    if (!this.cacheLoaded) {
      await this.reload();
    }
    const normalizedId = typeof id === 'string' ? parseInt(id, 10) : id;
    return this.storageConfigsCache.get(normalizedId) || this.storageConfigsCache.get(id) || null;
  }

  async getStorageConfigByType(type: string): Promise<any | null> {
    if (!this.cacheLoaded) {
      await this.reload();
    }
    for (const config of this.storageConfigsCache.values()) {
      if (config.type === type && config.isEnabled === true) {
        return config;
      }
    }
    return null;
  }

  async reload(): Promise<void> {
    const instanceId = this.instanceService.getInstanceId();

    try {
      const acquired = await this.cacheService.acquire(
        STORAGE_CONFIG_RELOAD_LOCK_KEY,
        instanceId,
        REDIS_TTL.RELOAD_LOCK_TTL
      );

      if (!acquired) {
        this.logger.log('Another instance is reloading storage configs, waiting for broadcast...');
        return;
      }

      this.logger.log(`Acquired storage config reload lock (instance ${instanceId.slice(0, 8)})`);

      try {
        const start = Date.now();
        this.logger.log('Reloading storage config cache...');

        const configs = await this.loadStorageConfigs();
        this.logger.log(`Loaded ${configs.length} storage configs in ${Date.now() - start}ms`);

        const configsMap = new Map();
        for (const config of configs) {
          const idField = this.queryBuilder.isMongoDb() ? '_id' : 'id';
          const id = config[idField];
          configsMap.set(id, config);
          if (typeof id === 'number') {
            configsMap.set(String(id), config);
          } else if (typeof id === 'string' && !isNaN(Number(id))) {
            configsMap.set(Number(id), config);
          }
        }

        await this.publish(Array.from(configsMap.entries()));
        this.storageConfigsCache = configsMap;
        this.cacheLoaded = true;
      } finally {
        await this.cacheService.release(STORAGE_CONFIG_RELOAD_LOCK_KEY, instanceId);
        this.logger.log('Released storage config reload lock');
      }
    } catch (error) {
      this.logger.error('Failed to reload storage config cache:', error);
      throw error;
    }
  }

  private async publish(configsArray: Array<[string | number, any]>): Promise<void> {
    try {
      const payload = {
        instanceId: this.instanceService.getInstanceId(),
        configs: configsArray,
        timestamp: Date.now(),
      };

      await this.redisPubSubService.publish(
        STORAGE_CONFIG_CACHE_SYNC_EVENT_KEY,
        JSON.stringify(payload),
      );

      this.logger.log(`Published storage config cache to other instances (${configsArray.length} configs)`);
    } catch (error) {
      this.logger.error('Failed to publish storage config cache sync:', error);
    }
  }

  private async loadStorageConfigs(): Promise<any[]> {
    const result = await this.queryBuilder.select({
      tableName: 'storage_config_definition',
      filter: { isEnabled: { _eq: true } },
      fields: ['*'],
    });

    return result.data || [];
  }
}

