import { Injectable } from '@nestjs/common';
import { OnEvent, EventEmitter2 } from '@nestjs/event-emitter';
import { QueryBuilderService } from '../../query-builder/query-builder.service';
import { RedisPubSubService } from './redis-pubsub.service';
import { CacheService } from './cache.service';
import { InstanceService } from '../../../shared/services/instance.service';
import { BaseCacheService, CacheConfig } from './base-cache.service';
import {
  STORAGE_CONFIG_CACHE_SYNC_EVENT_KEY,
  STORAGE_CONFIG_RELOAD_LOCK_KEY,
} from '../../../shared/utils/constant';
import { CACHE_EVENTS, CACHE_IDENTIFIERS, shouldReloadCache } from '../../../shared/utils/cache-events.constants';

const STORAGE_CONFIG: CacheConfig = {
  syncEventKey: STORAGE_CONFIG_CACHE_SYNC_EVENT_KEY,
  lockKey: STORAGE_CONFIG_RELOAD_LOCK_KEY,
  cacheIdentifier: CACHE_IDENTIFIERS.STORAGE,
  colorCode: '\x1b[37m',
  cacheName: 'StorageConfigCache',
};

@Injectable()
export class StorageConfigCacheService extends BaseCacheService<Map<string | number, any>> {
  constructor(
    private readonly queryBuilder: QueryBuilderService,
    redisPubSubService: RedisPubSubService,
    cacheService: CacheService,
    instanceService: InstanceService,
    eventEmitter: EventEmitter2,
  ) {
    super(STORAGE_CONFIG, redisPubSubService, cacheService, instanceService, eventEmitter);
  }

  @OnEvent(CACHE_EVENTS.METADATA_LOADED)
  async onMetadataLoaded() {
    await this.reload();
    this.eventEmitter?.emit(CACHE_EVENTS.STORAGE_LOADED);
  }

  @OnEvent(CACHE_EVENTS.INVALIDATE)
  async handleCacheInvalidation(payload: { tableName: string; action: string }) {
    if (shouldReloadCache(payload.tableName, this.config.cacheIdentifier)) {
      this.logger.log(`Cache invalidation event received for table: ${payload.tableName}`);
      await this.reload();
    }
  }

  protected async loadFromDb(): Promise<any[]> {
    const result = await this.queryBuilder.select({
      tableName: 'storage_config_definition',
      filter: { isEnabled: { _eq: true } },
      fields: ['*'],
    });

    return result.data || [];
  }

  protected transformData(configs: any[]): Map<string | number, any> {
    const configsMap = new Map<string | number, any>();
    const isMongoDb = this.queryBuilder.isMongoDb();

    for (const config of configs) {
      const idField = isMongoDb ? '_id' : 'id';
      let id = config[idField];

      if (!id) {
        this.logger.warn('Storage config missing ID, skipping');
        continue;
      }

      if (isMongoDb) {
        if (typeof id === 'object' && id !== null && typeof id.toString === 'function') {
          id = id.toString();
        } else {
          id = String(id);
        }
      }

      configsMap.set(id, config);

      if (!isMongoDb) {
        if (typeof id === 'number') {
          configsMap.set(String(id), config);
        } else if (typeof id === 'string' && !isNaN(Number(id))) {
          configsMap.set(Number(id), config);
        }
      }
    }

    return configsMap;
  }

  protected handleSyncData(data: Array<[string | number, any]>): void {
    this.cache = new Map();
    const isMongoDb = this.queryBuilder.isMongoDb();

    for (const [id, config] of data) {
      if (!id || id === null || id === undefined) {
        continue;
      }

      let normalizedId: string | number = id;

      if (isMongoDb) {
        if (typeof id === 'object' && id !== null && typeof (id as any).toString === 'function') {
          normalizedId = (id as any).toString();
        } else {
          normalizedId = String(id);
        }
      }

      this.cache.set(normalizedId, config);

      if (!isMongoDb) {
        if (typeof normalizedId === 'number') {
          this.cache.set(String(normalizedId), config);
        } else if (typeof normalizedId === 'string' && !isNaN(Number(normalizedId))) {
          this.cache.set(Number(normalizedId), config);
        }
      }
    }
  }

  protected deserializeSyncData(payload: any): any {
    return payload.configs;
  }

  protected serializeForPublish(cache: Map<string | number, any>): Record<string, any> {
    return { configs: Array.from(cache.entries()) };
  }

  protected getLogCount(): string {
    return `${this.cache.size} storage configs`;
  }

  protected logSyncSuccess(payload: any): void {
    this.logger.log(`Cache synced: ${payload.configs?.length || 0} configs`);
  }

  async getStorageConfigById(id: number | string | null | undefined): Promise<any | null> {
    await this.ensureLoaded();

    if (!id || id === null || id === undefined) {
      return null;
    }

    let normalizedId: string | number = id;
    const isMongoDb = this.queryBuilder.isMongoDb();

    if (isMongoDb) {
      if (typeof id === 'object' && id !== null && typeof (id as any).toString === 'function') {
        normalizedId = (id as any).toString();
      } else {
        normalizedId = String(id);
      }
    }

    let config = this.cache.get(normalizedId);
    if (config) {
      return config;
    }

    if (!isMongoDb) {
      const numId = typeof normalizedId === 'string' ? parseInt(normalizedId, 10) : normalizedId;
      if (!isNaN(numId as number)) {
        config = this.cache.get(numId);
        if (config) {
          return config;
        }
      }
      if (typeof normalizedId === 'number') {
        config = this.cache.get(String(normalizedId));
        if (config) {
          return config;
        }
      }
    }
    return null;
  }

  async getStorageConfigByType(type: string): Promise<any | null> {
    await this.ensureLoaded();
    const values = Array.from(this.cache.values());
    for (const config of values) {
      if (config.type === type && config.isEnabled === true) {
        return config;
      }
    }
    return null;
  }
}
