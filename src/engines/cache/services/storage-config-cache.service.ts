import { DatabaseConfigService } from '../../../shared/services';
import { EventEmitter2 } from 'eventemitter2';
import { QueryBuilderService } from '../../../kernel/query';
import { BaseCacheService, CacheConfig } from './base-cache.service';
import { RedisRuntimeCacheStore } from './redis-runtime-cache-store.service';
import { CACHE_IDENTIFIERS } from '../../../shared/utils/cache-events.constants';

const STORAGE_CONFIG: CacheConfig = {
  cacheIdentifier: CACHE_IDENTIFIERS.STORAGE,
  colorCode: '\x1b[37m',
  cacheName: 'StorageConfigCache',
};

export class StorageConfigCacheService extends BaseCacheService<
  Map<string | number, any>
> {
  private readonly queryBuilderService: QueryBuilderService;

  constructor(deps: {
    queryBuilderService: QueryBuilderService;
    eventEmitter?: EventEmitter2;
    redisRuntimeCacheStore?: RedisRuntimeCacheStore;
  }) {
    super(STORAGE_CONFIG, deps.eventEmitter, deps.redisRuntimeCacheStore);
    this.queryBuilderService = deps.queryBuilderService;
  }

  protected async loadFromDb(): Promise<any[]> {
    const result = await this.queryBuilderService.find({
      table: 'storage_config_definition',
      filter: { isEnabled: { _eq: true } },
      fields: ['*'],
    });

    return result.data || [];
  }

  protected transformData(configs: any[]): Map<string | number, any> {
    const configsMap = new Map<string | number, any>();
    const isMongoDb = this.queryBuilderService.isMongoDb();

    for (const config of configs) {
      const idField = DatabaseConfigService.getPkField();
      let id = config[idField];

      if (!id) {
        this.logger.warn('Storage config missing ID, skipping');
        continue;
      }

      if (isMongoDb) {
        if (
          typeof id === 'object' &&
          id !== null &&
          typeof id.toString === 'function'
        ) {
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

  protected getLogCount(): string {
    return `${this.cache.size} storage configs`;
  }

  async getStorageConfigById(
    id: number | string | null | undefined,
  ): Promise<any | null> {
    const cache = await this.getCacheAsync();

    if (!id || id === null || id === undefined) {
      return null;
    }

    let normalizedId: string | number = id;
    const isMongoDb = this.queryBuilderService.isMongoDb();

    if (isMongoDb) {
      if (
        typeof id === 'object' &&
        id !== null &&
        typeof (id as any).toString === 'function'
      ) {
        normalizedId = (id as any).toString();
      } else {
        normalizedId = String(id);
      }
    }

    let config = cache.get(normalizedId);
    if (config) {
      return config;
    }

    if (!isMongoDb) {
      const numId =
        typeof normalizedId === 'string'
          ? parseInt(normalizedId, 10)
          : normalizedId;
      if (!isNaN(numId as number)) {
        config = cache.get(numId);
        if (config) {
          return config;
        }
      }
      if (typeof normalizedId === 'number') {
        config = cache.get(String(normalizedId));
        if (config) {
          return config;
        }
      }
    }
    return null;
  }

  async getStorageConfigByType(type: string): Promise<any | null> {
    const cache = await this.getCacheAsync();
    const values = Array.from(cache.values());
    for (const config of values) {
      if (config.type === type && config.isEnabled) {
        return config;
      }
    }
    return null;
  }
}
