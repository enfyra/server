import { EventEmitter2 } from 'eventemitter2';
import { QueryBuilderService } from '@enfyra/kernel';
import { BaseCacheService, type CacheConfig } from './base-cache.service';
import { RedisRuntimeCacheStore } from './redis-runtime-cache-store.service';
import { CACHE_IDENTIFIERS } from '../../../shared/utils/cache-events.constants';
import { SYSTEM_TABLES } from '../../../shared/utils/system-tables.constants';

const CORS_ORIGIN_CACHE_CONFIG: CacheConfig = {
  cacheIdentifier: CACHE_IDENTIFIERS.CORS_ORIGIN,
  colorCode: '\x1b[36m',
  cacheName: 'CorsOriginCache',
};

export interface CorsOriginPolicy {
  loaded: boolean;
  origins: string[];
}

export class CorsOriginCacheBuilder extends BaseCacheService<CorsOriginPolicy> {
  private readonly queryBuilderService: QueryBuilderService;

  constructor(deps: {
    queryBuilderService: QueryBuilderService;
    eventEmitter?: EventEmitter2;
    redisRuntimeCacheStore?: RedisRuntimeCacheStore;
  }) {
    super(
      CORS_ORIGIN_CACHE_CONFIG,
      deps.eventEmitter,
      deps.redisRuntimeCacheStore,
    );
    this.queryBuilderService = deps.queryBuilderService;
  }

  protected async loadFromDb(): Promise<any[]> {
    try {
      const result = await this.queryBuilderService.find({
        table: SYSTEM_TABLES.corsOrigin,
        fields: ['value', 'isEnabled'],
        limit: -1,
      });
      return Array.isArray(result.data) ? result.data : [];
    } catch (error) {
      this.logger.warn(
        `CORS origin table is unavailable; treating the allow-list as unset: ${String(error)}`,
      );
      return [];
    }
  }

  protected transformData(rawData: any[]): CorsOriginPolicy {
    const origins = rawData
      .filter((record) => record?.isEnabled !== false)
      .map((record) =>
        typeof record?.value === 'string' ? record.value.trim() : '',
      )
      .filter((origin) => origin.length > 0);

    return { loaded: true, origins: [...new Set(origins)] };
  }
}
