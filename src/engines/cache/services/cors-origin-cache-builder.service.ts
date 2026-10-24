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
      .map((record) => normalizeAllowedOrigin(record?.value))
      .filter((origin): origin is string => origin !== null);

    return { loaded: true, origins: [...new Set(origins)] };
  }
}

/**
 * Only an absolute http(s) origin can be granted, because that is the only
 * shape a browser sends as `Origin`. This rejects the `"null"` origin, which
 * any sandboxed iframe or `data:` URL can present, along with bare hosts and
 * paths that could never match a real request anyway.
 */
function normalizeAllowedOrigin(value: unknown): string | null {
  if (typeof value !== 'string') return null;
  const trimmed = value.trim();
  if (trimmed.length === 0) return null;
  try {
    const parsed = new URL(trimmed);
    if (parsed.protocol !== 'http:' && parsed.protocol !== 'https:')
      return null;
    if (!parsed.host) return null;
    return parsed.origin;
  } catch {
    return null;
  }
}
