import { EventEmitter2 } from 'eventemitter2';
import { QueryBuilderService } from '@enfyra/kernel';
import { BaseCacheService, CacheConfig } from './base-cache.service';
import { RedisRuntimeCacheStore } from './redis-runtime-cache-store.service';
import {
  DEFAULT_MAX_QUERY_DEPTH,
  DEFAULT_MAX_UPLOAD_FILE_SIZE_MB,
  DEFAULT_MAX_REQUEST_BODY_SIZE_MB,
} from '../../../shared/utils/constant';
import { CACHE_IDENTIFIERS } from '../../../shared/utils/cache-events.constants';

const SETTING_CACHE_CONFIG: CacheConfig = {
  cacheIdentifier: CACHE_IDENTIFIERS.SETTING,
  colorCode: '\x1b[36m',
  cacheName: 'SettingCache',
};

interface SettingData {
  maxQueryDepth: number;
  maxUploadFileSize: number;
  maxRequestBodySize: number;
  [key: string]: any;
}

export class SettingCacheService extends BaseCacheService<SettingData> {
  private readonly queryBuilderService: QueryBuilderService;

  constructor(deps: {
    queryBuilderService: QueryBuilderService;
    eventEmitter?: EventEmitter2;
    redisRuntimeCacheStore?: RedisRuntimeCacheStore;
  }) {
    super(
      SETTING_CACHE_CONFIG,
      deps.eventEmitter,
      deps.redisRuntimeCacheStore,
    );
    this.queryBuilderService = deps.queryBuilderService;
  }

  protected async loadFromDb(): Promise<any> {
    try {
      const result = await this.queryBuilderService.find({
        table: 'setting_definition',
        limit: 1,
      });
      return result?.data?.[0] || {};
    } catch {
      return {};
    }
  }

  protected transformData(raw: any): SettingData {
    const maxQueryDepth =
      typeof raw?.maxQueryDepth === 'number' && raw.maxQueryDepth > 0
        ? raw.maxQueryDepth
        : DEFAULT_MAX_QUERY_DEPTH;
    const maxUploadFileSize =
      typeof raw?.maxUploadFileSize === 'number' && raw.maxUploadFileSize > 0
        ? raw.maxUploadFileSize
        : DEFAULT_MAX_UPLOAD_FILE_SIZE_MB;
    const maxRequestBodySize =
      typeof raw?.maxRequestBodySize === 'number' && raw.maxRequestBodySize > 0
        ? raw.maxRequestBodySize
        : DEFAULT_MAX_REQUEST_BODY_SIZE_MB;

    return {
      ...raw,
      maxQueryDepth,
      maxUploadFileSize,
      maxRequestBodySize,
    };
  }

  protected getLogCount(): string {
    return '1 setting record';
  }

  async getMaxQueryDepth(): Promise<number> {
    const cache = await this.getCacheAsync();
    return cache?.maxQueryDepth ?? DEFAULT_MAX_QUERY_DEPTH;
  }

  async getMaxUploadFileSizeBytes(): Promise<number> {
    const cache = await this.getCacheAsync();
    return (
      (cache?.maxUploadFileSize ?? DEFAULT_MAX_UPLOAD_FILE_SIZE_MB) *
      1024 *
      1024
    );
  }

  async getMaxRequestBodySizeBytes(): Promise<number> {
    const cache = await this.getCacheAsync();
    return (
      (cache?.maxRequestBodySize ?? DEFAULT_MAX_REQUEST_BODY_SIZE_MB) *
      1024 *
      1024
    );
  }

  async getSetting<T = any>(key: string): Promise<T | undefined> {
    const cache = await this.getCacheAsync();
    return cache?.[key];
  }
}
