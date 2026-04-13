import { Injectable } from '@nestjs/common';
import { EventEmitter2 } from '@nestjs/event-emitter';
import { QueryBuilderService } from '../../query-builder/query-builder.service';
import { BaseCacheService, CacheConfig } from './base-cache.service';
import { DEFAULT_MAX_QUERY_DEPTH, DEFAULT_MAX_UPLOAD_FILE_SIZE_MB, DEFAULT_MAX_REQUEST_BODY_SIZE_MB } from '../../../shared/utils/constant';
import {
  CACHE_IDENTIFIERS,
} from '../../../shared/utils/cache-events.constants';

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

@Injectable()
export class SettingCacheService extends BaseCacheService<SettingData> {
  constructor(
    private readonly queryBuilder: QueryBuilderService,
    eventEmitter: EventEmitter2,
  ) {
    super(SETTING_CACHE_CONFIG, eventEmitter);
  }

  protected async loadFromDb(): Promise<any> {
    try {
      const result = await this.queryBuilder.find({
        table: 'setting_definition',
        limit: 1,
      });
      return result?.data?.[0] || {};
    } catch {
      return {};
    }
  }

  protected transformData(raw: any): SettingData {
    const maxQueryDepth = typeof raw?.maxQueryDepth === 'number' && raw.maxQueryDepth > 0
      ? raw.maxQueryDepth
      : DEFAULT_MAX_QUERY_DEPTH;
    const maxUploadFileSize = typeof raw?.maxUploadFileSize === 'number' && raw.maxUploadFileSize > 0
      ? raw.maxUploadFileSize
      : DEFAULT_MAX_UPLOAD_FILE_SIZE_MB;
    const maxRequestBodySize = typeof raw?.maxRequestBodySize === 'number' && raw.maxRequestBodySize > 0
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

  getMaxQueryDepth(): number {
    return this.cache?.maxQueryDepth ?? DEFAULT_MAX_QUERY_DEPTH;
  }

  getMaxUploadFileSizeBytes(): number {
    return (this.cache?.maxUploadFileSize ?? DEFAULT_MAX_UPLOAD_FILE_SIZE_MB) * 1024 * 1024;
  }

  getMaxRequestBodySizeBytes(): number {
    return (this.cache?.maxRequestBodySize ?? DEFAULT_MAX_REQUEST_BODY_SIZE_MB) * 1024 * 1024;
  }

  getSetting<T = any>(key: string): T | undefined {
    return this.cache?.[key];
  }
}
