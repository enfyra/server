import { Injectable } from '@nestjs/common';
import { OnEvent, EventEmitter2 } from '@nestjs/event-emitter';
import { QueryBuilderService } from '../../query-builder/query-builder.service';
import { RedisPubSubService } from './redis-pubsub.service';
import { InstanceService } from '../../../shared/services/instance.service';
import { BaseCacheService, CacheConfig } from './base-cache.service';
import { SETTING_CACHE_SYNC_EVENT_KEY, DEFAULT_MAX_QUERY_DEPTH } from '../../../shared/utils/constant';
import {
  CACHE_EVENTS,
  CACHE_IDENTIFIERS,
  shouldReloadCache,
} from '../../../shared/utils/cache-events.constants';

const SETTING_CACHE_CONFIG: CacheConfig = {
  syncEventKey: SETTING_CACHE_SYNC_EVENT_KEY,
  cacheIdentifier: CACHE_IDENTIFIERS.SETTING,
  colorCode: '\x1b[36m',
  cacheName: 'SettingCache',
};

interface SettingData {
  maxQueryDepth: number;
  [key: string]: any;
}

@Injectable()
export class SettingCacheService extends BaseCacheService<SettingData> {
  constructor(
    private readonly queryBuilder: QueryBuilderService,
    redisPubSubService: RedisPubSubService,
    instanceService: InstanceService,
    eventEmitter: EventEmitter2,
  ) {
    super(SETTING_CACHE_CONFIG, redisPubSubService, instanceService, eventEmitter);
  }

  @OnEvent(CACHE_EVENTS.METADATA_LOADED)
  async onMetadataLoaded() {
    try {
      await this.reload(false);
    } catch {
      this.cache = this.transformData({});
      this.cacheLoaded = true;
    }
    this.eventEmitter?.emit(CACHE_EVENTS.SETTING_LOADED);
  }

  @OnEvent(CACHE_EVENTS.INVALIDATE)
  async handleCacheInvalidation(payload: { tableName: string; action: string }) {
    if (shouldReloadCache(payload.tableName, this.config.cacheIdentifier)) {
      await this.reload();
    }
  }

  protected async loadFromDb(): Promise<any> {
    try {
      const result = await this.queryBuilder.select({
        tableName: 'setting_definition',
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

    return {
      ...raw,
      maxQueryDepth,
    };
  }

  protected getLogCount(): string {
    return '1 setting record';
  }

  getMaxQueryDepth(): number {
    return this.cache?.maxQueryDepth ?? DEFAULT_MAX_QUERY_DEPTH;
  }

  getSetting<T = any>(key: string): T | undefined {
    return this.cache?.[key];
  }
}
