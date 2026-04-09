import { Injectable } from '@nestjs/common';
import { OnEvent, EventEmitter2 } from '@nestjs/event-emitter';
import { QueryBuilderService } from '../../query-builder/query-builder.service';
import { RedisPubSubService } from './redis-pubsub.service';
import { InstanceService } from '../../../shared/services/instance.service';
import { BaseCacheService, CacheConfig } from './base-cache.service';
import { OAUTH_CONFIG_CACHE_SYNC_EVENT_KEY } from '../../../shared/utils/constant';
import {
  CACHE_EVENTS,
  CACHE_IDENTIFIERS,
  shouldReloadCache,
} from '../../../shared/utils/cache-events.constants';

const OAUTH_CONFIG: CacheConfig = {
  syncEventKey: OAUTH_CONFIG_CACHE_SYNC_EVENT_KEY,
  cacheIdentifier: CACHE_IDENTIFIERS.OAUTH_CONFIG,
  colorCode: '\x1b[33m',
  cacheName: 'OAuthConfigCache',
};

export interface OAuthConfig {
  id: number;
  provider: 'google' | 'facebook' | 'github';
  clientId: string;
  clientSecret: string;
  redirectUri: string;
  appCallbackUrl: string;
  isEnabled: boolean;
  description?: string;
}

@Injectable()
export class OAuthConfigCacheService extends BaseCacheService<
  Map<string, OAuthConfig>
> {
  constructor(
    private readonly queryBuilder: QueryBuilderService,
    redisPubSubService: RedisPubSubService,
    instanceService: InstanceService,
    eventEmitter: EventEmitter2,
  ) {
    super(OAUTH_CONFIG, redisPubSubService, instanceService, eventEmitter);
  }

  @OnEvent(CACHE_EVENTS.METADATA_LOADED)
  async onMetadataLoaded() {
    await this.reload(false);
    this.eventEmitter?.emit(CACHE_EVENTS.OAUTH_CONFIG_LOADED);
  }

  @OnEvent(CACHE_EVENTS.INVALIDATE)
  async handleCacheInvalidation(payload: {
    tableName: string;
    action: string;
  }) {
    if (shouldReloadCache(payload.tableName, this.config.cacheIdentifier)) {
      await this.reload();
    }
  }

  protected async loadFromDb(): Promise<OAuthConfig[]> {
    const result = await this.queryBuilder.select({
      tableName: 'oauth_config_definition',
      filter: { isEnabled: { _eq: true } },
    });
    if (!result.data || result.data.length === 0) {
      return [];
    }
    return result.data.map((config: any) => ({
      id: config.id,
      provider: config.provider,
      clientId: config.clientId,
      clientSecret: config.clientSecret,
      redirectUri: config.redirectUri,
      appCallbackUrl: config.appCallbackUrl,
      isEnabled: config.isEnabled !== false,
      description: config.description,
    }));
  }

  protected transformData(configs: OAuthConfig[]): Map<string, OAuthConfig> {
    const map = new Map<string, OAuthConfig>();
    for (const config of configs) {
      map.set(config.provider, config);
    }
    return map;
  }

  protected getLogCount(): string {
    return `${this.cache.size} OAuth configs`;
  }

  async getConfigByProvider(provider: string): Promise<OAuthConfig | null> {
    await this.ensureLoaded();
    return this.cache.get(provider) || null;
  }

  getDirectConfigByProvider(provider: string): OAuthConfig | null {
    return this.cache.get(provider) || null;
  }

  getAllProviders(): string[] {
    return Array.from(this.cache.keys());
  }
}
