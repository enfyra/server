import { Injectable, Logger, OnModuleInit, OnApplicationBootstrap } from '@nestjs/common';
import { OnEvent } from '@nestjs/event-emitter';
import { QueryBuilderService } from '../../query-builder/query-builder.service';
import { RedisPubSubService } from './redis-pubsub.service';
import { CacheService } from './cache.service';
import { InstanceService } from '../../../shared/services/instance.service';
import { CACHE_EVENTS, CACHE_IDENTIFIERS, shouldReloadCache } from '../../../shared/utils/cache-events.constants';

const OAUTH_CONFIG_CACHE_SYNC_EVENT_KEY = 'cache:oauth-config:sync';
const OAUTH_CONFIG_RELOAD_LOCK_KEY = 'lock:oauth-config:reload';
const RELOAD_LOCK_TTL = 10000;

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
export class OAuthConfigCacheService implements OnModuleInit, OnApplicationBootstrap {
  private readonly logger = new Logger(OAuthConfigCacheService.name);
  private configsCache: Map<string, OAuthConfig> = new Map();
  private cacheLoaded = false;
  private messageHandler: ((channel: string, message: string) => void) | null = null;

  constructor(
    private readonly queryBuilder: QueryBuilderService,
    private readonly redisPubSubService: RedisPubSubService,
    private readonly cacheService: CacheService,
    private readonly instanceService: InstanceService,
  ) {}

  async onModuleInit() {
    this.subscribe();
  }

  async onApplicationBootstrap() {
    try {
      await this.reload();
      this.logger.log('OAuthConfigCacheService initialization completed');
    } catch (error) {
      this.logger.error('OAuthConfigCacheService initialization failed:', error);
      throw error;
    }
  }

  private subscribe() {
    const sub = this.redisPubSubService.sub;
    if (!sub) {
      this.logger.warn('Redis subscription not available for OAuth config cache sync');
      return;
    }
    if (this.messageHandler) {
      return;
    }
    this.messageHandler = async (channel: string, message: string) => {
      if (channel === OAUTH_CONFIG_CACHE_SYNC_EVENT_KEY) {
        try {
          const payload = JSON.parse(message);
          const myInstanceId = this.instanceService.getInstanceId();
          if (payload.instanceId === myInstanceId) {
            return;
          }
          this.logger.log(`Received OAuth config cache sync from instance ${payload.instanceId.slice(0, 8)}...`);
          this.configsCache = new Map();
          for (const [provider, config] of payload.configs) {
            this.configsCache.set(provider, config);
          }
          this.cacheLoaded = true;
          this.logger.log(`OAuth config cache synced: ${payload.configs.length} configs`);
        } catch (error) {
          this.logger.error('Failed to parse OAuth config cache sync message:', error);
        }
      }
    };
    this.redisPubSubService.subscribeWithHandler(
      OAUTH_CONFIG_CACHE_SYNC_EVENT_KEY,
      this.messageHandler
    );
  }

  @OnEvent(CACHE_EVENTS.INVALIDATE)
  async handleCacheInvalidation(payload: { tableName: string; action: string }) {
    if (shouldReloadCache(payload.tableName, CACHE_IDENTIFIERS.OAUTH_CONFIG)) {
      this.logger.log(`Cache invalidation event received for table: ${payload.tableName}`);
      await this.reload();
    }
  }

  private async loadConfigsFromDb(): Promise<OAuthConfig[]> {
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

  async reload(): Promise<void> {
    const instanceId = this.instanceService.getInstanceId();
    try {
      const acquired = await this.cacheService.acquire(
        OAUTH_CONFIG_RELOAD_LOCK_KEY,
        instanceId,
        RELOAD_LOCK_TTL
      );
      if (!acquired) {
        this.logger.log('Another instance is reloading OAuth config, waiting for broadcast...');
        return;
      }
      this.logger.log(`Acquired OAuth config reload lock (instance ${instanceId.slice(0, 8)})`);
      try {
        const start = Date.now();
        this.logger.log('Reloading OAuth config cache...');
        const configs = await this.loadConfigsFromDb();
        this.logger.log(`Loaded ${configs.length} OAuth configs in ${Date.now() - start}ms`);
        const configsMap = new Map<string, OAuthConfig>();
        for (const config of configs) {
          configsMap.set(config.provider, config);
        }
        await this.publish(Array.from(configsMap.entries()));
        this.configsCache = configsMap;
        this.cacheLoaded = true;
      } finally {
        await this.cacheService.release(OAUTH_CONFIG_RELOAD_LOCK_KEY, instanceId);
        this.logger.log('Released OAuth config reload lock');
      }
    } catch (error) {
      this.logger.error('Failed to reload OAuth config cache:', error);
      throw error;
    }
  }

  private async publish(configsArray: Array<[string, OAuthConfig]>): Promise<void> {
    try {
      const payload = {
        instanceId: this.instanceService.getInstanceId(),
        configs: configsArray,
        timestamp: Date.now(),
      };
      await this.redisPubSubService.publish(
        OAUTH_CONFIG_CACHE_SYNC_EVENT_KEY,
        JSON.stringify(payload)
      );
      this.logger.log(`Published OAuth config cache to other instances (${configsArray.length} configs)`);
    } catch (error) {
      this.logger.error('Failed to publish OAuth config cache sync:', error);
    }
  }

  async getConfigByProvider(provider: string): Promise<OAuthConfig | null> {
    if (!this.cacheLoaded) {
      await this.reload();
    }
    return this.configsCache.get(provider) || null;
  }

  getDirectConfigByProvider(provider: string): OAuthConfig | null {
    return this.configsCache.get(provider) || null;
  }

  getAllProviders(): string[] {
    return Array.from(this.configsCache.keys());
  }
}
