import { Injectable, Logger, OnModuleInit, OnApplicationBootstrap } from '@nestjs/common';
import { QueryBuilderService } from '../../query-builder/query-builder.service';
import { RedisPubSubService } from './redis-pubsub.service';
import { CacheService } from './cache.service';
import { InstanceService } from '../../../shared/services/instance.service';
import {
  AI_CONFIG_CACHE_SYNC_EVENT_KEY,
  AI_CONFIG_RELOAD_LOCK_KEY,
  REDIS_TTL,
} from '../../../shared/utils/constant';

export interface AiConfig {
  id: number;
  provider: string;
  apiKey?: string;
  model: string;
  baseUrl?: string | null;
  isEnabled: boolean;
  maxConversationMessages: number;
  summaryThreshold: number;
  llmTimeout: number;
  maxToolIterations?: number;
  description?: string;
}

@Injectable()
export class AiConfigCacheService implements OnModuleInit, OnApplicationBootstrap {
  private readonly logger = new Logger(AiConfigCacheService.name);
  private configsCache: Map<number, AiConfig> = new Map();
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
      this.logger.log('AiConfigCacheService initialization completed');
    } catch (error) {
      this.logger.error('AiConfigCacheService initialization failed:', error);
      throw error;
    }
  }

  private subscribe() {
    const sub = this.redisPubSubService.sub;
    if (!sub) {
      this.logger.warn('Redis subscription not available for AI config cache sync');
      return;
    }

    if (this.messageHandler) {
      return;
    }

    this.messageHandler = async (channel: string, message: string) => {
      if (channel === AI_CONFIG_CACHE_SYNC_EVENT_KEY) {
        try {
          const payload = JSON.parse(message);
          const myInstanceId = this.instanceService.getInstanceId();

          if (payload.instanceId === myInstanceId) {
            return;
          }

          this.logger.log(`Received AI config cache sync from instance ${payload.instanceId.slice(0, 8)}...`);

          this.configsCache = new Map();
          for (const [id, config] of payload.configs) {
            this.configsCache.set(Number(id), config);
          }
          this.cacheLoaded = true;
          this.logger.log(`AI config cache synced: ${payload.configs.length} configs`);
        } catch (error) {
          this.logger.error('Failed to parse AI config cache sync message:', error);
        }
      }
    };

    this.redisPubSubService.subscribeWithHandler(
      AI_CONFIG_CACHE_SYNC_EVENT_KEY,
      this.messageHandler
    );
  }

  private async loadConfigsFromDb(): Promise<AiConfig[]> {
    const result = await this.queryBuilder.select({
      tableName: 'ai_config_definition',
    });

    if (!result.data || result.data.length === 0) {
      return [];
    }

    return result.data.map((config: any) => ({
      id: config.id,
      provider: config.provider,
      apiKey: config.apiKey,
      model: config.model,
      baseUrl: config.baseUrl,
      isEnabled: config.isEnabled !== false,
      maxConversationMessages: config.maxConversationMessages || 10,
      summaryThreshold: config.summaryThreshold || 20,
      llmTimeout: config.llmTimeout || 30000,
      description: config.description,
    }));
  }

  async reload(): Promise<void> {
    const instanceId = this.instanceService.getInstanceId();

    try {
      const acquired = await this.cacheService.acquire(
        AI_CONFIG_RELOAD_LOCK_KEY,
        instanceId,
        REDIS_TTL.RELOAD_LOCK_TTL
      );

      if (!acquired) {
        this.logger.log('Another instance is reloading AI config, waiting for broadcast...');
        return;
      }

      this.logger.log(`Acquired AI config reload lock (instance ${instanceId.slice(0, 8)})`);

      try {
        const start = Date.now();
        this.logger.log('Reloading AI config cache...');

        const configs = await this.loadConfigsFromDb();
        this.logger.log(`Loaded ${configs.length} AI configs in ${Date.now() - start}ms`);

        const configsMap = new Map<number, AiConfig>();
        for (const config of configs) {
          configsMap.set(config.id, config);
        }

        await this.publish(Array.from(configsMap.entries()));
        this.configsCache = configsMap;
        this.cacheLoaded = true;
      } finally {
        await this.cacheService.release(AI_CONFIG_RELOAD_LOCK_KEY, instanceId);
        this.logger.log('Released AI config reload lock');
      }
    } catch (error) {
      this.logger.error('Failed to reload AI config cache:', error);
      throw error;
    }
  }

  private async publish(configsArray: Array<[number, AiConfig]>): Promise<void> {
    try {
      const payload = {
        instanceId: this.instanceService.getInstanceId(),
        configs: configsArray,
        timestamp: Date.now(),
      };

      await this.redisPubSubService.publish(
        AI_CONFIG_CACHE_SYNC_EVENT_KEY,
        JSON.stringify(payload),
      );

      this.logger.log(`Published AI config cache to other instances (${configsArray.length} configs)`);
    } catch (error) {
      this.logger.error('Failed to publish AI config cache sync:', error);
    }
  }

  async getConfigById(id: string | number): Promise<AiConfig | null> {
    if (!this.cacheLoaded) {
      await this.reload();
    }
    // Convert string to number if needed for Map lookup
    const numericId = typeof id === 'string' ? parseInt(id, 10) : id;
    if (isNaN(numericId)) {
      return null;
    }
    return this.configsCache.get(numericId) || null;
  }

  getDirectConfigById(id: string | number): AiConfig | null {
    // Convert string to number if needed for Map lookup
    const numericId = typeof id === 'string' ? parseInt(id, 10) : id;
    if (isNaN(numericId)) {
      return null;
    }
    return this.configsCache.get(numericId) || null;
  }
}

