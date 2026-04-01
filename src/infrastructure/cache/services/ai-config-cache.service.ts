import { Injectable } from '@nestjs/common';
import { OnEvent, EventEmitter2 } from '@nestjs/event-emitter';
import { QueryBuilderService } from '../../query-builder/query-builder.service';
import { RedisPubSubService } from './redis-pubsub.service';
import { InstanceService } from '../../../shared/services/instance.service';
import { BaseCacheService, CacheConfig } from './base-cache.service';
import { AI_CONFIG_CACHE_SYNC_EVENT_KEY } from '../../../shared/utils/constant';
import { CACHE_EVENTS, CACHE_IDENTIFIERS, shouldReloadCache } from '../../../shared/utils/cache-events.constants';

const AI_CONFIG: CacheConfig = {
  syncEventKey: AI_CONFIG_CACHE_SYNC_EVENT_KEY,
  cacheIdentifier: CACHE_IDENTIFIERS.AI_CONFIG,
  colorCode: '\x1b[34m',
  cacheName: 'AiConfigCache',
};

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
export class AiConfigCacheService extends BaseCacheService<Map<number, AiConfig>> {
  constructor(
    private readonly queryBuilder: QueryBuilderService,
    redisPubSubService: RedisPubSubService,
    instanceService: InstanceService,
    eventEmitter: EventEmitter2,
  ) {
    super(AI_CONFIG, redisPubSubService, instanceService, eventEmitter);
  }

  @OnEvent(CACHE_EVENTS.METADATA_LOADED)
  async onMetadataLoaded() {
    await this.reload();
    this.eventEmitter?.emit(CACHE_EVENTS.AI_CONFIG_LOADED);
  }

  @OnEvent(CACHE_EVENTS.INVALIDATE)
  async handleCacheInvalidation(payload: { tableName: string; action: string }) {
    if (shouldReloadCache(payload.tableName, this.config.cacheIdentifier)) {
      this.logger.log(`Cache invalidation event received for table: ${payload.tableName}`);
      await this.reload();
    }
  }

  protected async loadFromDb(): Promise<AiConfig[]> {
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
      maxToolIterations: config.maxToolIterations ?? 10,
      description: config.description,
    }));
  }

  protected transformData(configs: AiConfig[]): Map<number, AiConfig> {
    const map = new Map<number, AiConfig>();
    for (const config of configs) {
      map.set(config.id, config);
    }
    return map;
  }

  protected handleSyncData(data: Array<[number, AiConfig]>): void {
    this.cache = new Map(data);
  }

  protected deserializeSyncData(payload: any): any {
    return payload.configs;
  }

  protected serializeForPublish(cache: Map<number, AiConfig>): Record<string, any> {
    return { configs: Array.from(cache.entries()) };
  }

  protected getLogCount(): string {
    return `${this.cache.size} AI configs`;
  }

  protected logSyncSuccess(payload: any): void {
    this.logger.log(`Cache synced: ${payload.configs?.length || 0} configs`);
  }

  async getConfigById(id: string | number): Promise<AiConfig | null> {
    await this.ensureLoaded();
    const numericId = typeof id === 'string' ? parseInt(id, 10) : id;
    if (isNaN(numericId)) {
      return null;
    }
    return this.cache.get(numericId) || null;
  }

  getDirectConfigById(id: string | number): AiConfig | null {
    const numericId = typeof id === 'string' ? parseInt(id, 10) : id;
    if (isNaN(numericId)) {
      return null;
    }
    return this.cache.get(numericId) || null;
  }

}
