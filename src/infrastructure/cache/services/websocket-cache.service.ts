import { Injectable } from '@nestjs/common';
import { OnEvent, EventEmitter2 } from '@nestjs/event-emitter';
import { QueryBuilderService } from '../../query-builder/query-builder.service';
import { RedisPubSubService } from './redis-pubsub.service';
import { InstanceService } from '../../../shared/services/instance.service';
import { BaseCacheService, CacheConfig } from './base-cache.service';
import { transformCode } from '../../executor-engine/code-transformer';
import { WEBSOCKET_CACHE_SYNC_EVENT_KEY } from '../../../shared/utils/constant';
import { CACHE_EVENTS, CACHE_IDENTIFIERS, shouldReloadCache } from '../../../shared/utils/cache-events.constants';

const WEBSOCKET_CONFIG: CacheConfig = {
  syncEventKey: WEBSOCKET_CACHE_SYNC_EVENT_KEY,
  cacheIdentifier: CACHE_IDENTIFIERS.WEBSOCKET,
  colorCode: '\x1b[32m',
  cacheName: 'WebsocketCache',
};

interface WebSocketEvent {
  id: number | string;
  eventName: string;
  isEnabled: boolean;
  isSystem: boolean;
  description: string | null;
  handlerScript: string | null;
  timeout: number;
  gatewayId: number | string;
}

interface WebSocketGateway {
  id: number | string;
  path: string;
  isEnabled: boolean;
  isSystem: boolean;
  description: string | null;
  requireAuth: boolean;
  connectionHandlerScript: string | null;
  connectionHandlerTimeout: number;
  events: WebSocketEvent[];
}

@Injectable()
export class WebsocketCacheService extends BaseCacheService<WebSocketGateway[]> {
  constructor(
    private readonly queryBuilder: QueryBuilderService,
    redisPubSubService: RedisPubSubService,
    instanceService: InstanceService,
    eventEmitter: EventEmitter2,
  ) {
    super(WEBSOCKET_CONFIG, redisPubSubService, instanceService, eventEmitter);
  }

  @OnEvent(CACHE_EVENTS.METADATA_LOADED)
  async onMetadataLoaded() {
    await this.reload();
  }

  @OnEvent(CACHE_EVENTS.INVALIDATE)
  async handleCacheInvalidation(payload: { tableName: string; action: string }) {
    if (shouldReloadCache(payload.tableName, this.config.cacheIdentifier)) {
      await this.reload();
    }
  }

  protected async loadFromDb(): Promise<WebSocketGateway[]> {
    const result = await this.queryBuilder.select({
      tableName: 'websocket_definition',
      filter: { isEnabled: { _eq: true } },
      fields: ['*', 'events.*'],
    });

    return result.data.map((gateway: any) => {
      if (gateway.connectionHandlerScript) {
        gateway.connectionHandlerScript = transformCode(gateway.connectionHandlerScript);
      }

      gateway.events = (gateway.events || []).filter((e: any) => e.isEnabled);
      for (const event of gateway.events) {
        if (event.handlerScript) {
          event.handlerScript = transformCode(event.handlerScript);
        }
      }

      return gateway;
    });
  }

  protected transformData(gateways: WebSocketGateway[]): WebSocketGateway[] {
    return gateways;
  }

  protected handleSyncData(data: WebSocketGateway[]): void {
    this.cache = data;
  }

  protected deserializeSyncData(payload: any): any {
    return payload.gateways;
  }

  protected serializeForPublish(gateways: WebSocketGateway[]): Record<string, any> {
    return { gateways };
  }

  protected getLogCount(): string {
    return `${this.cache.length} websocket gateways`;
  }

  protected logSyncSuccess(payload: any): void {
    this.logger.log(`Cache synced: ${payload.gateways?.length || 0} gateways`);
  }

  protected emitLoadedEvent(): void {
    this.eventEmitter.emit(CACHE_EVENTS.WEBSOCKET_LOADED);
  }

  async getGateways(): Promise<WebSocketGateway[]> {
    await this.ensureLoaded();
    return this.cache;
  }

  async getGatewayByPath(path: string): Promise<WebSocketGateway | null> {
    const gateways = await this.getGateways();
    return gateways.find(g => g.path === path) || null;
  }

  async getEventsByGatewayId(gatewayId: number | string): Promise<WebSocketEvent[]> {
    const gateway = await this.getGatewayByPath(gatewayId as string);
    if (!gateway) return [];
    return gateway.events || [];
  }
}
