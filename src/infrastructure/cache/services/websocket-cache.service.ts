import { Injectable } from '@nestjs/common';
import { EventEmitter2 } from '@nestjs/event-emitter';
import { QueryBuilderService } from '../../query-builder/query-builder.service';
import { BaseCacheService, CacheConfig } from './base-cache.service';
import { transformCode } from '../../executor-engine/code-transformer';
import {
  CACHE_EVENTS,
  CACHE_IDENTIFIERS,
} from '../../../shared/utils/cache-events.constants';

const WEBSOCKET_CONFIG: CacheConfig = {
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
export class WebsocketCacheService extends BaseCacheService<
  WebSocketGateway[]
> {
  constructor(
    private readonly queryBuilder: QueryBuilderService,
    eventEmitter: EventEmitter2,
  ) {
    super(WEBSOCKET_CONFIG, eventEmitter);
  }

  protected async loadFromDb(): Promise<WebSocketGateway[]> {
    const result = await this.queryBuilder.select({
      tableName: 'websocket_definition',
      filter: { isEnabled: { _eq: true } },
      fields: ['*', 'events.*'],
    });

    return result.data.map((gateway: any) => {
      if (gateway.connectionHandlerScript) {
        gateway.connectionHandlerScript = transformCode(
          gateway.connectionHandlerScript,
        );
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

  protected getLogCount(): string {
    return `${this.cache.length} websocket gateways`;
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
    return gateways.find((g) => g.path === path) || null;
  }

  async getEventsByGatewayId(
    gatewayId: number | string,
  ): Promise<WebSocketEvent[]> {
    const gateways = await this.getGateways();
    const gateway = gateways.find(
      (g) =>
        g.path === String(gatewayId) ||
        String(g.id) === String(gatewayId) ||
        String((g as any)._id) === String(gatewayId),
    );
    if (!gateway) return [];
    return gateway.events || [];
  }
}
