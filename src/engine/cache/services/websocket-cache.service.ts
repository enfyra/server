import { EventEmitter2 } from 'eventemitter2';
import { QueryBuilderService } from '../../query-builder/query-builder.service';
import { BaseCacheService, CacheConfig } from './base-cache.service';
import { transformCode } from '../../../domain/shared/code-transformer';
import { CACHE_IDENTIFIERS } from '../../../shared/utils/cache-events.constants';

const WEBSOCKET_CONFIG: CacheConfig = {
  cacheIdentifier: CACHE_IDENTIFIERS.WEBSOCKET,
  colorCode: '\x1b[34m',
  cacheName: 'WebsocketCache',
};

export interface WebSocketEvent {
  id: number;
  name: string;
  logic: string;
  gatewayId: number;
  isEnabled: boolean;
}

export interface WebSocketGateway {
  id: number;
  path: string;
  isEnabled: boolean;
  events: WebSocketEvent[];
}

export class WebsocketCacheService extends BaseCacheService<
  WebSocketGateway[]
> {
  private readonly queryBuilderService: QueryBuilderService;

  constructor(deps: {
    queryBuilderService: QueryBuilderService;
    eventEmitter?: EventEmitter2;
  }) {
    super(WEBSOCKET_CONFIG, deps.eventEmitter);
    this.queryBuilderService = deps.queryBuilderService;
  }

  protected async loadFromDb(): Promise<WebSocketGateway[]> {
    const result = await this.queryBuilderService.find({
      table: 'websocket_definition',
      fields: ['*', 'events.*'],
      filter: { isEnabled: { _eq: true } },
    });

    const gateways = result.data || [];

    for (const gateway of gateways) {
      if (gateway.events) {
        for (const event of gateway.events) {
          if (event.logic && typeof event.logic === 'string') {
            event.logic = transformCode(event.logic);
          }
        }
      }
    }

    return gateways;
  }

  protected transformData(gateways: WebSocketGateway[]): WebSocketGateway[] {
    return gateways;
  }

  protected getLogCount(): string {
    return `${this.cache.length} websocket gateways`;
  }

  protected emitLoadedEvent(): void {
    if (this.eventEmitter) {
      this.eventEmitter.emit(`${this.config.cacheIdentifier}_LOADED`);
    }
  }

  async getGateways(): Promise<WebSocketGateway[]> {
    await this.ensureLoaded();
    return this.cache;
  }

  async getGatewayByPath(path: string): Promise<WebSocketGateway | null> {
    await this.ensureLoaded();
    return this.cache.find((g) => g.path === path) || null;
  }

  async getEventsByGatewayId(
    gatewayId: number | string,
  ): Promise<WebSocketEvent[]> {
    await this.ensureLoaded();
    const gateway = this.cache.find((g) => g.id === gatewayId);
    return gateway?.events || [];
  }
}
