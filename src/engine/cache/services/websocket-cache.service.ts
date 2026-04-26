import { EventEmitter2 } from 'eventemitter2';
import { QueryBuilderService } from '../../../kernel/query';
import { BaseCacheService, CacheConfig } from './base-cache.service';
import { DatabaseConfigService } from '../../../shared/services';
import {
  normalizeScriptRecord,
  resolveExecutableScript,
} from '../../../kernel/execution';
import { CACHE_IDENTIFIERS } from '../../../shared/utils/cache-events.constants';

const WEBSOCKET_CONFIG: CacheConfig = {
  cacheIdentifier: CACHE_IDENTIFIERS.WEBSOCKET,
  colorCode: '\x1b[34m',
  cacheName: 'WebsocketCache',
};

export interface WebSocketEvent {
  id: number;
  name: string;
  sourceCode?: string | null;
  compiledCode?: string | null;
  handlerScript?: string | null;
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
      const normalizedGateway = normalizeScriptRecord(
        'websocket_definition',
        gateway,
      );
      Object.assign(gateway, normalizedGateway);
      const connectionCode = await this.resolveAndRepairScript(
        'websocket_definition',
        gateway,
      );
      if (connectionCode) {
        gateway.connectionHandlerScript = connectionCode;
      }
      if (gateway.events) {
        for (const event of gateway.events) {
          const normalizedEvent = normalizeScriptRecord(
            'websocket_event_definition',
            event,
          );
          Object.assign(event, normalizedEvent);
          const code = await this.resolveAndRepairScript(
            'websocket_event_definition',
            event,
          );
          if (code) {
            event.handlerScript = code;
          }
        }
      }
    }

    return gateways;
  }

  private async resolveAndRepairScript(
    tableName: string,
    record: any,
  ): Promise<string | null> {
    const result = resolveExecutableScript(record);
    if (result.shouldPersistCompiledCode) {
      record.compiledCode = result.compiledCode;
      const id = DatabaseConfigService.getRecordId(record);
      if (id != null) {
        await this.queryBuilderService.update(tableName, id, {
          compiledCode: result.compiledCode,
        });
      }
    }
    return result.code;
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
