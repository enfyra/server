import { EventEmitter2 } from 'eventemitter2';
import { QueryBuilderService } from '@enfyra/kernel';
import { BaseCacheService, CacheConfig } from './base-cache.service';
import { RedisRuntimeCacheStore } from './redis-runtime-cache-store.service';
import {
  compileScriptSource,
  normalizeScriptRecord,
  resolveExecutableScript,
} from '../../../shared/utils/script-code.util';
import {
  CACHE_IDENTIFIERS,
  type TCacheInvalidationPayload,
} from '../../../shared/utils/cache-events.constants';
import { DatabaseConfigService } from '../../../shared/services';

const WEBSOCKET_CONFIG: CacheConfig = {
  cacheIdentifier: CACHE_IDENTIFIERS.WEBSOCKET,
  colorCode: '\x1b[34m',
  cacheName: 'WebsocketCache',
};

export interface WebSocketEvent {
  id: number;
  name: string;
  eventName?: string;
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

export class WebsocketCacheBuilder extends BaseCacheService<
  WebSocketGateway[]
> {
  private readonly queryBuilderService: QueryBuilderService;

  constructor(deps: {
    queryBuilderService: QueryBuilderService;
    eventEmitter?: EventEmitter2;
    redisRuntimeCacheStore?: RedisRuntimeCacheStore;
  }) {
    super(WEBSOCKET_CONFIG, deps.eventEmitter, deps.redisRuntimeCacheStore);
    this.queryBuilderService = deps.queryBuilderService;
    this.cache = [];
  }

  protected async loadFromDb(): Promise<WebSocketGateway[]> {
    const result = await this.queryBuilderService.find({
      table: 'enfyra_websocket',
      fields: ['*'],
      filter: { isEnabled: { _eq: true } },
    });

    const gateways = result.data || [];
    await this.attachEvents(gateways);

    await this.prepareGateways(gateways);

    return gateways;
  }

  private resolveScriptCode(record: any): string | null {
    if (typeof record?.sourceCode === 'string' && record.sourceCode !== '') {
      const compiledCode = compileScriptSource(
        record.sourceCode,
        record.scriptLanguage,
      );
      record.compiledCode = compiledCode;
      return compiledCode;
    }

    const result = resolveExecutableScript(record);
    if (result.shouldPersistCompiledCode) {
      record.compiledCode = result.compiledCode;
    }
    return result.code;
  }

  protected transformData(gateways: WebSocketGateway[]): WebSocketGateway[] {
    return gateways;
  }

  supportsPartialReload(): boolean {
    return true;
  }

  async partialReload(
    payload: TCacheInvalidationPayload,
    publish = true,
  ): Promise<void> {
    if (
      payload.table !== 'enfyra_websocket' &&
      payload.table !== 'enfyra_websocket_event'
    ) {
      await this.reload(publish);
      return;
    }
    await super.partialReload(payload, publish);
  }

  protected async applyPartialUpdate(
    payload: TCacheInvalidationPayload,
  ): Promise<void> {
    const ids = payload.ids ?? [];
    if (ids.length === 0) return;

    if (payload.table === 'enfyra_websocket') {
      await this.reloadSpecificGateways(ids);
      return;
    }

    const gatewayIds = await this.resolveGatewayIdsForEvents(ids);
    if (gatewayIds.length === 0) return;
    await this.reloadSpecificGateways(gatewayIds);
  }

  private async reloadSpecificGateways(
    gatewayIds: (string | number)[],
  ): Promise<void> {
    const idField = this.queryBuilderService.isMongoDb() ? '_id' : 'id';
    const result = await this.queryBuilderService.find({
      table: 'enfyra_websocket',
      fields: ['*'],
      filter: {
        _and: [
          { isEnabled: { _eq: true } },
          { [idField]: { _in: gatewayIds } },
        ],
      },
      limit: gatewayIds.length,
    });

    const updatedGateways = result?.data ?? [];
    await this.attachEvents(updatedGateways);
    await this.prepareGateways(updatedGateways);

    const idSet = new Set(gatewayIds.map(String));
    this.cache = this.cache.filter((gateway) => {
      const id = DatabaseConfigService.getRecordId(gateway);
      return id == null || !idSet.has(String(id));
    });
    this.cache.push(...updatedGateways);
  }

  private async resolveGatewayIdsForEvents(
    eventIds: (string | number)[],
  ): Promise<(string | number)[]> {
    const idField = this.queryBuilderService.isMongoDb() ? '_id' : 'id';
    const result = await this.queryBuilderService.find({
      table: 'enfyra_websocket_event',
      fields: ['gateway.id', 'gateway._id'],
      filter: { [idField]: { _in: eventIds } },
      limit: eventIds.length,
    });

    const gatewayIds = new Map<string, string | number>();
    for (const row of result?.data ?? []) {
      const gatewayId = row?.gateway?._id ?? row?.gateway?.id ?? row?.gatewayId;
      if (gatewayId != null) gatewayIds.set(String(gatewayId), gatewayId);
    }

    const eventIdSet = new Set(eventIds.map(String));
    for (const gateway of this.cache) {
      const gatewayId = DatabaseConfigService.getRecordId(gateway);
      if (gatewayId == null) continue;
      for (const event of gateway.events ?? []) {
        const eventId = DatabaseConfigService.getRecordId(event);
        if (eventId != null && eventIdSet.has(String(eventId))) {
          gatewayIds.set(String(gatewayId), gatewayId);
          break;
        }
      }
    }

    return [...gatewayIds.values()];
  }

  private async attachEvents(gateways: any[]): Promise<void> {
    if (!gateways.length) return;

    const gatewayIds = gateways
      .map((gateway) => DatabaseConfigService.getRecordId(gateway))
      .filter((id) => id !== undefined && id !== null);
    if (!gatewayIds.length) return;

    const gatewayById = new Map<string, any>();
    for (const gateway of gateways) {
      gateway.events = [];
      const id = DatabaseConfigService.getRecordId(gateway);
      if (id !== undefined && id !== null) gatewayById.set(String(id), gateway);
    }

    const eventResult = await this.queryBuilderService.find({
      table: 'enfyra_websocket_event',
      fields: ['*', 'gateway.id', 'gateway._id'],
      filter: {
        _and: [
          { isEnabled: { _eq: true } },
          {
            gateway: {
              [DatabaseConfigService.getPkField()]: { _in: gatewayIds },
            },
          },
        ],
      },
      limit: Math.max(gatewayIds.length * 100, 100),
    });

    for (const event of eventResult?.data ?? []) {
      const gatewayId =
        event?.gateway?._id ?? event?.gateway?.id ?? event?.gatewayId;
      const gateway = gatewayById.get(String(gatewayId));
      if (gateway) gateway.events.push(event);
    }
  }

  private async prepareGateways(gateways: any[]): Promise<void> {
    for (const gateway of gateways) {
      const normalizedGateway = normalizeScriptRecord(
        'enfyra_websocket',
        gateway,
      );
      Object.assign(gateway, normalizedGateway);
      const connectionCode = this.resolveScriptCode(gateway);
      if (connectionCode) {
        gateway.connectionHandlerScript = connectionCode;
      }
      if (gateway.events) {
        for (const event of gateway.events) {
          const normalizedEvent = normalizeScriptRecord(
            'enfyra_websocket_event',
            event,
          );
          Object.assign(event, normalizedEvent);
          const code = this.resolveScriptCode(event);
          if (code) {
            event.handlerScript = code;
          }
        }
      }
    }
  }

  protected getLogCount(): string {
    return `${this.cache.length} websocket gateways`;
  }

  protected emitLoadedEvent(): void {
    if (this.eventEmitter) {
      this.eventEmitter.emit(`${this.config.cacheIdentifier}_LOADED`);
    }
  }
}
