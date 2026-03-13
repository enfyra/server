import { Injectable, Logger, OnModuleInit, OnApplicationBootstrap } from '@nestjs/common';
import { OnEvent } from '@nestjs/event-emitter';
import { QueryBuilderService } from '../../query-builder/query-builder.service';
import { RedisPubSubService } from './redis-pubsub.service';
import { CacheService } from './cache.service';
import { MetadataCacheService } from './metadata-cache.service';
import { InstanceService } from '../../../shared/services/instance.service';
import { transformCode } from '../../handler-executor/code-transformer';
import {
  WEBSOCKET_CACHE_SYNC_EVENT_KEY,
  WEBSOCKET_RELOAD_LOCK_KEY,
  REDIS_TTL,
} from '../../../shared/utils/constant';
import { CACHE_EVENTS, CACHE_IDENTIFIERS, shouldReloadCache } from '../../../shared/utils/cache-events.constants';

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
export class WebsocketCacheService implements OnModuleInit, OnApplicationBootstrap {
  private readonly logger = new Logger(WebsocketCacheService.name);
  private gatewaysCache: WebSocketGateway[] = [];
  private cacheLoaded = false;
  private messageHandler: ((channel: string, message: string) => void) | null = null;

  constructor(
    private readonly queryBuilder: QueryBuilderService,
    private readonly redisPubSubService: RedisPubSubService,
    private readonly cacheService: CacheService,
    private readonly instanceService: InstanceService,
    private readonly metadataCacheService: MetadataCacheService,
  ) {}

  async onModuleInit() {
    this.subscribe();
  }

  async onApplicationBootstrap() {
    await this.reload();
  }

  private subscribe() {
    if (this.messageHandler) {
      return;
    }

    this.messageHandler = (channel: string, message: string) => {
      const isWebsocketChannel = channel === WEBSOCKET_CACHE_SYNC_EVENT_KEY || channel.startsWith(WEBSOCKET_CACHE_SYNC_EVENT_KEY + ':');
      if (isWebsocketChannel) {
        try {
          const payload = JSON.parse(message);
          const myInstanceId = this.instanceService.getInstanceId();

          if (payload.instanceId === myInstanceId) {
            return;
          }

          this.logger.log(`Received websocket cache sync from instance ${payload.instanceId.slice(0, 8)}...`);

          this.gatewaysCache = payload.gateways;
          this.cacheLoaded = true;
          this.logger.log(`WebSocket cache synced: ${payload.gateways.length} gateways`);
        } catch (error) {
          this.logger.error('Failed to parse websocket cache sync message:', error);
        }
      }
    };

    this.redisPubSubService.subscribeWithHandler(
      WEBSOCKET_CACHE_SYNC_EVENT_KEY,
      this.messageHandler
    );
  }

  @OnEvent(CACHE_EVENTS.INVALIDATE)
  async handleCacheInvalidation(payload: { tableName: string; action: string }) {
    const shouldReload = shouldReloadCache(payload.tableName, CACHE_IDENTIFIERS.WEBSOCKET);

    if (shouldReload) {
      this.logger.log(`Cache invalidation event received for table: ${payload.tableName}`);
      await this.reload();
    }
  }

  async getGateways(): Promise<WebSocketGateway[]> {
    if (!this.cacheLoaded) {
      await this.reload();
    }
    return this.gatewaysCache;
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

  async reload(): Promise<void> {
    const instanceId = this.instanceService.getInstanceId();

    try {
      const acquired = await this.cacheService.acquire(
        WEBSOCKET_RELOAD_LOCK_KEY,
        instanceId,
        REDIS_TTL.RELOAD_LOCK_TTL
      );

      if (!acquired) {
        this.logger.log('Another instance is reloading websockets, waiting for broadcast...');
        return;
      }

      this.logger.log(`Acquired websocket reload lock (instance ${instanceId.slice(0, 8)})`);

      try {
        const start = Date.now();
        this.logger.log('Reloading websocket cache...');

        this.logger.log('Waiting for metadata cache to be loaded...');
        const metadataLoaded = await this.metadataCacheService.waitForLoad();
        if (!metadataLoaded) {
          this.logger.error('Metadata cache not loaded, cannot reload websocket cache');
          return;
        }
        this.logger.log('Metadata cache is ready, proceeding with websocket cache reload');

        const gateways = await this.loadGateways();
        this.logger.log(`Loaded ${gateways.length} websocket gateways in ${Date.now() - start}ms`);

        this.gatewaysCache = gateways;
        await this.publish(gateways);

        this.cacheLoaded = true;
      } finally {
        await this.cacheService.release(WEBSOCKET_RELOAD_LOCK_KEY, instanceId);
        this.logger.log('Released websocket reload lock');
      }
    } catch (error) {
      this.logger.error('Failed to reload websocket cache:', error);
      throw error;
    }
  }

  private async publish(gateways: WebSocketGateway[]): Promise<void> {
    try {
      const payload = {
        instanceId: this.instanceService.getInstanceId(),
        gateways: gateways,
        timestamp: Date.now(),
      };

      await this.redisPubSubService.publish(
        WEBSOCKET_CACHE_SYNC_EVENT_KEY,
        JSON.stringify(payload),
      );

      this.logger.log(`Published websocket cache to other instances (${gateways.length} gateways)`);
    } catch (error) {
      this.logger.error('Failed to publish websocket cache sync:', error);
    }
  }

  private async loadGateways(): Promise<WebSocketGateway[]> {
    const isMongoDB = this.queryBuilder.isMongoDb();

    const result = await this.queryBuilder.select({
      tableName: 'websocket_definition',
      filter: { isEnabled: { _eq: true } },
      fields: ['*'],
    });
    const gateways = result.data;

    for (const gateway of gateways) {
      if (gateway.connectionHandlerScript) {
        gateway.connectionHandlerScript = transformCode(gateway.connectionHandlerScript);
      }

      const filterValue = isMongoDB ? gateway._id : gateway.id;
      this.logger.debug(`Loading events for gateway ${gateway.path || gateway.id}, filter value: ${filterValue}`);

      const eventsResult = await this.queryBuilder.select({
        tableName: 'websocket_event_definition',
        filter: {
          _and: [
            { isEnabled: { _eq: true } },
            { gateway: { _eq: filterValue } },
          ],
        },
        fields: ['*'],
      });

      this.logger.debug(`Events result for gateway ${gateway.path || gateway.id}: ${eventsResult.data?.length || 0} events`);

      for (const event of eventsResult.data) {
        if (event.handlerScript) {
          event.handlerScript = transformCode(event.handlerScript);
        }
      }

      gateway.events = eventsResult.data;
    }

    return gateways;
  }
}
