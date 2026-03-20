import { Logger } from '@nestjs/common';
import { EventEmitter2 } from '@nestjs/event-emitter';
import { RedisPubSubService } from './redis-pubsub.service';
import { InstanceService } from '../../../shared/services/instance.service';
import { CACHE_IDENTIFIERS } from '../../../shared/utils/cache-events.constants';

type CacheIdentifier = (typeof CACHE_IDENTIFIERS)[keyof typeof CACHE_IDENTIFIERS];

export interface CacheConfig {
  syncEventKey: string;
  cacheIdentifier: CacheIdentifier;
  colorCode: string;
  cacheName: string;
}

export abstract class BaseCacheService<T> {
  protected readonly logger: Logger;
  protected cache!: T;
  protected cacheLoaded = false;
  protected isLoading = false;
  protected loadingPromise: Promise<void> | null = null;
  private messageHandler: ((channel: string, message: string) => void) | null = null;

  constructor(
    protected readonly config: CacheConfig,
    protected readonly redisPubSubService: RedisPubSubService,
    protected readonly instanceService: InstanceService,
    protected readonly eventEmitter?: EventEmitter2,
  ) {
    this.logger = new Logger(`${config.colorCode}${config.cacheName}\x1b[0m`);
    this.setupSubscription();
  }

  private setupSubscription(): void {
    if (this.messageHandler) return;

    this.messageHandler = async (channel: string, message: string) => {
      if (channel === this.config.syncEventKey) {
        await this.handleIncomingMessage(message);
      }
    };

    this.redisPubSubService.subscribeWithHandler(
      this.config.syncEventKey,
      this.messageHandler
    );
  }

  private async handleIncomingMessage(message: string): Promise<void> {
    try {
      const payload = JSON.parse(message);
      const myInstanceId = this.instanceService.getInstanceId();

      if (payload.instanceId === myInstanceId) {
        return;
      }

      if (payload.type === 'RELOAD_SIGNAL') {
        this.logger.log(`Received reload signal from instance ${payload.instanceId.slice(0, 8)}..., reloading from DB`);
        await this.reload();
      }
    } catch (error) {
      this.logger.error('Failed to parse cache sync message:', error);
    }
  }

  async reload(): Promise<void> {
    if (this.isLoading && this.loadingPromise) {
      return this.loadingPromise;
    }

    this.isLoading = true;
    this.loadingPromise = (async () => {
      try {
        await this.publishReloadSignal();

        await this.beforeLoad();

        const rawData = await this.loadFromDb();

        this.cache = this.transformData(rawData);

        await this.afterTransform(this.cache);

        this.cacheLoaded = true;

        this.emitLoadedEvent();

        this.logger.log(`Loaded ${this.getLogCount()} from database`);
      } catch (error) {
        this.logger.error('Failed to reload cache:', error);
        throw error;
      } finally {
        this.isLoading = false;
        this.loadingPromise = null;
      }
    })();

    return this.loadingPromise;
  }

  private async publishReloadSignal(): Promise<void> {
    try {
      const payload = {
        instanceId: this.instanceService.getInstanceId(),
        type: 'RELOAD_SIGNAL',
        timestamp: Date.now(),
      };
      await this.redisPubSubService.publish(
        this.config.syncEventKey,
        JSON.stringify(payload)
      );
    } catch (error) {
      this.logger.error('Failed to publish reload signal:', error);
    }
  }

  protected abstract loadFromDb(): Promise<any>;

  protected abstract transformData(rawData: any): T;

  protected abstract handleSyncData(data: any): void;

  protected async beforeLoad(): Promise<void> {}

  protected async afterTransform(data: T): Promise<void> {}

  protected deserializeSyncData(payload: any): any {
    return payload.data ?? payload;
  }

  protected serializeForPublish(data: T): Record<string, any> {
    return { data };
  }

  protected emitLoadedEvent(): void {
    if (this.eventEmitter) {
      this.eventEmitter.emit(`${this.config.cacheIdentifier}_LOADED`);
    }
  }

  protected getLogCount(): string {
    return `${this.getCount()} items`;
  }

  protected getCount(): number {
    if (this.cache instanceof Map) {
      return this.cache.size;
    }
    if (Array.isArray(this.cache)) {
      return this.cache.length;
    }
    return 1;
  }

  protected logSyncSuccess(payload: any): void {
    this.logger.log(`Cache synced: ${this.getLogCount()}`);
  }

  isLoaded(): boolean {
    return this.cacheLoaded;
  }

  protected async ensureLoaded(): Promise<void> {
    if (!this.cacheLoaded) {
      await this.reload();
    }
  }

  getRawCache(): T {
    return this.cache;
  }

  get cacheIdentifier(): CacheIdentifier {
    return this.config.cacheIdentifier;
  }
}
