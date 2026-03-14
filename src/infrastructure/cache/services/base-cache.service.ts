import { Logger } from '@nestjs/common';
import { EventEmitter2 } from '@nestjs/event-emitter';
import { RedisPubSubService } from './redis-pubsub.service';
import { CacheService } from './cache.service';
import { InstanceService } from '../../../shared/services/instance.service';
import { REDIS_TTL } from '../../../shared/utils/constant';
import { CACHE_IDENTIFIERS } from '../../../shared/utils/cache-events.constants';

type CacheIdentifier = (typeof CACHE_IDENTIFIERS)[keyof typeof CACHE_IDENTIFIERS];

export interface CacheConfig {
  syncEventKey: string;
  lockKey: string;
  cacheIdentifier: CacheIdentifier;
  colorCode: string;
  cacheName: string;
  lockTtl?: number;
}

export abstract class BaseCacheService<T> {
  protected readonly logger: Logger;
  protected cache!: T;
  protected cacheLoaded = false;
  private messageHandler: ((channel: string, message: string) => void) | null = null;

  constructor(
    protected readonly config: CacheConfig,
    protected readonly redisPubSubService: RedisPubSubService,
    protected readonly cacheService: CacheService,
    protected readonly instanceService: InstanceService,
    protected readonly eventEmitter?: EventEmitter2,
  ) {
    this.logger = new Logger(`${config.colorCode}${config.cacheName}\x1b[0m`);
    this.setupSubscription();
  }

  private setupSubscription(): void {
    if (this.messageHandler) return;

    this.messageHandler = (channel: string, message: string) => {
      if (channel === this.config.syncEventKey) {
        this.handleIncomingMessage(message);
      }
    };

    this.redisPubSubService.subscribeWithHandler(
      this.config.syncEventKey,
      this.messageHandler
    );
  }

  private handleIncomingMessage(message: string): void {
    try {
      const payload = JSON.parse(message);
      const myInstanceId = this.instanceService.getInstanceId();

      if (payload.instanceId === myInstanceId) {
        return;
      }

      this.logger.log(`Received cache sync from instance ${payload.instanceId.slice(0, 8)}...`);

      const data = this.deserializeSyncData(payload);
      this.handleSyncData(data);
      this.cacheLoaded = true;

      // Emit loaded event to trigger dependent services on this instance
      this.emitLoadedEvent();

      this.logSyncSuccess(payload);
    } catch (error) {
      this.logger.error('Failed to parse cache sync message:', error);
    }
  }

  async reload(): Promise<void> {
    const instanceId = this.instanceService.getInstanceId();

    try {
      const lockTtl = this.config.lockTtl ?? REDIS_TTL.RELOAD_LOCK_TTL;
      const acquired = await this.cacheService.acquire(
        this.config.lockKey,
        instanceId,
        lockTtl
      );

      if (!acquired) {
        this.logger.log('Another instance is reloading, waiting for broadcast...');
        return;
      }

      try {
        const start = Date.now();

        await this.beforeLoad();

        const rawData = await this.loadFromDb();

        this.cache = this.transformData(rawData);

        await this.afterTransform(this.cache);

        await this.publish(this.cache);

        this.cacheLoaded = true;

        this.emitLoadedEvent();

        this.logger.log(`Loaded ${this.getLogCount()} in ${Date.now() - start}ms`);
      } finally {
        await this.cacheService.release(this.config.lockKey, instanceId);
      }
    } catch (error) {
      this.logger.error('Failed to reload cache:', error);
      throw error;
    }
  }

  private async publish(data: T): Promise<void> {
    try {
      const payload = {
        instanceId: this.instanceService.getInstanceId(),
        ...this.serializeForPublish(data),
        timestamp: Date.now(),
      };
      await this.redisPubSubService.publish(
        this.config.syncEventKey,
        JSON.stringify(payload)
      );
    } catch (error) {
      this.logger.error('Failed to publish cache sync:', error);
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

  protected emitLoadedEvent(): void {}

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
