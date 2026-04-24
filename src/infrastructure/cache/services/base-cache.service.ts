import { Logger } from '../../../shared/logger';
import { EventEmitter2 } from 'eventemitter2';
import { getErrorMessage } from '../../../shared/utils/error.util';
import {
  CACHE_IDENTIFIERS,
  TCacheInvalidationPayload,
} from '../../../shared/utils/cache-events.constants';

type CacheIdentifier =
  (typeof CACHE_IDENTIFIERS)[keyof typeof CACHE_IDENTIFIERS];

export interface CacheConfig {
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

  constructor(
    protected readonly config: CacheConfig,
    protected readonly eventEmitter?: EventEmitter2,
  ) {
    this.logger = new Logger(`${config.colorCode}${config.cacheName}\x1b[0m`);
  }

  async reload(_publish = true): Promise<void> {
    if (this.isLoading && this.loadingPromise) {
      return this.loadingPromise;
    }

    this.isLoading = true;
    this.loadingPromise = (async () => {
      try {
        const start = Date.now();
        await this.beforeLoad();

        const rawData = await this.loadFromDb();

        this.cache = this.transformData(rawData);

        await this.afterTransform(this.cache);

        this.cacheLoaded = true;

        const elapsed = Date.now() - start;
        this.logger.log(`Loaded ${this.getLogCount()} in ${elapsed}ms`);

        this.emitLoadedEvent();
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

  async partialReload(
    payload: TCacheInvalidationPayload,
    _publish = true,
  ): Promise<void> {
    if (this.isLoading && this.loadingPromise) {
      await this.loadingPromise;
      return;
    }
    try {
      const start = Date.now();
      await this.applyPartialUpdate(payload);
      const elapsed = Date.now() - start;
      this.logger.log(
        `Partial reload (${payload.ids?.length ?? 0} ids) in ${elapsed}ms`,
      );

      this.emitLoadedEvent();
    } catch (error) {
      this.logger.warn(
        `Partial reload failed, falling back to full reload: ${getErrorMessage(error)}`,
      );
      await this.reload(_publish);
    }
  }

  supportsPartialReload(): boolean {
    return false;
  }

  protected async applyPartialUpdate(
    _payload: TCacheInvalidationPayload,
  ): Promise<void> {
    throw new Error('applyPartialUpdate not implemented');
  }

  protected abstract loadFromDb(): Promise<any>;

  protected abstract transformData(rawData: any): T;

  protected async beforeLoad(): Promise<void> {}

  protected async afterTransform(data: T): Promise<void> {}

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

  isLoaded(): boolean {
    return this.cacheLoaded;
  }

  protected async ensureLoaded(): Promise<void> {
    if (!this.cacheLoaded) {
      await this.reload();
    }
    if (this.isLoading && this.loadingPromise) {
      await this.loadingPromise;
    }
  }

  getRawCache(): T {
    if (this.isLoading && this.loadingPromise) {
      this.logger.warn(
        'Cache reload in progress, returning stale data. Consider using await ensureLoaded() before access.',
      );
    }
    return this.cache;
  }

  async getCacheAsync(): Promise<T> {
    await this.ensureLoaded();
    return this.cache;
  }

  get cacheIdentifier(): CacheIdentifier {
    return this.config.cacheIdentifier;
  }
}
