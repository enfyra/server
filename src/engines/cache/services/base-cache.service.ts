import { Logger } from '../../../shared/logger';
import { EventEmitter2 } from 'eventemitter2';
import { getErrorMessage } from '../../../shared/utils/error.util';
import {
  CACHE_EVENTS,
  CACHE_IDENTIFIERS,
  TCacheInvalidationPayload,
} from '../../../shared/utils/cache-events.constants';
import { RedisRuntimeCacheStore } from './redis-runtime-cache-store.service';

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
  private sharedRefreshLockValue: string | null = null;
  private sharedRuntimeCacheSystemReady = false;

  constructor(
    protected readonly config: CacheConfig,
    protected readonly eventEmitter?: EventEmitter2,
    protected readonly redisRuntimeCacheStore?: RedisRuntimeCacheStore,
  ) {
    this.logger = new Logger(`${config.colorCode}${config.cacheName}\x1b[0m`);
    this.eventEmitter?.once(CACHE_EVENTS.SYSTEM_READY, () => {
      this.sharedRuntimeCacheSystemReady = true;
      this.releaseLocalCacheAfterSharedAccess();
    });
  }

  async reload(_publish = true): Promise<void> {
    if (this.isLoading && this.loadingPromise) {
      return this.loadingPromise;
    }

    this.isLoading = true;
    this.loadingPromise = (async () => {
      try {
        const start = Date.now();
        const data = await this.loadFreshCacheData();
        await this.setLoadedCache(data, { persistShared: true });

        const elapsed = Date.now() - start;
        this.logger.log(`Loaded ${this.getLogCount()} in ${elapsed}ms`);

        this.emitLoadedEvent();
      } catch (error) {
        await this.releaseActiveSharedLock();
        this.logger.error('Failed to reload cache:', error);
        throw error;
      } finally {
        this.releaseLocalCacheAfterSharedAccess();
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
      if (this.usesSharedRuntimeCache()) {
        const lockValue =
          await this.redisRuntimeCacheStore!.acquireRefreshLockWithWait(
            this.config.cacheIdentifier,
          );
        if (!lockValue) {
          throw new Error(
            `${this.config.cacheName} shared cache refresh lock timed out`,
          );
        }
        this.sharedRefreshLockValue = lockValue;
        const snapshot =
          await this.redisRuntimeCacheStore!.getSnapshot<T>(
            this.config.cacheIdentifier,
          );
        if (!snapshot) {
          await this.releaseActiveSharedLock();
          await this.reload(_publish);
          return;
        }
        this.cache = snapshot.data;
      }
      await this.applyPartialUpdate(payload);
      if (this.usesSharedRuntimeCache()) {
        if (this.cache === undefined) {
          const snapshot =
            await this.redisRuntimeCacheStore!.getSnapshot<T>(
              this.config.cacheIdentifier,
            );
          if (!snapshot) {
            throw new Error(
              `${this.config.cacheName} shared cache is unavailable after reload`,
            );
          }
          this.cacheLoaded = true;
        } else {
          await this.persistSharedCache(this.cache);
        }
      }
      const elapsed = Date.now() - start;
      this.logger.log(
        `Partial reload (${payload.ids?.length ?? 0} ids) in ${elapsed}ms`,
      );

      this.emitLoadedEvent();
    } catch (error) {
      await this.releaseActiveSharedLock();
      this.logger.warn(
        `Partial reload failed, falling back to full reload: ${getErrorMessage(error)}`,
      );
      await this.reload(_publish);
    } finally {
      await this.releaseActiveSharedLock();
      this.releaseLocalCacheAfterSharedAccess();
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

  protected async afterTransform(_data: T): Promise<void> {}

  protected async afterSharedCachePersist(_data: T): Promise<void> {}

  protected async afterSharedCacheHydrate(_data: T): Promise<void> {}

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
    if (this.usesSharedRuntimeCache()) {
      const snapshot =
        await this.redisRuntimeCacheStore!.getSnapshot<T>(
          this.config.cacheIdentifier,
        );
      if (snapshot) {
        this.cacheLoaded = true;
        return;
      }
    }
    if (!this.cacheLoaded) {
      await this.reload();
    }
    if (this.isLoading && this.loadingPromise) {
      await this.loadingPromise;
    }
  }

  getRawCache(): T {
    if (this.usesSharedRuntimeCache()) {
      throw new Error(
        `${this.config.cacheName} is Redis-backed; use getCacheAsync()`,
      );
    }
    if (this.isLoading && this.loadingPromise) {
      this.logger.warn(
        'Cache reload in progress, returning stale data. Consider using await ensureLoaded() before access.',
      );
    }
    return this.cache;
  }

  async getCacheAsync(): Promise<T> {
    if (this.usesSharedRuntimeCache()) {
      const snapshot =
        await this.redisRuntimeCacheStore!.getSnapshot<T>(
          this.config.cacheIdentifier,
        );
      if (snapshot) {
        this.cacheLoaded = true;
        return snapshot.data;
      }
      await this.reload();
      const refreshed =
        await this.redisRuntimeCacheStore!.getSnapshot<T>(
          this.config.cacheIdentifier,
        );
      if (!refreshed) {
        throw new Error(`${this.config.cacheName} shared cache is unavailable`);
      }
      return refreshed.data;
    }
    await this.ensureLoaded();
    return this.cache;
  }

  get cacheIdentifier(): CacheIdentifier {
    return this.config.cacheIdentifier;
  }

  usesSharedRuntimeCache(): boolean {
    return this.redisRuntimeCacheStore?.isEnabled() === true;
  }

  async syncFromSharedCache(timeoutMs = 10000): Promise<void> {
    if (!this.usesSharedRuntimeCache()) {
      await this.reload(false);
      return;
    }
    const snapshot =
      await this.redisRuntimeCacheStore!.waitForSnapshot<T>(
        this.config.cacheIdentifier,
        timeoutMs,
      );
    if (!snapshot) {
      throw new Error(`${this.config.cacheName} shared cache is unavailable`);
    }
    await this.afterSharedCacheHydrate(snapshot.data);
    this.cacheLoaded = true;
    this.emitLoadedEvent();
  }

  private async loadFreshCacheData(): Promise<T> {
    if (!this.usesSharedRuntimeCache()) {
      await this.beforeLoad();
      const rawData = await this.loadFromDb();
      return this.transformData(rawData);
    }

    const lockValue = await this.redisRuntimeCacheStore!.acquireRefreshLock(
      this.config.cacheIdentifier,
    );
    if (!lockValue) {
      const snapshot =
        await this.redisRuntimeCacheStore!.waitForSnapshot<T>(
          this.config.cacheIdentifier,
        );
      if (snapshot) {
        return snapshot.data;
      }
    }

    try {
      await this.beforeLoad();
      const rawData = await this.loadFromDb();
      return this.transformData(rawData);
    } finally {
      this.sharedRefreshLockValue = lockValue;
    }
  }

  private async setLoadedCache(
    data: T,
    options: { persistShared: boolean },
  ): Promise<void> {
    this.cache = data;
    await this.afterTransform(data);
    if (this.usesSharedRuntimeCache() && options.persistShared) {
      await this.persistSharedCache(data);
    }
    this.cacheLoaded = true;
    await this.releaseActiveSharedLock();
  }

  private async persistSharedCache(data: T): Promise<void> {
    await this.redisRuntimeCacheStore!.setSnapshot(
      this.config.cacheIdentifier,
      data,
    );
    await this.afterSharedCachePersist(data);
  }

  private releaseLocalCacheAfterSharedAccess(): void {
    if (!this.usesSharedRuntimeCache()) return;
    if (!this.sharedRuntimeCacheSystemReady) return;
    this.cache = undefined as T;
  }

  private async releaseActiveSharedLock(): Promise<void> {
    if (!this.usesSharedRuntimeCache() || !this.sharedRefreshLockValue) return;
    const lockValue = this.sharedRefreshLockValue;
    this.sharedRefreshLockValue = null;
    await this.redisRuntimeCacheStore!.releaseRefreshLock(
      this.config.cacheIdentifier,
      lockValue,
    );
  }
}
