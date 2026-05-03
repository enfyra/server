import { EventEmitter2 } from 'eventemitter2';
import { QueryBuilderService } from '@enfyra/kernel';
import {
  PackageCdnLoaderService,
  extractErrorMessage,
} from './package-cdn-loader.service';
import { BaseCacheService, CacheConfig } from './base-cache.service';
import { RedisRuntimeCacheStore } from './redis-runtime-cache-store.service';
import { ENFYRA_ADMIN_WEBSOCKET_NAMESPACE } from '../../../shared/utils/constant';
import { getErrorMessage } from '../../../shared/utils/error.util';
import {
  CACHE_EVENTS,
  CACHE_IDENTIFIERS,
} from '../../../shared/utils/cache-events.constants';
import type { Cradle } from '../../../container';

const PACKAGE_CONFIG: CacheConfig = {
  cacheIdentifier: CACHE_IDENTIFIERS.PACKAGE,
  colorCode: '\x1b[35m',
  cacheName: 'PackageCache',
};

const SYSTEM_EVENT_PREFIX = '$system:package';

export class PackageCacheService extends BaseCacheService<string[]> {
  private readonly queryBuilderService: QueryBuilderService;
  private readonly packageCdnLoaderService: PackageCdnLoaderService;
  private readonly lazyRef: Cradle;
  private systemReady = false;
  private preloadScheduled = false;
  private preloadRunning = false;
  private preloadRequested = false;

  constructor(deps: {
    queryBuilderService: QueryBuilderService;
    eventEmitter: EventEmitter2;
    packageCdnLoaderService: PackageCdnLoaderService;
    lazyRef: Cradle;
    redisRuntimeCacheStore?: RedisRuntimeCacheStore;
  }) {
    super(PACKAGE_CONFIG, deps.eventEmitter, deps.redisRuntimeCacheStore);
    this.queryBuilderService = deps.queryBuilderService;
    this.lazyRef = deps.lazyRef;
    this.packageCdnLoaderService = deps.packageCdnLoaderService;
    deps.eventEmitter.once(CACHE_EVENTS.SYSTEM_READY, () => {
      this.systemReady = true;
      if (this.preloadRequested) {
        this.schedulePackagePreload();
      }
    });
  }

  protected async loadFromDb(): Promise<string[]> {
    const result = await this.queryBuilderService.find({
      table: 'package_definition',
      fields: ['name'],
      filter: {
        isEnabled: true,
        type: 'Server',
        status: 'installed',
      },
    });

    return result.data.map((p: any) => p.name);
  }

  protected transformData(packages: string[]): string[] {
    return packages;
  }

  protected getLogCount(): string {
    return `${this.cache.length} packages`;
  }

  protected async afterTransform(): Promise<void> {
    this.schedulePackagePreload();
  }

  protected async afterSharedCacheHydrate(): Promise<void> {
    this.schedulePackagePreload();
  }

  private schedulePackagePreload(): void {
    this.preloadRequested = true;
    if (!this.systemReady) return;
    if (this.preloadScheduled || this.preloadRunning) return;

    this.preloadScheduled = true;
    setImmediate(() => {
      this.preloadScheduled = false;
      this.preloadRunning = true;
      this.preloadRequested = false;
      this.preloadPackagesFromCdn()
        .catch((error) => {
          this.logger.error(
            `CDN preload failed (non-blocking): ${getErrorMessage(error)}`,
          );
        })
        .finally(() => {
          this.preloadRunning = false;
          if (this.preloadRequested) {
            this.schedulePackagePreload();
          }
        });
    });
  }

  private emitEvent(event: string, data: any) {
    try {
      const gateway = this.lazyRef.dynamicWebSocketGateway;
      if (!gateway?.server) return;
      gateway.emitToNamespace(
        ENFYRA_ADMIN_WEBSOCKET_NAMESPACE,
        `${SYSTEM_EVENT_PREFIX}:${event}`,
        data,
      );
    } catch (error) {
      this.logger.warn(
        `Failed to emit WS event ${event}: ${getErrorMessage(error)}`,
      );
    }
  }

  private async updatePackageStatus(
    id: string | number,
    status: string,
    extra?: Record<string, any>,
  ) {
    try {
      await this.queryBuilderService.update('package_definition', id, {
        status,
        ...extra,
      });
    } catch (error) {
      this.logger.error(
        `Failed to update status to ${status} for package ${id}: ${getErrorMessage(error)}`,
      );
    }
  }

  private async preloadPackagesFromCdn(): Promise<void> {
    const packagesWithMeta = await this.loadPackagesForSync();
    const toPreload = packagesWithMeta.filter(
      (pkg) =>
        !this.packageCdnLoaderService.isLoaded(pkg.name, pkg.version) &&
        ['installed', 'failed', 'installing', 'updating'].includes(pkg.status),
    );

    if (toPreload.length === 0) return;

    const retryCount = toPreload.filter((p) =>
      ['failed', 'installing', 'updating'].includes(p.status),
    ).length;
    this.logger.log(
      `Preloading ${toPreload.length} packages from CDN${retryCount ? ` (${retryCount} retrying)` : ''}...`,
    );

    this.emitEvent('installing', {
      packages: toPreload.map((p) => ({ id: p.id, name: p.name })),
    });

    let loaded = 0;
    let failed = 0;

    for (const pkg of toPreload) {
      try {
        await this.packageCdnLoaderService.loadPackage(pkg.name, pkg.version);
        await this.updatePackageStatus(pkg.id, 'installed', {
          lastError: null,
        });
        this.emitEvent('installed', { id: pkg.id, name: pkg.name });
        loaded++;
      } catch (error) {
        const errorDetail = extractErrorMessage(error);
        this.logger.error(`CDN preload failed for ${pkg.name}: ${errorDetail}`);
        await this.updatePackageStatus(pkg.id, 'failed', {
          lastError: errorDetail,
        });
        this.emitEvent('failed', {
          id: pkg.id,
          name: pkg.name,
          error: errorDetail,
          operation: 'preload',
        });
        failed++;
      }
    }

    this.logger.log(
      `CDN preload done: ${loaded} loaded${failed ? `, ${failed} failed` : ''}`,
    );
  }

  private async loadPackagesForSync(): Promise<
    Array<{
      id: string | number;
      name: string;
      version: string;
      status: string;
    }>
  > {
    const result = await this.queryBuilderService.find({
      table: 'package_definition',
      fields: ['id', 'name', 'version', 'status'],
      filter: {
        isEnabled: true,
        type: 'Server',
      },
    });

    return result.data.map((p: any) => ({
      id: p.id || p._id,
      name: p.name,
      version: p.version || 'latest',
      status: p.status || 'installed',
    }));
  }

  getCdnLoader(): PackageCdnLoaderService {
    return this.packageCdnLoaderService;
  }

  async getPackages(): Promise<string[]> {
    return this.getCacheAsync();
  }
}
