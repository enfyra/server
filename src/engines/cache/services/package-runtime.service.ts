import { EventEmitter2 } from 'eventemitter2';
import { QueryBuilderService } from '@enfyra/kernel';
import { Logger } from '../../../shared/logger';
import { ENFYRA_ADMIN_WEBSOCKET_NAMESPACE } from '../../../shared/utils/constant';
import { getErrorMessage } from '../../../shared/utils/error.util';
import {
  CACHE_EVENTS,
  CACHE_IDENTIFIERS,
} from '../../../shared/utils/cache-events.constants';
import type { Cradle } from '../../../container';
import {
  PackageCdnLoaderService,
  extractErrorMessage,
} from './package-cdn-loader.service';

const SYSTEM_EVENT_PREFIX = '$system:package';

export interface PackageRuntimeStatus {
  initialized: boolean;
  systemReady: boolean;
  preloadRunning: boolean;
  preloadRequested: boolean;
  lastPreload?: {
    status: 'running' | 'ok' | 'degraded';
    startedAt: string;
    completedAt?: string;
    loaded?: number;
    failed?: number;
    error?: string;
  };
}

export class PackageRuntimeService {
  private readonly logger = new Logger(PackageRuntimeService.name);
  private readonly queryBuilderService: QueryBuilderService;
  private readonly packageCdnLoaderService: PackageCdnLoaderService;
  private readonly eventEmitter: EventEmitter2;
  private readonly lazyRef: Cradle;
  private initialized = false;
  private systemReady = false;
  private preloadScheduled = false;
  private preloadRunning = false;
  private preloadRequested = false;
  private lastPreload: PackageRuntimeStatus['lastPreload'];

  constructor(deps: {
    queryBuilderService: QueryBuilderService;
    eventEmitter: EventEmitter2;
    packageCdnLoaderService: PackageCdnLoaderService;
    lazyRef: Cradle;
  }) {
    this.queryBuilderService = deps.queryBuilderService;
    this.eventEmitter = deps.eventEmitter;
    this.packageCdnLoaderService = deps.packageCdnLoaderService;
    this.lazyRef = deps.lazyRef;
  }

  init(): void {
    if (this.initialized) return;
    this.initialized = true;
    const schedule = () => {
      this.schedulePackagePreload();
    };
    this.eventEmitter.on(CACHE_EVENTS.PACKAGE_LOADED, schedule);
    this.eventEmitter.on(`${CACHE_IDENTIFIERS.PACKAGE}_LOADED`, schedule);
    this.eventEmitter.once(CACHE_EVENTS.SYSTEM_READY, () => {
      this.systemReady = true;
      if (this.preloadRequested) {
        this.schedulePackagePreload();
      }
    });
  }

  schedulePackagePreload(): void {
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
          const message = getErrorMessage(error);
          this.lastPreload = {
            status: 'degraded',
            startedAt: this.lastPreload?.startedAt ?? new Date().toISOString(),
            completedAt: new Date().toISOString(),
            error: message,
          };
          this.logger.error(`CDN preload failed (non-blocking): ${message}`);
        })
        .finally(() => {
          this.preloadRunning = false;
          if (this.preloadRequested) {
            this.schedulePackagePreload();
          }
        });
    });
  }

  getStatus(): PackageRuntimeStatus {
    return {
      initialized: this.initialized,
      systemReady: this.systemReady,
      preloadRunning: this.preloadRunning,
      preloadRequested: this.preloadRequested,
      lastPreload: this.lastPreload ? { ...this.lastPreload } : undefined,
    };
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
      await this.queryBuilderService.update('enfyra_package', id, {
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
    const startedAt = new Date().toISOString();
    this.lastPreload = { status: 'running', startedAt };

    const packagesWithMeta = await this.loadPackagesForSync();
    const toPreload = packagesWithMeta.filter(
      (pkg) =>
        !this.packageCdnLoaderService.isLoaded(pkg.name, pkg.version) &&
        ['installed', 'failed', 'installing', 'updating'].includes(pkg.status),
    );

    if (toPreload.length === 0) {
      this.lastPreload = {
        status: 'ok',
        startedAt,
        completedAt: new Date().toISOString(),
        loaded: 0,
        failed: 0,
      };
      return;
    }

    const retryCount = toPreload.filter((p) =>
      ['failed', 'installing', 'updating'].includes(p.status),
    ).length;
    this.logger.log(
      `Preloading ${toPreload.length} packages from CDN${retryCount ? ` (${retryCount} retrying)` : ''}...`,
    );

    this.emitEvent('installing', {
      packages: toPreload.map((p) => ({
        id: p.id,
        name: p.name,
        version: p.version,
      })),
    });

    let loaded = 0;
    let failed = 0;

    for (const pkg of toPreload) {
      try {
        await this.packageCdnLoaderService.loadPackage(pkg.name, pkg.version);
        await this.updatePackageStatus(pkg.id, 'installed', {
          lastError: null,
        });
        this.emitEvent('installed', {
          id: pkg.id,
          name: pkg.name,
          version: pkg.version,
        });
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
          version: pkg.version,
          error: errorDetail,
          operation: 'preload',
        });
        failed++;
      }
    }

    this.lastPreload = {
      status: failed > 0 ? 'degraded' : 'ok',
      startedAt,
      completedAt: new Date().toISOString(),
      loaded,
      failed,
    };
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
      table: 'enfyra_package',
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
}
