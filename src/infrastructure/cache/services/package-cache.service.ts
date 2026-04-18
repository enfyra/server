import { EventEmitter2 } from 'eventemitter2';
import { QueryBuilderService } from '../../query-builder/query-builder.service';
import {
  PackageCdnLoaderService,
  extractErrorMessage,
} from './package-cdn-loader.service';
import { BaseCacheService, CacheConfig } from './base-cache.service';
import { ENFYRA_ADMIN_WEBSOCKET_NAMESPACE } from '../../../shared/utils/constant';
import { CACHE_IDENTIFIERS } from '../../../shared/utils/cache-events.constants';
import { DynamicWebSocketGateway } from '../../../modules/websocket/gateway/dynamic-websocket.gateway';

const PACKAGE_CONFIG: CacheConfig = {
  cacheIdentifier: CACHE_IDENTIFIERS.PACKAGE,
  colorCode: '\x1b[35m',
  cacheName: 'PackageCache',
};

const SYSTEM_EVENT_PREFIX = '$system:package';

export class PackageCacheService extends BaseCacheService<string[]> {
  private readonly queryBuilderService: QueryBuilderService;
  private _dynamicWebSocketGateway?: DynamicWebSocketGateway;
  private readonly packageCdnLoaderService: PackageCdnLoaderService;
  private _container?: any;

  constructor(deps: {
    queryBuilderService: QueryBuilderService;
    eventEmitter: EventEmitter2;
    packageCdnLoaderService: PackageCdnLoaderService;
    _container?: any;
  }) {
    super(PACKAGE_CONFIG, deps.eventEmitter);
    this.queryBuilderService = deps.queryBuilderService;
    this._container = deps._container;
    this.packageCdnLoaderService = deps.packageCdnLoaderService;
  }

  private get dynamicWebSocketGateway(): DynamicWebSocketGateway | undefined {
    if (!this._dynamicWebSocketGateway && this._container) {
      this._dynamicWebSocketGateway = this._container.cradle?.dynamicWebSocketGateway;
    }
    return this._dynamicWebSocketGateway;
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
    this.preloadPackagesFromCdn().catch((error) => {
      this.logger.error(`CDN preload failed (non-blocking): ${error.message}`);
    });
  }

  private emitEvent(event: string, data: any) {
    try {
      this.dynamicWebSocketGateway?.emitToNamespace(
        ENFYRA_ADMIN_WEBSOCKET_NAMESPACE,
        `${SYSTEM_EVENT_PREFIX}:${event}`,
        data,
      );
    } catch (error) {
      this.logger.warn(`Failed to emit WS event ${event}: ${error.message}`);
    }
  }

  private async updatePackageStatus(
    id: string | number,
    status: string,
    extra?: Record<string, any>,
  ) {
    try {
      await this.queryBuilderService.update(
        'package_definition',
        { where: [{ field: 'id', operator: '=', value: id }] },
        { status, ...extra },
      );
    } catch (error) {
      this.logger.error(
        `Failed to update status to ${status} for package ${id}: ${error.message}`,
      );
    }
  }

  private async preloadPackagesFromCdn(): Promise<void> {
    const packagesWithMeta = await this.loadPackagesForSync();
    const toPreload = packagesWithMeta.filter(
      (pkg) =>
        !this.packageCdnLoaderService.isLoaded(pkg.name) &&
        (pkg.status === 'installed' || pkg.status === 'failed'),
    );

    if (toPreload.length === 0) return;

    const retryCount = toPreload.filter((p) => p.status === 'failed').length;
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
    await this.ensureLoaded();
    return this.cache;
  }
}
