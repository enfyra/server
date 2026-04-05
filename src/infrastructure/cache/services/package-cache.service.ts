import { Injectable } from '@nestjs/common';
import { OnEvent, EventEmitter2 } from '@nestjs/event-emitter';
import { QueryBuilderService } from '../../query-builder/query-builder.service';
import { RedisPubSubService } from './redis-pubsub.service';
import { InstanceService } from '../../../shared/services/instance.service';
import { PackageCdnLoaderService, extractErrorMessage } from './package-cdn-loader.service';
import { BaseCacheService, CacheConfig } from './base-cache.service';
import { ENFYRA_ADMIN_WEBSOCKET_NAMESPACE, PACKAGE_CACHE_SYNC_EVENT_KEY } from '../../../shared/utils/constant';
import { CACHE_EVENTS, CACHE_IDENTIFIERS, shouldReloadCache } from '../../../shared/utils/cache-events.constants';
import { DynamicWebSocketGateway } from '../../../modules/websocket/gateway/dynamic-websocket.gateway';

const PACKAGE_CONFIG: CacheConfig = {
  syncEventKey: PACKAGE_CACHE_SYNC_EVENT_KEY,
  cacheIdentifier: CACHE_IDENTIFIERS.PACKAGE,
  colorCode: '\x1b[35m',
  cacheName: 'PackageCache',
};

const SYSTEM_EVENT_PREFIX = '$system:package';

@Injectable()
export class PackageCacheService extends BaseCacheService<string[]> {
  constructor(
    private readonly queryBuilder: QueryBuilderService,
    redisPubSubService: RedisPubSubService,
    instanceService: InstanceService,
    eventEmitter: EventEmitter2,
    private readonly websocketGateway: DynamicWebSocketGateway,
    private readonly cdnLoader: PackageCdnLoaderService,
  ) {
    super(PACKAGE_CONFIG, redisPubSubService, instanceService, eventEmitter);
  }

  @OnEvent(CACHE_EVENTS.METADATA_LOADED)
  async onMetadataLoaded() {
    await this.reload();
    this.eventEmitter?.emit(CACHE_EVENTS.PACKAGE_LOADED);
  }

  @OnEvent(CACHE_EVENTS.INVALIDATE)
  async handleCacheInvalidation(payload: { tableName: string; action: string }) {
    if (shouldReloadCache(payload.tableName, this.config.cacheIdentifier)) {
      this.logger.log(`Cache invalidation event received for table: ${payload.tableName}`);
      await this.reload();
    }
  }

  protected async loadFromDb(): Promise<string[]> {
    const result = await this.queryBuilder.select({
      tableName: 'package_definition',
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

  protected handleSyncData(data: string[]): void {
    this.cache = data;
  }

  protected deserializeSyncData(payload: any): any {
    return payload.packages;
  }

  protected serializeForPublish(packages: string[]): Record<string, any> {
    return { packages };
  }

  protected getLogCount(): string {
    return `${this.cache.length} packages`;
  }

  protected logSyncSuccess(payload: any): void {
    this.logger.log(`Package cache synced: ${payload.packages?.length || 0} packages`);
  }

  protected async afterTransform(): Promise<void> {
    this.preloadPackagesFromCdn().catch((error) => {
      this.logger.error(`CDN preload failed (non-blocking): ${error.message}`);
    });
  }

  private emitEvent(event: string, data: any) {
    try {
      this.websocketGateway.emitToNamespace(
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
      await this.queryBuilder.update({
        table: 'package_definition',
        where: [{ field: 'id', operator: '=', value: id }],
        data: { status, ...extra },
      });
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
        !this.cdnLoader.isLoaded(pkg.name) &&
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
        await this.cdnLoader.loadPackage(pkg.name, pkg.version);
        await this.updatePackageStatus(pkg.id, 'installed', { lastError: null });
        this.emitEvent('installed', { id: pkg.id, name: pkg.name });
        loaded++;
      } catch (error) {
        const errorDetail = extractErrorMessage(error);
        this.logger.error(`CDN preload failed for ${pkg.name}: ${errorDetail}`);
        await this.updatePackageStatus(pkg.id, 'failed', { lastError: errorDetail });
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
    Array<{ id: string | number; name: string; version: string; status: string }>
  > {
    const result = await this.queryBuilder.select({
      tableName: 'package_definition',
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
    return this.cdnLoader;
  }

  async getPackages(): Promise<string[]> {
    await this.ensureLoaded();
    return this.cache;
  }
}
