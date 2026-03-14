import { Injectable } from '@nestjs/common';
import { OnEvent, EventEmitter2 } from '@nestjs/event-emitter';
import { QueryBuilderService } from '../../query-builder/query-builder.service';
import { RedisPubSubService } from './redis-pubsub.service';
import { CacheService } from './cache.service';
import { InstanceService } from '../../../shared/services/instance.service';
import { PackageManagementService } from '../../../modules/package-management/services/package-management.service';
import { BaseCacheService, CacheConfig } from './base-cache.service';
import {
  PACKAGE_CACHE_SYNC_EVENT_KEY,
  PACKAGE_RELOAD_LOCK_KEY,
} from '../../../shared/utils/constant';
import { CACHE_EVENTS, CACHE_IDENTIFIERS, shouldReloadCache } from '../../../shared/utils/cache-events.constants';

const PACKAGE_CONFIG: CacheConfig = {
  syncEventKey: PACKAGE_CACHE_SYNC_EVENT_KEY,
  lockKey: PACKAGE_RELOAD_LOCK_KEY,
  cacheIdentifier: CACHE_IDENTIFIERS.PACKAGE,
  colorCode: '\x1b[35m',
  cacheName: 'PackageCache',
};

@Injectable()
export class PackageCacheService extends BaseCacheService<string[]> {
  constructor(
    private readonly queryBuilder: QueryBuilderService,
    redisPubSubService: RedisPubSubService,
    cacheService: CacheService,
    instanceService: InstanceService,
    eventEmitter: EventEmitter2,
    private readonly packageManagementService: PackageManagementService,
  ) {
    super(PACKAGE_CONFIG, redisPubSubService, cacheService, instanceService, eventEmitter);
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
    await this.ensurePackagesInstalled();
  }

  private async ensurePackagesInstalled(): Promise<void> {
    const packagesWithVersion = await this.loadPackagesWithVersion();

    for (const pkg of packagesWithVersion) {
      const isInstalled = await this.packageManagementService.isPackageInstalled(pkg.name);

      if (!isInstalled) {
        this.logger.log(`Package ${pkg.name} not found in package.json, installing...`);

        try {
          await this.packageManagementService.installPackage({
            name: pkg.name,
            type: 'Server',
            version: pkg.version,
          });

          this.logger.log(`Successfully installed package ${pkg.name}@${pkg.version}`);
        } catch (error) {
          this.logger.error(`Failed to install package ${pkg.name}:`, error.message);
          throw error;
        }
      }
    }
  }

  private async loadPackagesWithVersion(): Promise<Array<{ name: string; version: string }>> {
    const result = await this.queryBuilder.select({
      tableName: 'package_definition',
      fields: ['name', 'version'],
      filter: {
        isEnabled: true,
        type: 'Server',
      },
    });

    return result.data.map((p: any) => ({
      name: p.name,
      version: p.version || 'latest',
    }));
  }

  async getPackages(): Promise<string[]> {
    await this.ensureLoaded();
    return this.cache;
  }
}
