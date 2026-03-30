import { Injectable } from '@nestjs/common';
import { OnEvent, EventEmitter2 } from '@nestjs/event-emitter';
import { QueryBuilderService } from '../../query-builder/query-builder.service';
import { RedisPubSubService } from './redis-pubsub.service';
import { InstanceService } from '../../../shared/services/instance.service';
import { PackageManagementService } from '../../../modules/package-management/services/package-management.service';
import { BaseCacheService, CacheConfig } from './base-cache.service';
import { PACKAGE_CACHE_SYNC_EVENT_KEY } from '../../../shared/utils/constant';
import { CACHE_EVENTS, CACHE_IDENTIFIERS, shouldReloadCache } from '../../../shared/utils/cache-events.constants';

const PACKAGE_CONFIG: CacheConfig = {
  syncEventKey: PACKAGE_CACHE_SYNC_EVENT_KEY,
  cacheIdentifier: CACHE_IDENTIFIERS.PACKAGE,
  colorCode: '\x1b[35m',
  cacheName: 'PackageCache',
};

@Injectable()
export class PackageCacheService extends BaseCacheService<string[]> {
  constructor(
    private readonly queryBuilder: QueryBuilderService,
    redisPubSubService: RedisPubSubService,
    instanceService: InstanceService,
    eventEmitter: EventEmitter2,
    private readonly packageManagementService: PackageManagementService,
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
    try {
      await this.ensurePackagesInstalled();
      await this.ensurePackagesCleanedUp();
    } catch (error) {
      this.logger.error(`Package sync failed (non-blocking): ${error.message}`);
    }
  }

  private async ensurePackagesInstalled(): Promise<void> {
    const packagesWithVersion = await this.loadPackagesWithVersion();

    const missing = packagesWithVersion.filter(
      (pkg) => !this.packageManagementService.isPackageInstalled(pkg.name),
    );

    if (missing.length === 0) return;

    this.logger.log(`${missing.length} packages missing, acquiring machine lock...`);
    const locked = await this.packageManagementService.acquireMachineLock();
    if (!locked) {
      this.logger.warn('Could not acquire machine lock, skipping package install');
      return;
    }

    try {
      const stillMissing = missing.filter(
        (pkg) => !this.packageManagementService.isPackageInstalled(pkg.name),
      );

      if (stillMissing.length === 0) {
        this.logger.log('All packages already installed (by another instance)');
        return;
      }

      this.logger.log(`Installing ${stillMissing.length} missing packages...`);
      await this.packageManagementService.installBatch(stillMissing);

      await this.packageManagementService.renewMachineLock();
    } finally {
      await this.packageManagementService.releaseMachineLock();
    }
  }

  private async ensurePackagesCleanedUp(): Promise<void> {
    const dbPackages = new Set(this.cache);

    let localPackages: string[];
    try {
      const projectPkgPath = require('path').join(process.cwd(), 'package.json');
      const content = require('fs').readFileSync(projectPkgPath, 'utf-8');
      const pkgJson = JSON.parse(content);
      localPackages = Object.keys(pkgJson.dependencies || {});
    } catch {
      return;
    }

    const allDbPackages = await this.loadAllDbPackageNames();
    const orphans = allDbPackages.size > 0
      ? localPackages.filter((name) => allDbPackages.has(name) && !dbPackages.has(name))
      : [];

    if (orphans.length === 0) return;

    this.logger.log(`Found ${orphans.length} orphan packages to clean up: ${orphans.join(', ')}`);

    const locked = await this.packageManagementService.acquireMachineLock();
    if (!locked) return;

    try {
      await this.packageManagementService.uninstallOrphan(orphans);
    } finally {
      await this.packageManagementService.releaseMachineLock();
    }
  }

  private async loadAllDbPackageNames(): Promise<Set<string>> {
    try {
      const result = await this.queryBuilder.select({
        tableName: 'package_definition',
        fields: ['name'],
        filter: { type: 'Server' },
      });
      return new Set(result.data.map((p: any) => p.name));
    } catch {
      return new Set();
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
