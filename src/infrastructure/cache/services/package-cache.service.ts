import { Injectable } from '@nestjs/common';
import { OnEvent, EventEmitter2 } from '@nestjs/event-emitter';
import { QueryBuilderService } from '../../query-builder/query-builder.service';
import { RedisPubSubService } from './redis-pubsub.service';
import { InstanceService } from '../../../shared/services/instance.service';
import { PackageManagementService } from '../../../modules/package-management/services/package-management.service';
import { BaseCacheService, CacheConfig } from './base-cache.service';
import { PACKAGE_CACHE_SYNC_EVENT_KEY } from '../../../shared/utils/constant';
import { CACHE_EVENTS, CACHE_IDENTIFIERS, shouldReloadCache } from '../../../shared/utils/cache-events.constants';
import { DynamicWebSocketGateway } from '../../../modules/websocket/gateway/dynamic-websocket.gateway';

const PACKAGE_CONFIG: CacheConfig = {
  syncEventKey: PACKAGE_CACHE_SYNC_EVENT_KEY,
  cacheIdentifier: CACHE_IDENTIFIERS.PACKAGE,
  colorCode: '\x1b[35m',
  cacheName: 'PackageCache',
};

const ADMIN_WS_PATH = '/admin';
const SYSTEM_EVENT_PREFIX = '$system:package';

@Injectable()
export class PackageCacheService extends BaseCacheService<string[]> {
  constructor(
    private readonly queryBuilder: QueryBuilderService,
    redisPubSubService: RedisPubSubService,
    instanceService: InstanceService,
    eventEmitter: EventEmitter2,
    private readonly packageManagementService: PackageManagementService,
    private readonly websocketGateway: DynamicWebSocketGateway,
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
    this.ensurePackagesInstalled().catch((error) => {
      this.logger.error(`Package install failed (non-blocking): ${error.message}`);
    });

    this.ensurePackagesCleanedUp().catch((error) => {
      this.logger.error(`Package cleanup failed (non-blocking): ${error.message}`);
    });
  }

  private emitEvent(event: string, data: any) {
    try {
      this.websocketGateway.emitToNamespace(
        ADMIN_WS_PATH,
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

  private async ensurePackagesInstalled(): Promise<void> {
    const packagesWithMeta = await this.loadPackagesForSync();

    const candidates = packagesWithMeta.filter(
      (pkg) => pkg.status === 'installed' || pkg.status === 'failed',
    );

    const missing = candidates.filter(
      (pkg) => !this.packageManagementService.isPackageInstalled(pkg.name),
    );

    if (missing.length === 0) return;

    this.logger.log(`${missing.length} packages missing, acquiring machine lock...`);

    for (const pkg of missing) {
      await this.updatePackageStatus(pkg.id, 'installing', { lastError: null });
    }

    this.emitEvent('installing', {
      packages: missing.map((p) => ({ id: p.id, name: p.name })),
    });

    const locked = await this.packageManagementService.acquireMachineLock();
    if (!locked) {
      this.logger.warn('Could not acquire machine lock, skipping package install');
      for (const pkg of missing) {
        await this.updatePackageStatus(pkg.id, 'failed', {
          lastError: 'Could not acquire machine lock',
        });
      }
      this.emitEvent('failed', {
        packages: missing.map((p) => ({ id: p.id, name: p.name })),
        error: 'Could not acquire machine lock',
        operation: 'install',
      });
      return;
    }

    try {
      const stillMissing = missing.filter(
        (pkg) => !this.packageManagementService.isPackageInstalled(pkg.name),
      );

      if (stillMissing.length === 0) {
        this.logger.log('All packages already installed (by another instance)');
        for (const pkg of missing) {
          await this.updatePackageStatus(pkg.id, 'installed', { lastError: null });
        }
        await this.reload();
        this.emitEvent('installed', {
          packages: missing.map((p) => ({ id: p.id, name: p.name })),
        });
        return;
      }

      this.logger.log(`Installing ${stillMissing.length} missing packages...`);

      try {
        await this.packageManagementService.installBatch(
          stillMissing.map((p) => ({
            name: p.name,
            version: p.version,
            timeoutMs: (p.installTimeout || 60) * 1000,
          })),
        );

        await this.packageManagementService.renewMachineLock();

        for (const pkg of stillMissing) {
          const isNowInstalled =
            this.packageManagementService.isPackageInstalled(pkg.name);
          if (isNowInstalled) {
            await this.updatePackageStatus(pkg.id, 'installed', { lastError: null });
            this.emitEvent('installed', { id: pkg.id, name: pkg.name });
          } else {
            await this.updatePackageStatus(pkg.id, 'failed', {
              lastError: 'Package not found in node_modules after batch install',
            });
            this.emitEvent('failed', {
              id: pkg.id,
              name: pkg.name,
              error: 'Package not found in node_modules after batch install',
              operation: 'install',
            });
          }
        }

        await this.reload();
      } catch (batchError) {
        this.logger.error(`Batch install failed: ${batchError.message}`);

        for (const pkg of stillMissing) {
          const isNowInstalled =
            this.packageManagementService.isPackageInstalled(pkg.name);
          if (isNowInstalled) {
            await this.updatePackageStatus(pkg.id, 'installed', { lastError: null });
            this.emitEvent('installed', { id: pkg.id, name: pkg.name });
          } else {
            await this.updatePackageStatus(pkg.id, 'failed', {
              lastError: batchError.message,
            });
            this.emitEvent('failed', {
              id: pkg.id,
              name: pkg.name,
              error: batchError.message,
              operation: 'install',
            });
          }
        }

        await this.reload();
      }
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

  private async loadPackagesForSync(): Promise<
    Array<{ id: string | number; name: string; version: string; installTimeout: number; status: string }>
  > {
    const result = await this.queryBuilder.select({
      tableName: 'package_definition',
      fields: ['id', 'name', 'version', 'installTimeout', 'status'],
      filter: {
        isEnabled: true,
        type: 'Server',
      },
    });

    return result.data.map((p: any) => ({
      id: p.id || p._id,
      name: p.name,
      version: p.version || 'latest',
      installTimeout: p.installTimeout || 60,
      status: p.status || 'installed',
    }));
  }

  async getPackages(): Promise<string[]> {
    await this.ensureLoaded();
    return this.cache;
  }
}
