import { EventEmitter2 } from 'eventemitter2';
import { QueryBuilderService } from '@enfyra/kernel';
import { PackageCdnLoaderService } from './package-cdn-loader.service';
import { BaseCacheService, CacheConfig } from './base-cache.service';
import { RedisRuntimeCacheStore } from './redis-runtime-cache-store.service';
import { CACHE_IDENTIFIERS } from '../../../shared/utils/cache-events.constants';
import type { Cradle } from '../../../container';
import { RuntimeRegistryService } from './runtime-registry.service';

const PACKAGE_CONFIG: CacheConfig = {
  cacheIdentifier: CACHE_IDENTIFIERS.PACKAGE,
  colorCode: '\x1b[35m',
  cacheName: 'PackageCache',
};

export class PackageCacheService extends BaseCacheService<string[]> {
  private readonly queryBuilderService: QueryBuilderService;
  private readonly packageCdnLoaderService: PackageCdnLoaderService;
  private readonly runtimeRegistryService?: RuntimeRegistryService;

  constructor(deps: {
    queryBuilderService: QueryBuilderService;
    eventEmitter: EventEmitter2;
    packageCdnLoaderService: PackageCdnLoaderService;
    runtimeRegistryService?: RuntimeRegistryService;
    lazyRef: Cradle;
    redisRuntimeCacheStore?: RedisRuntimeCacheStore;
  }) {
    super(PACKAGE_CONFIG, deps.eventEmitter, deps.redisRuntimeCacheStore);
    this.queryBuilderService = deps.queryBuilderService;
    this.packageCdnLoaderService = deps.packageCdnLoaderService;
    this.runtimeRegistryService = deps.runtimeRegistryService;
  }

  protected async loadFromDb(): Promise<string[]> {
    const result = await this.queryBuilderService.find({
      table: 'enfyra_package',
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

  getCdnLoader(): PackageCdnLoaderService {
    return this.packageCdnLoaderService;
  }

  async getPackages(): Promise<string[]> {
    return (
      this.runtimeRegistryService?.getSnapshot<string[]>(
        CACHE_IDENTIFIERS.PACKAGE,
      )?.data ?? (await this.getCacheAsync())
    );
  }
}
