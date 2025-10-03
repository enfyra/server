import { Injectable, Logger } from '@nestjs/common';
import { DataSourceService } from '../../../core/database/data-source/data-source.service';
import { CacheService } from './cache.service';
import { Repository } from 'typeorm';
import { REDIS_TTL } from '../../../shared/utils/constant';

const GLOBAL_PACKAGES_KEY = 'global:packages';
const STALE_PACKAGES_KEY = 'stale:packages';
const REVALIDATING_PACKAGES_KEY = 'revalidating:packages';

@Injectable()
export class PackageCacheService {
  private readonly logger = new Logger(PackageCacheService.name);

  constructor(
    private readonly dataSourceService: DataSourceService,
    private readonly cacheService: CacheService,
  ) {}

  private async loadPackages(): Promise<string[]> {
    const packageRepo: Repository<any> =
      this.dataSourceService.getRepository('package_definition');

    const packages = await packageRepo
      .createQueryBuilder('package')
      .select('package.name')
      .where('package.isEnabled = :enabled', { enabled: true })
      .andWhere('package.type = :type', { type: 'Backend' })
      .getMany();

    return packages.map(p => p.name);
  }

  async loadAndCachePackages(): Promise<string[]> {
    const loadId = Math.random().toString(36).substring(7);
    const loadStart = Date.now();

    this.logger.log(`[LOAD:${loadId}] 📦 Loading packages from database...`);
    const packages = await this.loadPackages();
    this.logger.log(
      `[LOAD:${loadId}] 📦 Loaded ${packages.length} packages in ${Date.now() - loadStart}ms`,
    );

    const cacheStart = Date.now();
    // Update both main cache and stale cache
    await Promise.all([
      this.cacheService.acquire(GLOBAL_PACKAGES_KEY, packages, 300000), // 5 minutes
      this.cacheService.set(STALE_PACKAGES_KEY, packages, 0),
    ]);
    this.logger.log(
      `[LOAD:${loadId}] 💾 Cached packages in ${Date.now() - cacheStart}ms`,
    );

    return packages;
  }

  async reloadPackageCache(): Promise<void> {
    const reloadId = Math.random().toString(36).substring(7);

    try {
      this.logger.log(
        `[RELOAD:${reloadId}] 🔄 Manual package cache reload requested...`,
      );
      const reloadStart = Date.now();

      const packages = await this.loadPackages();
      this.logger.log(
        `[RELOAD:${reloadId}] 📦 Loaded ${packages.length} packages in ${Date.now() - reloadStart}ms`,
      );

      const cacheStart = Date.now();
      await Promise.all([
        this.cacheService.set(GLOBAL_PACKAGES_KEY, packages, REDIS_TTL.PACKAGES_CACHE_TTL),
        this.cacheService.set(STALE_PACKAGES_KEY, packages, REDIS_TTL.STALE_CACHE_TTL),
      ]);
      this.logger.log(
        `[RELOAD:${reloadId}] 💾 Updated cache in ${Date.now() - cacheStart}ms`,
      );

      this.logger.log(
        `[RELOAD:${reloadId}] ✅ Reloaded package cache with ${packages.length} packages in ${Date.now() - reloadStart}ms total`,
      );
    } catch (error) {
      this.logger.error(
        `[RELOAD:${reloadId}] ❌ Failed to reload package cache:`,
        error.stack || error.message,
      );
    }
  }

  async getPackagesWithSWR(): Promise<string[]> {
    const overallStart = Date.now();

    // Try to get fresh packages from cache
    const cacheStart = Date.now();
    const cachedPackages = await this.cacheService.get(GLOBAL_PACKAGES_KEY);
    const cacheTime = Date.now() - cacheStart;

    if (cachedPackages) {
      if (cacheTime > 10) {
        const requestId = Math.random().toString(36).substring(7);
        this.logger.warn(
          `[SWR:${requestId}] ⚠️ Cache hit but Redis slow: ${cacheTime}ms`,
        );
      }
      return cachedPackages;
    }

    // ❌ Cache miss - hết TTL, bắt đầu SWR logic
    const requestId = Math.random().toString(36).substring(7);

    this.logger.log(
      `[SWR:${requestId}] ❌ Cache EXPIRED (Redis: ${cacheTime}ms) - checking stale data...`,
    );

    // Cache miss - check if we have stale data in Redis to return immediately
    const staleStart = Date.now();
    const [stalePackages, isRevalidating] = await Promise.all([
      this.cacheService.get(STALE_PACKAGES_KEY),
      this.cacheService.get(REVALIDATING_PACKAGES_KEY),
    ]);
    const staleTime = Date.now() - staleStart;

    this.logger.log(
      `[SWR:${requestId}] Stale check (${staleTime}ms): ${stalePackages ? `${stalePackages.length} packages` : 'NONE'}, Revalidating: ${!!isRevalidating}`,
    );

    if (stalePackages) {
      if (!isRevalidating) {
        this.logger.log(
          `[SWR:${requestId}] 🔄 Starting background revalidation...`,
        );
        // Start background revalidation (non-blocking)
        this.backgroundRevalidate().catch((err) =>
          this.logger.error(
            `[SWR:${requestId}] Background revalidation error:`,
            err,
          ),
        );
      } else {
        this.logger.log(
          `[SWR:${requestId}] ⏳ Already revalidating, skip background task`,
        );
      }

      const totalTime = Date.now() - overallStart;
      this.logger.log(
        `[SWR:${requestId}] ⚡ Serving STALE data - returned ${stalePackages.length} packages in ${totalTime}ms (cache:${cacheTime}ms + stale:${staleTime}ms)`,
      );
      return stalePackages;
    }

    // No stale data available - fetch synchronously
    this.logger.warn(
      `[SWR:${requestId}] 🐌 SLOW PATH - No cache, no stale data - fetching from DB...`,
    );
    const packages = await this.loadAndCachePackages();
    const totalTime = Date.now() - overallStart;
    this.logger.warn(
      `[SWR:${requestId}] 🐌 DB fetch completed - ${packages.length} packages in ${totalTime}ms`,
    );
    return packages;
  }

  private async backgroundRevalidate(): Promise<void> {
    const bgId = Math.random().toString(36).substring(7);

    // Set revalidating flag in Redis (multi-instance safe)
    const acquired = await this.cacheService.acquire(
      REVALIDATING_PACKAGES_KEY,
      'true',
      REDIS_TTL.REVALIDATION_LOCK_TTL,
    );

    if (!acquired) {
      this.logger.log(
        `[BG:${bgId}] ⏸️ Another instance is already revalidating - skipping`,
      );
      return; // Another instance is already revalidating
    }

    this.logger.log(`[BG:${bgId}] 🔄 Starting background revalidation...`);
    const bgStart = Date.now();

    try {
      await this.reloadPackageCache();
      this.logger.log(
        `[BG:${bgId}] ✅ Background revalidation completed in ${Date.now() - bgStart}ms`,
      );
    } catch (error) {
      this.logger.error(
        `[BG:${bgId}] ❌ Background revalidation failed:`,
        error,
      );
    } finally {
      // Clear revalidating flag
      const released = await this.cacheService.release(
        REVALIDATING_PACKAGES_KEY,
        'true',
      );
      this.logger.log(
        `[BG:${bgId}] 🔓 Released revalidation lock: ${released}`,
      );
    }
  }
}