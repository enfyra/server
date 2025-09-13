import { Injectable, Logger } from '@nestjs/common';
import { DataSourceService } from '../../../core/database/data-source/data-source.service';
import { RedisLockService } from './redis-lock.service';
import { Repository, IsNull } from 'typeorm';
import { GLOBAL_ROUTES_KEY } from '../../../shared/utils/constant';

const STALE_ROUTES_KEY = 'stale:routes';
const REVALIDATING_KEY = 'revalidating:routes';

@Injectable()
export class RouteCacheService {
  private readonly logger = new Logger(RouteCacheService.name);

  constructor(
    private readonly dataSourceService: DataSourceService,
    private readonly redisLockService: RedisLockService,
  ) {}

  private async loadRoutes(): Promise<any[]> {
    const routeDefRepo: Repository<any> =
      this.dataSourceService.getRepository('route_definition');
    const hookRepo: Repository<any> =
      this.dataSourceService.getRepository('hook_definition');

    const [globalHooks, routes] = await Promise.all([
      hookRepo.find({
        where: { isEnabled: true, route: IsNull() },
        order: { priority: 'ASC' },
        relations: ['methods', 'route'],
      }),
      routeDefRepo
        .createQueryBuilder('route')
        .leftJoinAndSelect('route.mainTable', 'mainTable')
        .leftJoinAndSelect('route.targetTables', 'targetTables')
        .leftJoinAndSelect(
          'route.hooks',
          'hooks',
          'hooks.isEnabled = :enabled',
          { enabled: true },
        )
        .leftJoinAndSelect('hooks.methods', 'hooks_method')
        .leftJoinAndSelect('hooks.route', 'hooks_route')
        .leftJoinAndSelect('route.handlers', 'handlers')
        .leftJoinAndSelect('handlers.method', 'handlers_method')
        .leftJoinAndSelect(
          'route.routePermissions',
          'routePermissions',
          'routePermissions.isEnabled = :enabled',
          { enabled: true },
        )
        .leftJoinAndSelect('routePermissions.role', 'role')
        .leftJoinAndSelect('routePermissions.allowedUsers', 'allowedUsers')
        .leftJoinAndSelect('routePermissions.methods', 'methods')
        .leftJoinAndSelect('route.publishedMethods', 'publishedMethods')
        .where('route.isEnabled = :enabled', { enabled: true })
        .getMany(),
    ]);

    // Merge global hooks into each route
    for (const route of routes) {
      route.hooks = [
        ...(globalHooks || []),
        ...(route.hooks ?? []).sort(
          (a, b) => (a.priority ?? 0) - (b.priority ?? 0),
        ),
      ];
    }

    return routes;
  }

  async loadAndCacheRoutes(): Promise<any[]> {
    const loadId = Math.random().toString(36).substring(7);
    const loadStart = Date.now();

    this.logger.log(`[LOAD:${loadId}] 📊 Loading routes from database...`);
    const routes = await this.loadRoutes();
    this.logger.log(
      `[LOAD:${loadId}] 📊 Loaded ${routes.length} routes in ${Date.now() - loadStart}ms`,
    );

    const cacheStart = Date.now();
    // Update both main cache and stale cache
    await Promise.all([
      this.redisLockService.acquire(GLOBAL_ROUTES_KEY, routes, 60000), // 1 minute
      this.redisLockService.set(STALE_ROUTES_KEY, routes, 0),
    ]);
    this.logger.log(
      `[LOAD:${loadId}] 💾 Cached routes in ${Date.now() - cacheStart}ms`,
    );

    return routes;
  }

  async reloadRouteCache(): Promise<void> {
    const reloadId = Math.random().toString(36).substring(7);

    try {
      this.logger.log(
        `[RELOAD:${reloadId}] 🔄 Manual route cache reload requested...`,
      );
      const reloadStart = Date.now();

      const routes = await this.loadRoutes();
      this.logger.log(
        `[RELOAD:${reloadId}] 📊 Loaded ${routes.length} routes in ${Date.now() - reloadStart}ms`,
      );

      const cacheStart = Date.now();
      await Promise.all([
        this.redisLockService.set(GLOBAL_ROUTES_KEY, routes, 60000),
        this.redisLockService.set(STALE_ROUTES_KEY, routes, 0),
      ]);
      this.logger.log(
        `[RELOAD:${reloadId}] 💾 Updated cache in ${Date.now() - cacheStart}ms`,
      );

      this.logger.log(
        `[RELOAD:${reloadId}] ✅ Reloaded route cache with ${routes.length} routes in ${Date.now() - reloadStart}ms total`,
      );
    } catch (error) {
      this.logger.error(
        `[RELOAD:${reloadId}] ❌ Failed to reload route cache:`,
        error.stack || error.message,
      );
    }
  }

  async getRoutesWithSWR(): Promise<any[]> {
    const overallStart = Date.now();

    // Try to get fresh routes from cache
    const cacheStart = Date.now();
    const cachedRoutes = await this.redisLockService.get(GLOBAL_ROUTES_KEY);
    const cacheTime = Date.now() - cacheStart;

    if (cachedRoutes) {
      if (cacheTime > 10) {
        const requestId = Math.random().toString(36).substring(7);
        this.logger.warn(
          `[SWR:${requestId}] ⚠️ Cache hit but Redis slow: ${cacheTime}ms`,
        );
      }
      return cachedRoutes;
    }

    // ❌ Cache miss - hết TTL, bắt đầu SWR logic
    const requestId = Math.random().toString(36).substring(7);

    this.logger.log(
      `[SWR:${requestId}] ❌ Cache EXPIRED (Redis: ${cacheTime}ms) - checking stale data...`,
    );

    // Cache miss - check if we have stale data in Redis to return immediately
    const staleStart = Date.now();
    const [staleRoutes, isRevalidating] = await Promise.all([
      this.redisLockService.get(STALE_ROUTES_KEY),
      this.redisLockService.get(REVALIDATING_KEY),
    ]);
    const staleTime = Date.now() - staleStart;

    this.logger.log(
      `[SWR:${requestId}] Stale check (${staleTime}ms): ${staleRoutes ? `${staleRoutes.length} routes` : 'NONE'}, Revalidating: ${!!isRevalidating}`,
    );

    if (staleRoutes) {
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
        `[SWR:${requestId}] ⚡ Serving STALE data - returned ${staleRoutes.length} routes in ${totalTime}ms (cache:${cacheTime}ms + stale:${staleTime}ms)`,
      );
      return staleRoutes;
    }

    // No stale data available - fetch synchronously
    this.logger.warn(
      `[SWR:${requestId}] 🐌 SLOW PATH - No cache, no stale data - fetching from DB...`,
    );
    const routes = await this.loadAndCacheRoutes();
    const totalTime = Date.now() - overallStart;
    this.logger.warn(
      `[SWR:${requestId}] 🐌 DB fetch completed - ${routes.length} routes in ${totalTime}ms`,
    );
    return routes;
  }

  private async backgroundRevalidate(): Promise<void> {
    const bgId = Math.random().toString(36).substring(7);

    // Set revalidating flag in Redis (multi-instance safe)
    const acquired = await this.redisLockService.acquire(
      REVALIDATING_KEY,
      'true',
      30000, // 30s TTL for revalidation lock
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
      await this.reloadRouteCache();
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
      const released = await this.redisLockService.release(
        REVALIDATING_KEY,
        'true',
      );
      this.logger.log(
        `[BG:${bgId}] 🔓 Released revalidation lock: ${released}`,
      );
    }
  }
}
