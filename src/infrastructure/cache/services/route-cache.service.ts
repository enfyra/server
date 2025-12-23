import { Injectable, Logger, OnModuleInit, OnApplicationBootstrap } from '@nestjs/common';
import { QueryBuilderService } from '../../query-builder/query-builder.service';
import { RedisPubSubService } from './redis-pubsub.service';
import { CacheService } from './cache.service';
import { MetadataCacheService } from './metadata-cache.service';
import { InstanceService } from '../../../shared/services/instance.service';
import {
  ROUTE_CACHE_SYNC_EVENT_KEY,
  ROUTE_RELOAD_LOCK_KEY,
  REDIS_TTL,
} from '../../../shared/utils/constant';
import { EnfyraRouteEngine } from '../../../shared/utils/enfyra-route-engine';
import { transformCode } from '../../handler-executor/code-transformer';

@Injectable()
export class RouteCacheService implements OnModuleInit, OnApplicationBootstrap {
  private readonly logger = new Logger(RouteCacheService.name);
  private routesCache: any[] = [];
  private routeEngine: EnfyraRouteEngine;
  private cacheLoaded = false;
  private messageHandler: ((channel: string, message: string) => void) | null = null;
  private allMethods: string[] = [];

  constructor(
    private readonly queryBuilder: QueryBuilderService,
    private readonly redisPubSubService: RedisPubSubService,
    private readonly cacheService: CacheService,
    private readonly metadataCacheService: MetadataCacheService,
    private readonly instanceService: InstanceService,
  ) {
    this.routeEngine = new EnfyraRouteEngine(false);
  }

  async onModuleInit() {
    this.subscribe();
  }

  async onApplicationBootstrap() {
    await this.metadataCacheService.getMetadata();
    await this.reload();
  }

  private subscribe() {
    const sub = this.redisPubSubService.sub;
    if (!sub) {
      this.logger.warn('Redis subscription not available for route cache sync');
      return;
    }

    if (this.messageHandler) {
      return;
    }

    this.messageHandler = (channel: string, message: string) => {
      if (channel === ROUTE_CACHE_SYNC_EVENT_KEY) {
        try {
          const payload = JSON.parse(message);
          const myInstanceId = this.instanceService.getInstanceId();

          if (payload.instanceId === myInstanceId) {
            return;
          }

          this.logger.log(`Received route cache sync from instance ${payload.instanceId.slice(0, 8)}...`);

          this.routesCache = payload.routes;
          this.allMethods = payload.methods || [];
          this.buildRouteEngine(this.routesCache);

          this.cacheLoaded = true;
          this.logger.log(`Route cache synced: ${payload.routes.length} routes, ${this.allMethods.length} methods`);
        } catch (error) {
          this.logger.error('Failed to parse route cache sync message:', error);
        }
      }
    };

    // Subscribe via RedisPubSubService (prevents duplicates)
    this.redisPubSubService.subscribeWithHandler(
      ROUTE_CACHE_SYNC_EVENT_KEY,
      this.messageHandler
    );
  }

  async getRoutes(): Promise<any[]> {
    if (!this.cacheLoaded) {
      await this.reload();
    }
    return this.routesCache;
  }

  async reload(): Promise<void> {
    const instanceId = this.instanceService.getInstanceId();

    try {
      const acquired = await this.cacheService.acquire(
        ROUTE_RELOAD_LOCK_KEY,
        instanceId,
        REDIS_TTL.RELOAD_LOCK_TTL
      );

      if (!acquired) {
        this.logger.log('Another instance is reloading routes, waiting for broadcast...');
        return;
      }

      this.logger.log(`Acquired route reload lock (instance ${instanceId.slice(0, 8)})`);

      try {
        const start = Date.now();
        this.logger.log('Reloading routes cache...');

        const routes = await this.loadRoutes();
        this.logger.log(`Loaded ${routes.length} routes in ${Date.now() - start}ms`);

        this.routesCache = routes;
        await this.publish(routes);
        this.buildRouteEngine(routes);

        this.cacheLoaded = true;
      } finally {
        await this.cacheService.release(ROUTE_RELOAD_LOCK_KEY, instanceId);
        this.logger.log('Released route reload lock');
      }
    } catch (error) {
      this.logger.error('Failed to reload route cache:', error);
      throw error;
    }
  }
  
  private async publish(routes: any[]): Promise<void> {
    try {
      const payload = {
        instanceId: this.instanceService.getInstanceId(),
        routes: routes,
        methods: this.allMethods,
        timestamp: Date.now(),
      };

      await this.redisPubSubService.publish(
        ROUTE_CACHE_SYNC_EVENT_KEY,
        JSON.stringify(payload),
      );

      this.logger.log(`Published route cache to other instances (${routes.length} routes, ${this.allMethods.length} methods)`);
    } catch (error) {
      this.logger.error('Failed to publish route cache sync:', error);
    }
  }

  private async loadRoutes(): Promise<any[]> {
    if (this.allMethods.length === 0) {
      const methodsResult = await this.queryBuilder.select({
        tableName: 'method_definition',
      });
      this.allMethods = methodsResult.data.map((m: any) => m.method);
      this.logger.log(`Loaded ${this.allMethods.length} methods: [${this.allMethods.join(', ')}]`);
    }

    const result = await this.queryBuilder.select({
      tableName: 'route_definition',
      filter: { isEnabled: { _eq: true } },
      fields: [
        '*',
        'mainTable.*',
        'handlers.*',
        'handlers.method.*',
        'routePermissions.*',
        'routePermissions.role.*',
        'preHook.*',
        'preHook.methods.*',
        'postHook.*',
        'postHook.methods.*',
        'publishedMethods.*',
        'targetTables.*',
      ],
    });
    const routes = result.data;

    const isMongoDB = this.queryBuilder.isMongoDb();
    const routeFieldName = isMongoDB ? 'route' : 'routeId';

    const globalPreHooksResult = await this.queryBuilder.select({
      tableName: 'pre_hook_definition',
      filter: {
        _and: [
          {
            isEnabled: { _eq: true },
          },
          {
            [routeFieldName]: { _is_null: true }
          }
        ],
      },
      fields: ['*', 'methods.*'],
      sort: ['priority'],
    });
    const globalPreHooks = globalPreHooksResult.data;

    const globalPostHooksResult = await this.queryBuilder.select({
      tableName: 'post_hook_definition',
      filter: {
        _and: [
          {
            isEnabled: { _eq: true },
          },
          {
            [routeFieldName]: { _is_null: true }
          }
        ],
      },
      fields: ['*', 'methods.*'],
      sort: ['priority'],
    });
    const globalPostHooks = globalPostHooksResult.data;

    for (const hook of globalPreHooks) {
      if (hook.code) {
        hook.code = transformCode(hook.code);
      }
    }
    for (const hook of globalPostHooks) {
      if (hook.code) {
        hook.code = transformCode(hook.code);
      }
    }

    for (const route of routes) {
      const enabledRoutePreHooks = Array.isArray(route.preHook)
        ? route.preHook.filter((h: any) => h?.isEnabled === true)
        : [];
      const allPreHooks = [...globalPreHooks, ...enabledRoutePreHooks];

      const uniquePreHooks = allPreHooks.filter((hook, index, self) =>
        index === self.findIndex((h) => {
          const hId = isMongoDB ? h?._id?.toString() : h?.id;
          const hookId = isMongoDB ? hook?._id?.toString() : hook?.id;
          return hId === hookId;
        })
      );

      route.preHook = uniquePreHooks;

      const enabledRoutePostHooks = Array.isArray(route.postHook)
        ? route.postHook.filter((h: any) => h?.isEnabled === true)
        : [];
      const allPostHooks = [...globalPostHooks, ...enabledRoutePostHooks];

      const uniquePostHooks = allPostHooks.filter((hook, index, self) =>
        index === self.findIndex((h) => {
          const hId = isMongoDB ? h?._id?.toString() : h?.id;
          const hookId = isMongoDB ? hook?._id?.toString() : hook?.id;
          return hId === hookId;
        })
      );

      route.postHook = uniquePostHooks;

      if (!route.targetTables) {
        route.targetTables = [];
      }

      if (route.handlers && Array.isArray(route.handlers)) {
        for (const handler of route.handlers) {
          if (handler.logic) {
            handler.logic = transformCode(handler.logic);
          }
        }
      }

      if (route.preHook && Array.isArray(route.preHook)) {
        for (const hook of route.preHook) {
          if (hook.code) {
            hook.code = transformCode(hook.code);
          }
        }
      }
      if (route.postHook && Array.isArray(route.postHook)) {
        for (const hook of route.postHook) {
          if (hook.code) {
            hook.code = transformCode(hook.code);
          }
        }
      }
    }

    return routes;
  }

  private buildRouteEngine(routes: any[]): void {
    const startTime = Date.now();
    this.routeEngine = new EnfyraRouteEngine(false);

    let insertedCount = 0;
    for (const route of routes) {
      this.insertRouteToEngine(route);
      insertedCount++;
    }

    const stats = this.routeEngine.getStats();
    this.logger.log(
      `Built Enfyra Route Engine: ${stats.totalRoutes} route entries from ${insertedCount} route definitions across methods [${stats.methods.join(', ')}] in ${Date.now() - startTime}ms`
    );
  }

  private insertRouteToEngine(route: any): void {
    if (!route.path) {
      this.logger.warn(`Route has no path, skipping:`);
      this.logger.warn(JSON.stringify(route, null, 2));
      return;
    }

    const basePath = route.path;

    for (const method of this.allMethods) {
      this.routeEngine.insert(method, basePath, route);

      // Add :id variant for DELETE and PATCH (REST methods only, not GraphQL)
      if (['DELETE', 'PATCH'].includes(method)) {
        this.routeEngine.insert(method, `${basePath}/:id`, route);
      }
    }
  }

  getRouteEngine(): EnfyraRouteEngine {
    return this.routeEngine;
  }
}
