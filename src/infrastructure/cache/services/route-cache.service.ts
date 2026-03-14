import { Injectable } from '@nestjs/common';
import { OnEvent, EventEmitter2 } from '@nestjs/event-emitter';
import { QueryBuilderService } from '../../query-builder/query-builder.service';
import { RedisPubSubService } from './redis-pubsub.service';
import { CacheService } from './cache.service';
import { InstanceService } from '../../../shared/services/instance.service';
import { BaseCacheService, CacheConfig } from './base-cache.service';
import {
  ROUTE_CACHE_SYNC_EVENT_KEY,
  ROUTE_RELOAD_LOCK_KEY,
} from '../../../shared/utils/constant';
import { EnfyraRouteEngine } from '../../../shared/utils/enfyra-route-engine';
import { transformCode } from '../../handler-executor/code-transformer';
import { CACHE_EVENTS, CACHE_IDENTIFIERS, shouldReloadCache } from '../../../shared/utils/cache-events.constants';
import { MetadataCacheService } from './metadata-cache.service';

const ROUTE_CONFIG: CacheConfig = {
  syncEventKey: ROUTE_CACHE_SYNC_EVENT_KEY,
  lockKey: ROUTE_RELOAD_LOCK_KEY,
  cacheIdentifier: CACHE_IDENTIFIERS.ROUTE,
  colorCode: '\x1b[31m',
  cacheName: 'RouteCache',
};

interface RouteData {
  routes: any[];
  methods: string[];
}

@Injectable()
export class RouteCacheService extends BaseCacheService<RouteData> {
  private routeEngine: EnfyraRouteEngine;
  private allMethods: string[] = [];

  constructor(
    private readonly queryBuilder: QueryBuilderService,
    private readonly metadataCacheService: MetadataCacheService,
    redisPubSubService: RedisPubSubService,
    cacheService: CacheService,
    instanceService: InstanceService,
    eventEmitter: EventEmitter2,
  ) {
    super(ROUTE_CONFIG, redisPubSubService, cacheService, instanceService, eventEmitter);
    this.routeEngine = new EnfyraRouteEngine(false);
    this.cache = { routes: [], methods: [] };
  }

  @OnEvent(CACHE_EVENTS.METADATA_LOADED)
  async onMetadataLoaded() {
    await this.metadataCacheService.getMetadata();
    await this.reload();
  }

  @OnEvent(CACHE_EVENTS.INVALIDATE)
  async handleCacheInvalidation(payload: { tableName: string; action: string }) {
    if (shouldReloadCache(payload.tableName, this.config.cacheIdentifier)) {
      this.logger.log(`Cache invalidation event received for table: ${payload.tableName}`);
      await this.reload();
    }
  }

  protected async loadFromDb(): Promise<any> {
    if (this.allMethods.length === 0) {
      const methodsResult = await this.queryBuilder.select({
        tableName: 'method_definition',
      });
      this.allMethods = methodsResult.data.map((m: any) => m.method);
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
        'preHook.methods.method',
        'postHook.*',
        'postHook.methods.method',
        'publishedMethods.*',
        'availableMethods.*',
        'targetTables.*',
      ],
    });

    const routes = result.data;
    const isMongoDB = this.queryBuilder.isMongoDb();

    const [globalPreHooks, globalPostHooks] = await Promise.all([
      this.loadGlobalHooks('pre_hook_definition'),
      this.loadGlobalHooks('post_hook_definition'),
    ]);

    for (const route of routes) {
      this.mergeHooks(route, globalPreHooks, globalPostHooks, isMongoDB);

      if (!route.targetTables) {
        route.targetTables = [];
      }

      this.transformRouteCode(route);
    }

    return { routes, methods: this.allMethods };
  }

  private async loadGlobalHooks(tableName: string): Promise<any[]> {
    const result = await this.queryBuilder.select({
      tableName,
      filter: {
        _and: [
          { isEnabled: { _eq: true } },
          { isGlobal: { _eq: true } },
        ],
      },
      fields: ['*', 'methods.method'],
      sort: ['priority'],
    });

    return result.data.map((hook: any) => {
      if (hook.code) {
        hook.code = transformCode(hook.code);
      }
      return hook;
    });
  }

  private mergeHooks(route: any, globalPreHooks: any[], globalPostHooks: any[], isMongoDB: boolean): void {
    const enabledRoutePreHooks = Array.isArray(route.preHook)
      ? route.preHook.filter((h: any) => h?.isEnabled === true && h?.isGlobal === false)
      : [];

    const allPreHooks = [...globalPreHooks, ...enabledRoutePreHooks];
    route.preHook = this.uniqueHooks(allPreHooks, isMongoDB);

    const enabledRoutePostHooks = Array.isArray(route.postHook)
      ? route.postHook.filter((h: any) => h?.isEnabled === true && h?.isGlobal === false)
      : [];

    const allPostHooks = [...globalPostHooks, ...enabledRoutePostHooks];
    route.postHook = this.uniqueHooks(allPostHooks, isMongoDB);
  }

  private uniqueHooks(hooks: any[], isMongoDB: boolean): any[] {
    return hooks.filter((hook, index, self) =>
      index === self.findIndex((h) => {
        const hId = isMongoDB ? h?._id?.toString() : h?.id;
        const hookId = isMongoDB ? hook?._id?.toString() : hook?.id;
        return hId === hookId;
      })
    );
  }

  private transformRouteCode(route: any): void {
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

  protected transformData(data: { routes: any[]; methods: string[] }): RouteData {
    return data;
  }

  protected async afterTransform(data: RouteData): Promise<void> {
    this.buildRouteEngine(data.routes);
  }

  protected handleSyncData(data: RouteData): void {
    this.cache = data;
    this.allMethods = data.methods || [];
    this.buildRouteEngine(data.routes);
  }

  protected deserializeSyncData(payload: any): any {
    return {
      routes: payload.routes,
      methods: payload.methods || [],
    };
  }

  protected serializeForPublish(data: RouteData): Record<string, any> {
    return {
      routes: data.routes,
      methods: data.methods,
    };
  }

  protected emitLoadedEvent(): void {
    this.eventEmitter?.emit(CACHE_EVENTS.ROUTE_LOADED);
  }

  protected getLogCount(): string {
    return `${this.cache.routes.length} routes, ${this.cache.methods.length} methods`;
  }

  protected getCount(): number {
    return this.cache.routes.length;
  }

  protected logSyncSuccess(payload: any): void {
    this.logger.log(`Cache synced: ${payload.routes?.length || 0} routes, ${payload.methods?.length || 0} methods`);
  }

  private buildRouteEngine(routes: any[]): void {
    this.routeEngine = new EnfyraRouteEngine(false);
    for (const route of routes) {
      this.insertRouteToEngine(route);
    }
  }

  private insertRouteToEngine(route: any): void {
    if (!route.path) {
      this.logger.warn(`Route has no path, skipping:`);
      this.logger.warn(JSON.stringify(route, null, 2));
      return;
    }

    const basePath = route.path;
    const raw = route.availableMethods;
    const methods =
      Array.isArray(raw) && raw.length > 0
        ? raw.map((m: any) => m?.method ?? m).filter(Boolean)
        : [];

    if (methods.length === 0) return;

    for (const method of methods) {
      this.routeEngine.insert(method, basePath, route);
      if (['DELETE', 'PATCH'].includes(method)) {
        this.routeEngine.insert(method, `${basePath}/:id`, route);
      }
    }
  }

  async getRoutes(): Promise<any[]> {
    await this.ensureLoaded();
    return this.cache.routes;
  }

  getRouteEngine(): EnfyraRouteEngine {
    return this.routeEngine;
  }
}
