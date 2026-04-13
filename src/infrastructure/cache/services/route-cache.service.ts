import { Injectable } from '@nestjs/common';
import { EventEmitter2 } from '@nestjs/event-emitter';
import { QueryBuilderService } from '../../query-builder/query-builder.service';
import { BaseCacheService, CacheConfig } from './base-cache.service';
import { EnfyraRouteEngine } from '../../../shared/utils/enfyra-route-engine';
import { transformCode } from '../../executor-engine/code-transformer';
import {
  CACHE_EVENTS,
  CACHE_IDENTIFIERS,
} from '../../../shared/utils/cache-events.constants';
import { TCacheInvalidationPayload } from '../../../shared/types/cache.types';
const ROUTE_CONFIG: CacheConfig = {
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
  private globalPreHooks: any[] = [];
  private globalPostHooks: any[] = [];

  constructor(
    private readonly queryBuilder: QueryBuilderService,
    eventEmitter: EventEmitter2,
  ) {
    super(ROUTE_CONFIG, eventEmitter);
    this.routeEngine = new EnfyraRouteEngine(false);
    this.cache = { routes: [], methods: [] };
  }

  supportsPartialReload(): boolean {
    return true;
  }

  protected async applyPartialUpdate(
    payload: TCacheInvalidationPayload,
  ): Promise<void> {
    const affectedTableNames = new Set<string>(payload.affectedTables || []);

    if (payload.table === 'table_definition' && payload.ids?.length) {
      const isMongoDB = this.queryBuilder.isMongoDb();
      const idField = DatabaseConfigService.getPkField();
      const mainTableField = isMongoDB ? 'mainTable' : 'mainTableId';

      const result = await this.queryBuilder.find({
        table: 'route_definition',
        filter: { [mainTableField]: { _in: payload.ids } },
        fields: [idField],
      });
      const routeIds = result.data.map((r: any) => r[idField]).filter(Boolean);
      if (routeIds.length > 0) {
        await this.reloadSpecificRoutes(routeIds);
        return;
      }
      for (const route of this.cache.routes) {
        const mainTableId = isMongoDB
          ? String(route.mainTable?._id)
          : route.mainTable?.id;
        if (payload.ids.some((id) => String(id) === String(mainTableId))) {
          affectedTableNames.add(route.mainTable?.name);
        }
      }
      if (affectedTableNames.size > 0) {
        this.cache.routes = this.cache.routes.filter((r: any) => {
          return !affectedTableNames.has(r.mainTable?.name);
        });
        this.buildRouteEngine(this.cache.routes);
        return;
      }
      return;
    }

    if (payload.table === 'route_definition' && payload.ids?.length) {
      await this.reloadSpecificRoutes(payload.ids);
      return;
    }

    if (
      ['route_handler_definition', 'route_permission_definition'].includes(
        payload.table,
      ) &&
      payload.ids?.length
    ) {
      const routeIds = await this.findRouteIdsForChildRecords(
        payload.table,
        payload.ids,
      );
      if (routeIds.length > 0) {
        await this.reloadSpecificRoutes(routeIds);
        return;
      }
    }

    if (
      ['pre_hook_definition', 'post_hook_definition'].includes(
        payload.table,
      )
    ) {
      await this.reloadGlobalHooksAndMerge();
      return;
    }

    if (['role_definition', 'method_definition'].includes(payload.table)) {
      await this.reload();
      return;
    }

    if (affectedTableNames.size > 0) {
      const routeIds: (string | number)[] = [];
      for (const route of this.cache.routes) {
        if (affectedTableNames.has(route.mainTable?.name)) {
          const rid = this.queryBuilder.isMongoDb()
            ? route._id
            : route.id;
          routeIds.push(rid);
        }
      }
      if (routeIds.length > 0) {
        await this.reloadSpecificRoutes(routeIds);
        return;
      }
    }

    await this.reload();
  }

  private async reloadSpecificRoutes(
    routeIds: (string | number)[],
  ): Promise<void> {
    const isMongoDB = this.queryBuilder.isMongoDb();
    const idField = DatabaseConfigService.getPkField();

    const result = await this.queryBuilder.find({
      table: 'route_definition',
      filter: {
        _and: [
          { isEnabled: { _eq: true } },
          { [idField]: { _in: routeIds } },
        ],
      },
      fields: [
        '*',
        'mainTable.*',
        'handlers.*',
        'handlers.method.*',
        'routePermissions.*',
        'routePermissions.role.*',
        'preHooks.*',
        'preHooks.methods.method',
        'postHooks.*',
        'postHooks.methods.method',
        'publishedMethods.*',
        'availableMethods.*',
      ],
    });

    const updatedRoutes = result.data;
    for (const route of updatedRoutes) {
      this.mergeHooks(route, this.globalPreHooks, this.globalPostHooks, isMongoDB);
      this.transformRouteCode(route);
    }

    const routeIdSet = new Set(routeIds.map(String));

    this.cache.routes = this.cache.routes.filter((r: any) => {
      const rid = String(DatabaseConfigService.getRecordId(r));
      return !routeIdSet.has(rid);
    });

    this.cache.routes.push(...updatedRoutes);

    this.buildRouteEngine(this.cache.routes);
  }

  private async findRouteIdsForChildRecords(
    tableName: string,
    ids: (string | number)[],
  ): Promise<(string | number)[]> {
    const isMongoDB = this.queryBuilder.isMongoDb();
    const idField = DatabaseConfigService.getPkField();
    const routeField = 'route';

    const result = await this.queryBuilder.find({
      table: tableName,
      filter: { [idField]: { _in: ids } },
      fields: [`${routeField}.*`],
    });

    const routeIds = new Set<string | number>();
    for (const record of result.data) {
      const routeId = isMongoDB
        ? record[routeField]?._id || record[routeField]
        : record[routeField]?.id || record[`${routeField}Id`];
      if (routeId) routeIds.add(routeId);
    }
    return [...routeIds];
  }

  private async reloadGlobalHooksAndMerge(): Promise<void> {
    const isMongoDB = this.queryBuilder.isMongoDb();

    const [newGlobalPreHooks, newGlobalPostHooks] = await Promise.all([
      this.loadGlobalHooks('pre_hook_definition'),
      this.loadGlobalHooks('post_hook_definition'),
    ]);

    this.globalPreHooks = newGlobalPreHooks;
    this.globalPostHooks = newGlobalPostHooks;

    for (const route of this.cache.routes) {
      this.mergeHooks(route, this.globalPreHooks, this.globalPostHooks, isMongoDB);
    }

    this.buildRouteEngine(this.cache.routes);
  }

  protected async loadFromDb(): Promise<any> {
    const methodsResult = await this.queryBuilder.find({
      table: 'method_definition',
    });
    this.allMethods = methodsResult.data.map((m: any) => m.method);

    const result = await this.queryBuilder.find({
      table: 'route_definition',
      filter: { isEnabled: { _eq: true } },
      fields: [
        '*',
        'mainTable.*',
        'handlers.*',
        'handlers.method.*',
        'routePermissions.*',
        'routePermissions.role.*',
        'preHooks.*',
        'preHooks.methods.method',
        'postHooks.*',
        'postHooks.methods.method',
        'publishedMethods.*',
        'availableMethods.*',
      ],
    });

    const routes = result.data;
    const isMongoDB = this.queryBuilder.isMongoDb();

    const [globalPreHooks, globalPostHooks] = await Promise.all([
      this.loadGlobalHooks('pre_hook_definition'),
      this.loadGlobalHooks('post_hook_definition'),
    ]);

    this.globalPreHooks = globalPreHooks;
    this.globalPostHooks = globalPostHooks;

    for (const route of routes) {
      this.mergeHooks(route, globalPreHooks, globalPostHooks, isMongoDB);

      this.transformRouteCode(route);
    }

    return { routes, methods: this.allMethods };
  }

  private async loadGlobalHooks(tableName: string): Promise<any[]> {
    const result = await this.queryBuilder.find({
      table: tableName,
      filter: {
        _and: [{ isEnabled: { _eq: true } }, { isGlobal: { _eq: true } }],
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

  private mergeHooks(
    route: any,
    globalPreHooks: any[],
    globalPostHooks: any[],
    isMongoDB: boolean,
  ): void {
    const enabledRoutePreHooks = Array.isArray(route.preHooks)
      ? route.preHooks.filter(
          (h: any) => h?.isEnabled === true && h?.isGlobal === false,
        )
      : [];

    const allPreHooks = [...globalPreHooks, ...enabledRoutePreHooks];
    route.preHooks = this.uniqueHooks(allPreHooks, isMongoDB);

    const enabledRoutePostHooks = Array.isArray(route.postHooks)
      ? route.postHooks.filter(
          (h: any) => h?.isEnabled === true && h?.isGlobal === false,
        )
      : [];

    const allPostHooks = [...globalPostHooks, ...enabledRoutePostHooks];
    route.postHooks = this.uniqueHooks(allPostHooks, isMongoDB);
  }

  private uniqueHooks(hooks: any[], isMongoDB: boolean): any[] {
    return hooks.filter(
      (hook, index, self) =>
        index ===
        self.findIndex((h) => {
          const hId = DatabaseConfigService.getRecordId(h)?.toString();
          const hookId = DatabaseConfigService.getRecordId(hook)?.toString();
          return hId === hookId;
        }),
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

    if (route.preHooks && Array.isArray(route.preHooks)) {
      for (const hook of route.preHooks) {
        if (hook.code) {
          hook.code = transformCode(hook.code);
        }
      }
    }

    if (route.postHooks && Array.isArray(route.postHooks)) {
      for (const hook of route.postHooks) {
        if (hook.code) {
          hook.code = transformCode(hook.code);
        }
      }
    }
  }

  protected transformData(data: {
    routes: any[];
    methods: string[];
  }): RouteData {
    return data;
  }

  protected async afterTransform(data: RouteData): Promise<void> {
    this.buildRouteEngine(data.routes);
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
