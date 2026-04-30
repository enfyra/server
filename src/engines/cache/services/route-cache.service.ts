import { DatabaseConfigService } from '../../../shared/services';
import { EventEmitter2 } from 'eventemitter2';
import { QueryBuilderService } from '@enfyra/kernel';
import { BaseCacheService, CacheConfig } from './base-cache.service';
import { MetadataCacheService } from './metadata-cache.service';
import { RedisRuntimeCacheStore } from './redis-runtime-cache-store.service';
import { EnfyraRouteEngine } from '../../../shared/utils/enfyra-route-engine';
import {
  normalizeScriptRecord,
  resolveExecutableScript,
} from '@enfyra/kernel';
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

interface RouteMatchResult {
  route: any;
  params: Record<string, string>;
}

interface RedisRouteMatchIndexEntry {
  key: string;
  path: string;
  methods: string[];
  order: number;
}

const ROUTE_CACHE_ROUTE_FIELDS = [
  'id',
  'path',
  'isEnabled',
  'isSystem',
  'icon',
  'description',
  'createdAt',
  'updatedAt',
  'mainTable',
  'handlers.id',
  'handlers.timeout',
  'handlers.description',
  'handlers.sourceCode',
  'handlers.scriptLanguage',
  'handlers.compiledCode',
  'handlers.method',
  'routePermissions.id',
  'routePermissions.isEnabled',
  'routePermissions.description',
  'routePermissions.role.id',
  'routePermissions.methods',
  'routePermissions.allowedUsers.id',
  'preHooks.id',
  'preHooks.name',
  'preHooks.priority',
  'preHooks.isEnabled',
  'preHooks.isGlobal',
  'preHooks.description',
  'preHooks.isSystem',
  'preHooks.sourceCode',
  'preHooks.scriptLanguage',
  'preHooks.compiledCode',
  'preHooks.methods',
  'postHooks.id',
  'postHooks.name',
  'postHooks.priority',
  'postHooks.isEnabled',
  'postHooks.isGlobal',
  'postHooks.description',
  'postHooks.isSystem',
  'postHooks.sourceCode',
  'postHooks.scriptLanguage',
  'postHooks.compiledCode',
  'postHooks.methods',
  'publishedMethods',
  'skipRoleGuardMethods',
  'availableMethods',
];

const ROUTE_CACHE_HOOK_FIELDS = [
  'id',
  'name',
  'priority',
  'isEnabled',
  'isGlobal',
  'description',
  'isSystem',
  'sourceCode',
  'scriptLanguage',
  'compiledCode',
  'methods',
];

export class RouteCacheService extends BaseCacheService<RouteData> {
  private readonly queryBuilderService: QueryBuilderService;
  private readonly metadataCacheService: MetadataCacheService;
  private routeEngine: EnfyraRouteEngine;
  private allMethods: string[] = [];
  private methodById = new Map<string, any>();
  private globalPreHooks: any[] = [];
  private globalPostHooks: any[] = [];

  constructor(deps: {
    queryBuilderService: QueryBuilderService;
    metadataCacheService: MetadataCacheService;
    eventEmitter: EventEmitter2;
    redisRuntimeCacheStore?: RedisRuntimeCacheStore;
  }) {
    super(ROUTE_CONFIG, deps.eventEmitter, deps.redisRuntimeCacheStore);
    this.queryBuilderService = deps.queryBuilderService;
    this.metadataCacheService = deps.metadataCacheService;
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
      const isMongoDB = this.queryBuilderService.isMongoDb();
      const idField = DatabaseConfigService.getPkField();
      const filter = isMongoDB
        ? { mainTable: { _id: { _in: payload.ids } } }
        : { mainTableId: { _in: payload.ids } };

      const result = await this.queryBuilderService.find({
        table: 'route_definition',
        filter,
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
        const cachedRouteIds: (string | number)[] = [];
        for (const route of this.cache.routes) {
          if (affectedTableNames.has(route.mainTable?.name)) {
            const rid = DatabaseConfigService.getRecordId(route);
            if (rid != null) cachedRouteIds.push(rid);
          }
        }
        if (cachedRouteIds.length > 0) {
          await this.reloadSpecificRoutes(cachedRouteIds);
        }
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
      const routeIds = await this.resolveAffectedRouteIds(
        payload.table,
        payload.ids,
      );
      if (routeIds.length > 0) {
        await this.reloadSpecificRoutes(routeIds);
        return;
      }
    }

    if (
      ['pre_hook_definition', 'post_hook_definition'].includes(payload.table)
    ) {
      await this.reloadGlobalHooksAndMerge();
      if (payload.ids?.length) {
        const routeIds = await this.resolveAffectedRouteIds(
          payload.table,
          payload.ids,
        );
        if (routeIds.length > 0) {
          await this.reloadSpecificRoutes(routeIds);
        }
      }
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
          const rid = this.queryBuilderService.isMongoDb()
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
    const isMongoDB = this.queryBuilderService.isMongoDb();
    const idField = DatabaseConfigService.getPkField();

    const result = await this.queryBuilderService.find({
      table: 'route_definition',
      filter: {
        _and: [{ isEnabled: { _eq: true } }, { [idField]: { _in: routeIds } }],
      },
      fields: ROUTE_CACHE_ROUTE_FIELDS,
    });

    const updatedRoutes = result.data;
    const metadata = await this.metadataCacheService.getMetadata();
    for (const route of updatedRoutes) {
      this.hydrateRouteMainTable(route, metadata);
      this.hydrateRouteMethods(route);
      this.mergeHooks(
        route,
        this.globalPreHooks,
        this.globalPostHooks,
        isMongoDB,
      );
      await this.transformRouteCode(route);
    }

    const routeIdSet = new Set(routeIds.map(String));

    this.cache.routes = this.cache.routes.filter((r: any) => {
      const rid = String(DatabaseConfigService.getRecordId(r));
      return !routeIdSet.has(rid);
    });

    this.cache.routes.push(...updatedRoutes);

    if (!this.usesSharedRuntimeCache()) {
      this.buildRouteEngine(this.cache.routes);
    }
  }

  private async findRouteIdsForChildRecords(
    tableName: string,
    ids: (string | number)[],
  ): Promise<(string | number)[]> {
    const isMongoDB = this.queryBuilderService.isMongoDb();
    const idField = DatabaseConfigService.getPkField();
    const routeField = 'route';

    const result = await this.queryBuilderService.find({
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

  private getChildArrayKeyForTable(tableName: string): string | null {
    switch (tableName) {
      case 'pre_hook_definition':
        return 'preHooks';
      case 'post_hook_definition':
        return 'postHooks';
      case 'route_handler_definition':
        return 'handlers';
      case 'route_permission_definition':
        return 'routePermissions';
      default:
        return null;
    }
  }

  private findCachedRouteIdsForChildRecords(
    tableName: string,
    ids: (string | number)[],
  ): (string | number)[] {
    const arrayKey = this.getChildArrayKeyForTable(tableName);
    if (!arrayKey) return [];
    const idSet = new Set(ids.map(String));
    const routeIds = new Map<string, string | number>();
    for (const route of this.cache.routes) {
      const children = route?.[arrayKey];
      if (!Array.isArray(children)) continue;
      for (const child of children) {
        const childId = DatabaseConfigService.getRecordId(child);
        if (childId == null) continue;
        if (idSet.has(String(childId))) {
          const rid = DatabaseConfigService.getRecordId(route);
          if (rid != null) routeIds.set(String(rid), rid);
          break;
        }
      }
    }
    return [...routeIds.values()];
  }

  private async resolveAffectedRouteIds(
    tableName: string,
    ids: (string | number)[],
  ): Promise<(string | number)[]> {
    const [fresh, cached] = await Promise.all([
      this.findRouteIdsForChildRecords(tableName, ids),
      Promise.resolve(this.findCachedRouteIdsForChildRecords(tableName, ids)),
    ]);
    const merged = new Map<string, string | number>();
    for (const rid of [...fresh, ...cached]) {
      if (rid != null) merged.set(String(rid), rid);
    }
    return [...merged.values()];
  }

  private async reloadGlobalHooksAndMerge(): Promise<void> {
    const isMongoDB = this.queryBuilderService.isMongoDb();

    const [newGlobalPreHooks, newGlobalPostHooks] = await Promise.all([
      this.loadGlobalHooks('pre_hook_definition'),
      this.loadGlobalHooks('post_hook_definition'),
    ]);

    this.globalPreHooks = newGlobalPreHooks;
    this.globalPostHooks = newGlobalPostHooks;

    for (const route of this.cache.routes) {
      this.mergeHooks(
        route,
        this.globalPreHooks,
        this.globalPostHooks,
        isMongoDB,
      );
    }

    if (!this.usesSharedRuntimeCache()) {
      this.buildRouteEngine(this.cache.routes);
    }
  }

  protected async loadFromDb(): Promise<any> {
    const methodsResult = await this.queryBuilderService.find({
      table: 'method_definition',
      fields: ['id', 'method'],
    });
    this.setMethodCache(methodsResult.data);

    const result = await this.queryBuilderService.find({
      table: 'route_definition',
      filter: { isEnabled: { _eq: true } },
      fields: ROUTE_CACHE_ROUTE_FIELDS,
    });

    const routes = result.data;
    const isMongoDB = this.queryBuilderService.isMongoDb();

    const [globalPreHooks, globalPostHooks] = await Promise.all([
      this.loadGlobalHooks('pre_hook_definition'),
      this.loadGlobalHooks('post_hook_definition'),
    ]);

    this.globalPreHooks = globalPreHooks;
    this.globalPostHooks = globalPostHooks;

    const metadata = await this.metadataCacheService.getMetadata();

    for (const route of routes) {
      this.hydrateRouteMainTable(route, metadata);
      this.hydrateRouteMethods(route);
      this.mergeHooks(route, globalPreHooks, globalPostHooks, isMongoDB);

      await this.transformRouteCode(route);
    }

    return { routes, methods: this.allMethods };
  }

  private async loadGlobalHooks(tableName: string): Promise<any[]> {
    const result = await this.queryBuilderService.find({
      table: tableName,
      filter: {
        _and: [{ isEnabled: { _eq: true } }, { isGlobal: { _eq: true } }],
      },
      fields: ROUTE_CACHE_HOOK_FIELDS,
      sort: ['priority'],
    });

    return Promise.all(
      result.data.map(async (hook: any) => {
        this.hydrateMethodList(hook.methods);
        const normalized = normalizeScriptRecord(tableName, hook);
        Object.assign(hook, normalized);
        const code = await this.resolveAndRepairScript(tableName, hook);
        if (code) {
          hook.code = code;
        }
        return hook;
      }),
    );
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

  private uniqueHooks(hooks: any[], _isMongoDB: boolean): any[] {
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

  private setMethodCache(methods: any[]): void {
    this.methodById = new Map();
    this.allMethods = [];
    for (const method of methods || []) {
      const id = DatabaseConfigService.getRecordId(method);
      if (id == null || !method?.method) continue;
      const normalized = { id, method: method.method };
      this.methodById.set(String(id), normalized);
      this.allMethods.push(method.method);
    }
  }

  private hydrateMethodRef(ref: any): any {
    const id = DatabaseConfigService.getRecordId(ref);
    if (id == null) return ref;
    return this.methodById.get(String(id)) ?? ref;
  }

  private hydrateMethodList(items: any[]): void {
    if (!Array.isArray(items)) return;
    for (let i = 0; i < items.length; i++) {
      items[i] = this.hydrateMethodRef(items[i]);
    }
  }

  private hydrateRouteMethods(route: any): void {
    this.hydrateMethodList(route.availableMethods);
    this.hydrateMethodList(route.publishedMethods);
    this.hydrateMethodList(route.skipRoleGuardMethods);

    for (const handler of route.handlers || []) {
      handler.method = this.hydrateMethodRef(handler.method);
    }
    for (const permission of route.routePermissions || []) {
      this.hydrateMethodList(permission.methods);
    }
    for (const hook of route.preHooks || []) {
      this.hydrateMethodList(hook.methods);
    }
    for (const hook of route.postHooks || []) {
      this.hydrateMethodList(hook.methods);
    }
  }

  private hydrateRouteMainTable(route: any, metadata: any): void {
    if (!metadata?.tablesList?.length || !route?.mainTable) return;

    const mainTableId = DatabaseConfigService.getRecordId(route.mainTable);
    if (mainTableId == null) return;

    const tableMeta = metadata.tablesList.find((table: any) => {
      const tableId = DatabaseConfigService.getRecordId(table);
      return String(tableId) === String(mainTableId);
    });

    if (tableMeta) {
      route.mainTable = tableMeta;
    }
  }

  private async transformRouteCode(route: any): Promise<void> {
    if (route.handlers && Array.isArray(route.handlers)) {
      for (const handler of route.handlers) {
        const normalized = normalizeScriptRecord(
          'route_handler_definition',
          handler,
        );
        Object.assign(handler, normalized);
        const code = await this.resolveAndRepairScript(
          'route_handler_definition',
          handler,
        );
        if (code) {
          handler.logic = code;
        }
      }
    }

    if (route.preHooks && Array.isArray(route.preHooks)) {
      for (const hook of route.preHooks) {
        const normalized = normalizeScriptRecord('pre_hook_definition', hook);
        Object.assign(hook, normalized);
        const code = await this.resolveAndRepairScript(
          'pre_hook_definition',
          hook,
        );
        if (code) {
          hook.code = code;
        }
      }
    }

    if (route.postHooks && Array.isArray(route.postHooks)) {
      for (const hook of route.postHooks) {
        const normalized = normalizeScriptRecord('post_hook_definition', hook);
        Object.assign(hook, normalized);
        const code = await this.resolveAndRepairScript(
          'post_hook_definition',
          hook,
        );
        if (code) {
          hook.code = code;
        }
      }
    }
  }

  private async resolveAndRepairScript(
    tableName: string,
    record: any,
  ): Promise<string | null> {
    const result = resolveExecutableScript(record);
    if (result.shouldPersistCompiledCode) {
      record.compiledCode = result.compiledCode;
      const id = DatabaseConfigService.getRecordId(record);
      if (id != null) {
        await this.queryBuilderService.update(tableName, id, {
          compiledCode: result.compiledCode,
        });
      }
    }
    return result.code;
  }

  protected transformData(data: {
    routes: any[];
    methods: string[];
  }): RouteData {
    return data;
  }

  protected async afterTransform(data: RouteData): Promise<void> {
    if (!this.usesSharedRuntimeCache()) {
      this.buildRouteEngine(data.routes);
    }
  }

  protected async afterSharedCachePersist(data: RouteData): Promise<void> {
    await this.persistRedisRouteLookup(data.routes);
  }

  protected emitLoadedEvent(): void {
    this.eventEmitter?.emit(CACHE_EVENTS.ROUTE_LOADED);
  }

  protected getLogCount(): string {
    return `${this.cache?.routes?.length ?? 0} routes, ${this.cache?.methods?.length ?? 0} methods`;
  }

  protected getCount(): number {
    return this.cache?.routes?.length ?? 0;
  }

  private buildRouteEngine(routes: any[]): void {
    this.routeEngine = new EnfyraRouteEngine(false);
    for (const route of routes) {
      this.insertRouteToEngine(route);
    }
  }

  private insertRouteToEngine(route: any): void {
    if (!route.path) {
      return;
    }

    const basePath = route.path;
    const raw = route.availableMethods;
    const methods =
      Array.isArray(raw) && raw.length > 0
        ? raw.map((m: any) => m?.method ?? m).filter(Boolean)
        : [];

    if (methods.length === 0) return;

    if (methods.length === 0) return;

    for (const method of methods) {
      this.routeEngine.insert(method, basePath, route);
      if (['DELETE', 'PATCH'].includes(method)) {
        this.routeEngine.insert(method, `${basePath}/:id`, route);
      }
    }
  }

  async getRoutes(): Promise<any[]> {
    const cache = await this.getCacheAsync();
    return cache.routes;
  }

  getRouteEngine(): EnfyraRouteEngine {
    if (this.usesSharedRuntimeCache()) {
      throw new Error('RouteCache is Redis-backed; use matchRoute()');
    }
    return this.routeEngine;
  }

  async matchRoute(
    method: string,
    path: string,
  ): Promise<RouteMatchResult | null> {
    if (!this.usesSharedRuntimeCache()) {
      return this.routeEngine.find(method, path);
    }

    const redisMatch = await this.matchRedisRoute(method, path);
    if (redisMatch) return redisMatch;

    const cache = await this.getCacheAsync();
    const normalizedPath = this.normalizePath(path);
    let best:
      | {
          route: any;
          params: Record<string, string>;
          score: number;
          index: number;
        }
      | null = null;

    for (let i = 0; i < cache.routes.length; i++) {
      const route = cache.routes[i];
      if (!this.isMethodAvailable(route, method)) continue;
      for (const candidate of this.getCandidatePaths(route, method)) {
        const match = this.matchPattern(candidate, normalizedPath);
        if (!match) continue;
        const score = this.scorePattern(candidate);
        if (!best || score > best.score) {
          best = { route, params: match, score, index: i };
        }
      }
    }

    return best ? { route: best.route, params: best.params } : null;
  }

  private async persistRedisRouteLookup(routes: any[]): Promise<void> {
    if (!this.usesSharedRuntimeCache()) return;
    const entries: RedisRouteMatchIndexEntry[] = [];
    await this.redisRuntimeCacheStore!.deleteAuxByPrefix(
      this.config.cacheIdentifier,
      'route:',
    );
    for (let i = 0; i < routes.length; i++) {
      const route = routes[i];
      const key = String(DatabaseConfigService.getRecordId(route) ?? i);
      const methods = this.getRouteMethods(route);
      entries.push({
        key,
        path: route.path,
        methods,
        order: i,
      });
      await this.redisRuntimeCacheStore!.setAux(
        this.config.cacheIdentifier,
        `route:${key}`,
        route,
      );
    }
    await this.redisRuntimeCacheStore!.setAux(
      this.config.cacheIdentifier,
      'match-index',
      entries,
    );
  }

  private async matchRedisRoute(
    method: string,
    path: string,
  ): Promise<RouteMatchResult | null> {
    const index = await this.redisRuntimeCacheStore!.getAux<
      RedisRouteMatchIndexEntry[]
    >(this.config.cacheIdentifier, 'match-index');
    if (!Array.isArray(index) || index.length === 0) return null;

    const normalizedPath = this.normalizePath(path);
    let best:
      | {
          entry: RedisRouteMatchIndexEntry;
          params: Record<string, string>;
          score: number;
        }
      | null = null;

    for (const entry of index) {
      if (!entry.methods.includes(method)) continue;
      for (const candidate of this.getCandidatePaths(entry, method)) {
        const match = this.matchPattern(candidate, normalizedPath);
        if (!match) continue;
        const score = this.scorePattern(candidate);
        if (
          !best ||
          score > best.score ||
          (score === best.score && entry.order < best.entry.order)
        ) {
          best = { entry, params: match, score };
        }
      }
    }

    if (!best) return null;
    const route = await this.redisRuntimeCacheStore!.getAux<any>(
      this.config.cacheIdentifier,
      `route:${best.entry.key}`,
    );
    return route ? { route, params: best.params } : null;
  }

  private isMethodAvailable(route: any, method: string): boolean {
    return this.getRouteMethods(route).includes(method);
  }

  private getRouteMethods(route: any): string[] {
    const methods = route?.availableMethods;
    if (!Array.isArray(methods) || methods.length === 0) return [];
    return methods.map((m: any) => m?.method ?? m).filter(Boolean);
  }

  private getCandidatePaths(route: any, method: string): string[] {
    const paths = [route.path];
    if (['DELETE', 'PATCH'].includes(method)) {
      paths.push(`${route.path}/:id`);
    }
    return paths.filter(Boolean);
  }

  private normalizePath(path: string): string {
    if (!path) return '/';
    let normalized = path.startsWith('/') ? path : `/${path}`;
    if (normalized.length > 1 && normalized.endsWith('/')) {
      normalized = normalized.slice(0, -1);
    }
    return normalized;
  }

  private splitPath(path: string): string[] {
    if (path === '/') return [];
    return path.split('/').filter((segment) => segment.length > 0);
  }

  private matchPattern(
    pattern: string,
    path: string,
  ): Record<string, string> | null {
    const patternSegments = this.splitPath(this.normalizePath(pattern));
    const pathSegments = this.splitPath(path);
    const params: Record<string, string> = {};

    for (let i = 0; i < patternSegments.length; i++) {
      const patternSegment = patternSegments[i];
      const pathSegment = pathSegments[i];
      if (patternSegment === '*' || patternSegment.startsWith('*')) {
        params.splat = pathSegments.slice(i).join('/');
        return params;
      }
      if (pathSegment === undefined) return null;
      if (patternSegment.startsWith(':')) {
        const paramName = patternSegment.slice(1);
        try {
          params[paramName] = decodeURIComponent(pathSegment);
        } catch {
          params[paramName] = pathSegment;
        }
        continue;
      }
      if (patternSegment !== pathSegment) return null;
    }

    return patternSegments.length === pathSegments.length ? params : null;
  }

  private scorePattern(pattern: string): number {
    const segments = this.splitPath(this.normalizePath(pattern));
    return segments.reduce((score, segment) => {
      if (segment === '*' || segment.startsWith('*')) return score;
      if (segment.startsWith(':')) return score + 10;
      return score + 100;
    }, segments.length);
  }
}
