import { DatabaseConfigService } from '../../../shared/services';
import { EventEmitter2 } from 'eventemitter2';
import { QueryBuilderService } from '@enfyra/kernel';
import { BaseCacheService, CacheConfig } from './base-cache.service';
import { MetadataCacheService } from './metadata-cache.service';
import { RedisRuntimeCacheStore } from './redis-runtime-cache-store.service';
import { EnfyraRouteEngine } from '../../../shared/utils/enfyra-route-engine';
import {
  compileScriptSource,
  normalizeScriptRecord,
  resolveExecutableScript,
} from '../../../shared/utils/script-code.util';
import {
  getRouteCandidatePaths,
  getRouteMethods,
  isRouteMethodAvailable,
  matchRouteInRoutes,
  matchRouteIndexEntry,
} from '../../../shared/utils/route-match.util';
import {
  CACHE_EVENTS,
  CACHE_IDENTIFIERS,
} from '../../../shared/utils/cache-events.constants';
import { TCacheInvalidationPayload } from '../../../shared/types/cache.types';
import type { RouteCacheData as RouteData } from '../types/route-cache.types';

const ROUTE_CONFIG: CacheConfig = {
  cacheIdentifier: CACHE_IDENTIFIERS.ROUTE,
  colorCode: '\x1b[31m',
  cacheName: 'RouteCache',
};

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
  'maxUploadFileSize',
  'description',
  'createdAt',
  'updatedAt',
  'mainTable',
  'methodConfigs.id',
  'methodConfigs.available',
  'methodConfigs.isPublic',
  'methodConfigs.skipRoleGuard',
  'methodConfigs.timeout',
  'methodConfigs.requestBodyType',
  'methodConfigs.maxUploadFileSize',
  'methodConfigs.maxFiles',
  'methodConfigs.description',
  'methodConfigs.isSystem',
  'methodConfigs.method',
  'methodConfigs.handler.id',
  'methodConfigs.handler.description',
  'methodConfigs.handler.sourceCode',
  'methodConfigs.handler.scriptLanguage',
  'methodConfigs.handler.compiledCode',
  'methodConfigs.routePermissions.id',
  'methodConfigs.routePermissions.isEnabled',
  'methodConfigs.routePermissions.description',
  'methodConfigs.routePermissions.role.id',
  'methodConfigs.routePermissions.allowedUsers.id',
  ...['preHooks', 'postHooks'].flatMap((kind) =>
    [
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
    ].map((field) => `methodConfigs.${kind}.${field}`),
  ),
  'methodConfigs.fileFields.id',
  'methodConfigs.fileFields.name',
  'methodConfigs.fileFields.required',
  'methodConfigs.fileFields.maxCount',
  'methodConfigs.fileFields.maxFileSize',
  'methodConfigs.fileFields.allowedMimeTypes',
  'methodConfigs.fileFields.sort',
  'methodConfigs.fileFields.description',
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
  'publicMethods',
  'skipRoleGuardMethods',
  'availableMethods',
];

const ROUTE_CACHE_ROUTE_DEEP = {
  methodConfigs: {
    limit: -1,
    deep: {
      routePermissions: { limit: -1, deep: { allowedUsers: { limit: -1 } } },
      preHooks: { limit: -1, deep: { methods: { limit: -1 } } },
      postHooks: { limit: -1, deep: { methods: { limit: -1 } } },
      fileFields: { limit: -1 },
    },
  },
  handlers: { limit: -1 },
  routePermissions: {
    limit: -1,
    deep: { methods: { limit: -1 }, allowedUsers: { limit: -1 } },
  },
  preHooks: { limit: -1, deep: { methods: { limit: -1 } } },
  postHooks: { limit: -1, deep: { methods: { limit: -1 } } },
  publicMethods: { limit: -1 },
  skipRoleGuardMethods: { limit: -1 },
  availableMethods: { limit: -1 },
};

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
  private sharedRouteChanges: Set<string> | null = null;

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

  override supportsPartialReload(): boolean {
    return true;
  }

  protected override async applyPartialUpdate(
    payload: TCacheInvalidationPayload,
  ): Promise<void> {
    if (
      !this.cache.methodDefinitions ||
      !this.cache.globalPreHooks ||
      !this.cache.globalPostHooks
    ) {
      await this.reloadFromPartial();
      return;
    }
    this.sharedRouteChanges = new Set();
    this.setMethodCache(this.cache.methodDefinitions);
    this.globalPreHooks = this.cache.globalPreHooks;
    this.globalPostHooks = this.cache.globalPostHooks;
    const affectedTableNames = new Set<string>(payload.affectedTables || []);

    if (
      ['enfyra_table', 'enfyra_column', 'enfyra_relation'].includes(
        payload.table,
      ) &&
      payload.ids?.length
    ) {
      const isMongoDB = this.queryBuilderService.isMongoDb();
      const idField = DatabaseConfigService.getPkField();
      const owners = new Set(payload.ids.map(String));
      const filter = {
        _or: [
          isMongoDB
            ? { mainTable: { _id: { _in: payload.ids } } }
            : { mainTableId: { _in: payload.ids } },
          ...(affectedTableNames.size
            ? [{ mainTable: { name: { _in: [...affectedTableNames] } } }]
            : []),
        ],
      };
      const result = await this.queryBuilderService.find({
        table: 'enfyra_route',
        filter,
        fields: [idField],
        limit: -1,
      });
      const routeIds = new Map<string, string | number>();
      for (const route of result.data) {
        const id = DatabaseConfigService.getRecordId(route);
        if (id != null) routeIds.set(String(id), id);
      }
      for (const route of this.cache.routes) {
        const owner = route.mainTable;
        if (
          owner &&
          (owners.has(String(DatabaseConfigService.getRecordId(owner))) ||
            affectedTableNames.has(owner.name))
        ) {
          const id = DatabaseConfigService.getRecordId(route);
          if (id != null) routeIds.set(String(id), id);
        }
      }
      if (routeIds.size)
        await this.reloadSpecificRoutes([...routeIds.values()]);
      return;
    }

    if (payload.table === 'enfyra_route' && payload.ids?.length) {
      await this.reloadSpecificRoutes(payload.ids);
      return;
    }

    if (
      [
        'enfyra_route_method_config',
        'enfyra_route_method_config_file_field',
        'enfyra_route_handler',
        'enfyra_route_permission',
      ].includes(payload.table) &&
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

    if (['enfyra_pre_hook', 'enfyra_post_hook'].includes(payload.table)) {
      if (!payload.ids?.length) {
        await this.reloadFromPartial();
        return;
      }
      const ids = new Set(payload.ids.map(String));
      const previousGlobals =
        payload.table === 'enfyra_pre_hook'
          ? this.globalPreHooks
          : this.globalPostHooks;
      const fresh = await this.loadChildOwners(payload.table, payload.ids);
      const routeIds = this.mergeAffectedRouteIds(
        payload.table,
        payload.ids,
        fresh,
      );
      const globalChanged =
        previousGlobals.some((hook) =>
          ids.has(String(DatabaseConfigService.getRecordId(hook))),
        ) || fresh.some((record) => record.isGlobal === true);
      if (globalChanged) await this.reloadGlobalHooksAndMerge();
      if (routeIds.length > 0) await this.reloadSpecificRoutes(routeIds);
      if (!globalChanged && routeIds.length === 0)
        await this.reloadFromPartial();
      return;
    }

    if (['enfyra_role', 'enfyra_method'].includes(payload.table)) {
      await this.reloadFromPartial();
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

    await this.reloadFromPartial();
  }

  private async reloadSpecificRoutes(
    routeIds: (string | number)[],
  ): Promise<void> {
    const isMongoDB = this.queryBuilderService.isMongoDb();
    const idField = DatabaseConfigService.getPkField();

    const result = await this.queryBuilderService.find({
      table: 'enfyra_route',
      filter: {
        _and: [{ isEnabled: { _eq: true } }, { [idField]: { _in: routeIds } }],
      },
      fields: ROUTE_CACHE_ROUTE_FIELDS,
      deep: ROUTE_CACHE_ROUTE_DEEP,
      limit: -1,
    });

    const updatedRoutes = result.data;
    const metadata = await this.metadataCacheService.getMetadata();
    const routeIdSet = new Set(routeIds.map(String));

    await Promise.all(
      updatedRoutes.map(async (route: any) => {
        this.hydrateRouteMainTable(route, metadata);
        this.hydrateRouteMethods(route);
        this.mergeHooks(
          route,
          this.globalPreHooks,
          this.globalPostHooks,
          isMongoDB,
        );
        await this.transformRouteCode(route);
        this.finalizeRouteMethodConfigs(route);
      }),
    );

    const replacements = new Map(
      updatedRoutes.map((route: any) => [
        String(DatabaseConfigService.getRecordId(route)),
        route,
      ]),
    );
    const nextRoutes: any[] = [];
    for (const route of this.cache.routes) {
      const id = String(DatabaseConfigService.getRecordId(route));
      if (!routeIdSet.has(id)) nextRoutes.push(route);
      else if (replacements.has(id)) {
        nextRoutes.push(replacements.get(id));
        replacements.delete(id);
      }
    }
    nextRoutes.push(...replacements.values());
    this.cache.routes = nextRoutes;
    for (const id of routeIdSet) this.sharedRouteChanges?.add(id);

    if (!this.usesSharedRuntimeCache()) {
      this.buildRouteEngine(this.cache.routes);
    }
  }

  private async loadChildOwners(
    tableName: string,
    ids: (string | number)[],
  ): Promise<any[]> {
    const idField = DatabaseConfigService.getPkField();
    const fileField = tableName === 'enfyra_route_method_config_file_field';
    const hook =
      tableName === 'enfyra_pre_hook' || tableName === 'enfyra_post_hook';
    const permission = tableName === 'enfyra_route_permission';
    const result = await this.queryBuilderService.find({
      table: tableName,
      filter: { [idField]: { _in: ids } },
      fields: fileField
        ? [`routeMethodConfig.route.${idField}`]
        : [
            `route.${idField}`,
            ...(hook ? ['isGlobal'] : []),
            ...(hook || permission
              ? [`routeMethodConfigs.route.${idField}`]
              : []),
          ],
      ...(hook || permission
        ? { deep: { routeMethodConfigs: { limit: -1 } } }
        : {}),
      limit: -1,
    });
    return result.data;
  }

  private async findRouteIdsForChildRecords(
    tableName: string,
    ids: (string | number)[],
  ): Promise<(string | number)[]> {
    return this.mergeAffectedRouteIds(
      tableName,
      ids,
      await this.loadChildOwners(tableName, ids),
    );
  }

  private mergeAffectedRouteIds(
    tableName: string,
    ids: (string | number)[],
    records: any[],
  ): (string | number)[] {
    const routeIds = new Map<string, string | number>();
    for (const record of records) {
      const owners = [
        record.route,
        record.routeMethodConfig?.route,
        ...(record.routeMethodConfigs ?? []).map(
          (operation: any) => operation.route,
        ),
      ];
      for (const owner of owners) {
        const id =
          owner && typeof owner === 'object'
            ? DatabaseConfigService.getRecordId(owner)
            : owner;
        if (id != null) routeIds.set(String(id), id);
      }
      if (record.routeId != null)
        routeIds.set(String(record.routeId), record.routeId);
    }
    for (const id of this.findCachedRouteIdsForChildRecords(tableName, ids))
      routeIds.set(String(id), id);
    return [...routeIds.values()];
  }

  private getChildArrayKeyForTable(tableName: string): string | null {
    switch (tableName) {
      case 'enfyra_pre_hook':
        return 'preHooks';
      case 'enfyra_post_hook':
        return 'postHooks';
      case 'enfyra_route_handler':
        return 'handlers';
      case 'enfyra_route_permission':
        return 'routePermissions';
      case 'enfyra_route_method_config':
        return 'methodConfigs';
      case 'enfyra_route_method_config_file_field':
        return 'fileFields';
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
      const children = [
        ...(Array.isArray(route?.[arrayKey]) ? route[arrayKey] : []),
        ...(route.methodConfigs ?? []).flatMap((operation: any) =>
          arrayKey === 'handlers'
            ? [operation.handler].filter(Boolean)
            : (operation[arrayKey] ?? []),
        ),
      ];
      for (const child of children) {
        if (
          (tableName === 'enfyra_pre_hook' ||
            tableName === 'enfyra_post_hook') &&
          child.isGlobal === true
        )
          continue;
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
    return this.findRouteIdsForChildRecords(tableName, ids);
  }

  private async reloadGlobalHooksAndMerge(): Promise<void> {
    const isMongoDB = this.queryBuilderService.isMongoDb();

    const [newGlobalPreHooks, newGlobalPostHooks] = await Promise.all([
      this.loadGlobalHooks('enfyra_pre_hook'),
      this.loadGlobalHooks('enfyra_post_hook'),
    ]);

    [this.globalPreHooks, this.globalPostHooks] = await Promise.all([
      this.hydrateAndTransformHooks('enfyra_pre_hook', newGlobalPreHooks),
      this.hydrateAndTransformHooks('enfyra_post_hook', newGlobalPostHooks),
    ]);

    this.cache.globalPreHooks = this.globalPreHooks;
    this.cache.globalPostHooks = this.globalPostHooks;
    for (const route of this.cache.routes) {
      this.mergeHooks(
        route,
        this.globalPreHooks,
        this.globalPostHooks,
        isMongoDB,
      );
      this.finalizeRouteMethodConfigs(route);
      this.sharedRouteChanges?.add(
        String(DatabaseConfigService.getRecordId(route)),
      );
    }

    if (!this.usesSharedRuntimeCache()) {
      this.buildRouteEngine(this.cache.routes);
    }
  }

  protected async loadFromDb(): Promise<any> {
    this.sharedRouteChanges = null;
    const isMongoDB = this.queryBuilderService.isMongoDb();

    const [
      methodsResult,
      routesResult,
      globalPreHookRecords,
      globalPostHookRecords,
    ] = await Promise.all([
      this.queryBuilderService.find({
        table: 'enfyra_method',
        fields: ['id', 'name'],
        limit: -1,
      }),
      this.queryBuilderService.find({
        table: 'enfyra_route',
        filter: { isEnabled: { _eq: true } },
        fields: ROUTE_CACHE_ROUTE_FIELDS,
        deep: ROUTE_CACHE_ROUTE_DEEP,
        limit: -1,
      }),
      this.loadGlobalHooks('enfyra_pre_hook'),
      this.loadGlobalHooks('enfyra_post_hook'),
    ]);

    this.setMethodCache(methodsResult.data);
    const [globalPreHooks, globalPostHooks] = await Promise.all([
      this.hydrateAndTransformHooks('enfyra_pre_hook', globalPreHookRecords),
      this.hydrateAndTransformHooks('enfyra_post_hook', globalPostHookRecords),
    ]);
    this.globalPreHooks = globalPreHooks;
    this.globalPostHooks = globalPostHooks;

    const routes = routesResult.data || [];
    const metadata = await this.metadataCacheService.getMetadata();

    await Promise.all(
      routes.map(async (route: any) => {
        this.hydrateRouteMainTable(route, metadata);
        this.hydrateRouteMethods(route);
        this.mergeHooks(route, globalPreHooks, globalPostHooks, isMongoDB);

        await this.transformRouteCode(route);
        this.finalizeRouteMethodConfigs(route);
      }),
    );

    return {
      routes,
      methods: this.allMethods,
      methodDefinitions: methodsResult.data,
      globalPreHooks,
      globalPostHooks,
    };
  }

  private async loadGlobalHooks(tableName: string): Promise<any[]> {
    const result = await this.queryBuilderService.find({
      table: tableName,
      filter: {
        _and: [{ isEnabled: { _eq: true } }, { isGlobal: { _eq: true } }],
      },
      fields: ROUTE_CACHE_HOOK_FIELDS,
      deep: { methods: { limit: -1 } },
      sort: ['priority'],
      limit: -1,
    });

    return result.data || [];
  }

  private async hydrateAndTransformHooks(
    tableName: string,
    hooks: any[],
  ): Promise<any[]> {
    return Promise.all(
      hooks.map(async (hook: any) => {
        this.hydrateMethodList(hook.methods);
        const code = this.resolveScriptCode(tableName, hook);
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
    const enabledRoutePreHooks = [
      ...(route.preHooks ?? []),
      ...(route.methodConfigs ?? []).flatMap(
        (operation: any) => operation.preHooks ?? [],
      ),
    ].filter(
      (hook: any) => hook?.isEnabled === true && hook?.isGlobal === false,
    );

    const allPreHooks = [...globalPreHooks, ...enabledRoutePreHooks];
    route.preHooks = this.uniqueHooks(allPreHooks, isMongoDB);

    const enabledRoutePostHooks = [
      ...(route.postHooks ?? []),
      ...(route.methodConfigs ?? []).flatMap(
        (operation: any) => operation.postHooks ?? [],
      ),
    ].filter(
      (hook: any) => hook?.isEnabled === true && hook?.isGlobal === false,
    );

    const allPostHooks = [...globalPostHooks, ...enabledRoutePostHooks];
    route.postHooks = this.uniqueHooks(allPostHooks, isMongoDB);
  }

  private uniqueHooks(hooks: any[], _isMongoDB: boolean): any[] {
    return hooks
      .filter(
        (hook, index, self) =>
          index ===
          self.findIndex((h) => {
            const hId = DatabaseConfigService.getRecordId(h)?.toString();
            const hookId = DatabaseConfigService.getRecordId(hook)?.toString();
            return hId === hookId;
          }),
      )
      .sort((a, b) => (a?.priority ?? 0) - (b?.priority ?? 0));
  }

  private setMethodCache(methods: any[]): void {
    this.methodById = new Map();
    this.allMethods = [];
    for (const method of methods || []) {
      const id = DatabaseConfigService.getRecordId(method);
      if (id == null || !method?.name) continue;
      const normalized = { id, name: method.name };
      this.methodById.set(String(id), normalized);
      this.allMethods.push(method.name);
    }
  }

  private hydrateMethodRef(ref: any): any {
    if (ref == null) return ref;
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
    this.hydrateMethodList(route.publicMethods);
    this.hydrateMethodList(route.skipRoleGuardMethods);

    for (const operation of route.methodConfigs || []) {
      operation.method = this.hydrateMethodRef(operation.method);
      for (const hook of [
        ...(operation.preHooks ?? []),
        ...(operation.postHooks ?? []),
      ])
        this.hydrateMethodList(hook.methods);
    }
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

  private finalizeRouteMethodConfigs(route: any): void {
    if (!Array.isArray(route.methodConfigs)) return;

    for (const operation of route.methodConfigs) {
      const methodName = operation.method?.name;
      const preHookIds = new Set(
        (Array.isArray(operation.preHooks) ? operation.preHooks : [])
          .map((hook: any) => DatabaseConfigService.getRecordId(hook))
          .filter((id: any) => id != null)
          .map(String),
      );
      const postHookIds = new Set(
        (Array.isArray(operation.postHooks) ? operation.postHooks : [])
          .map((hook: any) => DatabaseConfigService.getRecordId(hook))
          .filter((id: any) => id != null)
          .map(String),
      );
      operation.preHooks = this.uniqueHooks(
        route.preHooks.filter((hook: any) => {
          const hookId = DatabaseConfigService.getRecordId(hook);
          return hook.isGlobal === true
            ? hook.methods?.some((method: any) => method?.name === methodName)
            : hookId != null && preHookIds.has(String(hookId));
        }),
        this.queryBuilderService.isMongoDb(),
      );
      operation.postHooks = this.uniqueHooks(
        route.postHooks.filter((hook: any) => {
          const hookId = DatabaseConfigService.getRecordId(hook);
          return hook.isGlobal === true
            ? hook.methods?.some((method: any) => method?.name === methodName)
            : hookId != null && postHookIds.has(String(hookId));
        }),
        this.queryBuilderService.isMongoDb(),
      );
      operation.fileFields = Array.isArray(operation.fileFields)
        ? [...operation.fileFields].sort(
            (left: any, right: any) => (left.sort ?? 0) - (right.sort ?? 0),
          )
        : [];
    }

    route.availableMethods = route.methodConfigs
      .filter((operation: any) => operation.available === true)
      .map((operation: any) => operation.method);
    route.publicMethods = route.methodConfigs
      .filter((operation: any) => operation.isPublic === true)
      .map((operation: any) => operation.method);
    route.skipRoleGuardMethods = route.methodConfigs
      .filter((operation: any) => operation.skipRoleGuard === true)
      .map((operation: any) => operation.method);
    route.handlers = route.methodConfigs
      .filter((operation: any) => operation.handler)
      .map((operation: any) => ({
        ...operation.handler,
        method: operation.method,
      }));
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
    const handlers = Array.isArray(route.methodConfigs)
      ? route.methodConfigs
          .map((operation: any) => operation.handler)
          .filter(Boolean)
      : route.handlers;
    if (Array.isArray(handlers)) {
      for (const handler of handlers) {
        if (!handler.sourceCode && !handler.compiledCode && !handler.logic) {
          continue;
        }
        const code = this.resolveScriptCode('enfyra_route_handler', handler);
        if (code) {
          handler.logic = code;
        }
      }
    }

    if (route.preHooks && Array.isArray(route.preHooks)) {
      for (const hook of route.preHooks) {
        if (
          hook.isGlobal === true ||
          (!hook.sourceCode && !hook.compiledCode && !hook.code)
        ) {
          continue;
        }
        const code = this.resolveScriptCode('enfyra_pre_hook', hook);
        if (code) {
          hook.code = code;
        }
      }
    }

    if (route.postHooks && Array.isArray(route.postHooks)) {
      for (const hook of route.postHooks) {
        if (
          hook.isGlobal === true ||
          (!hook.sourceCode && !hook.compiledCode && !hook.code)
        ) {
          continue;
        }
        const code = this.resolveScriptCode('enfyra_post_hook', hook);
        if (code) {
          hook.code = code;
        }
      }
    }
  }

  private resolveScriptCode(tableName: string, record: any): string | null {
    const normalized = normalizeScriptRecord(tableName, record);
    Object.assign(record, normalized);

    if (
      typeof record?.compiledCode === 'string' &&
      record.compiledCode.length > 0
    ) {
      return record.compiledCode;
    }

    if (record.sourceCode) {
      const compiledCode = compileScriptSource(
        record.sourceCode,
        record.scriptLanguage || 'typescript',
      );
      record.compiledCode = compiledCode;
      return compiledCode;
    }

    const result = resolveExecutableScript(record);
    if (result.shouldPersistCompiledCode) {
      record.compiledCode = result.compiledCode;
    }
    return result.code;
  }

  protected transformData(data: {
    routes: any[];
    methods: string[];
  }): RouteData {
    return data;
  }

  protected override async afterTransform(data: RouteData): Promise<void> {
    if (!this.usesSharedRuntimeCache()) {
      this.buildRouteEngine(data.routes);
    }
  }

  protected override async afterSharedCachePersist(
    data: RouteData,
  ): Promise<void> {
    await this.persistRedisRouteLookup(data.routes);
  }

  protected override emitLoadedEvent(): void {
    this.eventEmitter?.emit(CACHE_EVENTS.ROUTE_LOADED);
  }

  protected override getLogCount(): string {
    return `${this.cache?.routes?.length ?? 0} routes, ${this.cache?.methods?.length ?? 0} methods`;
  }

  protected override getCount(): number {
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
        ? raw.map((m: any) => m?.name ?? m).filter(Boolean)
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
    return matchRouteInRoutes(cache.routes, method, path);
  }

  private async persistRedisRouteLookup(routes: any[]): Promise<void> {
    if (!this.usesSharedRuntimeCache()) return;
    const entries: RedisRouteMatchIndexEntry[] = [];
    const changes = this.sharedRouteChanges;
    if (changes === null)
      await this.redisRuntimeCacheStore!.deleteAuxByPrefix(
        this.config.cacheIdentifier,
        'route:',
      );
    const remaining = new Set(changes ?? []);
    for (let i = 0; i < routes.length; i++) {
      const route = routes[i];
      const key = String(DatabaseConfigService.getRecordId(route) ?? i);
      const methods = getRouteMethods(route);
      entries.push({
        key,
        path: route.path,
        methods,
        order: i,
      });
      remaining.delete(key);
      if (changes === null || changes.has(key)) {
        await this.redisRuntimeCacheStore!.setAux(
          this.config.cacheIdentifier,
          `route:${key}`,
          route,
        );
      }
    }
    for (const key of remaining)
      await this.redisRuntimeCacheStore!.deleteAux(
        this.config.cacheIdentifier,
        `route:${key}`,
      );
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

    const best = matchRouteIndexEntry(index, method, path);
    if (!best) return null;
    const route = await this.redisRuntimeCacheStore!.getAux<any>(
      this.config.cacheIdentifier,
      `route:${best.entry.key}`,
    );
    return route ? { route, params: best.params } : null;
  }

  private isMethodAvailable(route: any, method: string): boolean {
    return isRouteMethodAvailable(route, method);
  }

  private getRouteMethods(route: any): string[] {
    return getRouteMethods(route);
  }

  private getCandidatePaths(route: any, method: string): string[] {
    return getRouteCandidatePaths(route, method);
  }
}
