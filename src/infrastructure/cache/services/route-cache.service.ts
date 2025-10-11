import { Injectable, Logger, OnModuleInit } from '@nestjs/common';
import { KnexService } from '../../knex/knex.service';
import { RedisPubSubService } from './redis-pubsub.service';
import { InstanceService } from '../../../shared/services/instance.service';
import { ROUTE_CACHE_SYNC_EVENT_KEY } from '../../../shared/utils/constant';

@Injectable()
export class RouteCacheService implements OnModuleInit {
  private readonly logger = new Logger(RouteCacheService.name);
  private routesCache: any[] = [];
  private cacheLoaded = false;

  constructor(
    private readonly knexService: KnexService,
    private readonly redisPubSubService: RedisPubSubService,
    private readonly instanceService: InstanceService,
  ) {}

  async onModuleInit() {
    this.subscribeToRouteCacheSync();
  }

  /**
   * Subscribe to route cache sync events from other instances
   */
  private subscribeToRouteCacheSync() {
    const sub = this.redisPubSubService.sub;
    if (!sub) {
      this.logger.warn('Redis subscription not available for route cache sync');
      return;
    }

    sub.subscribe(ROUTE_CACHE_SYNC_EVENT_KEY);
    
    sub.on('message', (channel: string, message: string) => {
      if (channel === ROUTE_CACHE_SYNC_EVENT_KEY) {
        try {
          const payload = JSON.parse(message);
          const myInstanceId = this.instanceService.getInstanceId();
          
          if (payload.instanceId === myInstanceId) {
            this.logger.debug('⏭️  Skipping route cache sync from self');
            return;
          }

          this.logger.log(`📥 Received route cache sync from instance ${payload.instanceId.slice(0, 8)}...`);
          this.routesCache = payload.routes;
          this.cacheLoaded = true;
          this.logger.log(`✅ Route cache synced: ${payload.routes.length} routes`);
        } catch (error) {
          this.logger.error('Failed to parse route cache sync message:', error);
        }
      }
    });
  }

  /**
   * Get routes from in-memory cache
   */
  async getRoutes(): Promise<any[]> {
    if (!this.cacheLoaded) {
      await this.reloadRouteCache();
    }
    return this.routesCache;
  }

  /**
   * Reload routes cache and publish to other instances
   */
  async reloadRouteCache(): Promise<void> {
    const start = Date.now();
    this.logger.log('🔄 Reloading routes cache...');

    const routes = await this.loadRoutes();
    this.routesCache = routes;
    this.cacheLoaded = true;

    this.logger.log(
      `✅ Loaded ${routes.length} routes in ${Date.now() - start}ms`,
    );

    await this.publishRouteCacheSync(routes);
  }

  /**
   * Publish route cache to other instances via Redis
   */
  private async publishRouteCacheSync(routes: any[]): Promise<void> {
    try {
      const payload = {
        instanceId: this.instanceService.getInstanceId(),
        routes: routes,
        timestamp: Date.now(),
      };

      await this.redisPubSubService.publish(
        ROUTE_CACHE_SYNC_EVENT_KEY,
        JSON.stringify(payload),
      );

      this.logger.log(`📤 Published route cache to other instances (${routes.length} routes)`);
    } catch (error) {
      this.logger.error('Failed to publish route cache sync:', error);
    }
  }

  private async loadRoutes(): Promise<any[]> {
    const knex = this.knexService.getKnex();

    // Load all data in parallel with manual JOINs
    // Note: Bootstrap code - metadata not loaded yet, cannot use .withRelations()
    const [
      routes,
      globalHooks,
      allHooks,
      allHandlers,
      allPermissions,
      allTables,
      allPublishedMethodsJunctions,
      allTargetTablesJunctions,
    ] = await Promise.all([
      knex('route_definition')
        .where({ 'route_definition.isEnabled': true })
        .leftJoin('table_definition as mainTable', 'route_definition.mainTableId', 'mainTable.id')
        .select('route_definition.*', 'mainTable.id as mainTable_id', 'mainTable.name as mainTable_name'),
      knex('hook_definition').where('isEnabled', true).whereNull('routeId').orderBy('priority', 'asc').select('*'),
      knex('hook_definition').where('isEnabled', true).orderBy('priority', 'asc').select('*'),
      knex('route_handler_definition')
        .leftJoin('method_definition as method', 'route_handler_definition.methodId', 'method.id')
        .select('route_handler_definition.*', 'method.id as method_id', 'method.method as method_method'),
      knex('route_permission_definition')
        .where({ 'route_permission_definition.isEnabled': true })
        .leftJoin('role_definition as role', 'route_permission_definition.roleId', 'role.id')
        .select('route_permission_definition.*', 'role.id as role_id', 'role.name as role_name'),
      knex('table_definition').select('*'),
      knex('method_definition_routes_route_definition').select('*'),
      knex('route_definition_targetTables_table_definition').select('*'),
    ]);

    const tablesMap = new Map(allTables.map((t: any) => [t.id, t]));

    const hooksByRoute = new Map();
    allHooks.forEach((hook: any) => {
      if (hook.routeId) {
        if (!hooksByRoute.has(hook.routeId)) {
          hooksByRoute.set(hook.routeId, []);
        }
        hooksByRoute.get(hook.routeId).push(hook);
      }
    });

    const handlersByRoute = new Map();
    allHandlers.forEach((h: any) => {
      if (!handlersByRoute.has(h.routeId)) {
        handlersByRoute.set(h.routeId, []);
      }
      handlersByRoute.get(h.routeId).push(h);
    });

    const permsByRoute = new Map();
    allPermissions.forEach((p: any) => {
      if (!permsByRoute.has(p.routeId)) {
        permsByRoute.set(p.routeId, []);
      }
      permsByRoute.get(p.routeId).push(p);
    });

    const publishedMethodsByRoute = new Map();
    allPublishedMethodsJunctions.forEach((j: any) => {
      if (!publishedMethodsByRoute.has(j.routeDefinitionId)) {
        publishedMethodsByRoute.set(j.routeDefinitionId, []);
      }
      publishedMethodsByRoute.get(j.routeDefinitionId).push(j.methodDefinitionId);
    });

    const targetTablesByRoute = new Map();
    allTargetTablesJunctions.forEach((j: any) => {
      if (!targetTablesByRoute.has(j.routeDefinitionId)) {
        targetTablesByRoute.set(j.routeDefinitionId, []);
      }
      targetTablesByRoute.get(j.routeDefinitionId).push(j.tableDefinitionId);
    });

    for (const route of routes) {
      // Nest mainTable (from JOIN)
      if (route.mainTable_id) {
        route.mainTable = {
          id: route.mainTable_id,
          name: route.mainTable_name,
        };
        delete route.mainTable_id;
        delete route.mainTable_name;
      }
      
      // Map targetTables from junction
      const targetTableIds = targetTablesByRoute.get(route.id) || [];
      route.targetTables = targetTableIds
        .map((tid: any) => tablesMap.get(tid))
        .filter(Boolean);

      // Map publishedMethods from junction  
      const publishedMethodIds = publishedMethodsByRoute.get(route.id) || [];
      route.publishedMethods = publishedMethodIds
        .map((mid: any) => ({ id: mid }))
        .filter(Boolean);
      
      // Nest handlers (from JOIN)
      const handlers = handlersByRoute.get(route.id) || [];
      for (const handler of handlers) {
        if (handler.method_id) {
          handler.method = {
            id: handler.method_id,
            method: handler.method_method,
          };
          delete handler.method_id;
          delete handler.method_method;
        }
      }
      route.handlers = handlers;

      const hooks = hooksByRoute.get(route.id) || [];
      route.hooks = [...globalHooks, ...hooks];

      // Nest permissions (from JOIN)
      const permissions = permsByRoute.get(route.id) || [];
      for (const perm of permissions) {
        if (perm.role_id) {
          perm.role = {
            id: perm.role_id,
            name: perm.role_name,
          };
          delete perm.role_id;
          delete perm.role_name;
        }
        perm.allowedUsers = [];
        perm.methods = [];
      }
      route.routePermissions = permissions;
    }

    return routes;
  }
}
