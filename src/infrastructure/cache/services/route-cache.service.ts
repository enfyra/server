import { Injectable, Logger, OnModuleInit } from '@nestjs/common';
import { QueryBuilderService } from '../../query-builder/query-builder.service';
import { RedisPubSubService } from './redis-pubsub.service';
import { InstanceService } from '../../../shared/services/instance.service';
import { ROUTE_CACHE_SYNC_EVENT_KEY } from '../../../shared/utils/constant';
import { getForeignKeyColumnName } from '../../../shared/utils/naming-helpers';

@Injectable()
export class RouteCacheService implements OnModuleInit {
  private readonly logger = new Logger(RouteCacheService.name);
  private routesCache: any[] = [];
  private cacheLoaded = false;

  constructor(
    private readonly queryBuilder: QueryBuilderService,
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
    const isMongoDB = process.env.DB_TYPE === 'mongodb';
    
    // Get FK column names using naming convention (for SQL only)
    const routeDefinitionIdCol = getForeignKeyColumnName('route_definition');
    const methodDefinitionIdCol = getForeignKeyColumnName('method_definition');
    const tableDefinitionIdCol = getForeignKeyColumnName('table_definition');
    
    // Load all data in parallel - different queries for SQL vs MongoDB
    const [
      routes,
      globalHooks,
      allHooks,
      allHandlers,
      allPermissions,
      allTables,
      allMethods,
      allPublishedMethodsJunctions,
      allTargetTablesJunctions,
      allHookMethodsJunctions,
    ] = await Promise.all([
      // Routes query - different for SQL vs MongoDB
      isMongoDB ? this.queryBuilder.select({
        table: 'route_definition',
        where: [{ field: 'isEnabled', operator: '=', value: true }],
        // MongoDB: Use aggregation pipeline for joins
        pipeline: [
          {
            $match: { isEnabled: true }
          },
          {
            $lookup: {
              from: 'table_definition',
              localField: 'mainTable',
              foreignField: '_id',
              as: 'mainTableInfo'
            }
          },
          {
            $lookup: {
              from: 'method_definition',
              localField: 'publishedMethods',
              foreignField: '_id',
              as: 'publishedMethodsInfo'
            }
          },
          {
            $lookup: {
              from: 'route_permission_definition',
              let: { routeId: '$_id' },
              pipeline: [
                {
                  $match: {
                    $expr: {
                      $and: [
                        { $eq: ['$route', '$$routeId'] },
                        { $eq: ['$isEnabled', true] }
                      ]
                    }
                  }
                },
                {
                  $lookup: {
                    from: 'role_definition',
                    localField: 'role',
                    foreignField: '_id',
                    as: 'roleInfo'
                  }
                },
                {
                  $addFields: {
                    role: {
                      $cond: {
                        if: { $gt: [{ $size: '$roleInfo' }, 0] },
                        then: { $arrayElemAt: ['$roleInfo', 0] },
                        else: null
                      }
                    },
                    allowedUsers: [],
                    methods: []
                  }
                },
                {
                  $project: {
                    roleInfo: 0
                  }
                }
              ],
              as: 'routePermissions'
            }
          },
          {
            $lookup: {
              from: 'route_handler_definition',
              let: { routeId: '$_id' },
              pipeline: [
                {
                  $match: {
                    $expr: { $eq: ['$route', '$$routeId'] }
                  }
                },
                {
                  $lookup: {
                    from: 'method_definition',
                    localField: 'method',
                    foreignField: '_id',
                    as: 'methodInfo'
                  }
                },
                {
                  $addFields: {
                    method: {
                      $cond: {
                        if: { $gt: [{ $size: '$methodInfo' }, 0] },
                        then: { $arrayElemAt: ['$methodInfo', 0] },
                        else: null
                      }
                    }
                  }
                },
                {
                  $project: {
                    methodInfo: 0
                  }
                }
              ],
              as: 'handlers'
            }
          },
          {
            $lookup: {
              from: 'hook_definition',
              let: { routeId: '$_id' },
              pipeline: [
                {
                  $match: {
                    $expr: {
                      $and: [
                        { $eq: ['$route', '$$routeId'] },
                        { $eq: ['$isEnabled', true] }
                      ]
                    }
                  }
                },
                {
                  $sort: { priority: 1 }
                }
              ],
              as: 'hooks'
            }
          },
          {
            $addFields: {
              mainTable: {
                $cond: {
                  if: { $gt: [{ $size: '$mainTableInfo' }, 0] },
                  then: { $arrayElemAt: ['$mainTableInfo', 0] },
                  else: null
                }
              },
              publishedMethods: '$publishedMethodsInfo'
            }
          },
          {
            $project: {
              mainTableInfo: 0,
              publishedMethodsInfo: 0 // Remove the joined arrays
            }
          }
        ]
      }) : this.queryBuilder.select({
        table: 'route_definition',
        where: [{ field: 'route_definition.isEnabled', operator: '=', value: true }],
        join: [{ type: 'left', table: 'table_definition as mainTable', on: { local: 'route_definition.mainTableId', foreign: 'mainTable.id' }}],
        select: ['route_definition.*', 'mainTable.id as mainTable_id', 'mainTable.name as mainTable_name'],
      }),
      
      // Global hooks - different field names for SQL vs MongoDB
      this.queryBuilder.select({
        table: 'hook_definition',
        where: [
          { field: 'isEnabled', operator: '=', value: true }, 
          { field: isMongoDB ? 'route' : 'routeId', operator: 'is null', value: undefined }
        ],
        sort: [{ field: 'priority', direction: 'asc' }],
      }),
      
      // All hooks
      this.queryBuilder.select({
        table: 'hook_definition',
        where: [{ field: 'isEnabled', operator: '=', value: true }],
        sort: [{ field: 'priority', direction: 'asc' }],
      }),
      
      // Route handlers - different for SQL vs MongoDB
      isMongoDB ? this.queryBuilder.select({
        table: 'route_handler_definition',
        pipeline: [
          {
            $lookup: {
              from: 'method_definition',
              localField: 'method',
              foreignField: '_id',
              as: 'methodInfo'
            }
          },
          {
            $addFields: {
              method_id: { $arrayElemAt: ['$methodInfo._id', 0] },
              method_method: { $arrayElemAt: ['$methodInfo.method', 0] }
            }
          },
          {
            $project: {
              methodInfo: 0
            }
          }
        ]
      }) : this.queryBuilder.select({
        table: 'route_handler_definition',
        join: [{ type: 'left', table: 'method_definition as method', on: { local: 'route_handler_definition.methodId', foreign: 'method.id' }}],
        select: ['route_handler_definition.*', 'method.id as method_id', 'method.method as method_method'],
      }),
      
      // Route permissions - different for SQL vs MongoDB
      isMongoDB ? this.queryBuilder.select({
        table: 'route_permission_definition',
        where: [{ field: 'isEnabled', operator: '=', value: true }],
        pipeline: [
          {
            $match: { isEnabled: true }
          },
          {
            $lookup: {
              from: 'role_definition',
              localField: 'role',
              foreignField: '_id',
              as: 'roleInfo'
            }
          },
          {
            $addFields: {
              role_id: { $arrayElemAt: ['$roleInfo._id', 0] },
              role_name: { $arrayElemAt: ['$roleInfo.name', 0] }
            }
          },
          {
            $project: {
              roleInfo: 0
            }
          }
        ]
      }) : this.queryBuilder.select({
        table: 'route_permission_definition',
        where: [{ field: 'route_permission_definition.isEnabled', operator: '=', value: true }],
        join: [{ type: 'left', table: 'role_definition as role', on: { local: 'route_permission_definition.roleId', foreign: 'role.id' }}],
        select: ['route_permission_definition.*', 'role.id as role_id', 'role.name as role_name'],
      }),
      
      this.queryBuilder.select({ table: 'table_definition' }),
      this.queryBuilder.select({ table: 'method_definition' }),
      // Junction tables - only for SQL, MongoDB uses embedded arrays
      isMongoDB ? [] : this.queryBuilder.select({ table: 'method_definition_routes_route_definition' }),
      isMongoDB ? [] : this.queryBuilder.select({ table: 'route_definition_targetTables_table_definition' }),
      isMongoDB ? [] : this.queryBuilder.select({ table: 'hook_definition_methods_method_definition' }),
    ]);

    // Build maps using correct ID field for SQL vs MongoDB
    const idField = isMongoDB ? '_id' : 'id';
    const tablesMap = new Map(allTables.map((t: any) => [isMongoDB ? t._id?.toString() : t.id, t]));
    const methodsMap = new Map(allMethods.map((m: any) => [isMongoDB ? m._id?.toString() : m.id, m]));

    // Map hooks → methods from junction table
    const hookMethodsMap = new Map();
    if (isMongoDB) {
      // MongoDB: methods is already an array of ObjectIds in hook_definition
      allHooks.forEach((hook: any) => {
        if (hook.methods && hook.methods.length > 0) {
          hookMethodsMap.set(hook._id, hook.methods);
        }
      });
    } else {
      // SQL: Process junction table
      (allHookMethodsJunctions || []).forEach((j: any) => {
        const hookId = j.hookDefinitionId || j.hook_definition_id;
        const methodId = j.methodDefinitionId || j.method_definition_id;
        if (!hookMethodsMap.has(hookId)) {
          hookMethodsMap.set(hookId, []);
        }
        hookMethodsMap.get(hookId).push(methodId);
      });
    }

    const hooksByRoute = new Map();
    allHooks.forEach((hook: any) => {
      const routeId = isMongoDB ? hook.route?.toString() : hook.routeId;
      const hookId = isMongoDB ? hook._id?.toString() : hook.id;
      
      // Map method IDs to method objects
      const methodIds = hookMethodsMap.get(hookId) || [];
      hook.methods = methodIds
        .map((methodId: any) => methodsMap.get(isMongoDB ? methodId.toString() : methodId))
        .filter(Boolean);
      
      if (routeId) {
        if (!hooksByRoute.has(routeId)) {
          hooksByRoute.set(routeId, []);
        }
        hooksByRoute.get(routeId).push(hook);
      }
    });

    const handlersByRoute = new Map();
    allHandlers.forEach((h: any) => {
      const routeId = isMongoDB ? h.route?.toString() : h.routeId;
      if (routeId) {
        if (!handlersByRoute.has(routeId)) {
          handlersByRoute.set(routeId, []);
        }
        handlersByRoute.get(routeId).push(h);
      }
    });

    const permsByRoute = new Map();
    allPermissions.forEach((p: any) => {
      const routeId = isMongoDB ? p.route?.toString() : p.routeId;
      if (routeId) {
        if (!permsByRoute.has(routeId)) {
          permsByRoute.set(routeId, []);
        }
        permsByRoute.get(routeId).push(p);
      }
    });

    const publishedMethodsByRoute = new Map();
    if (isMongoDB) {
      // MongoDB: publishedMethods is already an array of ObjectIds in route_definition
      routes.forEach((route: any) => {
        if (route.publishedMethods && route.publishedMethods.length > 0) {
          publishedMethodsByRoute.set(route._id, route.publishedMethods);
        }
      });
    } else {
      // SQL: Process junction table
      allPublishedMethodsJunctions.forEach((j: any) => {
        const routeId = j[routeDefinitionIdCol];
        const methodId = j[methodDefinitionIdCol];
        if (!publishedMethodsByRoute.has(routeId)) {
          publishedMethodsByRoute.set(routeId, []);
        }
        publishedMethodsByRoute.get(routeId).push(methodId);
      });
    }

    const targetTablesByRoute = new Map();
    if (isMongoDB) {
      // MongoDB: targetTables is already an array of ObjectIds in route_definition (if exists)
      routes.forEach((route: any) => {
        if (route.targetTables && route.targetTables.length > 0) {
          targetTablesByRoute.set(route._id, route.targetTables);
        }
      });
    } else {
      // SQL: Process junction table
      allTargetTablesJunctions.forEach((j: any) => {
        const routeId = j[routeDefinitionIdCol];
        const tableId = j[tableDefinitionIdCol];
        if (!targetTablesByRoute.has(routeId)) {
          targetTablesByRoute.set(routeId, []);
        }
        targetTablesByRoute.get(routeId).push(tableId);
      });
    }

    for (const route of routes) {
      const routeId = isMongoDB ? route._id?.toString() : route.id;
      
      if (isMongoDB) {
        // MongoDB: Data already nested from aggregation pipeline
        // Just prepend global hooks
        route.hooks = [...globalHooks, ...(route.hooks || [])];
        
        // Ensure targetTables is empty array if not set
        if (!route.targetTables) {
          route.targetTables = [];
        }
      } else {
        // SQL: Need to nest and map data
        
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
        const targetTableIds = targetTablesByRoute.get(routeId) || [];
        route.targetTables = targetTableIds
          .map((tid: any) => tablesMap.get(tid))
          .filter(Boolean);

        // Map publishedMethods from junction with full method data
        const publishedMethodIds = publishedMethodsByRoute.get(routeId) || [];
        route.publishedMethods = publishedMethodIds
          .map((mid: any) => methodsMap.get(mid))
          .filter(Boolean);
        
        // Nest handlers (from JOIN)
        const handlers = handlersByRoute.get(routeId) || [];
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

        const hooks = hooksByRoute.get(routeId) || [];
        route.hooks = [...globalHooks, ...hooks];

        // Nest permissions (from JOIN)
        const permissions = permsByRoute.get(routeId) || [];
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
    }

    return routes;
  }
}
