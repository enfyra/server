import { Injectable, Logger, OnModuleInit } from '@nestjs/common';
import { QueryBuilderService } from '../../query-builder/query-builder.service';
import { RedisPubSubService } from './redis-pubsub.service';
import { CacheService } from './cache.service';
import { InstanceService } from '../../../shared/services/instance.service';
import { 
  ROUTE_CACHE_SYNC_EVENT_KEY,
  ROUTE_RELOAD_LOCK_KEY,
  REDIS_TTL,
} from '../../../shared/utils/constant';
import { getForeignKeyColumnName } from '../../../shared/utils/naming-helpers';

@Injectable()
export class RouteCacheService implements OnModuleInit {
  private readonly logger = new Logger(RouteCacheService.name);
  private routesCache: any[] = [];
  private cacheLoaded = false;

  constructor(
    private readonly queryBuilder: QueryBuilderService,
    private readonly redisPubSubService: RedisPubSubService,
    private readonly cacheService: CacheService,
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

  async reloadRouteCache(): Promise<void> {
    const instanceId = this.instanceService.getInstanceId();
    
    try {
      const acquired = await this.cacheService.acquire(
        ROUTE_RELOAD_LOCK_KEY, 
        instanceId, 
        REDIS_TTL.RELOAD_LOCK_TTL
      );
      
      if (!acquired) {
        this.logger.log('🔒 Another instance is reloading routes, waiting for broadcast...');
        return;
      }

      this.logger.log(`🔓 Acquired route reload lock (instance ${instanceId.slice(0, 8)})`);
      
      try {
        await this.performReload();
      } finally {
        await this.cacheService.release(ROUTE_RELOAD_LOCK_KEY, instanceId);
        this.logger.log('🔓 Released route reload lock');
      }
    } catch (error) {
      this.logger.error('❌ Failed to reload route cache:', error);
      throw error;
    }
  }

  private async performReload(): Promise<void> {
    const start = Date.now();
    this.logger.log('🔄 Reloading routes cache...');

    const routes = await this.loadRoutes();
    
    this.routesCache = routes;
    this.cacheLoaded = true;

    this.logger.log(`✅ Loaded ${routes.length} routes in ${Date.now() - start}ms`);

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
    
    // MongoDB uses aggregation pipeline (already optimized)
    if (isMongoDB) {
      return this.loadRoutesForMongoDB();
    }
    
    // SQL: Use single query with all JOINs
    return this.loadRoutesForSQL();
  }

  private async loadRoutesForMongoDB(): Promise<any[]> {
    const routes = await this.queryBuilder.select({
      table: 'route_definition',
      where: [{ field: 'isEnabled', operator: '=', value: true }],
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
            publishedMethodsInfo: 0
          }
        }
      ]
    });

    const globalHooks = await this.queryBuilder.select({
      table: 'hook_definition',
      where: [
        { field: 'isEnabled', operator: '=', value: true },
        { field: 'route', operator: 'is null', value: undefined }
      ],
      sort: [{ field: 'priority', direction: 'asc' }],
    });

    for (const route of routes) {
      route.hooks = [...globalHooks, ...(route.hooks || [])];
      if (!route.targetTables) {
        route.targetTables = [];
      }
    }

    return routes;
  }

  private async loadRoutesForSQL(): Promise<any[]> {
    const rows = await this.queryBuilder.select({
      table: 'route_definition',
      where: [{ field: 'isEnabled', operator: '=', value: true }],
      fields: [
        '*',
        'mainTable.*',
        'handlers.*',
        'handlers.method.*',
        'routePermissions.*',
        'routePermissions.role.*',
        'hooks.*',
        'hooks.methods.*',
        'publishedMethods.*',
        'targetTables.*',
      ],
      sort: [
        { field: 'id', direction: 'asc' },
        { field: 'hooks.priority', direction: 'asc' },
      ],
    });

    const globalHooksRows = await this.queryBuilder.select({
      table: 'hook_definition',
      where: [
        { field: 'isEnabled', operator: '=', value: true },
        { field: 'routeId', operator: 'is null', value: undefined },
      ],
      fields: ['*', 'methods.*'],
      sort: [{ field: 'priority', direction: 'asc' }],
    });

    return this.denormalizeRoutes(rows, globalHooksRows);
  }

  private denormalizeRoutes(rows: any[], globalHooks: any[]): any[] {
    const routesMap = new Map<number, any>();

    for (const row of rows) {
      const routeId = row.id;

      if (!routesMap.has(routeId)) {
        routesMap.set(routeId, {
          id: row.id,
          path: row.path,
          method: row.method,
          isEnabled: row.isEnabled,
          description: row.description,
          mainTableId: row.mainTableId,
          createdAt: row.createdAt,
          updatedAt: row.updatedAt,
          mainTable: row.mainTable_id ? {
            id: row.mainTable_id,
            name: row.mainTable_name,
          } : null,
          handlers: [],
          routePermissions: [],
          hooks: [],
          publishedMethods: [],
          targetTables: [],
        });
      }

      const route = routesMap.get(routeId);

      if (row.handler_id && !route.handlers.find((h: any) => h.id === row.handler_id)) {
        route.handlers.push({
          id: row.handler_id,
          name: row.handler_name,
          code: row.handler_code,
          isEnabled: row.handler_isEnabled,
          method: row.handler_method_id ? {
            id: row.handler_method_id,
            method: row.handler_method_method,
          } : null,
        });
      }

      // Add permission (if not already added)
      if (row.perm_id && !route.routePermissions.find((p: any) => p.id === row.perm_id)) {
        route.routePermissions.push({
          id: row.perm_id,
          description: row.perm_description,
          isEnabled: row.perm_isEnabled,
          role: row.perm_role_id ? {
            id: row.perm_role_id,
            name: row.perm_role_name,
          } : null,
          allowedUsers: [],
          methods: [],
        });
      }

      // Add hook (if not already added)
      if (row.hook_id && !route.hooks.find((h: any) => h.id === row.hook_id)) {
        const hook: any = {
          id: row.hook_id,
          name: row.hook_name,
          preHook: row.hook_preHook,
          afterHook: row.hook_afterHook,
          preHookTimeout: row.hook_preHookTimeout,
          afterHookTimeout: row.hook_afterHookTimeout,
          priority: row.hook_priority,
          description: row.hook_description,
          methods: [],
        };

        // Add hook method
        if (row.hook_method_id) {
          hook.methods.push({
            id: row.hook_method_id,
            method: row.hook_method_method,
          });
        }

        route.hooks.push(hook);
      } else if (row.hook_id && row.hook_method_id) {
        // Add method to existing hook
        const hook = route.hooks.find((h: any) => h.id === row.hook_id);
        if (hook && !hook.methods.find((m: any) => m.id === row.hook_method_id)) {
          hook.methods.push({
            id: row.hook_method_id,
            method: row.hook_method_method,
          });
        }
      }

      // Add published method (if not already added)
      if (row.published_method_id && !route.publishedMethods.find((m: any) => m.id === row.published_method_id)) {
        route.publishedMethods.push({
          id: row.published_method_id,
          method: row.published_method_method,
        });
      }

      // Add target table (if not already added)
      if (row.target_table_id && !route.targetTables.find((t: any) => t.id === row.target_table_id)) {
        route.targetTables.push({
          id: row.target_table_id,
          name: row.target_table_name,
        });
      }
    }

    // Process global hooks
    const processedGlobalHooks: any[] = [];
    const globalHooksMap = new Map<number, any>();

    for (const hookRow of globalHooks) {
      if (!globalHooksMap.has(hookRow.id)) {
        globalHooksMap.set(hookRow.id, {
          id: hookRow.id,
          name: hookRow.name,
          preHook: hookRow.preHook,
          afterHook: hookRow.afterHook,
          preHookTimeout: hookRow.preHookTimeout,
          afterHookTimeout: hookRow.afterHookTimeout,
          priority: hookRow.priority,
          description: hookRow.description,
          methods: [],
        });
      }

      const hook = globalHooksMap.get(hookRow.id);
      if (hookRow.method_id && !hook.methods.find((m: any) => m.id === hookRow.method_id)) {
        hook.methods.push({
          id: hookRow.method_id,
          method: hookRow.method_method,
        });
      }
    }

    processedGlobalHooks.push(...globalHooksMap.values());

    // Prepend global hooks to each route
    const routes = Array.from(routesMap.values());
    for (const route of routes) {
      route.hooks = [...processedGlobalHooks, ...route.hooks];
    }

    return routes;
  }
}
