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
import { getForeignKeyColumnName, getJunctionTableName } from '../../../shared/utils/naming-helpers';

@Injectable()
export class RouteCacheService implements OnModuleInit, OnApplicationBootstrap {
  private readonly logger = new Logger(RouteCacheService.name);
  private routesCache: any[] = [];
  private cacheLoaded = false;
  private messageHandler: ((channel: string, message: string) => void) | null = null;

  constructor(
    private readonly queryBuilder: QueryBuilderService,
    private readonly redisPubSubService: RedisPubSubService,
    private readonly cacheService: CacheService,
    private readonly metadataCacheService: MetadataCacheService,
    private readonly instanceService: InstanceService,
  ) {}

  async onModuleInit() {
    this.subscribe();
  }

  async onApplicationBootstrap() {
    // IMPORTANT: Check if system tables exist before loading routes
    // Even if metadata loads successfully, system tables may not exist yet
    const knex = this.queryBuilder.getKnex();

    const hasRouteTable = await knex.schema.hasTable('route_definition');
    if (!hasRouteTable) {
      this.logger.warn('⚠️ System tables not initialized yet, skipping route cache load');
      return;
    }

    await this.reload();
  }

  /**
   * Subscribe to route sync messages from other instances
   */
  private subscribe() {
    const sub = this.redisPubSubService.sub;
    if (!sub) {
      this.logger.warn('Redis subscription not available for route cache sync');
      return;
    }

    // Only subscribe if not already subscribed
    if (this.messageHandler) {
      return;
    }

    // Create and store handler
    this.messageHandler = (channel: string, message: string) => {
      if (channel === ROUTE_CACHE_SYNC_EVENT_KEY) {
        try {
          const payload = JSON.parse(message);
          const myInstanceId = this.instanceService.getInstanceId();

          if (payload.instanceId === myInstanceId) {
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
    };

    // Subscribe via RedisPubSubService (prevents duplicates)
    this.redisPubSubService.subscribeWithHandler(
      ROUTE_CACHE_SYNC_EVENT_KEY,
      this.messageHandler
    );
  }

  /**
   * Get routes from in-memory cache
   */
  async getRoutes(): Promise<any[]> {
    if (!this.cacheLoaded) {
      await this.reload();
    }
    return this.routesCache;
  }

  /**
   * Reload routes from DB (acquire lock → load → publish → save)
   */
  async reload(): Promise<void> {
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
        const start = Date.now();
        this.logger.log('🔄 Reloading routes cache...');

        // Load from DB
        const routes = await this.loadRoutes();
        this.logger.log(`✅ Loaded ${routes.length} routes in ${Date.now() - start}ms`);

        // Broadcast to other instances FIRST
        await this.publish(routes);

        // Then save to local memory cache
        this.routesCache = routes;
        this.cacheLoaded = true;
      } finally {
        await this.cacheService.release(ROUTE_RELOAD_LOCK_KEY, instanceId);
        this.logger.log('🔓 Released route reload lock');
      }
    } catch (error) {
      this.logger.error('❌ Failed to reload route cache:', error);
      throw error;
    }
  }

  /**
   * Publish routes to other instances via Redis PubSub
   */
  private async publish(routes: any[]): Promise<void> {
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
        'hooks.*',
        'hooks.methods.*',
        'publishedMethods.*',
        'targetTables.*',
      ],
    });
    const routes = result.data;

    // Get global hooks separately
    const globalHooksResult = await this.queryBuilder.select({
      tableName: 'hook_definition',
      filter: {
        isEnabled: { _eq: true },
        routeId: { _null: true }
      },
      fields: ['*', 'methods.*'],
      sort: ['priority'],
    });
    const globalHooks = globalHooksResult.data;

    // Merge global hooks with route hooks
    for (const route of routes) {
      route.hooks = [...globalHooks, ...(route.hooks || [])];
      if (!route.targetTables) {
        route.targetTables = [];
      }
    }

    return routes;
  }

  private async loadRoutesForSQL(): Promise<any[]> {
    // IMPORTANT: Use raw Knex queries instead of queryBuilder.select()
    // Reason: Avoid circular dependency on metadata during query building
    // Note: Caller ensures metadata is loaded, so all tables exist
    const knex = this.queryBuilder.getKnex();

    // Generate junction table names using naming convention helpers
    // IMPORTANT: Use the direction where the relation is DEFINED (not inverse)
    const hookMethodsJunction = getJunctionTableName('hook_definition', 'methods', 'method_definition');
    const publishedMethodsJunction = getJunctionTableName('method_definition', 'routes', 'route_definition'); // Defined in method_definition
    const targetTablesJunction = getJunctionTableName('route_definition', 'targetTables', 'table_definition');

    // Load routes with all nested relations using raw SQL
    // We use JSON subqueries instead of JOINs to avoid metadata dependency
    const routes = await knex('route_definition')
      .where('isEnabled', true)
      .select([
        'route_definition.*',
        // mainTable relation
        knex.raw(`(
          SELECT JSON_OBJECT(
            'id', td.id,
            'name', td.name,
            'alias', td.alias,
            'description', td.description
          )
          FROM table_definition td
          WHERE td.id = route_definition.mainTableId
        ) as mainTable`),
        // handlers relation with nested method
        knex.raw(`(
          SELECT IFNULL(JSON_ARRAYAGG(
            JSON_OBJECT(
              'id', h.id,
              'logic', h.logic,
              'timeout', h.timeout,
              'description', h.description
            )
          ), JSON_ARRAY())
          FROM route_handler_definition h
          WHERE h.routeId = route_definition.id
        ) as handlers`),
        // hooks relation with nested methods
        knex.raw(`(
          SELECT IFNULL(JSON_ARRAYAGG(
            JSON_OBJECT(
              'id', hd.id,
              'name', hd.name,
              'preHook', hd.preHook,
              'afterHook', hd.afterHook,
              'preHookTimeout', hd.preHookTimeout,
              'afterHookTimeout', hd.afterHookTimeout,
              'priority', hd.priority,
              'description', hd.description,
              'methods', (
                SELECT IFNULL(JSON_ARRAYAGG(JSON_OBJECT('id', m.id, 'method', m.method)), JSON_ARRAY())
                FROM \`${hookMethodsJunction}\` hm
                JOIN method_definition m ON m.id = hm.methodDefinitionId
                WHERE hm.hookDefinitionId = hd.id
              )
            )
          ), JSON_ARRAY())
          FROM hook_definition hd
          WHERE hd.routeId = route_definition.id AND hd.isEnabled = true
          ORDER BY hd.priority
        ) as hooks`),
        // routePermissions relation with nested role
        knex.raw(`(
          SELECT IFNULL(JSON_ARRAYAGG(
            JSON_OBJECT(
              'id', rp.id,
              'description', rp.description,
              'isEnabled', rp.isEnabled,
              'roleId', rp.roleId,
              'role', (
                SELECT JSON_OBJECT('id', r.id, 'name', r.name)
                FROM role_definition r
                WHERE r.id = rp.roleId
              )
            )
          ), JSON_ARRAY())
          FROM route_permission_definition rp
          WHERE rp.routeId = route_definition.id
        ) as routePermissions`),
        // publishedMethods relation
        knex.raw(`(
          SELECT IFNULL(JSON_ARRAYAGG(JSON_OBJECT('id', m.id, 'method', m.method)), JSON_ARRAY())
          FROM \`${publishedMethodsJunction}\` rpm
          JOIN method_definition m ON m.id = rpm.methodDefinitionId
          WHERE rpm.routeDefinitionId = route_definition.id
        ) as publishedMethods`),
        // targetTables relation
        knex.raw(`(
          SELECT IFNULL(JSON_ARRAYAGG(JSON_OBJECT('id', t.id, 'name', t.name)), JSON_ARRAY())
          FROM \`${targetTablesJunction}\` rtt
          JOIN table_definition t ON t.id = rtt.tableDefinitionId
          WHERE rtt.routeDefinitionId = route_definition.id
        ) as targetTables`),
      ])
      .orderBy('route_definition.id');

    // Load global hooks separately
    const globalHooks = await knex('hook_definition')
      .where('isEnabled', true)
      .whereNull('routeId')
      .select([
        'hook_definition.*',
        knex.raw(`(
          SELECT IFNULL(JSON_ARRAYAGG(JSON_OBJECT('id', m.id, 'method', m.method)), JSON_ARRAY())
          FROM \`${hookMethodsJunction}\` hm
          JOIN method_definition m ON m.id = hm.methodDefinitionId
          WHERE hm.hookDefinitionId = hook_definition.id
        ) as methods`),
      ])
      .orderBy('priority');

    // Parse JSON strings and merge global hooks
    for (const route of routes) {
      // Parse all JSON fields
      if (typeof route.mainTable === 'string') route.mainTable = JSON.parse(route.mainTable);
      if (typeof route.handlers === 'string') route.handlers = JSON.parse(route.handlers);
      if (typeof route.hooks === 'string') route.hooks = JSON.parse(route.hooks);
      if (typeof route.routePermissions === 'string') route.routePermissions = JSON.parse(route.routePermissions);
      if (typeof route.publishedMethods === 'string') route.publishedMethods = JSON.parse(route.publishedMethods);
      if (typeof route.targetTables === 'string') route.targetTables = JSON.parse(route.targetTables);

      // Merge global hooks (prepend to route-specific hooks)
      const parsedGlobalHooks = globalHooks.map(h => ({
        ...h,
        methods: typeof h.methods === 'string' ? JSON.parse(h.methods) : h.methods
      }));
      route.hooks = [...parsedGlobalHooks, ...route.hooks];
    }

    return routes;
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
