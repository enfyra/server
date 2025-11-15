import { DynamicRepository } from '../../../dynamic-api/repositories/dynamic.repository';
import { QueryBuilderService } from '../../../../infrastructure/query-builder/query-builder.service';
import { TableHandlerService } from '../../../table-management/services/table-handler.service';
import { QueryEngine } from '../../../../infrastructure/query-engine/services/query-engine.service';
import { RouteCacheService } from '../../../../infrastructure/cache/services/route-cache.service';
import { StorageConfigCacheService } from '../../../../infrastructure/cache/services/storage-config-cache.service';
import { AiConfigCacheService } from '../../../../infrastructure/cache/services/ai-config-cache.service';
import { MetadataCacheService } from '../../../../infrastructure/cache/services/metadata-cache.service';
import { SystemProtectionService } from '../../../dynamic-api/services/system-protection.service';
import { TableValidationService } from '../../../dynamic-api/services/table-validation.service';
import { SwaggerService } from '../../../../infrastructure/swagger/services/swagger.service';
import { GraphqlService } from '../../../graphql/services/graphql.service';
import { TDynamicContext } from '../../../../shared/interfaces/dynamic-context.interface';

export interface CheckPermissionExecutorDependencies {
  queryBuilder: QueryBuilderService;
  tableHandlerService: TableHandlerService;
  queryEngine: QueryEngine;
  routeCacheService: RouteCacheService;
  storageConfigCacheService: StorageConfigCacheService;
  aiConfigCacheService: AiConfigCacheService;
  metadataCacheService: MetadataCacheService;
  systemProtectionService: SystemProtectionService;
  tableValidationService: TableValidationService;
  swaggerService: SwaggerService;
  graphqlService: GraphqlService;
}

export async function executeCheckPermission(
  args: { routePath?: string; table?: string; operation: 'read' | 'create' | 'update' | 'delete' },
  context: TDynamicContext,
  deps: CheckPermissionExecutorDependencies,
): Promise<any> {
  const { routePath, table, operation } = args;
  const userId = context.$user?.id;

  const permissionCache: Map<string, any> =
    ((context as any).__permissionCache as Map<string, any>) ||
    (((context as any).__permissionCache = new Map<string, any>()) as Map<string, any>);
  const cacheKey = table
    ? `${userId || 'anon'}|${operation}|${table}|`
    : `${userId || 'anon'}|${operation}||${routePath || ''}`;

  if (permissionCache.has(cacheKey)) {
    return permissionCache.get(cacheKey);
  }

  const setCache = (result: any) => {
    const finalResult = { ...result, cacheKey };
    permissionCache.set(cacheKey, finalResult);
    return finalResult;
  };

  if (!userId) {
    return setCache({
      allowed: false,
      reason: 'not_authenticated',
      message: 'User is not authenticated. Please login first.',
    });
  }

  const operationToMethod: Record<string, string> = {
    read: 'GET',
    create: 'POST',
    update: 'PATCH',
    delete: 'DELETE',
  };
  const requiredMethod = operationToMethod[operation];

  const {
    queryBuilder,
    tableHandlerService,
    queryEngine,
    routeCacheService,
    storageConfigCacheService,
    aiConfigCacheService,
    metadataCacheService,
    systemProtectionService,
    tableValidationService,
    swaggerService,
    graphqlService,
  } = deps;

  const userRepo = new DynamicRepository({
    context,
    tableName: 'user_definition',
    queryBuilder,
    tableHandlerService,
    queryEngine,
    routeCacheService,
    storageConfigCacheService,
    aiConfigCacheService,
    metadataCacheService,
    systemProtectionService,
    tableValidationService,
    bootstrapScriptService: undefined,
    redisPubSubService: undefined,
    swaggerService,
    graphqlService,
  });

  await userRepo.init();

  const userResult = await userRepo.find({
    where: { id: { _eq: userId } },
    fields: 'id,email,isRootAdmin,role.id,role.name',
    limit: 1,
  });

  if (!userResult || !userResult.data || userResult.data.length === 0) {
    return setCache({
      allowed: false,
      reason: 'user_not_found',
      message: 'User not found in the system.',
    });
  }

  const user = userResult.data[0];

  if (user.isRootAdmin === true) {
    return setCache({
      allowed: true,
      reason: 'root_admin',
      message: 'User is root admin with full access.',
      userInfo: {
        id: user.id,
        email: user.email,
        isRootAdmin: true,
        role: user.role || null,
      },
    });
  }

  let finalRoutePath = routePath;
  if (!finalRoutePath && table) {
    const tableName = table.replace(/_definition$/, '');
    finalRoutePath = `/${tableName}`;
  }

  if (!finalRoutePath) {
    return setCache({
      allowed: false,
      reason: 'no_route_specified',
      message: 'Cannot determine route path. Please provide routePath or table parameter.',
    });
  }

  const routeRepo = new DynamicRepository({
    context,
    tableName: 'route_definition',
    queryBuilder,
    tableHandlerService,
    queryEngine,
    routeCacheService,
    storageConfigCacheService,
    aiConfigCacheService,
    metadataCacheService,
    systemProtectionService,
    tableValidationService,
    bootstrapScriptService: undefined,
    redisPubSubService: undefined,
    swaggerService,
    graphqlService,
  });

  await routeRepo.init();

  const routeResult = await routeRepo.find({
    where: { path: { _eq: finalRoutePath } },
    fields: 'id,path,routePermissions.methods.method,routePermissions.allowedUsers.id,routePermissions.role.id,routePermissions.role.name',
    limit: 1,
  });

  if (!routeResult || !routeResult.data || routeResult.data.length === 0) {
    if (operation === 'read') {
      return setCache({
        allowed: true,
        reason: 'route_not_found_public_read',
        message: `Route ${finalRoutePath} not found. Assuming public read access.`,
        userInfo: {
          id: user.id,
          email: user.email,
          isRootAdmin: false,
          role: user.role || null,
        },
      });
    } else {
      return setCache({
        allowed: false,
        reason: 'route_not_found_write_denied',
        message: `Route ${finalRoutePath} not found. Write operations require explicit permissions.`,
        userInfo: {
          id: user.id,
          email: user.email,
          isRootAdmin: false,
          role: user.role || null,
        },
      });
    }
  }

  const route = routeResult.data[0];
  const routePermissions = route.routePermissions || [];

  if (routePermissions.length === 0) {
    return setCache({
      allowed: false,
      reason: 'no_permissions_configured',
      message: `No permissions configured for ${finalRoutePath}.`,
      userInfo: {
        id: user.id,
        email: user.email,
        isRootAdmin: false,
        role: user.role || null,
      },
    });
  }

  for (const permission of routePermissions) {
    const allowedMethods = permission.methods || [];
    const hasMethodAccess = allowedMethods.some((m: any) => m.method === requiredMethod);

    if (!hasMethodAccess) {
      continue;
    }

    const allowedUsers = permission.allowedUsers || [];
    if (allowedUsers.some((u: any) => u?.id === userId)) {
      return setCache({
        allowed: true,
        reason: 'user_specific_access',
        message: `User has direct access to ${operation} on ${finalRoutePath}.`,
        userInfo: {
          id: user.id,
          email: user.email,
          isRootAdmin: false,
          role: user.role || null,
        },
      });
    }

    const allowedRole = permission.role || null;
    if (allowedRole && user.role && allowedRole.id === user.role.id) {
      return setCache({
        allowed: true,
        reason: 'role_based_access',
        message: `User has role-based access to ${operation} on ${finalRoutePath} via role: ${user.role.name || user.role.id}.`,
        userInfo: {
          id: user.id,
          email: user.email,
          isRootAdmin: false,
          role: user.role,
        },
      });
    }
  }

  return setCache({
    allowed: false,
    reason: 'permission_denied',
    message: `User does not have permission to ${operation} on ${finalRoutePath}.`,
    userInfo: {
      id: user.id,
      email: user.email,
      isRootAdmin: false,
      role: user.role || null,
    },
  });
}

