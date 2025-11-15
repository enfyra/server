import { QueryBuilderService } from '../../../../infrastructure/query-builder/query-builder.service';
import { RouteCacheService } from '../../../../infrastructure/cache/services/route-cache.service';
import { TDynamicContext } from '../../../../shared/interfaces/dynamic-context.interface';

export interface CheckPermissionExecutorDependencies {
  queryBuilder: QueryBuilderService;
  routeCacheService: RouteCacheService;
}

export async function executeCheckPermission(
  args: {
    table: string;
    operation: 'read' | 'create' | 'update' | 'delete';
  },
  context: TDynamicContext,
  deps: CheckPermissionExecutorDependencies,
): Promise<{ allowed: boolean; reason?: string }> {
  const { queryBuilder, routeCacheService } = deps;
  const user = context.$user;

  // Root admin always allowed
  if (user?.isRootAdmin) {
    return { allowed: true };
  }

  // System user always allowed
  if (user?.isSystem) {
    return { allowed: true };
  }

  // For metadata tables, check route permissions
  const isMetadataTable = args.table?.endsWith('_definition');
  if (isMetadataTable) {
    const method = args.operation === 'read' ? 'GET' : args.operation === 'create' ? 'POST' : args.operation === 'update' ? 'PUT' : 'DELETE';
    const routePath = `/${args.table}`;
    
    const routes = await routeCacheService.getRoutes();
    const route = routes.find(r => r.path === routePath);
    
    if (!route) {
      return { allowed: false, reason: `Route ${routePath} not found` };
    }

    const routePermissions = await queryBuilder.findWhere('route_permission_definition', {
      routeId: route.id,
      isEnabled: true,
    });

    if (routePermissions.length === 0) {
      return { allowed: false, reason: 'No route permissions configured' };
    }

    // Check if user has permission
    const hasPermission = routePermissions.some((rp: any) => {
      if (!rp.isEnabled) return false;
      
      // Check if user is in allowedUsers
      if (rp.allowedUsers && Array.isArray(rp.allowedUsers)) {
        if (rp.allowedUsers.some((au: any) => (au.id || au) === user?.id)) {
          return true;
        }
      }

      // Check if user's role has permission
      if (user?.role?.id && rp.roleId === user.role.id) {
        return true;
      }

      return false;
    });

    if (!hasPermission) {
      return { allowed: false, reason: 'User does not have permission for this route' };
    }

    // Check method permission
    const methodPermissions = await queryBuilder.findWhere('method_definition', {
      routePermissionId: routePermissions[0].id,
      method,
    });

    if (methodPermissions.length === 0) {
      return { allowed: false, reason: `Method ${method} not allowed for this route` };
    }

    return { allowed: true };
  }

  // For business tables, check via route permissions
  const method = args.operation === 'read' ? 'GET' : args.operation === 'create' ? 'POST' : args.operation === 'update' ? 'PUT' : 'DELETE';
  const routePath = `/${args.table}`;
  
  const routes = await routeCacheService.getRoutes();
  const route = routes.find(r => r.path === routePath);
  
  if (!route) {
    return { allowed: false, reason: `Route ${routePath} not found` };
  }

  const routePermissions = await queryBuilder.findWhere('route_permission_definition', {
    routeId: route.id,
    isEnabled: true,
  });

  if (routePermissions.length === 0) {
    return { allowed: false, reason: 'No route permissions configured' };
  }

  // Check if user has permission
  const hasPermission = routePermissions.some((rp: any) => {
    if (!rp.isEnabled) return false;
    
    // Check if user is in allowedUsers
    if (rp.allowedUsers && Array.isArray(rp.allowedUsers)) {
      if (rp.allowedUsers.some((au: any) => (au.id || au) === user?.id)) {
        return true;
      }
    }

    // Check if user's role has permission
    if (user?.role?.id && rp.roleId === user.role.id) {
      return true;
    }

    return false;
  });

  if (!hasPermission) {
    return { allowed: false, reason: 'User does not have permission for this route' };
  }

  return { allowed: true };
}

