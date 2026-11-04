import type { TRouteOperationSource } from '../types/route-operation.types';

export function resolveRouteMethodConfig<T extends TRouteOperationSource>(
  route: T,
  method: string,
): NonNullable<T['methodConfigs']>[number] | null {
  if (route.isEnabled === false) return null;
  return (
    route.methodConfigs?.find(
      (entry) =>
        entry.available === true &&
        (typeof entry.method === 'string'
          ? entry.method
          : entry.method?.name) === method,
    ) ?? null
  );
}

export function resolveRouteOperation<T extends TRouteOperationSource>(
  route: T,
  method: string,
) {
  const config = resolveRouteMethodConfig(route, method);
  return config
    ? {
        ...route,
        routeMethodConfig: config,
        routePermissions: config.routePermissions ?? [],
      }
    : null;
}
