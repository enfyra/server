import type { TRouteOperationSource } from '../types/route-operation.types';

export function resolveRouteOperation<T extends TRouteOperationSource>(route: T, method: string) {
  if (route.isEnabled === false) return null;
  const config = route.methodConfigs?.find((entry) =>
    entry.available === true &&
    (typeof entry.method === 'string' ? entry.method : entry.method?.name) === method,
  );
  return config ? { ...route, routeMethodConfig: config, routePermissions: config.routePermissions ?? [] } : null;
}
