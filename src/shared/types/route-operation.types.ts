export interface TRouteOperationConfig {
  available?: boolean;
  method?: string | { name?: string };
  isPublic?: boolean;
  skipRoleGuard?: boolean;
  routePermissions?: unknown[];
}

export interface TRouteOperationSource {
  isEnabled?: boolean;
  methodConfigs?: TRouteOperationConfig[];
}
