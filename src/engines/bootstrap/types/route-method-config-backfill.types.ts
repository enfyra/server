export interface LegacyRouteMethodPair {
  routeId: unknown;
  methodId: unknown;
}

export interface LegacyRouteMethodFlags {
  available: LegacyRouteMethodPair[];
  public: LegacyRouteMethodPair[];
  skipRoleGuard: LegacyRouteMethodPair[];
}

export interface LegacyRouteHandlerPair extends LegacyRouteMethodPair {
  handlerId: unknown;
  timeout: unknown;
}

export interface RouteMethodConfigBackfillDraft extends LegacyRouteMethodPair {
  available: boolean;
  isPublic: boolean;
  skipRoleGuard: boolean;
  timeout: number;
  isSystem: boolean;
  handlerId?: unknown;
}
