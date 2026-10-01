import type {
  LegacyRouteHandlerPair,
  LegacyRouteMethodFlags,
  LegacyRouteMethodPair,
  RouteMethodConfigBackfillDraft,
} from '../types/route-method-config-backfill.types';

const DEFAULT_ROUTE_METHOD_TIMEOUT_MS = 30_000;
const LEGACY_DYNAMIC_ROUTE_FALLBACK_TIMEOUT_MS = 60_000;

function pairKey(pair: LegacyRouteMethodPair): string {
  return `${String(pair.routeId)}\u0000${String(pair.methodId)}`;
}

function normalizeLegacyTimeout(value: unknown): number {
  const timeout = Number(value);
  if (!Number.isFinite(timeout) || timeout === 0) {
    return LEGACY_DYNAMIC_ROUTE_FALLBACK_TIMEOUT_MS;
  }
  return Math.max(1, Math.trunc(timeout));
}

export function buildRouteMethodConfigBackfillDrafts(options: {
  routeIds: unknown[];
  methodIds: unknown[];
  flags: LegacyRouteMethodFlags;
  handlers: LegacyRouteHandlerPair[];
  systemRouteIds?: unknown[];
}): RouteMethodConfigBackfillDraft[] {
  const available = new Set(options.flags.available.map(pairKey));
  const publicPairs = new Set(options.flags.public.map(pairKey));
  const skipRoleGuard = new Set(options.flags.skipRoleGuard.map(pairKey));
  const systemRoutes = new Set((options.systemRouteIds ?? []).map(String));
  const handlers = new Map(
    options.handlers.map((handler) => [pairKey(handler), handler]),
  );
  const drafts: RouteMethodConfigBackfillDraft[] = [];

  for (const routeId of options.routeIds) {
    for (const methodId of options.methodIds) {
      const pair = { routeId, methodId };
      const key = pairKey(pair);
      const handler = handlers.get(key);
      drafts.push({
        ...pair,
        available: available.has(key),
        isPublic: publicPairs.has(key),
        skipRoleGuard: skipRoleGuard.has(key),
        timeout: handler
          ? normalizeLegacyTimeout(handler.timeout)
          : DEFAULT_ROUTE_METHOD_TIMEOUT_MS,
        isSystem: systemRoutes.has(String(routeId)),
        ...(handler ? { handlerId: handler.handlerId } : {}),
      });
    }
  }

  return drafts;
}
