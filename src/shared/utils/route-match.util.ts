export interface RouteMatchResult {
  route: any;
  params: Record<string, string>;
}

export interface RouteMatchIndexEntry {
  key?: string;
  path: string;
  methods: string[];
  order?: number;
}

export function matchRouteInRoutes(
  routes: any[],
  method: string,
  path: string,
): RouteMatchResult | null {
  const normalizedPath = normalizeRoutePath(path);
  let best: {
    route: any;
    params: Record<string, string>;
    score: number;
    index: number;
  } | null = null;

  for (let i = 0; i < routes.length; i++) {
    const route = routes[i];
    if (!isRouteMethodAvailable(route, method)) continue;
    for (const candidate of getRouteCandidatePaths(route, method)) {
      const match = matchRoutePattern(candidate, normalizedPath);
      if (!match) continue;
      const score = scoreRoutePattern(candidate);
      if (!best || score > best.score) {
        best = { route, params: match, score, index: i };
      }
    }
  }

  return best ? { route: best.route, params: best.params } : null;
}

export function matchRouteIndexEntry(
  entries: RouteMatchIndexEntry[],
  method: string,
  path: string,
): { entry: RouteMatchIndexEntry; params: Record<string, string> } | null {
  const normalizedPath = normalizeRoutePath(path);
  let best: {
    entry: RouteMatchIndexEntry;
    params: Record<string, string>;
    score: number;
    order: number;
  } | null = null;

  for (let i = 0; i < entries.length; i++) {
    const entry = entries[i];
    if (!entry.methods.includes(method)) continue;
    for (const candidate of getRouteCandidatePaths(entry, method)) {
      const match = matchRoutePattern(candidate, normalizedPath);
      if (!match) continue;
      const score = scoreRoutePattern(candidate);
      const order = entry.order ?? i;
      if (
        !best ||
        score > best.score ||
        (score === best.score && order < best.order)
      ) {
        best = { entry, params: match, score, order };
      }
    }
  }

  return best ? { entry: best.entry, params: best.params } : null;
}

export function getRouteMethods(route: any): string[] {
  const methods = route?.availableMethods ?? route?.methods;
  if (!Array.isArray(methods) || methods.length === 0) return [];
  return methods.map((item: any) => item?.name ?? item).filter(Boolean);
}

export function isRouteMethodAvailable(route: any, method: string): boolean {
  return getRouteMethods(route).includes(method);
}

export function getRouteCandidatePaths(route: any, method: string): string[] {
  const paths = [route.path];
  if (['DELETE', 'PATCH'].includes(method)) {
    paths.push(`${route.path}/:id`);
  }
  return paths.filter(Boolean);
}

export function normalizeRoutePath(path: string): string {
  if (!path) return '/';
  let normalized = path.startsWith('/') ? path : `/${path}`;
  if (normalized.length > 1 && normalized.endsWith('/')) {
    normalized = normalized.slice(0, -1);
  }
  return normalized;
}

export function matchRoutePattern(
  pattern: string,
  path: string,
): Record<string, string> | null {
  const patternSegments = splitRoutePath(normalizeRoutePath(pattern));
  const pathSegments = splitRoutePath(path);
  const params: Record<string, string> = {};

  for (let i = 0; i < patternSegments.length; i++) {
    const patternSegment = patternSegments[i];
    const pathSegment = pathSegments[i];
    if (patternSegment === '*' || patternSegment.startsWith('*')) {
      params.splat = pathSegments.slice(i).join('/');
      return params;
    }
    if (pathSegment === undefined) return null;
    if (patternSegment.startsWith(':')) {
      const paramName = patternSegment.slice(1);
      try {
        params[paramName] = decodeURIComponent(pathSegment);
      } catch {
        params[paramName] = pathSegment;
      }
      continue;
    }
    if (patternSegment !== pathSegment) return null;
  }

  return patternSegments.length === pathSegments.length ? params : null;
}

export function scoreRoutePattern(pattern: string): number {
  const segments = splitRoutePath(normalizeRoutePath(pattern));
  return segments.reduce((score, segment) => {
    if (segment === '*' || segment.startsWith('*')) return score;
    if (segment.startsWith(':')) return score + 10;
    return score + 100;
  }, segments.length);
}

function splitRoutePath(path: string): string[] {
  if (path === '/') return [];
  return path.split('/').filter((segment) => segment.length > 0);
}
