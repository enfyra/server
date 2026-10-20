import type { NextFunction, Request, Response } from 'express';
import { ForbiddenException } from '../../domain/exceptions';
import type { RuntimeRegistryService } from '../../engines/cache/services/runtime-registry.service';

const SAFE_METHODS = new Set(['GET', 'HEAD', 'OPTIONS']);
const ALLOWED_METHODS = 'GET, POST, PUT, PATCH, DELETE, OPTIONS';
const DEFAULT_ALLOWED_HEADERS =
  'Content-Type, Authorization, X-Requested-With, X-Correlation-ID, Range';

export interface CorsOriginPolicyInput {
  loaded: boolean;
  origins: string[];
}

export interface CorsOriginDecision {
  allowed: boolean;
  corsOrigin: string | null;
}

/**
 * The one CORS contract, shared with the Nuxt bridge and the WebSocket bridge.
 *
 * - No Origin header: not a browser cross-origin request, allowed without CORS.
 * - Same origin as the request's public host: allowed without CORS, because a
 *   browser never needs the grant for its own origin.
 * - Allow-list not loaded yet: denied, so a cold cache never grants access.
 * - Allow-list loaded: only a listed origin is allowed, and the value echoed
 *   back is the configured entry rather than the raw header. An allow-list with
 *   no entries therefore grants no cross-origin access.
 *
 * The echoed value is never the request's own header: with credentials enabled
 * that would let any site read authenticated responses.
 *
 * State-changing methods from a denied origin are rejected with 403 so the
 * control holds even when the caller ignores CORS. Safe methods keep serving
 * without the CORS grant, which is what makes a browser block the read.
 */
export function decideCorsOrigin(
  origin: string | undefined,
  publicHost: string | undefined,
  policy: CorsOriginPolicyInput | null,
): CorsOriginDecision {
  if (!origin) return { allowed: true, corsOrigin: null };
  if (isSameHost(origin, publicHost))
    return { allowed: true, corsOrigin: null };
  if (!policy?.loaded) return { allowed: false, corsOrigin: null };
  const corsOrigin =
    policy.origins.find((candidate) => candidate === origin) ?? null;
  return corsOrigin
    ? { allowed: true, corsOrigin }
    : { allowed: false, corsOrigin: null };
}

function isSameHost(origin: string, publicHost: string | undefined): boolean {
  if (!publicHost) return false;
  try {
    return new URL(origin).host.toLowerCase() === publicHost.toLowerCase();
  } catch {
    return false;
  }
}

function publicHostOf(req: Request): string | undefined {
  const forwarded = req.headers['x-forwarded-host'];
  const raw = Array.isArray(forwarded)
    ? forwarded[0]
    : (forwarded ?? req.headers.host);
  return typeof raw === 'string' ? raw.split(',')[0]?.trim() : undefined;
}

function requestedHeadersOf(req: Request): string {
  const value = req.headers['access-control-request-headers'];
  const raw = Array.isArray(value) ? value[0] : value;
  return typeof raw === 'string' && raw.length > 0
    ? raw
    : DEFAULT_ALLOWED_HEADERS;
}

export function corsOriginMiddleware(
  runtimeRegistryService: RuntimeRegistryService,
) {
  return (req: Request, res: Response, next: NextFunction) => {
    const origin =
      typeof req.headers.origin === 'string' ? req.headers.origin : undefined;
    const decision = decideCorsOrigin(
      origin,
      publicHostOf(req),
      runtimeRegistryService.getCorsOriginPolicy(),
    );
    const isPreflight =
      req.method === 'OPTIONS' &&
      origin !== undefined &&
      req.headers['access-control-request-method'] !== undefined;

    if (decision.corsOrigin) {
      res.setHeader('Access-Control-Allow-Origin', decision.corsOrigin);
      res.setHeader('Access-Control-Allow-Credentials', 'true');
      res.setHeader('Vary', 'Origin');
    }

    if (isPreflight) {
      if (decision.allowed) {
        res.setHeader('Access-Control-Allow-Methods', ALLOWED_METHODS);
        res.setHeader('Access-Control-Allow-Headers', requestedHeadersOf(req));
        res.setHeader('Access-Control-Max-Age', '600');
        res.status(204).end();
        return;
      }
      res.status(403).end();
      return;
    }

    if (!decision.allowed && !SAFE_METHODS.has(req.method)) {
      next(new ForbiddenException('Request origin is not allowed'));
      return;
    }

    next();
  };
}
