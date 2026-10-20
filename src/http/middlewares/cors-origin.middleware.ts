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

/**
 * The one CORS contract, shared with the Nuxt bridge and the WebSocket bridge.
 *
 * - No Origin header: not a browser cross-origin request, allowed.
 * - Allow-list not loaded yet: denied, so a cold cache never grants access.
 * - Allow-list loaded and empty: every origin is allowed, which is what an
 *   unconfigured deployment expects (same rule as the socket bridge).
 * - Allow-list loaded and non-empty: the origin must be listed.
 *
 * State-changing methods from a denied origin are rejected with 403 so the
 * control holds even when the caller ignores CORS. Safe methods keep serving
 * without the CORS grant, which is what makes a browser block the read.
 */
export function evaluateCorsOrigin(
  origin: string | undefined,
  policy: CorsOriginPolicyInput | null,
): boolean {
  if (!origin) return true;
  if (!policy?.loaded) return false;
  return policy.origins.length === 0 || policy.origins.includes(origin);
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
    const allowed = evaluateCorsOrigin(
      origin,
      runtimeRegistryService.getCorsOriginPolicy(),
    );
    const isPreflight =
      req.method === 'OPTIONS' &&
      origin !== undefined &&
      req.headers['access-control-request-method'] !== undefined;

    if (allowed && origin) {
      res.setHeader('Access-Control-Allow-Origin', origin);
      res.setHeader('Access-Control-Allow-Credentials', 'true');
      res.setHeader('Vary', 'Origin');
    }

    if (isPreflight) {
      if (allowed) {
        res.setHeader('Access-Control-Allow-Methods', ALLOWED_METHODS);
        res.setHeader('Access-Control-Allow-Headers', requestedHeadersOf(req));
        res.setHeader('Access-Control-Max-Age', '600');
        res.status(204).end();
        return;
      }
      res.status(403).end();
      return;
    }

    if (!allowed && !SAFE_METHODS.has(req.method)) {
      next(new ForbiddenException('Request origin is not allowed'));
      return;
    }

    next();
  };
}
