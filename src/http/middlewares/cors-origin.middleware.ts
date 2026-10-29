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
  corsOrigin?: string;
}

/**
 * The one CORS contract, shared with the Nuxt bridge and the WebSocket bridge.
 *
 * - No Origin header: not a browser cross-origin request, allowed without CORS.
 * - Allow-list not loaded yet: denied, so a cold cache never grants access.
 * - Allow-list loaded and empty: grant public CORS without credentials.
 * - Allow-list loaded with entries: only an exact entry is allowed, and that
 *   entry is the value granted with credentials.
 *
 * State-changing methods from a denied origin are rejected with 403 so the
 * control holds even when the caller ignores CORS. Safe methods keep serving
 * without the CORS grant, which is what makes a browser block the read.
 */
export function decideCorsOrigin(
  origin: string | undefined,
  policy: CorsOriginPolicyInput | undefined,
): CorsOriginDecision {
  if (!origin) return { allowed: true };
  if (!policy?.loaded) return { allowed: false };
  if (policy.origins.length === 0) return { allowed: true, corsOrigin: '*' };
  const corsOrigin = policy.origins.find((candidate) => candidate === origin);
  return corsOrigin === undefined
    ? { allowed: false }
    : { allowed: true, corsOrigin };
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
      runtimeRegistryService.getCorsOriginPolicy(),
    );
    const isPreflight =
      req.method === 'OPTIONS' &&
      origin !== undefined &&
      req.headers['access-control-request-method'] !== undefined;

    if (decision.corsOrigin !== undefined) {
      res.setHeader('Access-Control-Allow-Origin', decision.corsOrigin);
      if (decision.corsOrigin !== '*') {
        res.setHeader('Access-Control-Allow-Credentials', 'true');
      }
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
