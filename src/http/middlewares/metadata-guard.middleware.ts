import { Response, NextFunction } from 'express';
import { GuardBlockedException } from '../../domain/exceptions';
import {
  GuardCacheBuilder,
  GuardPosition,
  GuardEvaluatorService,
  GuardEvalContext,
  GuardRateLimitSnapshot,
  GuardAlertService,
} from '../../engines/cache';
import { RuntimeRegistryService } from '../../engines/cache/services/runtime-registry.service';

function setRateLimitSnapshotHeaders(
  res: Response,
  snapshots: GuardRateLimitSnapshot[],
): void {
  if (res.headersSent || snapshots.length === 0) return;
  const snap = snapshots.reduce((strictest, current) => {
    const strictestRatio =
      strictest.limit > 0 ? strictest.remaining / strictest.limit : 0;
    const currentRatio = current.limit > 0 ? current.remaining / current.limit : 0;
    return currentRatio < strictestRatio ? current : strictest;
  });
  res.setHeader('X-RateLimit-Limit', String(snap.limit));
  res.setHeader('X-RateLimit-Remaining', String(snap.remaining));
  res.setHeader('X-RateLimit-Reset', String(snap.resetAt));
  res.setHeader('X-RateLimit-Window', String(snap.windowSeconds));
  res.setHeader('X-RateLimit-Scope', snap.scope);
  const usedRequests = Math.max(0, snap.limit - snap.remaining);
  res.setHeader('X-RateLimit-Used', String(usedRequests));
}

async function runMetadataGuards(
  position: GuardPosition,
  req: any,
  res: Response,
  guardCacheBuilder: GuardCacheBuilder,
  runtimeRegistryService: RuntimeRegistryService,
  guardEvaluatorService: GuardEvaluatorService,
  guardAlertService: GuardAlertService,
): Promise<boolean> {
  if (!req.routeData) return true;

  await guardCacheBuilder.ensureGuardsLoaded();

  const routePath =
    req.routeData.path ||
    req.routeData.route?.path ||
    req.baseUrl ||
    req.path ||
    'unknown';
  const method = req.method;

  const guards = runtimeRegistryService.getGuardsForRoute(
    position,
    routePath,
    method,
  );
  if (guards.length === 0) return true;

  const evalCtx: GuardEvalContext = {
    clientIp: req.routeData.context?.$req?.ip || req.ip || 'unknown',
    routePath,
    userId:
      position === 'post_auth' && req.user?.id != null
        ? String(req.user.id)
        : null,
  };

  const collectedSnapshots: GuardRateLimitSnapshot[] = [];

  for (const guard of guards) {
    const { reject, rateLimitSnapshots } =
      await guardEvaluatorService.evaluateGuard(guard, evalCtx);
    for (const snap of rateLimitSnapshots) collectedSnapshots.push(snap);

    if (reject) {
      if (reject.headers) {
        for (const [key, value] of Object.entries(reject.headers)) {
          if (!res.headersSent) res.setHeader(key, value);
        }
      }
      const scope = 'reason' in reject.details && reject.details.reason === 'rate_limit'
        ? reject.details.scope
        : 'ip';
      const scopeKey = scope === 'ip' ? evalCtx.clientIp
        : scope === 'user' ? (evalCtx.userId || 'anonymous')
        : routePath;
      guardAlertService.recordAlert({
        scope,
        scopeKey,
        routePath,
        method,
        errorCode: reject.errorCode,
        guardName: guard.name,
      });
      throw new GuardBlockedException(
        reject.message,
        reject.statusCode,
        reject.errorCode,
        reject.details,
      );
    }
  }

  setRateLimitSnapshotHeaders(res, collectedSnapshots);

  return true;
}

export function preAuthMetadataGuard(
  guardCacheBuilder: GuardCacheBuilder,
  runtimeRegistryService: RuntimeRegistryService,
  guardEvaluatorService: GuardEvaluatorService,
  guardAlertService: GuardAlertService,
) {
  return async (req: any, res: Response, next: NextFunction) => {
    try {
      await runMetadataGuards(
        'pre_auth',
        req,
        res,
        guardCacheBuilder,
        runtimeRegistryService,
        guardEvaluatorService,
        guardAlertService,
      );
      next();
    } catch (error) {
      next(error);
    }
  };
}

export function postAuthMetadataGuard(
  guardCacheBuilder: GuardCacheBuilder,
  runtimeRegistryService: RuntimeRegistryService,
  guardEvaluatorService: GuardEvaluatorService,
  guardAlertService: GuardAlertService,
) {
  return async (req: any, res: Response, next: NextFunction) => {
    try {
      await runMetadataGuards(
        'post_auth',
        req,
        res,
        guardCacheBuilder,
        runtimeRegistryService,
        guardEvaluatorService,
        guardAlertService,
      );
      next();
    } catch (error) {
      next(error);
    }
  };
}
