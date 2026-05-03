import { Response, NextFunction } from 'express';
import { HttpException } from '../../domain/exceptions';
import {
  GuardCacheService,
  GuardPosition,
  GuardEvaluatorService,
  GuardEvalContext,
} from '../../engines/cache';
async function runMetadataGuards(
  position: GuardPosition,
  req: any,
  res: Response,
  guardCacheService: GuardCacheService,
  guardEvaluatorService: GuardEvaluatorService,
): Promise<boolean> {
  if (!req.routeData) return true;

  await guardCacheService.ensureGuardsLoaded();

  const routePath =
    req.routeData.path ||
    req.routeData.route?.path ||
    req.baseUrl ||
    req.path ||
    'unknown';
  const method = req.method;

  const guards = await guardCacheService.getGuardsForRoute(
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

  for (const guard of guards) {
    const reject = await guardEvaluatorService.evaluateGuard(guard, evalCtx);
    if (reject) {
      if (reject.headers) {
        for (const [key, value] of Object.entries(reject.headers)) {
          res.setHeader(key, value);
        }
      }
      throw new HttpException(
        { statusCode: reject.statusCode, message: reject.message },
        reject.statusCode,
      );
    }
  }

  return true;
}

export function preAuthMetadataGuard(
  guardCacheService: GuardCacheService,
  guardEvaluatorService: GuardEvaluatorService,
) {
  return async (req: any, res: Response, next: NextFunction) => {
    try {
      await runMetadataGuards(
        'pre_auth',
        req,
        res,
        guardCacheService,
        guardEvaluatorService,
      );
      next();
    } catch (error) {
      next(error);
    }
  };
}

export function postAuthMetadataGuard(
  guardCacheService: GuardCacheService,
  guardEvaluatorService: GuardEvaluatorService,
) {
  return async (req: any, res: Response, next: NextFunction) => {
    try {
      await runMetadataGuards(
        'post_auth',
        req,
        res,
        guardCacheService,
        guardEvaluatorService,
      );
      next();
    } catch (error) {
      next(error);
    }
  };
}
