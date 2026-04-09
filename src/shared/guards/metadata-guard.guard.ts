import {
  CanActivate,
  ExecutionContext,
  HttpException,
  Injectable,
} from '@nestjs/common';
import {
  GuardCacheService,
  GuardPosition,
} from '../../infrastructure/cache/services/guard-cache.service';
import {
  GuardEvaluatorService,
  GuardEvalContext,
} from '../../infrastructure/cache/services/guard-evaluator.service';

@Injectable()
export class PreAuthMetadataGuard implements CanActivate {
  constructor(
    private readonly guardCacheService: GuardCacheService,
    private readonly guardEvaluatorService: GuardEvaluatorService,
  ) {}

  async canActivate(context: ExecutionContext): Promise<boolean> {
    return runMetadataGuards(
      'pre_auth',
      context,
      this.guardCacheService,
      this.guardEvaluatorService,
    );
  }
}

@Injectable()
export class PostAuthMetadataGuard implements CanActivate {
  constructor(
    private readonly guardCacheService: GuardCacheService,
    private readonly guardEvaluatorService: GuardEvaluatorService,
  ) {}

  async canActivate(context: ExecutionContext): Promise<boolean> {
    return runMetadataGuards(
      'post_auth',
      context,
      this.guardCacheService,
      this.guardEvaluatorService,
    );
  }
}

async function runMetadataGuards(
  position: GuardPosition,
  context: ExecutionContext,
  guardCacheService: GuardCacheService,
  guardEvaluatorService: GuardEvaluatorService,
): Promise<boolean> {
  const req = context.switchToHttp().getRequest();
  if (!req.routeData) return true;

  await guardCacheService.ensureGuardsLoaded();

  const routePath = req.routeData.route?.path || req.baseUrl;
  const method = req.method;

  const guards = guardCacheService.getGuardsForRoute(
    position,
    routePath,
    method,
  );
  if (guards.length === 0) return true;

  const evalCtx: GuardEvalContext = {
    clientIp: req.routeData.context?.$req?.ip || req.ip || 'unknown',
    routePath,
    userId: position === 'post_auth' ? req.user?.id || null : null,
  };

  for (const guard of guards) {
    const reject = await guardEvaluatorService.evaluateGuard(guard, evalCtx);
    if (reject) {
      const res = context.switchToHttp().getResponse();
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
