import { Response, NextFunction } from 'express';
import { ExecutorEngineService } from '../../engine/executor-engine/services/executor-engine.service';

export function dynamicInterceptorBegin(
  executorEngineService: ExecutorEngineService,
) {
  return async (req: any, res: Response, next: NextFunction) => {
    const preHooks = req.routeData?.preHooks;
    if (preHooks?.length) {
      for (const hook of preHooks) {
        if (!hook.code) continue;
        executorEngineService.register(req, {
          code: hook.code,
          type: 'preHook',
        });
      }
    }
    next();
  };
}

export function dynamicInterceptorEnd(
  req: any,
  res: Response,
  next: NextFunction,
) {
  if (res.headersSent) {
    return next();
  }

  const originalJson = res.json.bind(res);
  res.json = function (data: any) {
    const logs = req.routeData?.context?.$share?.$logs;
    if (logs?.length) {
      data = { ...data, logs };
    }
    return originalJson(data);
  };

  next();
}
