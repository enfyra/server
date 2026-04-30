import { Response, NextFunction } from 'express';
import { ExecutorEngineService } from '@enfyra/kernel';

function isAdminTestRunRequest(req: any): boolean {
  const path = String(
    req.path || req.routeData?.path || req.originalUrl?.split('?')?.[0] || '',
  );
  return req.method === 'POST' && path === '/admin/test/run';
}

export function dynamicInterceptorBegin(
  executorEngineService: ExecutorEngineService,
) {
  return async (req: any, res: Response, next: NextFunction) => {
    if (!req.routeData) {
      return next();
    }
    if (isAdminTestRunRequest(req)) {
      return next();
    }

    const appendLogs = (data: any) => {
      const logs = req.routeData?.context?.$share?.$logs;
      return logs?.length ? { ...data, logs } : data;
    };

    if (!req.routeData.__jsonWrappedForHooks) {
      req.routeData.__jsonWrappedForHooks = true;
      const originalJson = res.json.bind(res);
      res.json = function (data: any) {
        const postHooks = req.routeData?.postHooks;
        const hasDynamicHandler = Boolean(req.routeData?.handler?.trim?.());

        if (!hasDynamicHandler && postHooks?.length) {
          Promise.resolve()
            .then(async () => {
              req.routeData.context.$data = data;
              req.routeData.context.$statusCode = res.statusCode;
              for (const hook of postHooks) {
                if (!hook.code) continue;
                executorEngineService.register(req, {
                  code: hook.code,
                  type: 'postHook',
                });
              }
              await executorEngineService.runBatch(req);
              req.routeData.__codeBlocks = [];
              const responseData =
                req.routeData.context.$data !== undefined
                  ? req.routeData.context.$data
                  : data;
              return appendLogs(responseData);
            })
            .then((responseData) => originalJson(responseData))
            .catch(next);
          return res;
        }

        return originalJson(appendLogs(data));
      };
    }

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

    const hasDynamicHandler = Boolean(req.routeData?.handler?.trim?.());
    if (!hasDynamicHandler && preHooks?.length) {
      try {
        await executorEngineService.runBatch(req);
        req.routeData.__codeBlocks = [];
        if (req.routeData.context?.$body !== undefined) {
          req.body = req.routeData.context.$body;
        }
      } catch (error) {
        return next(error);
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
