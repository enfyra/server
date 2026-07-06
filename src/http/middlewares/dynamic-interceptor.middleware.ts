import { Response, NextFunction } from 'express';
import { ExecutorEngineService } from '@enfyra/kernel';
import { RuntimeScriptRepairService } from '../../engines/cache';
import { getErrorMessage } from '../../shared/utils/error.util';

function isAdminTestRunRequest(req: any): boolean {
  const path = String(
    req.path || req.routeData?.path || req.originalUrl?.split('?')?.[0] || '',
  );
  return req.method === 'POST' && path === '/admin/test/run';
}

function isErrorResponse(res: Response, data: any): boolean {
  return (
    res.statusCode >= 400 ||
    data?.success === false ||
    (data?.error && Number(data?.statusCode || res.statusCode) >= 400)
  );
}

export function dynamicInterceptorBegin(
  executorEngineService: ExecutorEngineService,
  runtimeScriptRepairService?: RuntimeScriptRepairService,
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
    const repairCompiledCode = (tableName: string, record: any) => {
      if (!runtimeScriptRepairService) return undefined;
      return async () => {
        try {
          await runtimeScriptRepairService.repairScriptRecord(
            tableName,
            record,
          );
        } catch (error) {
          console.warn(
            `Failed to repair ${tableName} compiledCode after executor retry: ${getErrorMessage(error)}`,
          );
        }
      };
    };

    if (!req.routeData.__jsonWrappedForHooks) {
      req.routeData.__jsonWrappedForHooks = true;
      const originalJson = res.json.bind(res);
      res.json = function (data: any) {
        const postHooks = req.routeData?.postHooks;
        const hasDynamicHandler = Boolean(req.routeData?.handler?.trim?.());

        if (isErrorResponse(res, data)) {
          return originalJson(appendLogs(data));
        }

        if (!hasDynamicHandler && postHooks?.length) {
          Promise.resolve()
            .then(async () => {
              req.routeData.context.$data = data;
              req.routeData.context.$statusCode = res.statusCode;
              for (const hook of postHooks) {
                if (!hook.code) continue;
                executorEngineService.register(req, {
                  code: hook.code,
                  sourceCode: hook.sourceCode ?? hook.code,
                  scriptLanguage: hook.scriptLanguage ?? 'typescript',
                  onCompiledCodeRepair: repairCompiledCode(
                    'enfyra_post_hook',
                    hook,
                  ),
                  type: 'postHook',
                } as any);
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
          sourceCode: hook.sourceCode ?? hook.code,
          scriptLanguage: hook.scriptLanguage ?? 'typescript',
          onCompiledCodeRepair: repairCompiledCode('enfyra_pre_hook', hook),
          type: 'preHook',
        } as any);
      }
      try {
        const result = await executorEngineService.runBatch(req);
        req.routeData.__codeBlocks = [];
        if (req.routeData.context?.$body !== undefined) {
          req.body = req.routeData.context.$body;
        }
        if (req.routeData.context?.$query !== undefined) {
          req.query = req.routeData.context.$query;
        }
        if (result.shortCircuit) {
          return res.json(appendLogs(result.value));
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
