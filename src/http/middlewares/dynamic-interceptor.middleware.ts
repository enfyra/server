import { Response, NextFunction } from 'express';
import { attachDynamicDeliveryDiagnostics } from '../../modules/dynamic-api/utils/dynamic-delivery-diagnostics.util';
import { RuntimeScriptRepairService, type RuntimeScriptExecutorService } from '../../engines/cache';
import { getErrorMessage } from '../../shared/utils/error.util';
import { HttpException } from '../../domain/exceptions';
import type { DynamicCancellationService } from '../../modules/dynamic-api/services/dynamic-cancellation.service';
import {
  finishDynamicHttpFailure,
  getDynamicHttpFailure,
  isDynamicExecutionTimeout,
  recordDynamicExecutionFailure,
} from '../../modules/dynamic-api/utils/dynamic-http-failure.util';
import {
  beginDynamicRouteExecution,
  completeDynamicRouteExecution,
  runDynamicRouteBatch,
} from '../../modules/dynamic-api/utils/dynamic-route-execution.util';
import {
  attachStreamResponseHelper,
  persistDynamicScriptLogs,
  writeRawScriptErrorJson,
} from '../../modules/dynamic-api/services/dynamic.service';

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
  executorEngineService: RuntimeScriptExecutorService,
  runtimeScriptRepairService?: RuntimeScriptRepairService,
  dynamicCancellationService?: DynamicCancellationService,
) {
  return async (req: any, res: Response, next: NextFunction) => {
    if (!req.routeData) {
      return next();
    }
    if (isAdminTestRunRequest(req)) {
      return next();
    }

    beginDynamicRouteExecution(req, res, dynamicCancellationService);
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

        if (isErrorResponse(res, data)) {
          persistDynamicScriptLogs(req, res.statusCode);
          return originalJson(appendLogs(data));
        }

        if (postHooks?.length && !req.routeData.__postHooksExecuted) {
          Promise.resolve()
            .then(async () => {
              req.routeData.context.$data = data;
              await runPostHooks();
              completeDynamicRouteExecution(req);
              const responseData =
                req.routeData.context.$data !== undefined
                  ? req.routeData.context.$data
                  : data;
              return appendLogs(responseData);
            })
            .then((responseData) => originalJson(responseData))
            .catch(async (error) => {
              completeDynamicRouteExecution(req);
              const failure = getDynamicHttpFailure(res);
              if (failure) {
                await dynamicCancellationService?.run(req, failure);
                finishDynamicHttpFailure(res, failure);
                return;
              }
              next(error);
            });
          return res;
        }

        return originalJson(appendLogs(data));
      };
    }

    const runPostHooks = async (error?: unknown) => {
      if (req.routeData.__postHooksExecuted) return;
      req.routeData.__postHooksExecuted = true;
      const context = req.routeData.context;
      context.$statusCode = res.statusCode;
      if (error) {
        const err = error as { statusCode?: number; message?: string; name?: string; stack?: string; details?: unknown };
        const statusCode = error instanceof HttpException ? error.getStatus() : Number(err.statusCode) || 500;
        const info = {
          message: getErrorMessage(error), name: err.name || 'Error', stack: err.stack || '',
          statusCode, details: err.details || {}, timestamp: new Date().toISOString(),
        };
        context.$error = info;
        context.$data = null;
        context.$statusCode = statusCode;
        if (context.$api) {
          context.$api.error = info;
          context.$api.response = { statusCode, timestamp: info.timestamp };
        }
      }
      req.routeData.__codeBlocks = [];
      for (const hook of req.routeData.postHooks ?? []) {
        if (!hook.code) continue;
        executorEngineService.register(req, {
          code: hook.code,
          sourceCode: hook.sourceCode ?? hook.code,
          scriptLanguage: hook.scriptLanguage ?? 'typescript',
          scriptId: hook.id,
          ...(runtimeScriptRepairService ? { onCompiledCodeRepair: repairCompiledCode('enfyra_post_hook', hook)! } : {}),
          type: 'postHook',
        });
      }
      if (req.routeData.postHooks?.some((hook: { code?: string }) => hook.code)) {
        const previousLogs = context.$share?.$logs ?? [];
        try {
          await runDynamicRouteBatch(req, res, executorEngineService);
        } finally {
          req.routeData.__codeBlocks = [];
          const logs: unknown[] = [];
          let bytes = 0;
          const currentLogs = context.$share?.$logs;
          for (const entry of [...previousLogs.slice(0, 201), ...(currentLogs === previousLogs ? [] : (currentLogs ?? []).slice(0, 201))]) {
            const size = Buffer.byteLength(JSON.stringify(entry) ?? 'null');
            if (logs.length >= 200 || bytes + size > 40_000) {
              context.$share.$logsTruncated = true;
              break;
            }
            logs.push(entry);
            bytes += size;
          }
          context.$share.$logs = logs;
        }
      }
    };

    const preHooks = req.routeData?.preHooks;
    if (preHooks?.length && !req.routeData.__preHooksExecuted) {
      req.routeData.__preHooksExecuted = true;
      attachDynamicDeliveryDiagnostics(req, res);
      attachStreamResponseHelper(res);
      req.routeData.context.$res = res;
      for (const hook of preHooks) {
        if (!hook.code) continue;
        executorEngineService.register(req, {
          code: hook.code,
          sourceCode: hook.sourceCode ?? hook.code,
          scriptLanguage: hook.scriptLanguage ?? 'typescript',
          scriptId: hook.id,
          onCompiledCodeRepair: repairCompiledCode('enfyra_pre_hook', hook),
          type: 'preHook',
        } as any);
      }
      try {
        await runDynamicRouteBatch(req, res, executorEngineService);
        req.routeData.__codeBlocks = [];
        if (req.routeData.context?.$body !== undefined) {
          req.body = req.routeData.context.$body;
        }
        if (req.routeData.context?.$query !== undefined) {
          req.query = req.routeData.context.$query;
        }
        if ((res as any).__enfyraStreamStarted) {
          await runPostHooks();
          completeDynamicRouteExecution(req);
          return;
        }
      } catch (error) {
        if (!getDynamicHttpFailure(res) && !isDynamicExecutionTimeout(error) && (error as { code?: string }).code !== 'ERR_EXECUTION_ABORTED') {
          try { await runPostHooks(error); }
          catch (postError) { error = postError; }
        }
        completeDynamicRouteExecution(req);
        const code = (error as { code?: string }).code;
        let failure = getDynamicHttpFailure(res);
        if (!failure && isDynamicExecutionTimeout(error)) {
          failure = recordDynamicExecutionFailure(res, error);
        }
        if (failure) {
          await dynamicCancellationService?.run(req, failure);
          finishDynamicHttpFailure(res, failure);
          return;
        }
        if (code === 'ERR_EXECUTION_ABORTED') return;
        const statusCode =
          error instanceof HttpException
            ? error.getStatus()
            : typeof (error as any)?.statusCode === 'number'
              ? (error as any).statusCode
              : 500;
        persistDynamicScriptLogs(req, statusCode);
        if (await writeRawScriptErrorJson(error, res)) return;
        return next(error);
      } finally {
        delete req.routeData.context.$res;
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
