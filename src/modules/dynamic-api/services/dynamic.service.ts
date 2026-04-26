import { Logger } from '../../../shared/logger';
import { HttpException } from '../../../domain/exceptions';
import {
  getErrorMessage,
  getErrorStack,
} from '../../../shared/utils/error.util';
import {
  ScriptExecutionException,
  BusinessLogicException,
  isCustomException,
} from '../../../domain/exceptions';
import { LoggingService } from '../../../domain/exceptions';
import { ExecutorEngineService } from '../../../kernel/execution';
import { RequestWithRouteData } from '../../../shared/types';

export class DynamicService {
  private readonly logger = new Logger(DynamicService.name);
  private readonly executorEngineService: ExecutorEngineService;
  private readonly loggingService: LoggingService;

  constructor(deps: {
    executorEngineService: ExecutorEngineService;
    loggingService: LoggingService;
  }) {
    this.executorEngineService = deps.executorEngineService;
    this.loggingService = deps.loggingService;
  }

  async runHandler(req: RequestWithRouteData) {
    const isTableDefinitionOperation =
      req.routeData.mainTable?.name === 'table_definition';
    try {
      const handler = req.routeData.handler?.trim();
      if (!handler) {
        throw new BusinessLogicException(
          `No handler configured for method '${req.method}' on route '${req.routeData.route?.path || req.url}'`,
          { method: req.method, route: req.routeData.route?.path },
        );
      }

      const res = req.routeData.res;
      if (res) {
        req.routeData.context.$res = res;
      }

      this.executorEngineService.register(req, {
        code: handler,
        type: 'handler',
      });

      const postHooks = req.routeData?.postHooks;
      if (postHooks?.length) {
        for (const hook of postHooks) {
          if (!hook.code) continue;
          this.executorEngineService.register(req, {
            code: hook.code,
            type: 'postHook',
          });
        }
      }

      const routeHandler = req.routeData.handlers?.find(
        (h) => h.method?.method === req.method,
      );
      const timeoutMs = routeHandler?.timeout || undefined;

      let value: any;
      let shortCircuit = false;
      try {
        const result = await this.executorEngineService.runBatch(
          req,
          timeoutMs,
        );
        value = result.value;
        shortCircuit = result.shortCircuit;
      } finally {
        delete req.routeData.context.$res;
      }

      if (shortCircuit) {
        const httpRes = req.routeData.res;
        if (httpRes && !httpRes.headersSent) {
          let response = req.routeData.context.$share.$logs.length
            ? { result: value, logs: req.routeData.context.$share.$logs }
            : value;
          const debug: any = (req as any)._debug;
          if (debug && req.routeData?.context?.$query?.debugMode) {
            response = { ...response, debug: debug.toJSON() };
          }
          httpRes.status(200).json(response);
        }
        return undefined;
      }

      return value;
    } catch (error) {
      const err = error as { statusCode?: number; details?: any };
      const httpStatus =
        error instanceof HttpException
          ? error.getStatus()
          : typeof err.statusCode === 'number'
            ? err.statusCode
            : undefined;
      const isClientError =
        httpStatus !== undefined && httpStatus >= 400 && httpStatus < 500;

      if (!isClientError) {
        this.loggingService.error('Handler execution failed', {
          context: 'runHandler',
          error: getErrorMessage(error),
          stack: getErrorStack(error),
          method: req.method,
          url: req.url,
          handler: req.routeData?.handler,
          isTableOperation: isTableDefinitionOperation,
          userId: req.user?.id,
        });
      }
      if (isCustomException(error) || error instanceof HttpException) {
        throw error;
      }
      if (isClientError) {
        const details = err.details;
        throw new HttpException(
          details && typeof details === 'object'
            ? details
            : getErrorMessage(error),
          httpStatus!,
        );
      }
      throw new ScriptExecutionException(
        getErrorMessage(error),
        req.routeData?.handler,
        {
          method: req.method,
          url: req.url,
          userId: req.user?.id,
          isTableOperation: isTableDefinitionOperation,
        },
      );
    }
  }
}
