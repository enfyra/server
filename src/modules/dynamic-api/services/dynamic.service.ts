import { Logger } from '../../../shared/logger';
import { HttpException } from '../../../core/exceptions/custom-exceptions';
import {
  ScriptExecutionException,
  BusinessLogicException,
  isCustomException,
} from '../../../core/exceptions/custom-exceptions';
import { LoggingService } from '../../../core/exceptions/services/logging.service';
import { ExecutorEngineService } from '../../../infrastructure/executor-engine/services/executor-engine.service';
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
          httpRes
            .status(200)
            .json(
              req.routeData.context.$share.$logs.length
                ? { result: value, logs: req.routeData.context.$share.$logs }
                : value,
            );
        }
        return undefined;
      }

      return value;
    } catch (error) {
      const httpStatus =
        error instanceof HttpException
          ? error.getStatus()
          : typeof error?.statusCode === 'number'
            ? error.statusCode
            : undefined;
      const isClientError =
        httpStatus !== undefined && httpStatus >= 400 && httpStatus < 500;

      if (!isClientError) {
        this.loggingService.error('Handler execution failed', {
          context: 'runHandler',
          error: error.message,
          stack: error.stack,
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
        const details = (error as any)?.details;
        throw new HttpException(
          details && typeof details === 'object' ? details : error.message,
          httpStatus!,
        );
      }
      throw new ScriptExecutionException(
        error.message,
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
