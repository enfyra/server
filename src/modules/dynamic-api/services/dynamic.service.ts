import { Injectable, Logger } from '@nestjs/common';
import {
  ScriptExecutionException,
  BusinessLogicException,
} from '../../../core/exceptions/custom-exceptions';
import { LoggingService } from '../../../core/exceptions/services/logging.service';
import { HandlerExecutorService } from '../../../infrastructure/handler-executor/services/handler-executor.service';
import { RequestWithRouteData } from '../../../shared/types';
@Injectable()
export class DynamicService {
  private logger = new Logger(DynamicService.name);
  constructor(
    private handlerExecutorService: HandlerExecutorService,
    private loggingService: LoggingService,
  ) {}
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

      this.handlerExecutorService.register(req, {
        code: handler,
        type: 'handler',
      });

      const postHooks = req.routeData?.postHooks;
      if (postHooks?.length) {
        for (const hook of postHooks) {
          if (!hook.code) continue;
          this.handlerExecutorService.register(req, {
            code: hook.code,
            type: 'postHook',
          });
        }
      }

      const routeHandler = req.routeData.handlers?.find(
        (h) => h.method?.method === req.method,
      );
      const timeoutMs = routeHandler?.timeout || undefined;

      const { value, shortCircuit } = await this.handlerExecutorService.runBatch(req, timeoutMs);

      delete req.routeData.context.$res;

      if (shortCircuit) {
        const httpRes = req.routeData.res;
        if (httpRes && !httpRes.headersSent) {
          httpRes.status(200).json(
            req.routeData.context.$share.$logs.length
              ? { result: value, logs: req.routeData.context.$share.$logs }
              : value,
          );
        }
        return undefined;
      }

      return value;
    } catch (error) {
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
      if (error.constructor.name.includes('Exception')) {
        throw error;
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
