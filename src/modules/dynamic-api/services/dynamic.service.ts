// @nestjs packages
import { Injectable, Logger } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';

// Internal imports
import {
  ScriptExecutionException,
  BusinessLogicException,
} from '../../../core/exceptions/custom-exceptions';
import { LoggingService } from '../../../core/exceptions/services/logging.service';
import { HandlerExecutorService } from '../../../infrastructure/handler-executor/services/handler-executor.service';
import { RequestWithRouteData } from '../../../shared/interfaces/dynamic-context.interface';

@Injectable()
export class DynamicService {
  private logger = new Logger(DynamicService.name);

  constructor(
    private handlerExecutorService: HandlerExecutorService,
    private loggingService: LoggingService,
    private configService: ConfigService,
  ) {}

  async runHandler(req: RequestWithRouteData) {
    const startTime = Date.now();
    // Calculate timeout outside try block so it's available in catch
    const isTableDefinitionOperation =
      req.routeData.mainTable?.name === 'table_definition' ||
      req.routeData.targetTables?.some(
        (table) => table.name === 'table_definition',
      );

    try {
      const userHandler = req.routeData.handler?.trim();
      const hasMainTable =
        req.routeData.mainTable && req.routeData.context.$repos?.main;
      const defaultHandler = hasMainTable
        ? this.getDefaultHandler(req.method)
        : null;

      if (!userHandler && !defaultHandler) {
        throw new BusinessLogicException(
          `No handler configured for method '${req.method}' on route '${req.routeData.route?.path || req.url}'`,
          { method: req.method, route: req.routeData.route?.path }
        );
      }

      const scriptCode = userHandler || defaultHandler;

      const routeHandler = req.routeData.handlers?.find(
        (handler) => handler.method?.method === req.method
      );
      const timeoutMs = routeHandler?.timeout || this.configService.get<number>('DEFAULT_HANDLER_TIMEOUT', 5000);

      // Inject $res ONLY for handler (not for hooks)
      const res = req.routeData.res;
      if (res) {
        req.routeData.context.$res = res;
      }

      const result = await this.handlerExecutorService.run(
        scriptCode,
        req.routeData.context,
        timeoutMs,
      );

      // Remove $res after handler to prevent hooks from using it
      delete req.routeData.context.$res;

      return result;
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

      // Re-throw custom exceptions as-is (they already have proper error codes)
      if (error.constructor.name.includes('Exception')) {
        throw error;
      }

      // Handle other script errors
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

  private getDefaultHandler(method: string): string {
    switch (method) {
      case 'DELETE':
        return `return await $ctx.$repos.main.delete({ id: $ctx.$params.id });`;
      case 'POST':
        return `return await $ctx.$repos.main.create({ data: $ctx.$body });`;
      case 'PATCH':
        return `return await $ctx.$repos.main.update({ id: $ctx.$params.id, data: $ctx.$body });`;
      default:
        return `return await $ctx.$repos.main.find();`;
    }
  }
}
