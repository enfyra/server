import { Logger } from '../../../shared/logger';
import {
  HttpException,
  LoggingService,
  ScriptExecutionException,
  BusinessLogicException,
  isCustomException,
} from '../../../domain/exceptions';
import {
  getErrorMessage,
  getErrorStack,
} from '../../../shared/utils/error.util';
import { ExecutorEngineService } from '@enfyra/kernel';
import { RequestWithRouteData } from '../../../shared/types';
import { Readable } from 'stream';
import { RuntimeScriptRepairService } from '../../../engines/cache';

export function attachStreamResponseHelper(res: any): void {
  if (!res || res.stream) return;
  res.stream = (
    stream: NodeJS.ReadableStream,
    options?: {
      statusCode?: number;
      mimetype?: string;
      filename?: string;
      headers?: Record<
        string,
        string | number | readonly string[] | undefined | null
      >;
    },
  ) => {
    const readable =
      stream && typeof stream.pipe === 'function'
        ? stream
        : stream && typeof (Readable as any).fromWeb === 'function'
          ? (Readable as any).fromWeb(stream)
          : null;
    if (!readable || typeof readable.pipe !== 'function') {
      throw new Error('@RES.stream requires a readable stream');
    }
    for (const [key, value] of Object.entries(options?.headers ?? {})) {
      if (value !== undefined && value !== null) {
        res.setHeader(key, value);
      }
    }
    if (options?.mimetype) res.setHeader('Content-Type', options.mimetype);
    if (options?.filename) {
      const safeFilename = String(options.filename).replace(/["\r\n]/g, '_');
      res.setHeader(
        'Content-Disposition',
        `attachment; filename="${safeFilename}"`,
      );
    }
    res.status(options?.statusCode || 200);
    res.__enfyraStreamStarted = true;
    readable.on('error', (error: Error) => {
      if (!res.headersSent) {
        res.status(500).json({
          success: false,
          message: 'Stream failed',
          statusCode: 500,
          error: {
            code: 'STREAM_FAILED',
            message: error.message,
            timestamp: new Date().toISOString(),
          },
        });
      } else {
        res.destroy(error);
      }
    });
    readable.pipe(res);
  };
}

export class DynamicService {
  private readonly logger = new Logger(DynamicService.name);
  private readonly executorEngineService: ExecutorEngineService;
  private readonly loggingService: LoggingService;
  private readonly runtimeScriptRepairService?: RuntimeScriptRepairService;

  constructor(deps: {
    executorEngineService: ExecutorEngineService;
    loggingService: LoggingService;
    runtimeScriptRepairService?: RuntimeScriptRepairService;
  }) {
    this.executorEngineService = deps.executorEngineService;
    this.loggingService = deps.loggingService;
    this.runtimeScriptRepairService = deps.runtimeScriptRepairService;
  }

  private repairCompiledCode(tableName: string, record: any) {
    if (!this.runtimeScriptRepairService) return undefined;
    return async () => {
      try {
        await this.runtimeScriptRepairService?.repairScriptRecord(
          tableName,
          record,
        );
      } catch (error) {
        this.logger.warn(
          `Failed to repair ${tableName} compiledCode after executor retry: ${getErrorMessage(error)}`,
        );
      }
    };
  }

  async runHandler(req: RequestWithRouteData) {
    const routeData = req.routeData;
    if (!routeData) {
      throw new BusinessLogicException('Route data is required');
    }
    const isTableDefinitionOperation =
      routeData.mainTable?.name === 'enfyra_table';
    try {
      const handler = routeData.handler?.trim();
      if (!handler) {
        throw new BusinessLogicException(
          `No handler configured for method '${req.method}' on route '${routeData.route?.path || req.url}'`,
          { method: req.method, route: routeData.route?.path },
        );
      }

      const res = routeData.res;
      if (res) {
        attachStreamResponseHelper(res);
        routeData.context.$res = res;
      }

      this.executorEngineService.register(req, {
        code: handler,
        sourceCode: routeData.handlerRecord?.sourceCode ?? handler,
        scriptLanguage: routeData.handlerRecord?.scriptLanguage ?? 'typescript',
        onCompiledCodeRepair: this.repairCompiledCode(
          'enfyra_route_handler',
          routeData.handlerRecord,
        ),
        type: 'handler',
      } as any);

      const postHooks = routeData.postHooks;
      if (postHooks?.length) {
        for (const hook of postHooks) {
          if (!hook.code) continue;
          this.executorEngineService.register(req, {
            code: hook.code,
            sourceCode: hook.sourceCode ?? hook.code,
            scriptLanguage: hook.scriptLanguage ?? 'typescript',
            onCompiledCodeRepair: this.repairCompiledCode(
              'enfyra_post_hook',
              hook,
            ),
            type: 'postHook',
          } as any);
        }
      }

      const routeHandler = routeData.handlers?.find(
        (h) => h.method?.name === req.method,
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
        delete routeData.context.$res;
      }

      if (shortCircuit) {
        const httpRes = routeData.res;
        if (httpRes && !httpRes.headersSent) {
          let response = routeData.context.$share.$logs.length
            ? { result: value, logs: routeData.context.$share.$logs }
            : value;
          const debug: any = (req as any)._debug;
          if (debug && routeData.context.$query?.debugMode) {
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
          handler: routeData.handler,
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
            ? { message: getErrorMessage(error), details }
            : getErrorMessage(error),
          httpStatus!,
        );
      }
      throw new ScriptExecutionException(
        getErrorMessage(error),
        routeData.handler,
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
