import { Logger } from '../../../shared/logger';
import {
  attachDynamicDeliveryDiagnostics,
  recordDynamicDeliveryTransport,
} from '../utils/dynamic-delivery-diagnostics.util';
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
import type { RequestWithRouteData } from '../../../shared/types';
import type { DynamicResponse } from '../../../shared/types/dynamic-context.types';
import { Readable } from 'stream';
import { RuntimeScriptRepairService, type RuntimeScriptExecutorService } from '../../../engines/cache';
import { recordUserLog } from '../../../shared/runtime-log-buffer';
import { buildErrorResponseTrace } from '../../../shared/utils/error-response-trace.util';
import type { DynamicCancellationService } from './dynamic-cancellation.service';
import {
  finishDynamicHttpFailure,
  getDynamicHttpFailure,
  isDynamicExecutionTimeout,
  recordDynamicExecutionFailure,
  recordDynamicStreamFailure,
} from '../utils/dynamic-http-failure.util';
import {
  beginDynamicRouteExecution,
  completeDynamicRouteExecution,
  runDynamicRouteBatch,
} from '../utils/dynamic-route-execution.util';

const streamLogger = new Logger('DynamicResponseStream');

export function persistDynamicScriptLogs(
  req: RequestWithRouteData,
  statusCode: number,
): void {
  const logs = req.routeData?.context?.$share?.$logs;
  if (!Array.isArray(logs) || logs.length === 0) return;

  const routeData = req.routeData as any;
  if (routeData.__scriptLogsPersisted) return;
  routeData.__scriptLogsPersisted = true;

  recordUserLog(logs, { component: 'Script', sourceKind: 'route', correlationId: req.routeData?.context?.$api?.request?.correlationId, statusCode });
}

function applyResponseOptions(
  res: any,
  options?: {
    statusCode?: number;
    mimetype?: string;
    filename?: string;
    headers?: Record<
      string,
      string | number | readonly string[] | undefined | null
    >;
  },
): void {
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
}

function beginNativeResponse(
  res: any,
  options: { allowHeadersSent?: boolean } = {},
): void {
  if (
    res.__enfyraStreamStarted ||
    (!options.allowHeadersSent && res.headersSent)
  ) {
    throw new Error('Dynamic response has already started');
  }
  res.__enfyraStreamStarted = true;
}

function mergeErrorTrace(
  jsonText: string,
  res: any,
  statusCode: number,
): { responseText: string; correlationId: string } {
  const body = JSON.parse(jsonText);
  if (!body || typeof body !== 'object' || Array.isArray(body)) {
    throw new TypeError('@THROW.json body must be a JSON object');
  }
  if (
    body.error !== undefined
    && (!body.error || typeof body.error !== 'object' || Array.isArray(body.error))
  ) {
    throw new TypeError('@THROW.json body.error must be a JSON object');
  }

  const trace = buildErrorResponseTrace(res.req, res);
  const customError = { ...(body.error ?? {}) };
  delete customError.statusCode;
  return {
    correlationId: trace.correlationId,
    responseText: JSON.stringify({
      ...body,
      success: false,
      statusCode,
      error: {
        ...customError,
        ...trace,
      },
    }),
  };
}

export async function writeRawScriptErrorJson(
  error: unknown,
  res: any,
  script?: string,
): Promise<boolean> {
  const carrier = error as {
    details?: {
      errorJsonText?: unknown;
      errorJsonOptions?: unknown;
    };
    errorPath?: string;
    isRawScriptErrorCarrier?: boolean;
  };
  if (
    carrier.isRawScriptErrorCarrier !== true
    || carrier.errorPath !== '$throw.json'
  ) {
    return false;
  }

  const errorJsonText = carrier.details?.errorJsonText;
  const errorJsonOptions = carrier.details?.errorJsonOptions;
  const writeErrorJson = res?.__enfyraErrorJson;
  if (
    typeof errorJsonText !== 'string'
    || typeof writeErrorJson !== 'function'
  ) {
    throw new ScriptExecutionException(
      'Custom JSON error response could not be written',
      script,
    );
  }
  await writeErrorJson.call(res, errorJsonText, errorJsonOptions);
  return true;
}

export function attachStreamResponseHelper(res: any): void {
  if (!res) return;
  const writeJson = async (
    jsonText: string,
    options: {
      statusCode?: number;
      headers?: Record<
        string,
        string | number | readonly string[] | undefined | null
      >;
    } = {},
    kind: 'success' | 'error',
  ): Promise<void> => {
    const statusCode = options.statusCode ?? (kind === 'error' ? 500 : 200);
    const minimum = kind === 'error' ? 400 : 200;
    const maximum = kind === 'error' ? 599 : 399;
    if (
      !Number.isInteger(statusCode)
      || statusCode < minimum
      || statusCode > maximum
    ) {
      throw new TypeError(
        kind === 'error'
          ? '@THROW.json statusCode must be from 400 to 599'
          : '@RES.json statusCode must be from 200 to 399',
      );
    }
    const errorResponse = kind === 'error'
      ? mergeErrorTrace(jsonText, res, statusCode)
      : null;
    const responseText = errorResponse?.responseText ?? jsonText;
    beginNativeResponse(res);
    applyResponseOptions(res, {
      ...options,
      statusCode,
      headers: {
        ...options.headers,
        ...(errorResponse
          ? { 'X-Correlation-ID': errorResponse.correlationId }
          : {}),
      },
      mimetype: 'application/json; charset=utf-8',
    });
    res.end(responseText);
  };
  if (!res.__enfyraJson) {
    res.__enfyraJson = (jsonText: string, options?: any) =>
      writeJson(jsonText, options, 'success');
  }
  if (!res.__enfyraErrorJson) {
    res.__enfyraErrorJson = (jsonText: string, options?: any) =>
      writeJson(jsonText, options, 'error');
  }
  if (!res.__enfyraBytes) res.__enfyraBytes = async (
    bytes: Uint8Array,
    options?: {
      statusCode?: number;
      mimetype?: string;
      filename?: string;
      headers?: Record<
        string,
        string | number | readonly string[] | undefined | null
      >;
    },
  ): Promise<void> => {
    beginNativeResponse(res);
    applyResponseOptions(res, {
      ...options,
      mimetype: options?.mimetype || 'application/octet-stream',
    });
    res.end(Buffer.from(bytes));
  };
  if (res.stream) return;
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
  ): Promise<void> => {
    const readable =
      stream && typeof stream.pipe === 'function'
        ? stream
        : stream && typeof (Readable as any).fromWeb === 'function'
          ? (Readable as any).fromWeb(stream)
          : null;
    if (!readable || typeof readable.pipe !== 'function') {
      throw new Error('@RES.stream requires a readable stream');
    }
    beginNativeResponse(res, { allowHeadersSent: true });
    applyResponseOptions(res, options);
    recordDynamicDeliveryTransport(res, 'stream_start');

    return new Promise<void>((resolve, reject) => {
      let settled = false;
      const settle = (callback: () => void) => {
        if (settled) return;
        settled = true;
        callback();
      };

      readable.on('data', (chunk: unknown) => {
        const bytes =
          typeof chunk === 'string'
            ? Buffer.byteLength(chunk)
            : ArrayBuffer.isView(chunk)
              ? chunk.byteLength
              : 0;
        recordDynamicDeliveryTransport(res, 'stream_chunk', bytes);
      });
      readable.on('error', (error: Error) => {
        recordDynamicDeliveryTransport(res, 'stream_error', undefined, error);
        const failure = recordDynamicStreamFailure(res, error);
        streamLogger.error({
          message: 'Dynamic response stream failed',
          error: error.message,
          method: res.req?.method,
          url: res.req?.originalUrl ?? res.req?.url,
          statusCode: res.statusCode,
        });
        settle(() => reject(error));
        finishDynamicHttpFailure(res, failure);
      });
      readable.on('end', () => {
        recordDynamicDeliveryTransport(res, 'stream_end');
        settle(resolve);
      });
      res.once('close', () => settle(resolve));
      readable.pipe(res);
    });
  };
}

export class DynamicService {
  private readonly logger = new Logger(DynamicService.name);
  private readonly executorEngineService: RuntimeScriptExecutorService;
  private readonly loggingService: LoggingService;
  private readonly runtimeScriptRepairService?: RuntimeScriptRepairService;
  private readonly dynamicCancellationService: DynamicCancellationService;

  constructor(deps: {
    executorEngineService: RuntimeScriptExecutorService;
    loggingService: LoggingService;
    runtimeScriptRepairService?: RuntimeScriptRepairService;
    dynamicCancellationService: DynamicCancellationService;
  }) {
    this.executorEngineService = deps.executorEngineService;
    this.loggingService = deps.loggingService;
    if (deps.runtimeScriptRepairService !== undefined) {
      this.runtimeScriptRepairService = deps.runtimeScriptRepairService;
    }
    this.dynamicCancellationService = deps.dynamicCancellationService;
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

  private persistScriptLogs(
    req: RequestWithRouteData,
    statusCode: number,
  ): void {
    persistDynamicScriptLogs(req, statusCode);
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
      if (res) attachDynamicDeliveryDiagnostics(req, res);
      if (res) beginDynamicRouteExecution(req, res, this.dynamicCancellationService);
      if (res) {
        attachStreamResponseHelper(res);
        routeData.context.$res = res as unknown as DynamicResponse;
      }

      this.executorEngineService.register(req, {
        code: handler,
        sourceCode: routeData.handlerRecord?.sourceCode ?? handler,
        scriptLanguage: routeData.handlerRecord?.scriptLanguage ?? 'typescript',
        scriptId: routeData.handlerRecord?.id,
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
            scriptId: hook.id,
            onCompiledCodeRepair: this.repairCompiledCode(
              'enfyra_post_hook',
              hook,
            ),
            type: 'postHook',
          } as any);
        }
      }

      let value: any;
      let shortCircuit = false;
      try {
        const result = res
          ? await runDynamicRouteBatch(req, res, this.executorEngineService)
          : await this.executorEngineService.runBatch(req, routeData.routeMethodConfig?.timeout || undefined);
        value = result.value;
        shortCircuit = result.shortCircuit;
      } catch (error) {
        if (res && isDynamicExecutionTimeout(error)) {
          recordDynamicExecutionFailure(res, error);
        }
        throw error;
      } finally {
        completeDynamicRouteExecution(req);
        delete routeData.context.$res;
      }

      this.persistScriptLogs(req, Number(routeData.res?.statusCode) || 200);

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
      completeDynamicRouteExecution(req);
      const err = error as {
        code?: string;
        errorCode?: string;
        errorPath?: string;
        isRawScriptErrorCarrier?: boolean;
        statusCode?: number;
        details?: any;
      };
      let failure = getDynamicHttpFailure(routeData.res);
      if (!failure && routeData.res && isDynamicExecutionTimeout(error)) {
        failure = recordDynamicExecutionFailure(routeData.res, err);
      }
      if (failure) {
        await this.dynamicCancellationService.run(req, failure);
        if (routeData.res) finishDynamicHttpFailure(routeData.res, failure);
        return undefined;
      }
      if (err.code === 'ERR_EXECUTION_ABORTED') return undefined;
      const httpStatus =
        error instanceof HttpException
          ? error.getStatus()
          : typeof err.statusCode === 'number'
            ? err.statusCode
            : undefined;
      this.persistScriptLogs(
        req,
        httpStatus || Number(routeData.res?.statusCode) || 500,
      );
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
          userId: req.user?.id ?? req.user?._id,
        });
      }
      const carrierCode = err.errorCode ?? err.code;
      const isRawScriptErrorCarrier = err.isRawScriptErrorCarrier === true;
      if (
        await writeRawScriptErrorJson(error, routeData.res, routeData.handler)
      ) {
        return undefined;
      }
      if (
        isRawScriptErrorCarrier
        && httpStatus !== undefined
        && httpStatus >= 400
        && httpStatus <= 599
      ) {
        throw new HttpException(getErrorMessage(error), httpStatus);
      }
      if (
        httpStatus !== undefined
        && carrierCode === `HTTP_${httpStatus}`
      ) {
        throw new HttpException(getErrorMessage(error), httpStatus);
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
          userId: req.user?.id ?? req.user?._id,
          isTableOperation: isTableDefinitionOperation,
        },
      );
    }
  }
}
