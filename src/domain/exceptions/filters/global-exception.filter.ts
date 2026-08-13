import {
  HttpException,
  isCustomException,
  getErrorCode,
} from '../custom-exceptions';
import { Request, Response, NextFunction } from 'express';
import { GraphQLError } from 'graphql';
import { Logger } from '../../../shared/logger';
import { AppError } from '../../../shared/errors';

interface ErrorResponse {
  success: false;
  message: string | string[];
  statusCode: number;
  error: {
    code: string;
    message: string | string[];
    details?: any;
    timestamp: string;
    path: string;
    method: string;
    correlationId?: string;
  };
}

const logger = new Logger('GlobalExceptionFilter');

export function globalExceptionMiddleware(
  err: unknown,
  req: Request,
  res: Response,
  _next: NextFunction,
): void {
  const correlationId =
    (req.headers['x-correlation-id'] as string) || generateCorrelationId();
  const { statusCode, errorCode, message, details } = getErrorDetails(err, req);
  logError(err, req, correlationId, statusCode);

  if (req.url?.includes('/graphql')) {
    if (typeof res.status === 'function') {
      res.status(statusCode).json({
        errors: [
          { message, extensions: { code: errorCode, correlationId, details } },
        ],
      });
    }
    return;
  }

  const errorResponse: ErrorResponse = {
    success: false,
    message,
    statusCode,
    error: {
      code: errorCode,
      message,
      details,
      timestamp: new Date().toISOString(),
      path: req.url,
      method: req.method,
      correlationId,
    },
  };
  const logs = (err as any)?.logs;
  if (logs && Array.isArray(logs) && logs.length > 0) {
    (errorResponse as any).logs = logs;
  }
  res.status(statusCode).json(errorResponse);
}

function getErrorDetails(exception: unknown, request?: Request): {
  statusCode: number;
  errorCode: string;
  message: string | string[];
  details?: any;
} {
  if (exception instanceof HttpException) {
    const status = exception.getStatus();
    const response = exception.getResponse() as any;
    return {
      statusCode: status,
      errorCode: getErrorCode(exception),
      message: response?.message || exception.message,
      details: response?.details || null,
    };
  }
  if (isCustomException(exception)) {
    return {
      statusCode: exception.statusCode,
      errorCode: exception.errorCode,
      message: exception.messages ?? exception.message,
      details: exception.details,
    };
  }
  if (exception instanceof AppError) {
    return {
      statusCode: exception.statusCode,
      errorCode: exception.code || exception.name,
      message: exception.message,
      details: exception.details,
    };
  }
  const bodyParserDetails = getBodyParserErrorDetails(exception, request);
  if (bodyParserDetails) {
    return bodyParserDetails;
  }
  if (
    exception instanceof Error &&
    typeof (exception as any).statusCode === 'number'
  ) {
    return {
      statusCode: (exception as any).statusCode,
      errorCode: (exception as any).errorCode || exception.name,
      message: (exception as any).messages ?? exception.message,
      details: (exception as any).details,
    };
  }
  if (exception instanceof GraphQLError) {
    return {
      statusCode: 400,
      errorCode: 'GRAPHQL_ERROR',
      message: exception.message,
      details: exception.extensions,
    };
  }
  if (exception instanceof SyntaxError && 'body' in (exception as any)) {
    return {
      statusCode: 400,
      errorCode: 'BAD_REQUEST',
      message: ['Invalid JSON body'],
      details:
        process.env.NODE_ENV === 'development'
          ? (exception as Error).message
          : null,
    };
  }
  if (exception instanceof Error) {
    return {
      statusCode: 500,
      errorCode: 'INTERNAL_SERVER_ERROR',
      message: exception.message || 'An unexpected error occurred',
      details: process.env.NODE_ENV === 'development' ? exception.stack : null,
    };
  }
  return {
    statusCode: 500,
    errorCode: 'UNKNOWN_ERROR',
    message: 'An unknown error occurred',
    details: exception,
  };
}

function getBodyParserErrorDetails(exception: unknown, request?: Request) {
  const candidate = exception as (Error & {
    type?: string;
    status?: number;
    statusCode?: number;
    code?: string;
    limit?: number | string;
    length?: number | string;
    received?: number | string;
    expected?: number | string;
  }) | null;
  const requestMetadata = request as Request & {
    requestBodyLimitBytes?: number;
    uploadFileSizeLimitBytes?: number;
    uploadProgressLoaded?: number;
    uploadProgressTotal?: number;
  };
  const formatMb = (bytes: number) => Number.isFinite(bytes) && bytes > 0
    ? Number((bytes / (1024 * 1024)).toFixed(2))
    : null;
  const requestDetails = {
    path: request?.originalUrl || request?.url || null,
    method: request?.method || null,
    contentType: request?.headers?.['content-type'] || null,
  };
  const contentLengthBytes = Number(request?.headers?.['content-length']);

  if (candidate?.code === 'LIMIT_FILE_SIZE') {
    const limitBytes = Number(requestMetadata.uploadFileSizeLimitBytes);
    const receivedBytes = Number(
      candidate.received
        ?? candidate.length
        ?? (Number.isFinite(contentLengthBytes) ? contentLengthBytes : undefined)
        ?? requestMetadata.uploadProgressLoaded,
    );
    return {
      statusCode: 413,
      errorCode: 'FILE_TOO_LARGE',
      message: 'Uploaded file exceeds the configured file-size limit.',
      details: {
        phase: 'multipart_upload',
        ...requestDetails,
        configuredLimitBytes: Number.isFinite(limitBytes) ? limitBytes : null,
        configuredLimitMb: formatMb(limitBytes),
        receivedBytes: Number.isFinite(receivedBytes) && receivedBytes > 0
          ? receivedBytes
          : null,
        receivedMb: formatMb(receivedBytes),
        guidance: 'Reduce the file size or increase the configured upload limit for this route or deployment.',
      },
    };
  }

  const isPayloadTooLarge = candidate?.type === 'entity.too.large'
    || candidate?.status === 413
    || candidate?.statusCode === 413
    || candidate?.name === 'PayloadTooLargeError';
  if (!isPayloadTooLarge) return null;

  const limitBytes = Number(
    candidate?.limit ?? requestMetadata.requestBodyLimitBytes,
  );
  const receivedBytes = Number(candidate?.length ?? candidate?.received);
  return {
    statusCode: 413,
    errorCode: 'REQUEST_ENTITY_TOO_LARGE',
    message: 'Request body exceeds the configured request limit.',
    details: {
      phase: 'body_parser',
      ...requestDetails,
      configuredLimitBytes: Number.isFinite(limitBytes) ? limitBytes : null,
      configuredLimitMb: formatMb(limitBytes),
      receivedBytes: Number.isFinite(receivedBytes) && receivedBytes > 0
        ? receivedBytes
        : null,
      receivedMb: formatMb(receivedBytes),
      guidance: 'Reduce the request context or increase enfyra_setting.maxRequestBodySize for this deployment. The request was rejected before route handling or upstream forwarding.',
    },
  };
}

function logError(
  exception: unknown,
  request: Request,
  correlationId: string,
  statusCode: number,
): void {
  let errorMessage: string;
  if (exception instanceof HttpException) {
    const response = exception.getResponse() as any;
    const responseMessage = response?.message;
    errorMessage =
      typeof responseMessage === 'string' ? responseMessage : exception.message;
  } else {
    errorMessage =
      exception instanceof Error ? exception.message : String(exception);
  }

  const errorName =
    exception instanceof Error ? exception.name : 'UnknownError';
  const logData = {
    correlationId,
    method: request.method,
    url: request.url,
    statusCode,
    errorCode: (exception as any)?.errorCode || (exception as any)?.code,
    errorName,
    details: (exception as any)?.details,
    userId: (request as any).user?.id,
    stack: exception instanceof Error ? exception.stack : undefined,
  };
  if (statusCode >= 500) {
    logger.error({ message: `[${statusCode}] ${errorMessage}`, ...logData });
  } else if (statusCode >= 400) {
    logger.warn({ message: `[${statusCode}] ${errorMessage}`, ...logData });
  } else {
    logger.log({ message: `[${statusCode}] ${errorMessage}`, ...logData });
  }
}

function generateCorrelationId(): string {
  return `req_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
}
