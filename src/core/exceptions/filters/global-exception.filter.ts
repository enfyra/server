import { HttpException } from '../custom-exceptions';
import { Request, Response } from 'express';
import { GraphQLError } from 'graphql';
import { Logger } from '../../../shared/logger';
import {
  isCustomException,
  getErrorCode,
  ScriptExecutionException,
  ScriptTimeoutException,
} from '../custom-exceptions';

export interface ErrorResponse {
  success: false;
  message: string;
  statusCode: number;
  error: {
    code: string;
    message: string;
    details?: any;
    timestamp: string;
    path: string;
    method: string;
    correlationId?: string;
  };
}

interface ArgumentsHost {
  switchToHttp(): {
    getResponse<T = Response>(): T;
    getRequest<T = Request>(): T;
  };
}

export class GlobalExceptionFilter {
  private readonly logger = new Logger(GlobalExceptionFilter.name);

  catch(exception: unknown, host: ArgumentsHost): void {
    const ctx = host.switchToHttp();
    const response = ctx.getResponse<Response>();
    const request = ctx.getRequest<Request>();
    const correlationId =
      (request.headers['x-correlation-id'] as string) ||
      this.generateCorrelationId();
    const { statusCode, errorCode, message, details } =
      this.getErrorDetails(exception);
    this.logError(exception, request, correlationId, statusCode);
    const errorResponse: ErrorResponse = {
      success: false,
      message,
      statusCode,
      error: {
        code: errorCode,
        message,
        details,
        timestamp: new Date().toISOString(),
        path: request.url,
        method: request.method,
        correlationId,
      },
    };
    if (this.isGraphQLContext(host)) {
      this.handleGraphQLError(exception, host, correlationId);
      return;
    }
    const logs = (exception as any)?.logs;
    if (logs && Array.isArray(logs) && logs.length > 0) {
      (errorResponse as any).logs = logs;
    }
    response.status(statusCode).json(errorResponse);
  }

  private getErrorDetails(exception: unknown): {
    statusCode: number;
    errorCode: string;
    message: string;
    details?: any;
  } {
    // Check HttpException first because it extends CustomException
    // and has special handling for response objects
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
        message: exception.message,
        details: exception.details,
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
    if (exception instanceof Error) {
      return {
        statusCode: 500,
        errorCode: 'INTERNAL_SERVER_ERROR',
        message: exception.message || 'An unexpected error occurred',
        details:
          process.env.NODE_ENV === 'development' ? exception.stack : null,
      };
    }
    return {
      statusCode: 500,
      errorCode: 'UNKNOWN_ERROR',
      message: 'An unknown error occurred',
      details: exception,
    };
  }

  private getErrorCodeFromStatus(status: number): string {
    const errorCodeMap: Record<number, string> = {
      400: 'BAD_REQUEST',
      401: 'UNAUTHORIZED',
      403: 'FORBIDDEN',
      404: 'NOT_FOUND',
      409: 'CONFLICT',
      422: 'UNPROCESSABLE_ENTITY',
      429: 'TOO_MANY_REQUESTS',
      500: 'INTERNAL_SERVER_ERROR',
      502: 'BAD_GATEWAY',
      503: 'SERVICE_UNAVAILABLE',
    };
    return errorCodeMap[status] || 'UNKNOWN_ERROR';
  }

  private logError(
    exception: unknown,
    request: Request,
    correlationId: string,
    statusCode: number,
  ): void {
    // Skip logging for script execution errors - already logged by handler executor
    if (
      exception instanceof ScriptExecutionException ||
      exception instanceof ScriptTimeoutException
    ) {
      return;
    }

    // Extract message from exception, handling HttpException's response object
    let errorMessage: string;
    if (exception instanceof HttpException) {
      const response = exception.getResponse() as any;
      const responseMessage = response?.message;
      errorMessage = typeof responseMessage === 'string' ? responseMessage : exception.message;
    } else {
      errorMessage = exception instanceof Error ? exception.message : String(exception);
    }

    const errorName =
      exception instanceof Error ? exception.name : 'UnknownError';
    const logData = {
      correlationId,
      method: request.method,
      url: request.url,
      statusCode,
      errorName,
      userId: (request as any).user?.id,
      stack: exception instanceof Error ? exception.stack : undefined,
    };
    if (statusCode >= 500) {
      this.logger.error({
        message: `[${statusCode}] ${errorMessage}`,
        ...logData,
      });
    } else if (statusCode >= 400) {
      this.logger.warn({
        message: `[${statusCode}] ${errorMessage}`,
        ...logData,
      });
    } else {
      this.logger.log({
        message: `[${statusCode}] ${errorMessage}`,
        ...logData,
      });
    }
  }

  private isGraphQLContext(host: ArgumentsHost): boolean {
    const request = host.switchToHttp().getRequest();
    return request?.url?.includes('/graphql') === true;
  }

  private handleGraphQLError(
    exception: unknown,
    host: ArgumentsHost,
    correlationId: string,
  ): void {
    this.logger.error('GraphQL Error', exception instanceof Error ? exception.stack : String(exception));
    const response = host.switchToHttp().getResponse();
    const { statusCode, errorCode, message, details } =
      this.getErrorDetails(exception);
    if (response && typeof response.status === 'function') {
      response.status(statusCode).json({
        errors: [
          { message, extensions: { code: errorCode, correlationId, details } },
        ],
      });
    }
  }

  private generateCorrelationId(): string {
    return `req_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
  }
}
