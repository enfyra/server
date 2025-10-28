import {
  ExceptionFilter,
  Catch,
  ArgumentsHost,
  HttpException,
  HttpStatus,
  Logger,
} from '@nestjs/common';
import { Request, Response } from 'express';
import { GraphQLError } from 'graphql';
import {
  CustomException,
  isCustomException,
  getErrorCode,
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

@Catch()
export class GlobalExceptionFilter implements ExceptionFilter {
  private readonly logger = new Logger(GlobalExceptionFilter.name);

  catch(exception: unknown, host: ArgumentsHost): void {
    const ctx = host.switchToHttp();
    const response = ctx.getResponse<Response>();
    const request = ctx.getRequest<Request>();

    // Extract correlation ID from headers or generate new one
    const correlationId =
      (request.headers['x-correlation-id'] as string) ||
      this.generateCorrelationId();

    // Determine status code and error details
    const { statusCode, errorCode, message, details } =
      this.getErrorDetails(exception);

    // Log error with structured format
    this.logError(exception, request, correlationId, statusCode);

    // Create standardized error response
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

    // Handle GraphQL errors differently
    if (this.isGraphQLContext(host)) {
      this.handleGraphQLError(exception, host, correlationId);
      return;
    }

    // Attach logs if available
    const logs = (exception as any)?.logs;
    if (logs && Array.isArray(logs) && logs.length > 0) {
      (errorResponse as any).logs = logs;
    }

    // Send HTTP response
    response.status(statusCode).json(errorResponse);
  }

  private getErrorDetails(exception: unknown): {
    statusCode: number;
    errorCode: string;
    message: string;
    details?: any;
  } {
    // Handle Custom Exceptions first
    if (isCustomException(exception)) {
      return {
        statusCode: exception.getStatus(),
        errorCode: exception.errorCode,
        message: exception.message,
        details: exception.details,
      };
    }

    // Handle HttpException (NestJS built-in exceptions)
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

    // Handle GraphQLError
    if (exception instanceof GraphQLError) {
      return {
        statusCode: HttpStatus.BAD_REQUEST,
        errorCode: 'GRAPHQL_ERROR',
        message: exception.message,
        details: exception.extensions,
      };
    }

    // Handle unknown errors
    if (exception instanceof Error) {
      return {
        statusCode: HttpStatus.INTERNAL_SERVER_ERROR,
        errorCode: 'INTERNAL_SERVER_ERROR',
        message: exception.message || 'An unexpected error occurred',
        details:
          process.env.NODE_ENV === 'development' ? exception.stack : null,
      };
    }

    // Handle other types of errors
    return {
      statusCode: HttpStatus.INTERNAL_SERVER_ERROR,
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
    const logData = {
      correlationId,
      method: request.method,
      url: request.url,
      statusCode,
      userAgent: request.headers['user-agent'],
      ip: request.ip,
      userId: (request as any).user?.id,
      error:
        exception instanceof Error
          ? {
              name: exception.name,
              message: exception.message,
              stack: exception.stack,
            }
          : exception,
    };

    if (statusCode >= 500) {
      this.logger.error('Server Error', logData);
    } else if (statusCode >= 400) {
      this.logger.warn('Client Error', logData);
    } else {
      this.logger.log('Other Error', logData);
    }
  }

  private isGraphQLContext(host: ArgumentsHost): boolean {
    const context = host.getType();
    // Check if this is a GraphQL context by looking at the request
    const request = host.switchToHttp().getRequest();
    return (
      request?.body?.query !== undefined || request?.url?.includes('/graphql')
    );
  }

  private handleGraphQLError(
    exception: unknown,
    host: ArgumentsHost,
    correlationId: string,
  ): void {
    // GraphQL errors are handled by the GraphQL context
    // This method can be extended if needed
    this.logger.error('GraphQL Error', { exception, correlationId });
  }

  private generateCorrelationId(): string {
    return `req_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
  }
}
