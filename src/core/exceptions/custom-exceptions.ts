import { HttpException, HttpStatus } from '@nestjs/common';

// Base custom exception class
export abstract class CustomException extends HttpException {
  constructor(
    message: string,
    statusCode: HttpStatus,
    public readonly errorCode: string,
    public readonly details?: any,
  ) {
    super(
      {
        message,
        errorCode,
        details,
      },
      statusCode,
    );
  }
}

// Business Logic Exceptions
export class BusinessLogicException extends CustomException {
  constructor(message: string, details?: any) {
    super(message, HttpStatus.BAD_REQUEST, 'BUSINESS_LOGIC_ERROR', details);
  }
}

export class ValidationException extends CustomException {
  constructor(message: string, details?: any) {
    super(
      message,
      HttpStatus.UNPROCESSABLE_ENTITY,
      'VALIDATION_ERROR',
      details,
    );
  }
}

export class ResourceNotFoundException extends CustomException {
  constructor(resource: string, identifier?: string) {
    const message = identifier
      ? `${resource} with identifier '${identifier}' not found`
      : `${resource} not found`;
    super(message, HttpStatus.NOT_FOUND, 'RESOURCE_NOT_FOUND', {
      resource,
      identifier,
    });
  }
}

export class DuplicateResourceException extends CustomException {
  constructor(resource: string, field: string, value: string) {
    super(
      `${resource} with ${field} '${value}' already exists`,
      HttpStatus.CONFLICT,
      'DUPLICATE_RESOURCE',
      { resource, field, value },
    );
  }
}

// Authentication & Authorization Exceptions
export class AuthenticationException extends CustomException {
  constructor(message: string = 'Authentication failed', details?: any) {
    super(message, HttpStatus.UNAUTHORIZED, 'AUTHENTICATION_ERROR', details);
  }
}

export class AuthorizationException extends CustomException {
  constructor(message: string = 'Insufficient permissions', details?: any) {
    super(message, HttpStatus.FORBIDDEN, 'AUTHORIZATION_ERROR', details);
  }
}

export class TokenExpiredException extends CustomException {
  constructor() {
    super('Token has expired', HttpStatus.UNAUTHORIZED, 'TOKEN_EXPIRED');
  }
}

export class InvalidTokenException extends CustomException {
  constructor() {
    super('Invalid token provided', HttpStatus.UNAUTHORIZED, 'INVALID_TOKEN');
  }
}

// Database Exceptions
export class DatabaseException extends CustomException {
  constructor(message: string, details?: any) {
    super(message, HttpStatus.INTERNAL_SERVER_ERROR, 'DATABASE_ERROR', details);
  }
}

export class DatabaseConnectionException extends CustomException {
  constructor() {
    super(
      'Database connection failed',
      HttpStatus.SERVICE_UNAVAILABLE,
      'DATABASE_CONNECTION_ERROR',
    );
  }
}

export class DatabaseQueryException extends CustomException {
  constructor(message: string, details?: any) {
    super(message, HttpStatus.BAD_REQUEST, 'DATABASE_QUERY_ERROR', details);
  }
}

// External Service Exceptions
export class ExternalServiceException extends CustomException {
  constructor(service: string, message: string, details?: any) {
    super(
      `External service '${service}' error: ${message}`,
      HttpStatus.BAD_GATEWAY,
      'EXTERNAL_SERVICE_ERROR',
      { service, details },
    );
  }
}

export class ServiceUnavailableException extends CustomException {
  constructor(service: string) {
    super(
      `Service '${service}' is currently unavailable`,
      HttpStatus.SERVICE_UNAVAILABLE,
      'SERVICE_UNAVAILABLE',
      { service },
    );
  }
}

// Rate Limiting & Throttling
export class RateLimitExceededException extends CustomException {
  constructor(limit: number, window: string) {
    super(
      `Rate limit exceeded. Maximum ${limit} requests per ${window}`,
      HttpStatus.TOO_MANY_REQUESTS,
      'RATE_LIMIT_EXCEEDED',
      { limit, window },
    );
  }
}

// Script & Dynamic Code Exceptions
export class ScriptExecutionException extends CustomException {
  constructor(message: string, scriptId?: string, details?: any) {
    super(
      `Script execution failed: ${message}`,
      HttpStatus.BAD_REQUEST,
      'SCRIPT_EXECUTION_ERROR',
      { scriptId, ...details },
    );
  }
}

export class ScriptTimeoutException extends CustomException {
  constructor(timeoutMs: number, scriptId?: string) {
    super(
      `Script execution timed out after ${timeoutMs}ms`,
      HttpStatus.REQUEST_TIMEOUT,
      'SCRIPT_TIMEOUT',
      { timeoutMs, scriptId },
    );
  }
}

// Schema & Configuration Exceptions
export class SchemaException extends CustomException {
  constructor(message: string, details?: any) {
    super(message, HttpStatus.BAD_REQUEST, 'SCHEMA_ERROR', details);
  }
}

export class ConfigurationException extends CustomException {
  constructor(message: string, configKey?: string) {
    super(
      `Configuration error: ${message}`,
      HttpStatus.INTERNAL_SERVER_ERROR,
      'CONFIGURATION_ERROR',
      { configKey },
    );
  }
}

// File & Upload Exceptions
export class FileUploadException extends CustomException {
  constructor(message: string, details?: any) {
    super(message, HttpStatus.BAD_REQUEST, 'FILE_UPLOAD_ERROR', details);
  }
}

export class FileNotFoundException extends CustomException {
  constructor(filePath: string) {
    super(
      `File not found: ${filePath}`,
      HttpStatus.NOT_FOUND,
      'FILE_NOT_FOUND',
      { filePath },
    );
  }
}

export class FileSizeExceededException extends CustomException {
  constructor(maxSize: string, actualSize: string) {
    super(
      `File size exceeded. Maximum: ${maxSize}, Actual: ${actualSize}`,
      HttpStatus.PAYLOAD_TOO_LARGE,
      'FILE_SIZE_EXCEEDED',
      { maxSize, actualSize },
    );
  }
}

// Utility function to check if an exception is a custom exception
export function isCustomException(
  exception: any,
): exception is CustomException {
  return exception instanceof CustomException;
}

// Utility function to get error code from any exception
export function getErrorCode(exception: any): string {
  if (isCustomException(exception)) {
    return exception.errorCode;
  }

  if (exception instanceof HttpException) {
    const status = exception.getStatus();
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

  return 'UNKNOWN_ERROR';
}
