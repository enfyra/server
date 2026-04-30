export abstract class CustomException extends Error {
  public messages?: string[];

  constructor(
    message: string,
    public readonly statusCode: number,
    public readonly errorCode: string,
    public readonly details?: any,
  ) {
    super(message);
    this.name = this.constructor.name;
    Error.captureStackTrace(this, this.constructor);
  }

  toJSON() {
    return {
      message: this.messages ?? this.message,
      errorCode: this.errorCode,
      details: this.details,
    };
  }
}

export class BusinessLogicException extends CustomException {
  constructor(message: string, details?: any) {
    super(message, 400, 'BUSINESS_LOGIC_ERROR', details);
  }
}

export class ValidationException extends CustomException {
  constructor(message: string, details?: any) {
    super(message, 422, 'VALIDATION_ERROR', details);
  }
}

export class ResourceNotFoundException extends CustomException {
  constructor(resource: string, identifier?: string) {
    const message = identifier
      ? `${resource} with identifier '${identifier}' not found`
      : `${resource} not found`;
    super(message, 404, 'RESOURCE_NOT_FOUND', {
      resource,
      identifier,
    });
  }
}

export class DuplicateResourceException extends CustomException {
  constructor(resource: string, field: string, value: string) {
    super(
      `${resource} with ${field} '${value}' already exists`,
      409,
      'DUPLICATE_RESOURCE',
      { resource, field, value },
    );
  }
}

export class AuthenticationException extends CustomException {
  constructor(message: string = 'Authentication failed', details?: any) {
    super(message, 401, 'AUTHENTICATION_ERROR', details);
  }
}

export class AuthorizationException extends CustomException {
  constructor(message: string = 'Insufficient permissions', details?: any) {
    super(message, 403, 'AUTHORIZATION_ERROR', details);
  }
}

export class TokenExpiredException extends CustomException {
  constructor() {
    super('Token has expired', 401, 'TOKEN_EXPIRED');
  }
}

export class InvalidTokenException extends CustomException {
  constructor() {
    super('Invalid token provided', 401, 'INVALID_TOKEN');
  }
}

export class DatabaseException extends CustomException {
  constructor(message: string, details?: any) {
    super(message, 500, 'DATABASE_ERROR', details);
  }
}

export class DatabaseConnectionException extends CustomException {
  constructor() {
    super('Database connection failed', 503, 'DATABASE_CONNECTION_ERROR');
  }
}

export class DatabaseQueryException extends CustomException {
  constructor(message: string, details?: any) {
    super(message, 400, 'DATABASE_QUERY_ERROR', details);
  }
}

export class ExternalServiceException extends CustomException {
  constructor(service: string, message: string, details?: any) {
    super(
      `External service '${service}' error: ${message}`,
      502,
      'EXTERNAL_SERVICE_ERROR',
      { service, details },
    );
  }
}

export class ServiceUnavailableException extends CustomException {
  constructor(service: string) {
    super(
      `Service '${service}' is currently unavailable`,
      503,
      'SERVICE_UNAVAILABLE',
      { service },
    );
  }
}

export class RateLimitExceededException extends CustomException {
  constructor(limit: number, window: string) {
    super(
      `Rate limit exceeded. Maximum ${limit} requests per ${window}`,
      429,
      'RATE_LIMIT_EXCEEDED',
      { limit, window },
    );
  }
}

export class ScriptExecutionException extends CustomException {
  constructor(message: string, scriptId?: string, details?: any) {
    super(
      `Script execution failed: ${message}`,
      400,
      'SCRIPT_EXECUTION_ERROR',
      { scriptId, ...details },
    );
  }
}

export class ScriptTimeoutException extends CustomException {
  constructor(timeoutMs: number, scriptId?: string) {
    super(
      `Script execution timed out after ${timeoutMs}ms`,
      408,
      'SCRIPT_TIMEOUT',
      { timeoutMs, scriptId },
    );
  }
}

export class SchemaException extends CustomException {
  constructor(message: string, details?: any) {
    super(message, 400, 'SCHEMA_ERROR', details);
  }
}

export class ConfigurationException extends CustomException {
  constructor(message: string, configKey?: string) {
    super(`Configuration error: ${message}`, 500, 'CONFIGURATION_ERROR', {
      configKey,
    });
  }
}

export class FileUploadException extends CustomException {
  constructor(message: string, details?: any) {
    super(message, 400, 'FILE_UPLOAD_ERROR', details);
  }
}

export class FileNotFoundException extends CustomException {
  constructor(filePath: string) {
    super(`File not found: ${filePath}`, 404, 'FILE_NOT_FOUND', { filePath });
  }
}

export class FileSizeExceededException extends CustomException {
  constructor(maxSize: string, actualSize: string) {
    super(
      `File size exceeded. Maximum: ${maxSize}, Actual: ${actualSize}`,
      413,
      'FILE_SIZE_EXCEEDED',
      { maxSize, actualSize },
    );
  }
}

export class BadRequestException extends CustomException {
  constructor(
    message: string | string[] | Record<string, any> = 'Bad Request',
    details?: any,
  ) {
    const isArray = Array.isArray(message);
    const primary = isArray
      ? (message as string[]).join('; ') || 'Bad Request'
      : typeof message === 'string'
        ? message
        : JSON.stringify(message);
    super(primary, 400, 'BAD_REQUEST', details);
    if (isArray) this.messages = message as string[];
  }
}

export class UnauthorizedException extends CustomException {
  constructor(message: string = 'Unauthorized', details?: any) {
    super(message, 401, 'UNAUTHORIZED', details);
  }
}

export class ForbiddenException extends CustomException {
  constructor(message: string = 'Forbidden', details?: any) {
    super(message, 403, 'FORBIDDEN', details);
  }
}

export class NotFoundException extends CustomException {
  constructor(message: string = 'Not Found', details?: any) {
    super(message, 404, 'NOT_FOUND', details);
  }
}

export class HttpException extends CustomException {
  private _response: string | object;
  constructor(message: string | object, statusCode: number) {
    const messageStr =
      typeof message === 'string' ? message : JSON.stringify(message);
    super(messageStr, statusCode, 'HTTP_ERROR');
    this._response = message;
  }
  getStatus() {
    return this.statusCode;
  }
  getResponse() {
    return this._response;
  }
}

export class MethodNotAllowedException extends CustomException {
  constructor(message: string = 'Method Not Allowed', details?: any) {
    super(message, 405, 'METHOD_NOT_ALLOWED', details);
  }
}

export class NotAcceptableException extends CustomException {
  constructor(message: string = 'Not Acceptable', details?: any) {
    super(message, 406, 'NOT_ACCEPTABLE', details);
  }
}

export class ConflictException extends CustomException {
  constructor(message: string = 'Conflict', details?: any) {
    super(message, 409, 'CONFLICT', details);
  }
}

export class TooManyRequestsException extends CustomException {
  constructor(message: string = 'Too Many Requests', details?: any) {
    super(message, 429, 'TOO_MANY_REQUESTS', details);
  }
}

export class InternalServerErrorException extends CustomException {
  constructor(message: string = 'Internal Server Error', details?: any) {
    super(message, 500, 'INTERNAL_SERVER_ERROR', details);
  }
}

export class BadGatewayException extends CustomException {
  constructor(message: string = 'Bad Gateway', details?: any) {
    super(message, 502, 'BAD_GATEWAY', details);
  }
}

export class GatewayTimeoutException extends CustomException {
  constructor(message: string = 'Gateway Timeout', details?: any) {
    super(message, 504, 'GATEWAY_TIMEOUT', details);
  }
}

export function isCustomException(
  exception: any,
): exception is CustomException {
  return exception instanceof CustomException;
}

export function getErrorCode(exception: any): string {
  if (isCustomException(exception)) {
    return exception.errorCode;
  }
  if (exception.statusCode) {
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
    return errorCodeMap[exception.statusCode] || 'UNKNOWN_ERROR';
  }
  return 'UNKNOWN_ERROR';
}
