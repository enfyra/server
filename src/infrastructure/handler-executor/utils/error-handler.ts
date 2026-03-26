import { Logger, HttpException, HttpStatus } from '@nestjs/common';
import {
  ScriptTimeoutException,
  ScriptExecutionException,
  AuthenticationException,
  AuthorizationException,
  BusinessLogicException,
  RateLimitExceededException,
  ResourceNotFoundException,
  DuplicateResourceException,
  ValidationException,
  ServiceUnavailableException,
  DatabaseException,
  TokenExpiredException,
  InvalidTokenException,
  ExternalServiceException,
  SchemaException,
  ConfigurationException,
  FileUploadException,
  FileNotFoundException,
  FileSizeExceededException,
} from '../../../core/exceptions/custom-exceptions';

export class ErrorHandler {
  private static readonly logger = new Logger(ErrorHandler.name);

  static createException(
    errorPath?: string,
    statusCode?: number,
    message?: string,
    code?: string,
    details?: any,
  ): any {
    if (errorPath?.includes('$throw')) {
      switch (errorPath) {
        // Named throws
        case '$throw.businessLogic':
          return new BusinessLogicException(message || 'Bad request', details);
        case '$throw.validation':
          return new ValidationException(message || 'Validation failed', details);
        case '$throw.notFound':
          return new ResourceNotFoundException(message || 'Resource');
        case '$throw.duplicate':
          return new DuplicateResourceException(message || 'Resource', 'field', 'value');
        case '$throw.unauthorized':
          return new AuthenticationException(message || 'Unauthorized');
        case '$throw.forbidden':
          return new AuthorizationException(message || 'Forbidden');
        case '$throw.tokenExpired':
          return new TokenExpiredException();
        case '$throw.invalidToken':
          return new InvalidTokenException();
        case '$throw.database':
          return new DatabaseException(message || 'Database error', details);
        case '$throw.dbQuery':
          return new DatabaseException(message || 'Query error', details);
        case '$throw.externalService':
          return new ExternalServiceException(message || 'Service', message || 'Error', details);
        case '$throw.serviceUnavailable':
          return new ServiceUnavailableException(message || 'Service');
        case '$throw.rateLimit':
          return new HttpException(message || 'Rate limit exceeded', HttpStatus.TOO_MANY_REQUESTS);
        case '$throw.scriptError':
          return new ScriptExecutionException(message || 'Script error', code, details);
        case '$throw.scriptTimeout':
          return new ScriptTimeoutException(details?.timeout || 5000, code);
        case '$throw.schema':
          return new SchemaException(message || 'Schema error', details);
        case '$throw.config':
          return new ConfigurationException(message || 'Configuration error', details);
        case '$throw.fileUpload':
          return new FileUploadException(message || 'File upload error', details);
        case '$throw.fileNotFound':
          return new FileNotFoundException(message || 'File not found');
        case '$throw.fileSizeExceeded':
          return new FileSizeExceededException(details?.maxSize || 'unknown', details?.actualSize || 'unknown');
        // Status code throws
        case '$throw.400':
          return new BusinessLogicException(message || 'Bad request');
        case '$throw.401':
          return new AuthenticationException(message || 'Authentication required');
        case '$throw.403':
          return new AuthorizationException(message || 'Insufficient permissions');
        case '$throw.404':
          return new ResourceNotFoundException(message || 'Resource');
        case '$throw.409':
          return new DuplicateResourceException(message || 'Resource', 'field', 'value');
        case '$throw.422':
          return new ValidationException(message || 'Validation failed');
        case '$throw.429':
          return new HttpException(message || 'Rate limit exceeded', HttpStatus.TOO_MANY_REQUESTS);
        case '$throw.500':
          return new DatabaseException(message || 'Internal server error', details);
        case '$throw.503':
          return new ServiceUnavailableException(message || 'Service');
        default:
          return new ScriptExecutionException(message || 'Unknown error', code);
      }
    }

    if (statusCode) {
      switch (statusCode) {
        case 400:
          return new BusinessLogicException(message || 'Bad request');
        case 401:
          return new AuthenticationException(message || 'Authentication required');
        case 403:
          return new AuthorizationException(message || 'Insufficient permissions');
        case 404:
          return new ResourceNotFoundException(message || 'Resource');
        case 409:
          return new DuplicateResourceException(message || 'Resource', 'field', 'value');
        case 422:
          return new ValidationException(message || 'Validation failed');
        case 429:
          return new HttpException(message || 'Rate limit exceeded', HttpStatus.TOO_MANY_REQUESTS);
        case 500:
          return new DatabaseException(message || 'Internal server error', details);
        case 503:
          return new ServiceUnavailableException(message || 'Service');
        default:
          return new ScriptExecutionException(
            message || 'Unknown error',
            code,
            details,
          );
      }
    }

    return new ScriptExecutionException(
      message || 'Unknown error',
      code,
      details,
    );
  }

  static logError(
    errorType: string,
    message: string,
    code: string,
    additionalData?: any,
  ): void {
    this.logger.error(errorType, {
      message,
      code: code.substring(0, 100),
      ...additionalData,
    });
  }

}
