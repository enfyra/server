import {
  BusinessLogicException,
  ValidationException,
  ResourceNotFoundException,
  DuplicateResourceException,
  AuthenticationException,
  AuthorizationException,
  TokenExpiredException,
  InvalidTokenException,
  DatabaseException,
  DatabaseQueryException,
  ExternalServiceException,
  ServiceUnavailableException,
  RateLimitExceededException,
  ScriptExecutionException,
  ScriptTimeoutException,
  SchemaException,
  ConfigurationException,
  FileUploadException,
  FileNotFoundException,
  FileSizeExceededException,
} from '../../core/exceptions/custom-exceptions';

/**
 * Factory to create error handlers for script execution context
 * These functions will be available in $throw object within scripts
 */
export class ScriptErrorFactory {
  /**
   * Create error handlers that can be used in script context
   * These will throw the actual custom exceptions when called
   */
  static createThrowHandlers() {
    return {
      // Business Logic Errors
      businessLogic: (message: string, details?: any) => {
        throw new BusinessLogicException(message, details);
      },

      validation: (message: string, details?: any) => {
        throw new ValidationException(message, details);
      },

      notFound: (resource: string, identifier?: string) => {
        throw new ResourceNotFoundException(resource, identifier);
      },

      duplicate: (resource: string, field: string, value: string) => {
        throw new DuplicateResourceException(resource, field, value);
      },

      // Authentication & Authorization
      unauthorized: (message?: string) => {
        throw new AuthenticationException(message || 'Unauthorized');
      },

      forbidden: (message?: string) => {
        throw new AuthorizationException(message || 'Forbidden');
      },

      tokenExpired: () => {
        throw new TokenExpiredException();
      },

      invalidToken: () => {
        throw new InvalidTokenException();
      },

      // Database Errors
      database: (message: string, details?: any) => {
        throw new DatabaseException(message, details);
      },

      dbQuery: (message: string, details?: any) => {
        throw new DatabaseQueryException(message, details);
      },

      // External Service Errors
      externalService: (service: string, message: string, details?: any) => {
        throw new ExternalServiceException(service, message, details);
      },

      serviceUnavailable: (service: string) => {
        throw new ServiceUnavailableException(service);
      },

      // Rate Limiting
      rateLimit: (limit: number, window: string) => {
        throw new RateLimitExceededException(limit, window);
      },

      // Script Errors
      scriptError: (message: string, scriptId?: string, details?: any) => {
        throw new ScriptExecutionException(message, scriptId, details);
      },

      scriptTimeout: (timeoutMs: number, scriptId?: string) => {
        throw new ScriptTimeoutException(timeoutMs, scriptId);
      },

      // Schema & Configuration
      schema: (message: string, details?: any) => {
        throw new SchemaException(message, details);
      },

      config: (message: string, configKey?: string) => {
        throw new ConfigurationException(message, configKey);
      },

      // File Errors
      fileUpload: (message: string, details?: any) => {
        throw new FileUploadException(message, details);
      },

      fileNotFound: (filePath: string) => {
        throw new FileNotFoundException(filePath);
      },

      fileSizeExceeded: (maxSize: string, actualSize: string) => {
        throw new FileSizeExceededException(maxSize, actualSize);
      },

      // Numeric throw methods for HTTP status codes
      '400': (message: string) => {
        throw new BusinessLogicException(message || 'Bad request');
      },

      '401': (message?: string) => {
        throw new AuthenticationException(message || 'Unauthorized');
      },

      '403': (message?: string) => {
        throw new AuthorizationException(message || 'Forbidden');
      },

      '404': (resource: string, id?: string) => {
        throw new ResourceNotFoundException(resource, id);
      },

      '409': (resource: string, field: string, value: string) => {
        throw new DuplicateResourceException(resource, field, value);
      },

      '422': (message: string, details?: any) => {
        throw new ValidationException(message, details);
      },

      '429': (limit: number, window: string) => {
        throw new RateLimitExceededException(limit, window);
      },

      '500': (message: string, details?: any) => {
        throw new DatabaseException(message, details);
      },

      '503': (service: string) => {
        throw new ServiceUnavailableException(service);
      },
    };
  }

  /**
   * Create a simplified error object for script context
   * This version doesn't throw but returns error objects
   */
  static createErrorBuilders() {
    return {
      // Create error object without throwing
      build: (code: string, message: string, details?: any) => ({
        code,
        message,
        details,
        isError: true,
      }),

      // Check if object is an error
      isError: (obj: any) => obj?.isError === true,
    };
  }
}
