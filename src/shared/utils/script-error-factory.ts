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
} from '../../domain/exceptions';
export class ScriptErrorFactory {
  static createThrowHandlers() {
    return {
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
      database: (message: string, details?: any) => {
        throw new DatabaseException(message, details);
      },
      dbQuery: (message: string, details?: any) => {
        throw new DatabaseQueryException(message, details);
      },
      externalService: (service: string, message: string, details?: any) => {
        throw new ExternalServiceException(service, message, details);
      },
      serviceUnavailable: (service: string) => {
        throw new ServiceUnavailableException(service);
      },
      rateLimit: (limit: number, window: string) => {
        throw new RateLimitExceededException(limit, window);
      },
      scriptError: (message: string, scriptId?: string, details?: any) => {
        throw new ScriptExecutionException(message, scriptId, details);
      },
      scriptTimeout: (timeoutMs: number, scriptId?: string) => {
        throw new ScriptTimeoutException(timeoutMs, scriptId);
      },
      schema: (message: string, details?: any) => {
        throw new SchemaException(message, details);
      },
      config: (message: string, configKey?: string) => {
        throw new ConfigurationException(message, configKey);
      },
      fileUpload: (message: string, details?: any) => {
        throw new FileUploadException(message, details);
      },
      fileNotFound: (filePath: string) => {
        throw new FileNotFoundException(filePath);
      },
      fileSizeExceeded: (maxSize: string, actualSize: string) => {
        throw new FileSizeExceededException(maxSize, actualSize);
      },
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
  static createErrorBuilders() {
    return {
      build: (code: string, message: string, details?: any) => ({
        code,
        message,
        details,
        isError: true,
      }),
      isError: (obj: any) => obj?.isError === true,
    };
  }
}
