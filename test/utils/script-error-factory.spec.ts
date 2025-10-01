import { ScriptErrorFactory } from '../../src/shared/utils/script-error-factory';
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
} from '../../src/core/exceptions/custom-exceptions';

describe('ScriptErrorFactory', () => {
  let errorHandlers: ReturnType<typeof ScriptErrorFactory.createThrowHandlers>;

  beforeEach(() => {
    errorHandlers = ScriptErrorFactory.createThrowHandlers();
  });

  describe('Business Logic Errors', () => {
    it('should throw BusinessLogicException with businessLogic method', () => {
      expect(() => errorHandlers.businessLogic('Invalid operation')).toThrow(
        BusinessLogicException,
      );

      expect(() =>
        errorHandlers.businessLogic('Invalid operation', { field: 'test' }),
      ).toThrow('Invalid operation');
    });

    it('should throw ValidationException with validation method', () => {
      expect(() => errorHandlers.validation('Invalid email')).toThrow(
        ValidationException,
      );

      try {
        errorHandlers.validation('Invalid email', { field: 'email' });
      } catch (error) {
        expect(error).toBeInstanceOf(ValidationException);
        expect(error.details).toEqual({ field: 'email' });
      }
    });

    it('should throw ResourceNotFoundException with notFound method', () => {
      expect(() => errorHandlers.notFound('User', '123')).toThrow(
        ResourceNotFoundException,
      );

      expect(() => errorHandlers.notFound('User')).toThrow('User not found');
    });

    it('should throw DuplicateResourceException with duplicate method', () => {
      expect(() =>
        errorHandlers.duplicate('User', 'email', 'test@example.com'),
      ).toThrow(DuplicateResourceException);

      expect(() =>
        errorHandlers.duplicate('User', 'email', 'test@example.com'),
      ).toThrow("User with email 'test@example.com' already exists");
    });
  });

  describe('Authentication & Authorization Errors', () => {
    it('should throw AuthenticationException with unauthorized method', () => {
      expect(() => errorHandlers.unauthorized()).toThrow(
        AuthenticationException,
      );

      expect(() => errorHandlers.unauthorized('Custom auth message')).toThrow(
        'Custom auth message',
      );
    });

    it('should throw AuthorizationException with forbidden method', () => {
      expect(() => errorHandlers.forbidden()).toThrow(AuthorizationException);

      expect(() => errorHandlers.forbidden('Admin only')).toThrow('Admin only');
    });

    it('should throw TokenExpiredException with tokenExpired method', () => {
      expect(() => errorHandlers.tokenExpired()).toThrow(TokenExpiredException);

      expect(() => errorHandlers.tokenExpired()).toThrow('Token has expired');
    });

    it('should throw InvalidTokenException with invalidToken method', () => {
      expect(() => errorHandlers.invalidToken()).toThrow(InvalidTokenException);

      expect(() => errorHandlers.invalidToken()).toThrow(
        'Invalid token provided',
      );
    });
  });

  describe('Database Errors', () => {
    it('should throw DatabaseException with database method', () => {
      expect(() => errorHandlers.database('Connection failed')).toThrow(
        DatabaseException,
      );

      try {
        errorHandlers.database('Transaction failed', { table: 'users' });
      } catch (error) {
        expect(error).toBeInstanceOf(DatabaseException);
        expect(error.details).toEqual({ table: 'users' });
      }
    });

    it('should throw DatabaseQueryException with dbQuery method', () => {
      expect(() => errorHandlers.dbQuery('Invalid SQL')).toThrow(
        DatabaseQueryException,
      );

      expect(() =>
        errorHandlers.dbQuery('Syntax error', { query: 'SELECT *' }),
      ).toThrow('Syntax error');
    });
  });

  describe('External Service Errors', () => {
    it('should throw ExternalServiceException with externalService method', () => {
      expect(() =>
        errorHandlers.externalService('PaymentAPI', 'Connection timeout'),
      ).toThrow(ExternalServiceException);

      expect(() =>
        errorHandlers.externalService('PaymentAPI', 'Failed'),
      ).toThrow("External service 'PaymentAPI' error: Failed");
    });

    it('should throw ServiceUnavailableException with serviceUnavailable method', () => {
      expect(() => errorHandlers.serviceUnavailable('EmailService')).toThrow(
        ServiceUnavailableException,
      );

      expect(() => errorHandlers.serviceUnavailable('EmailService')).toThrow(
        "Service 'EmailService' is currently unavailable",
      );
    });
  });

  describe('Rate Limiting Errors', () => {
    it('should throw RateLimitExceededException with rateLimit method', () => {
      expect(() => errorHandlers.rateLimit(100, '1 hour')).toThrow(
        RateLimitExceededException,
      );

      expect(() => errorHandlers.rateLimit(100, '1 hour')).toThrow(
        'Rate limit exceeded. Maximum 100 requests per 1 hour',
      );
    });
  });

  describe('Script Execution Errors', () => {
    it('should throw ScriptExecutionException with scriptError method', () => {
      expect(() => errorHandlers.scriptError('Runtime error')).toThrow(
        ScriptExecutionException,
      );

      try {
        errorHandlers.scriptError('Variable undefined', 'script_123', {
          line: 10,
        });
      } catch (error) {
        expect(error).toBeInstanceOf(ScriptExecutionException);
        expect(error.message).toContain('Variable undefined');
      }
    });

    it('should throw ScriptTimeoutException with scriptTimeout method', () => {
      expect(() => errorHandlers.scriptTimeout(5000)).toThrow(
        ScriptTimeoutException,
      );

      expect(() => errorHandlers.scriptTimeout(5000, 'script_123')).toThrow(
        'Script execution timed out after 5000ms',
      );
    });
  });

  describe('Schema & Configuration Errors', () => {
    it('should throw SchemaException with schema method', () => {
      expect(() => errorHandlers.schema('Invalid table')).toThrow(
        SchemaException,
      );

      expect(() =>
        errorHandlers.schema('Missing column', { table: 'users' }),
      ).toThrow('Missing column');
    });

    it('should throw ConfigurationException with config method', () => {
      expect(() => errorHandlers.config('Missing key')).toThrow(
        ConfigurationException,
      );

      expect(() => errorHandlers.config('Invalid value', 'API_KEY')).toThrow(
        'Configuration error: Invalid value',
      );
    });
  });

  describe('File Errors', () => {
    it('should throw FileUploadException with fileUpload method', () => {
      expect(() => errorHandlers.fileUpload('Invalid type')).toThrow(
        FileUploadException,
      );

      try {
        errorHandlers.fileUpload('Wrong format', { allowed: ['.jpg', '.png'] });
      } catch (error) {
        expect(error).toBeInstanceOf(FileUploadException);
        expect(error.details).toEqual({ allowed: ['.jpg', '.png'] });
      }
    });

    it('should throw FileNotFoundException with fileNotFound method', () => {
      expect(() => errorHandlers.fileNotFound('/path/to/file.txt')).toThrow(
        FileNotFoundException,
      );

      expect(() => errorHandlers.fileNotFound('/path/to/file.txt')).toThrow(
        'File not found: /path/to/file.txt',
      );
    });

    it('should throw FileSizeExceededException with fileSizeExceeded method', () => {
      expect(() => errorHandlers.fileSizeExceeded('10MB', '15MB')).toThrow(
        FileSizeExceededException,
      );

      expect(() => errorHandlers.fileSizeExceeded('10MB', '15MB')).toThrow(
        'File size exceeded. Maximum: 10MB, Actual: 15MB',
      );
    });
  });

  describe('Legacy HTTP Status Methods', () => {
    it('should throw appropriate exceptions for legacy throw methods', () => {
      expect(() => errorHandlers.throw400('Bad request')).toThrow(
        BusinessLogicException,
      );

      expect(() => errorHandlers.throw401()).toThrow(AuthenticationException);

      expect(() => errorHandlers.throw403()).toThrow(AuthorizationException);

      expect(() => errorHandlers.throw404('User', '123')).toThrow(
        ResourceNotFoundException,
      );

      expect(() =>
        errorHandlers.throw409('User', 'email', 'test@example.com'),
      ).toThrow(DuplicateResourceException);

      expect(() => errorHandlers.throw422('Invalid data')).toThrow(
        ValidationException,
      );

      expect(() => errorHandlers.throw429(100, '1 hour')).toThrow(
        RateLimitExceededException,
      );

      expect(() => errorHandlers.throw500('Server error')).toThrow(
        DatabaseException,
      );

      expect(() => errorHandlers.throw503('PaymentService')).toThrow(
        ServiceUnavailableException,
      );
    });

    it('should use default messages for legacy methods when not provided', () => {
      expect(() => errorHandlers.throw401()).toThrow('Unauthorized');

      expect(() => errorHandlers.throw403()).toThrow('Forbidden');
    });
  });

  describe('Error Builders', () => {
    let errorBuilders: ReturnType<
      typeof ScriptErrorFactory.createErrorBuilders
    >;

    beforeEach(() => {
      errorBuilders = ScriptErrorFactory.createErrorBuilders();
    });

    it('should build error objects without throwing', () => {
      const error = errorBuilders.build(
        'CUSTOM_ERROR',
        'Something went wrong',
        { id: 123 },
      );

      expect(error).toEqual({
        code: 'CUSTOM_ERROR',
        message: 'Something went wrong',
        details: { id: 123 },
        isError: true,
      });
    });

    it('should correctly identify error objects', () => {
      const error = errorBuilders.build('TEST_ERROR', 'Test message');
      const notError = { message: 'Not an error' };

      expect(errorBuilders.isError(error)).toBe(true);
      expect(errorBuilders.isError(notError)).toBe(false);
      expect(errorBuilders.isError(null)).toBe(false);
      expect(errorBuilders.isError(undefined)).toBe(false);
    });
  });

  describe('Error Handler Properties', () => {
    it('should have all expected error handler methods', () => {
      const expectedMethods = [
        'businessLogic',
        'validation',
        'notFound',
        'duplicate',
        'unauthorized',
        'forbidden',
        'tokenExpired',
        'invalidToken',
        'database',
        'dbQuery',
        'externalService',
        'serviceUnavailable',
        'rateLimit',
        'scriptError',
        'scriptTimeout',
        'schema',
        'config',
        'fileUpload',
        'fileNotFound',
        'fileSizeExceeded',
        '400',
        '401',
        '403',
        '404',
        '409',
        '422',
        '429',
        '500',
        '503',
      ];

      expectedMethods.forEach((method) => {
        expect(errorHandlers[method]).toBeDefined();
        expect(typeof errorHandlers[method]).toBe('function');
      });
    });
  });
});
