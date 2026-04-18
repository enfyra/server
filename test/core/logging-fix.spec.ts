jest.mock('../../src/shared/utils/winston-logger', () => ({
  winstonLogger: {
    info: jest.fn(),
    error: jest.fn(),
    warn: jest.fn(),
    debug: jest.fn(),
    verbose: jest.fn(),
  },
  shouldLog: jest.fn().mockReturnValue(true),
}));

import { AppLogger } from '../../src/shared/utils/app-logger';
import {
  winstonLogger,
  shouldLog,
} from '../../src/shared/utils/winston-logger';
import { GlobalExceptionFilter } from '../../src/core/exceptions/filters/global-exception.filter';
import { HttpException } from '../../src/core/exceptions/custom-exceptions';
import {
  BusinessLogicException,
  ScriptExecutionException,
  ScriptTimeoutException,
  ResourceNotFoundException,
  AuthorizationException,
} from '../../src/core/exceptions/custom-exceptions';

// Mock HttpStatus enum
enum HttpStatus {
  OK = 200,
  CREATED = 201,
  BAD_REQUEST = 400,
  UNAUTHORIZED = 401,
  FORBIDDEN = 403,
  NOT_FOUND = 404,
  CONFLICT = 409,
  UNPROCESSABLE_ENTITY = 422,
  INTERNAL_SERVER_ERROR = 500,
  BAD_GATEWAY = 502,
  SERVICE_UNAVAILABLE = 503,
}

// Mock ArgumentsHost interface
interface ArgumentsHost {
  switchToHttp(): {
    getRequest(): any;
    getResponse(): any;
  };
  getType(): string;
  getArgs(): any[];
  getArgByIndex(i: number): any;
  switchToRpc(): any;
  switchToWs(): any;
}

const mockedWinston = winstonLogger as jest.Mocked<typeof winstonLogger>;
const mockedShouldLog = shouldLog as jest.MockedFunction<typeof shouldLog>;

function createMockRequest(overrides: Partial<any> = {}): any {
  return {
    method: 'GET',
    url: '/api/test',
    headers: {},
    body: {},
    user: undefined,
    ...overrides,
  };
}

function createMockResponse(): any {
  const res: any = {};
  res.status = jest.fn().mockReturnValue(res);
  res.json = jest.fn().mockReturnValue(res);
  return res;
}

function createMockHost(req?: any, res?: any): ArgumentsHost {
  const request = req || createMockRequest();
  const response = res || createMockResponse();
  return {
    switchToHttp: () => ({
      getRequest: () => request,
      getResponse: () => response,
    }),
    getType: () => 'http',
    getArgs: () => [request, response],
    getArgByIndex: (i: number) => [request, response][i],
    switchToRpc: () => ({}) as any,
    switchToWs: () => ({}) as any,
  } as ArgumentsHost;
}

describe('AppLogger message deduplication', () => {
  let logger: AppLogger;

  beforeEach(() => {
    jest.clearAllMocks();
    mockedShouldLog.mockReturnValue(true);
    logger = new AppLogger();
  });

  describe('extractObjectMessage deduplication', () => {
    it('should extract message from object and not duplicate it in meta', () => {
      logger.log({ message: 'Client Error', correlationId: '123' }, 'TestCtx');

      expect(mockedWinston.info).toHaveBeenCalledTimes(1);
      const [msg, meta] = mockedWinston.info.mock.calls[0];
      expect(msg).toBe('Client Error');
      expect(meta).toEqual({ context: 'TestCtx', correlationId: '123' });
      expect(meta).not.toHaveProperty('message');
    });

    it('should extract message from error() call and not duplicate', () => {
      logger.error(
        {
          message: '[500] Database timeout',
          stack: 'Error at...',
          correlationId: 'req_1',
        },
        undefined,
        'DB',
      );

      expect(mockedWinston.error).toHaveBeenCalledTimes(1);
      const [msg, meta] = mockedWinston.error.mock.calls[0];
      expect(msg).toBe('[500] Database timeout');
      expect(meta).toEqual({
        context: 'DB',
        stack: 'Error at...',
        correlationId: 'req_1',
      });
      expect(meta).not.toHaveProperty('message');
    });

    it('should extract message from warn() call and not duplicate', () => {
      logger.warn(
        { message: '[404] Not Found', url: '/missing', method: 'GET' },
        'HttpWarn',
      );

      expect(mockedWinston.warn).toHaveBeenCalledTimes(1);
      const [msg, meta] = mockedWinston.warn.mock.calls[0];
      expect(msg).toBe('[404] Not Found');
      expect(meta).toEqual({
        context: 'HttpWarn',
        url: '/missing',
        method: 'GET',
      });
      expect(meta).not.toHaveProperty('message');
    });

    it('should extract message from debug() call and not duplicate', () => {
      logger.debug({ message: 'Cache hit', key: 'user:42', ttl: 300 }, 'Cache');

      expect(mockedWinston.debug).toHaveBeenCalledTimes(1);
      const [msg, meta] = mockedWinston.debug.mock.calls[0];
      expect(msg).toBe('Cache hit');
      expect(meta).toEqual({ context: 'Cache', key: 'user:42', ttl: 300 });
      expect(meta).not.toHaveProperty('message');
    });

    it('should extract message from verbose() call and not duplicate', () => {
      logger.verbose(
        { message: 'Query plan', tables: ['users', 'roles'] },
        'QEngine',
      );

      expect(mockedWinston.verbose).toHaveBeenCalledTimes(1);
      const [msg, meta] = mockedWinston.verbose.mock.calls[0];
      expect(msg).toBe('Query plan');
      expect(meta).toEqual({ context: 'QEngine', tables: ['users', 'roles'] });
      expect(meta).not.toHaveProperty('message');
    });

    it('should extract message from fatal() call and not duplicate', () => {
      logger.fatal(
        { message: 'OOM detected', heapUsed: 1800000000 },
        undefined,
        'System',
      );

      expect(mockedWinston.error).toHaveBeenCalledTimes(1);
      const [msg, meta] = mockedWinston.error.mock.calls[0];
      expect(msg).toBe('OOM detected');
      expect(meta).toEqual({
        context: 'System',
        fatal: true,
        heapUsed: 1800000000,
      });
      expect(meta).not.toHaveProperty('message');
    });
  });

  describe('fallback when message property is missing or falsy', () => {
    it('should use fallback "Log" when object has no message property', () => {
      logger.log({ correlationId: 'abc', statusCode: 200 }, 'Test');

      const [msg, meta] = mockedWinston.info.mock.calls[0];
      expect(msg).toBe('Log');
      expect(meta).toEqual({
        context: 'Test',
        correlationId: 'abc',
        statusCode: 200,
      });
    });

    it('should use fallback "Error" when error object has no message', () => {
      logger.error({ stack: 'trace here' }, undefined, 'Err');

      const [msg, meta] = mockedWinston.error.mock.calls[0];
      expect(msg).toBe('Error');
      expect(meta).toEqual({ context: 'Err', stack: 'trace here' });
    });

    it('should use fallback "Warning" for warn with empty string message', () => {
      logger.warn({ message: '', extra: true }, 'W');

      const [msg, meta] = mockedWinston.warn.mock.calls[0];
      expect(msg).toBe('Warning');
      expect(meta).toEqual({ context: 'W', extra: true });
    });

    it('should use fallback "Debug" for debug with undefined message', () => {
      logger.debug({ message: undefined, detail: 'x' }, 'D');

      const [msg, meta] = mockedWinston.debug.mock.calls[0];
      expect(msg).toBe('Debug');
      expect(meta).toEqual({ context: 'D', detail: 'x' });
    });

    it('should use fallback "Verbose" for verbose with null message', () => {
      logger.verbose({ message: null, info: 42 }, 'V');

      const [msg, meta] = mockedWinston.verbose.mock.calls[0];
      expect(msg).toBe('Verbose');
      expect(meta).toEqual({ context: 'V', info: 42 });
    });

    it('should use fallback "Fatal" for fatal with no message', () => {
      logger.fatal({ pid: 1234 }, undefined, 'F');

      const [msg, meta] = mockedWinston.error.mock.calls[0];
      expect(msg).toBe('Fatal');
      expect(meta).toEqual({ context: 'F', fatal: true, pid: 1234 });
    });
  });

  describe('string messages pass through unchanged', () => {
    it('should pass string message directly to info', () => {
      logger.log('Server started on port 1105', 'Main');

      expect(mockedWinston.info).toHaveBeenCalledWith(
        'Server started on port 1105',
        { context: 'Main' },
      );
    });

    it('should pass string message directly to error with trace', () => {
      logger.error('Connection refused', 'Error: ECONNREFUSED', 'DB');

      expect(mockedWinston.error).toHaveBeenCalledWith('Connection refused', {
        context: 'DB',
        trace: 'Error: ECONNREFUSED',
      });
    });

    it('should convert number to string', () => {
      logger.log(42, 'Ctx');

      expect(mockedWinston.info).toHaveBeenCalledWith('42', { context: 'Ctx' });
    });

    it('should convert boolean to string', () => {
      logger.warn(false, 'Ctx');

      expect(mockedWinston.warn).toHaveBeenCalledWith('false', {
        context: 'Ctx',
      });
    });

    it('should convert null to string', () => {
      logger.log(null, 'Ctx');

      expect(mockedWinston.info).toHaveBeenCalledWith('null', {
        context: 'Ctx',
      });
    });
  });

  describe('error() with object trace parameter', () => {
    it('should destructure trace object and strip its message key', () => {
      logger.error(
        'Something broke',
        { message: 'ignored', correlationId: 'r1', statusCode: 500 } as any,
        'Err',
      );

      expect(mockedWinston.error).toHaveBeenCalledTimes(1);
      const [msg, meta] = mockedWinston.error.mock.calls[0];
      expect(msg).toBe('Something broke');
      expect(meta).toEqual({
        context: 'Err',
        correlationId: 'r1',
        statusCode: 500,
      });
      expect(meta).not.toHaveProperty('message');
    });
  });

  describe('fatal() with object trace parameter', () => {
    it('should destructure trace object and strip its message key', () => {
      logger.fatal(
        'Process exit',
        { message: 'ignored', code: 137 } as any,
        'System',
      );

      expect(mockedWinston.error).toHaveBeenCalledTimes(1);
      const [msg, meta] = mockedWinston.error.mock.calls[0];
      expect(msg).toBe('Process exit');
      expect(meta).toEqual({ context: 'System', fatal: true, code: 137 });
      expect(meta).not.toHaveProperty('message');
    });
  });

  describe('shouldLog filtering', () => {
    it('should skip logging when shouldLog returns false', () => {
      mockedShouldLog.mockReturnValue(false);

      logger.log('should not appear', 'InstanceLoader');
      logger.error('should not appear', undefined, 'InstanceLoader');
      logger.warn('should not appear', 'InstanceLoader');
      logger.debug('should not appear', 'InstanceLoader');
      logger.verbose('should not appear', 'InstanceLoader');
      logger.fatal('should not appear', undefined, 'InstanceLoader');

      expect(mockedWinston.info).not.toHaveBeenCalled();
      expect(mockedWinston.error).not.toHaveBeenCalled();
      expect(mockedWinston.warn).not.toHaveBeenCalled();
      expect(mockedWinston.debug).not.toHaveBeenCalled();
      expect(mockedWinston.verbose).not.toHaveBeenCalled();
    });
  });

  describe('object with only message property', () => {
    it('should extract message and leave meta empty except context', () => {
      logger.log({ message: 'Just a message' }, 'Ctx');

      const [msg, meta] = mockedWinston.info.mock.calls[0];
      expect(msg).toBe('Just a message');
      expect(meta).toEqual({ context: 'Ctx' });
      expect(Object.keys(meta)).toEqual(['context']);
    });
  });

  describe('object with many metadata fields', () => {
    it('should preserve all non-message fields in meta', () => {
      logger.log(
        {
          message: 'Request handled',
          correlationId: 'req_abc',
          method: 'POST',
          url: '/api/users',
          statusCode: 201,
          duration: 42,
          userId: 'u_123',
        },
        'HTTP',
      );

      const [msg, meta] = mockedWinston.info.mock.calls[0];
      expect(msg).toBe('Request handled');
      expect(meta).toEqual({
        context: 'HTTP',
        correlationId: 'req_abc',
        method: 'POST',
        url: '/api/users',
        statusCode: 201,
        duration: 42,
        userId: 'u_123',
      });
      expect(meta).not.toHaveProperty('message');
    });
  });
});

describe('GlobalExceptionFilter meaningful log messages', () => {
  let filter: GlobalExceptionFilter;
  let mockResponse: any;
  let mockRequest: any;
  let loggerErrorSpy: jest.SpyInstance;
  let loggerWarnSpy: jest.SpyInstance;
  let loggerLogSpy: jest.SpyInstance;

  beforeEach(() => {
    jest.clearAllMocks();
    mockedShouldLog.mockReturnValue(true);
    filter = new GlobalExceptionFilter();
    mockResponse = createMockResponse();
    mockRequest = createMockRequest({
      headers: { 'x-correlation-id': 'test-corr-1' },
    });

    loggerErrorSpy = jest
      .spyOn((filter as any).logger, 'error')
      .mockImplementation(() => {});
    loggerWarnSpy = jest
      .spyOn((filter as any).logger, 'warn')
      .mockImplementation(() => {});
    loggerLogSpy = jest
      .spyOn((filter as any).logger, 'log')
      .mockImplementation(() => {});
  });

  afterEach(() => {
    loggerErrorSpy.mockRestore();
    loggerWarnSpy.mockRestore();
    loggerLogSpy.mockRestore();
  });

  describe('logs actual error message with status code prefix', () => {
    it('should log [500] with actual error message for server errors', () => {
      const error = new HttpException(
        'Database connection lost',
        HttpStatus.INTERNAL_SERVER_ERROR,
      );
      const host = createMockHost(mockRequest, mockResponse);

      filter.catch(error, host);

      expect(loggerErrorSpy).toHaveBeenCalledTimes(1);
      const logArg = loggerErrorSpy.mock.calls[0][0];
      expect(logArg.message).toBe('[500] Database connection lost');
      expect(logArg.correlationId).toBe('test-corr-1');
      expect(logArg.statusCode).toBe(500);
    });

    it('should log [404] with actual error message for not found', () => {
      const error = new HttpException('User not found', HttpStatus.NOT_FOUND);
      const host = createMockHost(mockRequest, mockResponse);

      filter.catch(error, host);

      expect(loggerWarnSpy).toHaveBeenCalledTimes(1);
      const logArg = loggerWarnSpy.mock.calls[0][0];
      expect(logArg.message).toBe('[404] User not found');
      expect(logArg.statusCode).toBe(404);
    });

    it('should log [400] with actual error message for bad request', () => {
      const error = new HttpException(
        'Invalid email format',
        HttpStatus.BAD_REQUEST,
      );
      const host = createMockHost(mockRequest, mockResponse);

      filter.catch(error, host);

      expect(loggerWarnSpy).toHaveBeenCalledTimes(1);
      const logArg = loggerWarnSpy.mock.calls[0][0];
      expect(logArg.message).toBe('[400] Invalid email format');
    });

    it('should log [401] with actual error message for unauthorized', () => {
      const error = new HttpException('Token expired', HttpStatus.UNAUTHORIZED);
      const host = createMockHost(mockRequest, mockResponse);

      filter.catch(error, host);

      expect(loggerWarnSpy).toHaveBeenCalledTimes(1);
      const logArg = loggerWarnSpy.mock.calls[0][0];
      expect(logArg.message).toBe('[401] Token expired');
    });

    it('should log [403] with actual error message for forbidden', () => {
      const error = new AuthorizationException('Cannot delete admin users');
      const host = createMockHost(mockRequest, mockResponse);

      filter.catch(error, host);

      expect(loggerWarnSpy).toHaveBeenCalledTimes(1);
      const logArg = loggerWarnSpy.mock.calls[0][0];
      expect(logArg.message).toBe('[403] Cannot delete admin users');
    });
  });

  describe('logs custom exception messages correctly', () => {
    it('should log BusinessLogicException message', () => {
      const error = new BusinessLogicException(
        'Order cannot be cancelled after shipment',
      );
      const host = createMockHost(mockRequest, mockResponse);

      filter.catch(error, host);

      expect(loggerWarnSpy).toHaveBeenCalledTimes(1);
      const logArg = loggerWarnSpy.mock.calls[0][0];
      expect(logArg.message).toBe(
        '[400] Order cannot be cancelled after shipment',
      );
    });

    it('should log ResourceNotFoundException message', () => {
      const error = new ResourceNotFoundException('Product', 'prod_999');
      const host = createMockHost(mockRequest, mockResponse);

      filter.catch(error, host);

      expect(loggerWarnSpy).toHaveBeenCalledTimes(1);
      const logArg = loggerWarnSpy.mock.calls[0][0];
      expect(logArg.message).toContain('[404]');
      expect(logArg.message).toContain('prod_999');
    });
  });

  describe('skips logging for script execution errors', () => {
    it('should not log ScriptExecutionException', () => {
      const error = new ScriptExecutionException(
        'ReferenceError: x is not defined',
        'hook_1',
      );
      const host = createMockHost(mockRequest, mockResponse);

      filter.catch(error, host);

      expect(loggerErrorSpy).not.toHaveBeenCalled();
      expect(loggerWarnSpy).not.toHaveBeenCalled();
      expect(loggerLogSpy).not.toHaveBeenCalled();
    });

    it('should not log ScriptTimeoutException', () => {
      const error = new ScriptTimeoutException(5000, 'handler_42');
      const host = createMockHost(mockRequest, mockResponse);

      filter.catch(error, host);

      expect(loggerErrorSpy).not.toHaveBeenCalled();
      expect(loggerWarnSpy).not.toHaveBeenCalled();
      expect(loggerLogSpy).not.toHaveBeenCalled();
    });
  });

  describe('plain Error objects', () => {
    it('should log plain Error message with [500] prefix', () => {
      const error = new Error('Unexpected null pointer');
      const host = createMockHost(mockRequest, mockResponse);

      filter.catch(error, host);

      expect(loggerErrorSpy).toHaveBeenCalledTimes(1);
      const logArg = loggerErrorSpy.mock.calls[0][0];
      expect(logArg.message).toBe('[500] Unexpected null pointer');
      expect(logArg.errorName).toBe('Error');
      expect(logArg.stack).toBeDefined();
    });

    it('should log TypeError with its actual name', () => {
      const error = new TypeError('Cannot read properties of undefined');
      const host = createMockHost(mockRequest, mockResponse);

      filter.catch(error, host);

      const logArg = loggerErrorSpy.mock.calls[0][0];
      expect(logArg.errorName).toBe('TypeError');
      expect(logArg.message).toBe('[500] Cannot read properties of undefined');
    });
  });

  describe('non-Error exceptions', () => {
    it('should handle string exception', () => {
      const host = createMockHost(mockRequest, mockResponse);

      filter.catch('something went wrong', host);

      expect(loggerErrorSpy).toHaveBeenCalledTimes(1);
      const logArg = loggerErrorSpy.mock.calls[0][0];
      expect(logArg.message).toBe('[500] something went wrong');
      expect(logArg.errorName).toBe('UnknownError');
    });

    it('should handle null exception', () => {
      const host = createMockHost(mockRequest, mockResponse);

      filter.catch(null, host);

      expect(loggerErrorSpy).toHaveBeenCalledTimes(1);
      const logArg = loggerErrorSpy.mock.calls[0][0];
      expect(logArg.message).toBe('[500] null');
    });

    it('should handle undefined exception', () => {
      const host = createMockHost(mockRequest, mockResponse);

      filter.catch(undefined, host);

      expect(loggerErrorSpy).toHaveBeenCalledTimes(1);
      const logArg = loggerErrorSpy.mock.calls[0][0];
      expect(logArg.message).toBe('[500] undefined');
    });
  });

  describe('correlation ID handling', () => {
    it('should use x-correlation-id from request headers', () => {
      const req = createMockRequest({
        headers: { 'x-correlation-id': 'custom-id-42' },
      });
      const error = new HttpException('Bad gateway', HttpStatus.BAD_GATEWAY);
      const host = createMockHost(req, mockResponse);

      filter.catch(error, host);

      expect(loggerErrorSpy).toHaveBeenCalledTimes(1);
      const logArg = loggerErrorSpy.mock.calls[0][0];
      expect(logArg.correlationId).toBe('custom-id-42');
    });

    it('should generate correlation ID when header is missing', () => {
      const req = createMockRequest({ headers: {} });
      const error = new HttpException('Not found', HttpStatus.NOT_FOUND);
      const host = createMockHost(req, mockResponse);

      filter.catch(error, host);

      expect(loggerWarnSpy).toHaveBeenCalledTimes(1);
      const logArg = loggerWarnSpy.mock.calls[0][0];
      expect(logArg.correlationId).toMatch(/^req_/);
    });
  });

  describe('log metadata includes request context', () => {
    it('should include method, url, userId in log data', () => {
      const req = createMockRequest({
        method: 'POST',
        url: '/api/orders',
        user: { id: 'usr_55' },
        headers: { 'x-correlation-id': 'c1' },
      });
      const error = new HttpException(
        'Validation failed',
        HttpStatus.UNPROCESSABLE_ENTITY,
      );
      const host = createMockHost(req, mockResponse);

      filter.catch(error, host);

      expect(loggerWarnSpy).toHaveBeenCalledTimes(1);
      const logArg = loggerWarnSpy.mock.calls[0][0];
      expect(logArg.method).toBe('POST');
      expect(logArg.url).toBe('/api/orders');
      expect(logArg.userId).toBe('usr_55');
      expect(logArg.statusCode).toBe(422);
    });
  });

  describe('log level selection based on status code', () => {
    it('should use logger.error for 5xx', () => {
      const error = new HttpException(
        'Internal',
        HttpStatus.INTERNAL_SERVER_ERROR,
      );
      filter.catch(error, createMockHost(mockRequest, mockResponse));

      expect(loggerErrorSpy).toHaveBeenCalledTimes(1);
      expect(loggerWarnSpy).not.toHaveBeenCalled();
    });

    it('should use logger.warn for 4xx', () => {
      const error = new HttpException('Bad Request', HttpStatus.BAD_REQUEST);
      filter.catch(error, createMockHost(mockRequest, mockResponse));

      expect(loggerWarnSpy).toHaveBeenCalledTimes(1);
      expect(loggerErrorSpy).not.toHaveBeenCalled();
    });

    it('should use logger.warn for 401', () => {
      const error = new HttpException('Unauthorized', HttpStatus.UNAUTHORIZED);
      filter.catch(error, createMockHost(mockRequest, mockResponse));

      expect(loggerWarnSpy).toHaveBeenCalledTimes(1);
      expect(loggerErrorSpy).not.toHaveBeenCalled();
    });

    it('should use logger.error for 502', () => {
      const error = new HttpException('Bad Gateway', HttpStatus.BAD_GATEWAY);
      filter.catch(error, createMockHost(mockRequest, mockResponse));

      expect(loggerErrorSpy).toHaveBeenCalledTimes(1);
      expect(loggerWarnSpy).not.toHaveBeenCalled();
    });

    it('should use logger.error for 503', () => {
      const error = new HttpException(
        'Service Unavailable',
        HttpStatus.SERVICE_UNAVAILABLE,
      );
      filter.catch(error, createMockHost(mockRequest, mockResponse));

      expect(loggerErrorSpy).toHaveBeenCalledTimes(1);
      expect(loggerWarnSpy).not.toHaveBeenCalled();
    });
  });

  describe('response body correctness', () => {
    it('should return structured error response with actual message', () => {
      const error = new HttpException(
        'Email already taken',
        HttpStatus.CONFLICT,
      );
      filter.catch(error, createMockHost(mockRequest, mockResponse));

      expect(mockResponse.status).toHaveBeenCalledWith(409);
      const body = mockResponse.json.mock.calls[0][0];
      expect(body.success).toBe(false);
      expect(body.message).toBe('Email already taken');
      expect(body.statusCode).toBe(409);
      expect(body.error.message).toBe('Email already taken');
      expect(body.error.code).toBeDefined();
      expect(body.error.path).toBe('/api/test');
      expect(body.error.method).toBe('GET');
    });

    it('should include logs array from exception when present', () => {
      const error: any = new HttpException(
        'Handler failed',
        HttpStatus.BAD_REQUEST,
      );
      error.logs = ['step 1 ok', 'step 2 failed'];
      filter.catch(error, createMockHost(mockRequest, mockResponse));

      const body = mockResponse.json.mock.calls[0][0];
      expect(body.logs).toEqual(['step 1 ok', 'step 2 failed']);
    });

    it('should not include logs key when exception has no logs', () => {
      const error = new HttpException('Simple error', HttpStatus.BAD_REQUEST);
      filter.catch(error, createMockHost(mockRequest, mockResponse));

      const body = mockResponse.json.mock.calls[0][0];
      expect(body).not.toHaveProperty('logs');
    });
  });

  describe('HttpException with object response', () => {
    it('should extract message from response object', () => {
      const error = new HttpException(
        {
          message: 'Validation failed',
          details: [{ field: 'email', issue: 'required' }],
        },
        HttpStatus.BAD_REQUEST,
      );
      filter.catch(error, createMockHost(mockRequest, mockResponse));

      const body = mockResponse.json.mock.calls[0][0];
      expect(body.message).toBe('Validation failed');

      expect(loggerWarnSpy).toHaveBeenCalledTimes(1);
      const logArg = loggerWarnSpy.mock.calls[0][0];
      expect(logArg.message).toBe('[400] Validation failed');
    });

    it('should extract array message from NestJS validation pipe', () => {
      const error = new HttpException(
        {
          message: ['email must be an email', 'name should not be empty'],
          error: 'Bad Request',
          statusCode: 400,
        },
        HttpStatus.BAD_REQUEST,
      );
      filter.catch(error, createMockHost(mockRequest, mockResponse));

      const body = mockResponse.json.mock.calls[0][0];
      expect(body.message).toEqual([
        'email must be an email',
        'name should not be empty',
      ]);
    });
  });
});
