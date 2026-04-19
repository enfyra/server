import { vi } from 'vitest';

vi.mock('../../src/shared/utils/winston-logger', () => ({
  winstonLogger: {
    info: vi.fn(),
    error: vi.fn(),
    warn: vi.fn(),
    debug: vi.fn(),
    verbose: vi.fn(),
  },
  shouldLog: vi.fn().mockReturnValue(true),
}));

import { AppLogger } from '../../src/shared/utils/app-logger';
import {
  winstonLogger,
  shouldLog,
} from '../../src/shared/utils/winston-logger';

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
