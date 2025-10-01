import { Test, TestingModule } from '@nestjs/testing';
import { HandlerExecutorService } from '../../src/infrastructure/handler-executor/services/handler-executor.service';
import { ExecutorPoolService } from '../../src/infrastructure/handler-executor/services/executor-pool.service';
import { ScriptErrorFactory } from '../../src/shared/utils/script-error-factory';
import {
  BusinessLogicException,
  ValidationException,
  ResourceNotFoundException,
  AuthenticationException,
  AuthorizationException,
  DatabaseException,
  RateLimitExceededException,
} from '../../src/core/exceptions/custom-exceptions';

describe.skip('Script Context Exceptions Integration', () => {
  let handlerExecutor: HandlerExecutorService;
  let executorPool: jest.Mocked<ExecutorPoolService>;

  beforeEach(async () => {
    const mockPool = {
      acquire: jest.fn().mockResolvedValue({
        send: jest.fn(),
        on: jest.fn(),
        removeAllListeners: jest.fn(),
        kill: jest.fn(),
      }),
      release: jest.fn(),
      drain: jest.fn(),
      clear: jest.fn(),
    };

    const mockExecutorPool = {
      getPool: jest.fn().mockReturnValue(mockPool),
    };

    const module: TestingModule = await Test.createTestingModule({
      providers: [
        HandlerExecutorService,
        { provide: ExecutorPoolService, useValue: mockExecutorPool },
      ],
    }).compile();

    handlerExecutor = module.get<HandlerExecutorService>(
      HandlerExecutorService,
    );
    executorPool = module.get(ExecutorPoolService);
  });

  afterEach(async () => {
    // Clean up executor pool
    if (executorPool) {
      const pool = executorPool.getPool();
      await pool.drain();
      await pool.clear();
    }
  });

  describe('Business Logic Exceptions in Scripts', () => {
    it('should throw BusinessLogicException from script using $errors.businessLogic', async () => {
      const script = `
        return (async function($ctx) {
          $ctx.$errors.businessLogic('Cannot process order: insufficient stock');
        })($ctx);
      `;

      const context = {
        $throw: ScriptErrorFactory.createThrowHandlers(),
        $body: { orderId: 123 },
      };

      await expect(handlerExecutor.run(script, context as any)).rejects.toThrow(
        BusinessLogicException,
      );
    });

    it('should throw ValidationException with details from script', async () => {
      const script = `
        return (async function($ctx) {
          const errors = [];
          if (!$ctx.$body.email) {
            errors.push({ field: 'email', message: 'Email is required' });
          }
          if (!$ctx.$body.password) {
            errors.push({ field: 'password', message: 'Password is required' });
          }
          
          if (errors.length > 0) {
            $ctx.$errors.validation('Validation failed', { errors });
          }
          
          return { success: true };
        })($ctx);
      `;

      const context = {
        $throw: ScriptErrorFactory.createThrowHandlers(),
        $body: {}, // Missing email and password
      };

      try {
        await handlerExecutor.run(script, context as any);
        fail('Should have thrown ValidationException');
      } catch (error) {
        expect(error).toBeInstanceOf(ValidationException);
        expect(error.details).toEqual({
          errors: [
            { field: 'email', message: 'Email is required' },
            { field: 'password', message: 'Password is required' },
          ],
        });
      }
    });

    it('should throw ResourceNotFoundException from script', async () => {
      const script = `
        return (async function($ctx) {
          const userId = $ctx.$body.userId;
          // Simulate user not found
          if (userId === 'non-existent') {
            $ctx.$errors.notFound('User', userId);
          }
          return { userId };
        })($ctx);
      `;

      const context = {
        $throw: ScriptErrorFactory.createThrowHandlers(),
        $body: { userId: 'non-existent' },
      };

      await expect(handlerExecutor.run(script, context as any)).rejects.toThrow(
        ResourceNotFoundException,
      );
    });
  });

  describe('Authentication & Authorization Exceptions in Scripts', () => {
    it('should throw AuthenticationException when user is not authenticated', async () => {
      const script = `
        return (async function($ctx) {
          if (!$ctx.$user) {
            $ctx.$errors.unauthorized('Please login to continue');
          }
          return { success: true };
        })($ctx);
      `;

      const context = {
        $throw: ScriptErrorFactory.createThrowHandlers(),
        $user: null,
      };

      await expect(handlerExecutor.run(script, context as any)).rejects.toThrow(
        AuthenticationException,
      );
    });

    it('should throw AuthorizationException for insufficient permissions', async () => {
      const script = `
        return (async function($ctx) {
          if ($ctx.$user.role !== 'admin') {
            $ctx.$errors.forbidden('Only admins can perform this action');
          }
          return { success: true };
        })($ctx);
      `;

      const context = {
        $throw: ScriptErrorFactory.createThrowHandlers(),
        $user: { id: 1, role: 'user' },
      };

      try {
        await handlerExecutor.run(script, context as any);
        fail('Should have thrown AuthorizationException');
      } catch (error) {
        expect(error).toBeInstanceOf(AuthorizationException);
        expect(error.message).toBe('Only admins can perform this action');
      }
    });
  });

  describe('Complex Script Scenarios', () => {
    it('should handle multiple validation checks before throwing exception', async () => {
      const script = `
        return (async function($ctx) {
          const { email, age, country } = $ctx.$body;
          const errors = [];
          
          // Email validation
          if (!email || !email.includes('@')) {
            errors.push({ field: 'email', message: 'Invalid email format' });
          }
          
          // Age validation
          if (age < 18) {
            errors.push({ field: 'age', message: 'Must be 18 or older' });
          }
          
          // Country validation
          const allowedCountries = ['US', 'UK', 'CA'];
          if (!allowedCountries.includes(country)) {
            errors.push({ 
              field: 'country', 
              message: 'Service not available in your country',
              allowed: allowedCountries 
            });
          }
          
          if (errors.length > 0) {
            $ctx.$errors.validation('Registration validation failed', { 
              errors,
              submitted: $ctx.$body 
            });
          }
          
          return { registered: true };
        })($ctx);
      `;

      const context = {
        $throw: ScriptErrorFactory.createThrowHandlers(),
        $body: {
          email: 'invalid-email',
          age: 16,
          country: 'BR',
        },
      };

      try {
        await handlerExecutor.run(script, context as any);
        fail('Should have thrown ValidationException');
      } catch (error) {
        expect(error).toBeInstanceOf(ValidationException);
        expect(error.details.errors).toHaveLength(3);
        expect(error.details.submitted).toEqual(context.$body);
      }
    });

    it('should handle rate limiting check in script', async () => {
      const script = `
        return (async function($ctx) {
          const requestCount = $ctx.$requestCount || 101;
          const limit = 100;
          const window = '1 hour';
          
          if (requestCount > limit) {
            $ctx.$errors.rateLimit(limit, window);
          }
          
          return { allowed: true };
        })($ctx);
      `;

      const context = {
        $throw: ScriptErrorFactory.createThrowHandlers(),
        $requestCount: 101,
      };

      try {
        await handlerExecutor.run(script, context as any);
        fail('Should have thrown RateLimitExceededException');
      } catch (error) {
        expect(error).toBeInstanceOf(RateLimitExceededException);
        expect(error.message).toContain('100 requests per 1 hour');
      }
    });

    it('should handle database error in script', async () => {
      const script = `
        return (async function($ctx) {
          try {
            // Simulate database operation
            if ($ctx.$simulateDbError) {
              throw new Error('Connection timeout');
            }
            return { data: 'success' };
          } catch (dbError) {
            $ctx.$errors.database('Failed to save user data', {
              operation: 'insert',
              table: 'users',
              error: dbError.message
            });
          }
        })($ctx);
      `;

      const context = {
        $throw: ScriptErrorFactory.createThrowHandlers(),
        $simulateDbError: true,
      };

      try {
        await handlerExecutor.run(script, context as any);
        fail('Should have thrown DatabaseException');
      } catch (error) {
        expect(error).toBeInstanceOf(DatabaseException);
        expect(error.details).toEqual({
          operation: 'insert',
          table: 'users',
          error: 'Connection timeout',
        });
      }
    });
  });

  describe('Legacy HTTP Status Methods', () => {
    it('should support legacy throw400 method', async () => {
      const script = `
        return (async function($ctx) {
          if (!$ctx.$body.required_field) {
            $ctx.$errors.throw400('Missing required field');
          }
          return { success: true };
        })($ctx);
      `;

      const context = {
        $throw: ScriptErrorFactory.createThrowHandlers(),
        $body: {},
      };

      await expect(handlerExecutor.run(script, context as any)).rejects.toThrow(
        BusinessLogicException,
      );
    });

    it('should support legacy throw401 method', async () => {
      const script = `
        return (async function($ctx) {
          if (!$ctx.$token) {
            $ctx.$errors.throw401();
          }
          return { success: true };
        })($ctx);
      `;

      const context = {
        $throw: ScriptErrorFactory.createThrowHandlers(),
        $token: null,
      };

      await expect(handlerExecutor.run(script, context as any)).rejects.toThrow(
        AuthenticationException,
      );
    });

    it('should support legacy throw404 method', async () => {
      const script = `
        return (async function($ctx) {
          const resourceId = $ctx.$body.id;
          if (!resourceId || resourceId === 'not-found') {
            $ctx.$errors.throw404('Product', resourceId);
          }
          return { found: true };
        })($ctx);
      `;

      const context = {
        $throw: ScriptErrorFactory.createThrowHandlers(),
        $body: { id: 'not-found' },
      };

      await expect(handlerExecutor.run(script, context as any)).rejects.toThrow(
        ResourceNotFoundException,
      );
    });
  });

  describe('Script Timeout Handling', () => {
    it('should handle script timeout correctly', async () => {
      const script = `
        return (async function($ctx) {
          // Simulate long-running operation
          await new Promise(resolve => setTimeout(resolve, 1000));
          return { success: true };
        })($ctx);
      `;

      const context = {
        $throw: ScriptErrorFactory.createThrowHandlers(),
      };

      // Run with 100ms timeout
      await expect(
        handlerExecutor.run(script, context as any, 100),
      ).rejects.toThrow('Script execution timed out');
    });
  });

  describe('Error Builder Functions', () => {
    it('should allow building error objects without throwing', async () => {
      const script = `
        return (async function($ctx) {
          const errorBuilders = $ctx.$errorBuilders;
          
          // Build error object without throwing
          const error = errorBuilders.build('CUSTOM_ERROR', 'Something went wrong', {
            userId: $ctx.$user?.id,
            timestamp: Date.now()
          });
          
          // Check if it's an error
          if (errorBuilders.isError(error)) {
            return { 
              hasError: true, 
              error: error 
            };
          }
          
          return { hasError: false };
        })($ctx);
      `;

      const context = {
        $errorBuilders: ScriptErrorFactory.createErrorBuilders(),
        $user: { id: 123 },
      };

      const result = await handlerExecutor.run(script, context as any);

      expect(result.hasError).toBe(true);
      expect(result.error.code).toBe('CUSTOM_ERROR');
      expect(result.error.message).toBe('Something went wrong');
      expect(result.error.details.userId).toBe(123);
      expect(result.error.isError).toBe(true);
    });
  });
});
