import { Test, TestingModule } from '@nestjs/testing';
import { HandlerExecutorService } from '../../../src/infrastructure/handler-executor/services/handler-executor.service';
import { ExecutorPoolService } from '../../../src/infrastructure/handler-executor/services/executor-pool.service';
import { TDynamicContext } from '../../../src/shared/utils/types/dynamic-context.type';
import { smartMergeContext } from '../../../src/infrastructure/handler-executor/utils/smart-merge';

describe('HandlerExecutorService - Smart Merge', () => {
  let service: HandlerExecutorService;
  let executorPoolService: ExecutorPoolService;

  beforeEach(async () => {
    const module: TestingModule = await Test.createTestingModule({
      providers: [
        HandlerExecutorService,
        {
          provide: ExecutorPoolService,
          useValue: {
            getPool: jest.fn().mockReturnValue({
              acquire: jest.fn().mockResolvedValue({
                on: jest.fn(),
                once: jest.fn(),
                send: jest.fn(),
                removeAllListeners: jest.fn(),
                kill: jest.fn(),
              }),
              release: jest.fn(),
            }),
          },
        },
      ],
    }).compile();

    service = module.get<HandlerExecutorService>(HandlerExecutorService);
    executorPoolService = module.get<ExecutorPoolService>(ExecutorPoolService);
  });

  describe('smartMergeContext', () => {
    let originalCtx: TDynamicContext;

    beforeEach(() => {
      originalCtx = {
        $repos: { main: { find: jest.fn() } },
        $body: { name: 'John', age: 30 },
        $query: { filter: { status: 'active' } },
        $params: { id: '123' },
        $user: { id: 'user123', name: 'John' },
        $logs: jest.fn(),
        $helpers: {
          $jwt: jest.fn(),
          $bcrypt: { hash: jest.fn(), compare: jest.fn() },
        },
        $req: {} as any,
        $errors: {},
        $share: { logs: [] },
        $data: { processed: false },
        $result: null,
        $statusCode: 200,
      };
    });

    it('should merge simple objects correctly', () => {
      const childCtx = {
        $body: { newField: 'new value', age: 35 },
        $query: { newParam: 'value' },
        $share: { customData: { key: 'value' } },
        $data: { processed: true },
        $params: { action: 'edit' },
      };

      const result = smartMergeContext(originalCtx, childCtx);

      // Should merge $body (combine both objects)
      expect(result.$body).toEqual({
        name: 'John',
        age: 35,
        newField: 'new value',
      });

      // Should merge $query (combine both objects)
      expect(result.$query).toEqual({
        filter: { status: 'active' },
        newParam: 'value',
      });

      // Should merge $share (combine both objects)
      expect(result.$share).toEqual({
        logs: [],
        customData: { key: 'value' },
      });

      // Should merge $data (combine both objects)
      expect(result.$data).toEqual({
        processed: true,
      });

      // Should merge $params (combine both objects)
      expect(result.$params).toEqual({
        id: '123',
        action: 'edit',
      });
    });

    it('should NOT merge non-mergeable properties', () => {
      const childCtx = {
        $repos: { newRepo: { find: jest.fn() } },
        $logs: jest.fn(),
        $helpers: { newHelper: jest.fn() },
        $user: { newField: 'value' },
        $req: { newField: 'value' },
        $errors: { newError: 'value' },
        $result: 'new result',
        $statusCode: 201,
      };

      const result = smartMergeContext(originalCtx, childCtx);

      // Should NOT merge non-mergeable properties
      expect(result.$repos).toBe(originalCtx.$repos);
      expect(result.$logs).toBe(originalCtx.$logs);
      expect(result.$helpers).toBe(originalCtx.$helpers);
      expect(result.$user).toBe(originalCtx.$user);
      expect(result.$req).toBe(originalCtx.$req);
      expect(result.$errors).toBe(originalCtx.$errors);
      // Should merge primitives like $result and $statusCode
      expect(result.$result).toBe('new result');
      expect(result.$statusCode).toBe(201);
    });

    it('should NOT merge arrays', () => {
      const childCtx = {
        $body: ['item1', 'item2'],
        $query: { items: ['a', 'b', 'c'] },
      };

      const result = smartMergeContext(originalCtx, childCtx);

      // Should NOT merge arrays (direct arrays)
      expect(result.$body).toBe(originalCtx.$body);
      // Should NOT merge objects containing arrays
      expect(result.$query).toBe(originalCtx.$query);
    });

    it('should NOT merge dates', () => {
      const childCtx = {
        $body: { createdAt: new Date('2024-01-01') },
        $data: { updatedAt: new Date('2024-01-02') },
      };

      const result = smartMergeContext(originalCtx, childCtx);

      // Should NOT merge objects containing dates
      expect(result.$body).toBe(originalCtx.$body);
      expect(result.$data).toBe(originalCtx.$data);
    });

    it('should NOT merge null or undefined', () => {
      const childCtx = {
        $body: null,
        $query: undefined,
        $share: null,
      };

      const result = smartMergeContext(originalCtx, childCtx);

      // Should NOT merge null/undefined
      expect(result.$body).toBe(originalCtx.$body);
      expect(result.$query).toBe(originalCtx.$query);
      expect(result.$share).toBe(originalCtx.$share);
    });

    it('should handle nested objects with functions', () => {
      const childCtx = {
        $body: {
          user: {
            name: 'Jane',
            helper: jest.fn(), // Function in nested object
          },
        },
        $share: {
          data: {
            processed: true,
            callback: jest.fn(), // Function in nested object
          },
        },
      };

      const result = smartMergeContext(originalCtx, childCtx);

      // Should NOT merge objects containing functions
      expect(result.$body).toBe(originalCtx.$body);
      expect(result.$share).toBe(originalCtx.$share);
    });

    it('should merge only mergeable properties', () => {
      const childCtx = {
        $body: { newField: 'value' },
        $query: { newParam: 'value' },
        $params: { newId: '456' },
        $share: { newData: 'value' },
        $data: { newStatus: 'completed' },
        // Non-mergeable properties
        $repos: { newRepo: {} },
        $logs: jest.fn(),
        $helpers: { newHelper: 'value' },
        $user: { newField: 'value' },
        $req: { newField: 'value' },
        $errors: { newError: 'value' },
        $result: 'new result',
        $statusCode: 200,
      };

      const result = smartMergeContext(originalCtx, childCtx);

      // Should merge mergeable properties
      expect(result.$body).toEqual({
        name: 'John',
        age: 30,
        newField: 'value',
      });
      expect(result.$query).toEqual({
        filter: { status: 'active' },
        newParam: 'value',
      });
      expect(result.$params).toEqual({
        id: '123',
        newId: '456',
      });
      expect(result.$share).toEqual({
        logs: [],
        newData: 'value',
      });
      expect(result.$data).toEqual({
        processed: false,
        newStatus: 'completed',
      });

      // Should NOT merge non-mergeable properties
      expect(result.$repos).toBe(originalCtx.$repos);
      expect(result.$logs).toBe(originalCtx.$logs);
      expect(result.$helpers).toBe(originalCtx.$helpers);
      expect(result.$user).toBe(originalCtx.$user);
      expect(result.$req).toBe(originalCtx.$req);
      expect(result.$errors).toBe(originalCtx.$errors);
      // Should merge primitives like $result and $statusCode
      expect(result.$result).toBe('new result');
      expect(result.$statusCode).toBe(200);
    });

    it('should handle empty child context', () => {
      const childCtx = {};

      const result = smartMergeContext(originalCtx, childCtx);

      // Should return original context unchanged
      expect(result).toEqual(originalCtx);
    });

    it('should handle child context with only non-mergeable properties', () => {
      const childCtx = {
        $repos: { newRepo: {} },
        $logs: jest.fn(),
        $helpers: { newHelper: 'value' },
      };

      const result = smartMergeContext(originalCtx, childCtx);

      // Should return original context unchanged
      expect(result).toEqual(originalCtx);
    });

    it('should merge new properties not in original context', () => {
      const childCtx = {
        $newProperty: { key: 'value' },
        $customData: { processed: true },
      };

      const result = smartMergeContext(originalCtx, childCtx);

      // Should merge new properties
      expect((result as any).$newProperty).toEqual({ key: 'value' });
      expect((result as any).$customData).toEqual({ processed: true });
    });

    it('should merge primitive values correctly', () => {
      const childCtx = {
        $statusCode: 201,
        $result: 'success',
        $newString: 'test string',
        $newNumber: 42,
        $newBoolean: true,
      };

      const result = smartMergeContext(originalCtx, childCtx);

      // Should merge primitive values directly
      expect(result.$statusCode).toBe(201);
      expect(result.$result).toBe('success');
      expect((result as any).$newString).toBe('test string');
      expect((result as any).$newNumber).toBe(42);
      expect((result as any).$newBoolean).toBe(true);
    });
  });

  describe('Integration Tests', () => {
    it('should handle complex merge scenarios', () => {
      const originalCtx: TDynamicContext = {
        $repos: { main: { find: jest.fn() } },
        $body: { name: 'John', age: 30, settings: { theme: 'dark' } },
        $query: { filter: { status: 'active' }, page: 1 },
        $params: { id: '123' },
        $user: { id: 'user123', name: 'John' },
        $logs: jest.fn(),
        $helpers: { $jwt: jest.fn(), $bcrypt: { hash: jest.fn() } },
        $req: {} as any,
        $errors: {},
        $share: { logs: [], session: { id: 'session123' } },
        $data: { processed: false, metadata: { version: '1.0' } },
        $result: null,
        $statusCode: 200,
      };

      const childCtx = {
        // Mergeable properties
        $body: { age: 35, newField: 'value', settings: { language: 'en' } },
        $query: { limit: 10, sort: 'name' },
        $params: { action: 'edit' },
        $share: {
          customData: { key: 'value' },
          session: { lastAccess: '2024-01-01' },
        },
        $data: { processed: true, metadata: { updated: true } },
        $newProperty: { key: 'value' },
        $customData: { processed: true },

        // Non-mergeable properties
        $repos: { newRepo: { find: jest.fn() } },
        $logs: jest.fn(),
        $helpers: { newHelper: 'value' },
        $user: { newField: 'value' },
        $req: { newField: 'value' },
        $errors: { newError: 'value' },
        $result: 'new result',
        $statusCode: 201,
      };

      const result = smartMergeContext(originalCtx, childCtx);

      // Should merge mergeable properties correctly
      expect(result.$body).toEqual({
        name: 'John',
        age: 35,
        newField: 'value',
        settings: { theme: 'dark', language: 'en' },
      });

      expect(result.$query).toEqual({
        filter: { status: 'active' },
        page: 1,
        limit: 10,
        sort: 'name',
      });

      expect(result.$params).toEqual({
        id: '123',
        action: 'edit',
      });

      expect(result.$share).toEqual({
        logs: [],
        session: { id: 'session123', lastAccess: '2024-01-01' },
        customData: { key: 'value' },
      });

      expect(result.$data).toEqual({
        processed: true,
        metadata: { version: '1.0', updated: true },
      });

      // Should merge new properties
      expect((result as any).$newProperty).toEqual({ key: 'value' });
      expect((result as any).$customData).toEqual({ processed: true });

      // Should merge primitive values
      expect(result.$statusCode).toBe(201);

      // Should NOT merge non-mergeable properties
      expect(result.$repos).toBe(originalCtx.$repos);
      expect(result.$logs).toBe(originalCtx.$logs);
      expect(result.$helpers).toBe(originalCtx.$helpers);
      expect(result.$user).toBe(originalCtx.$user);
      expect(result.$req).toBe(originalCtx.$req);
      expect(result.$errors).toBe(originalCtx.$errors);
      // Should merge primitives like $result
      expect(result.$result).toBe('new result');
    });
  });

  describe('Context Merge Integration', () => {
    it('should merge context changes from child process back to original context', async () => {
      const originalCtx: TDynamicContext = {
        $repos: { main: { find: jest.fn() } },
        $body: { name: 'John' },
        $query: { filter: { status: 'active' } },
        $params: { id: '123' },
        $user: { id: 'user123' },
        $logs: jest.fn(),
        $helpers: { $jwt: jest.fn() },
        $req: {} as any,
        $errors: {},
        $share: { logs: [] },
        $data: { processed: false },
        $result: null,
        $statusCode: 200,
      };

      const childCtx = {
        $query: { filter: { ok: 'ok' } },
        $statusCode: 201,
        $result: 'success',
      };

      // Simulate the merge process
      const mergedCtx = smartMergeContext(originalCtx, childCtx);

      // Update original context (simulating what happens in run method)
      Object.assign(originalCtx, mergedCtx);

      // Verify that changes are merged back
      expect(originalCtx.$query.filter).toEqual({ status: 'active', ok: 'ok' });
      expect(originalCtx.$statusCode).toBe(201);
      expect(originalCtx.$result).toBe('success');
    });
  });
});
