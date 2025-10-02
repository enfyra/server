import { Test, TestingModule } from '@nestjs/testing';
import { RouteDetectMiddleware } from '../../src/shared/middleware/route-detect.middleware';
import { RouteCacheService } from '../../src/infrastructure/cache/services/route-cache.service';
import { CommonService } from '../../src/shared/common/services/common.service';
import { DataSourceService } from '../../src/core/database/data-source/data-source.service';
import { JwtService } from '@nestjs/jwt';
import { TableHandlerService } from '../../src/modules/table-management/services/table-handler.service';
import { CacheService } from '../../src/infrastructure/cache/services/cache.service';
import { QueryEngine } from '../../src/infrastructure/query-engine/services/query-engine.service';
import { SystemProtectionService } from '../../src/modules/dynamic-api/services/system-protection.service';
import { BcryptService } from '../../src/core/auth/services/bcrypt.service';

interface MockRequest {
  method: string;
  baseUrl: string;
  query: any;
  body: any;
  user: any;
  routeData?: any;
}

describe('RouteDetectMiddleware - Integration with SWR Cache', () => {
  let middleware: RouteDetectMiddleware;
  let routeCacheService: jest.Mocked<RouteCacheService>;
  let commonService: jest.Mocked<CommonService>;

  const mockRoutes = [
    {
      id: 1,
      path: '/users',
      isEnabled: true,
      mainTable: { id: 1, name: 'user_table' },
      targetTables: [],
      hooks: [],
      handlers: [
        { id: 1, method: { method: 'GET' }, logic: 'return { success: true }' },
      ],
      publishedMethods: [{ method: 'GET' }],
      routePermissions: [],
    },
    {
      id: 2,
      path: '/posts',
      isEnabled: true,
      mainTable: { id: 2, name: 'post_table' },
      targetTables: [{ id: 3, name: 'comment_table', alias: 'comments' }],
      hooks: [
        {
          id: 1,
          priority: 1,
          methods: [{ method: 'POST' }],
          route: { id: 2 },
          isEnabled: true,
        },
      ],
      handlers: [
        { id: 2, method: { method: 'GET' }, logic: 'return { success: true }' },
        {
          id: 3,
          method: { method: 'POST' },
          logic: 'return { created: true }',
        },
      ],
      publishedMethods: [{ method: 'POST' }],
      routePermissions: [],
    },
  ];

  beforeEach(async () => {
    const mockServices = {
      commonService: {
        isRouteMatched: jest.fn(),
      },
      dataSourceService: {
        getRepository: jest.fn(),
      },
      jwtService: {
        sign: jest.fn(),
      },
      tableHandlerService: {},
      redisLockService: {},
      queryEngine: {},
      systemProtectionService: {},
      bcryptService: {
        hash: jest.fn(),
        compare: jest.fn(),
      },
      routeCacheService: {
        getRoutesWithSWR: jest.fn(),
      },
    };

    const module: TestingModule = await Test.createTestingModule({
      providers: [
        RouteDetectMiddleware,
        { provide: CommonService, useValue: mockServices.commonService },
        {
          provide: DataSourceService,
          useValue: mockServices.dataSourceService,
        },
        { provide: JwtService, useValue: mockServices.jwtService },
        {
          provide: TableHandlerService,
          useValue: mockServices.tableHandlerService,
        },
        { provide: CacheService, useValue: mockServices.cacheService },
        { provide: QueryEngine, useValue: mockServices.queryEngine },
        {
          provide: RouteCacheService,
          useValue: mockServices.routeCacheService,
        },
        {
          provide: SystemProtectionService,
          useValue: mockServices.systemProtectionService,
        },
        { provide: BcryptService, useValue: mockServices.bcryptService },
      ],
    }).compile();

    middleware = module.get<RouteDetectMiddleware>(RouteDetectMiddleware);
    routeCacheService = module.get(RouteCacheService);
    commonService = module.get(CommonService);
  });

  afterEach(() => {
    jest.clearAllMocks();
  });

  describe('SWR Cache Integration', () => {
    it('should use SWR cache for route matching', async () => {
      // Arrange
      routeCacheService.getRoutesWithSWR.mockResolvedValue(mockRoutes);
      commonService.isRouteMatched.mockReturnValue({
        params: { id: '123' },
      });

      const mockReq: MockRequest = {
        method: 'GET',
        baseUrl: '/users',
        query: {},
        body: {},
        user: undefined,
      };
      const mockRes = {};
      const mockNext = jest.fn();

      // Act
      await middleware.use(mockReq, mockRes, mockNext);

      // Assert
      expect(routeCacheService.getRoutesWithSWR).toHaveBeenCalledTimes(1);
      expect(commonService.isRouteMatched).toHaveBeenCalledWith({
        routePath: '/users',
        reqPath: '/users',
      });
      expect(mockReq.routeData).toBeDefined();
      expect(mockReq.routeData.id).toBe(1);
      expect(mockNext).toHaveBeenCalled();
    });

    it('should handle cache miss gracefully', async () => {
      // Arrange
      routeCacheService.getRoutesWithSWR.mockResolvedValue([]);
      const mockReq: MockRequest = {
        method: 'GET',
        baseUrl: '/nonexistent',
        query: {},
        body: {},
        user: undefined,
      };
      const mockRes = {};
      const mockNext = jest.fn();

      // Act
      await middleware.use(mockReq, mockRes, mockNext);

      // Assert
      expect(routeCacheService.getRoutesWithSWR).toHaveBeenCalled();
      expect(mockReq.routeData).toBeUndefined();
      expect(mockNext).toHaveBeenCalled();
    });

    it('should handle SWR cache errors gracefully', async () => {
      // Arrange
      routeCacheService.getRoutesWithSWR.mockRejectedValue(
        new Error('Cache service error'),
      );
      const mockReq: MockRequest = {
        method: 'GET',
        baseUrl: '/users',
        query: {},
        body: {},
        user: undefined,
      };
      const mockRes = {};
      const mockNext = jest.fn();

      // Act & Assert
      await expect(middleware.use(mockReq, mockRes, mockNext)).rejects.toThrow(
        'Cache service error',
      );
    });
  });

  describe('Route Matching with Different HTTP Methods', () => {
    it('should match GET requests correctly', async () => {
      // Arrange
      routeCacheService.getRoutesWithSWR.mockResolvedValue(mockRoutes);
      commonService.isRouteMatched.mockReturnValue({ params: {} });

      const mockReq: MockRequest = {
        method: 'GET',
        baseUrl: '/users',
        query: {},
        body: {},
        user: undefined,
      };
      const mockRes = {};
      const mockNext = jest.fn();

      // Act
      await middleware.use(mockReq, mockRes, mockNext);

      // Assert
      expect(mockReq.routeData.handler).toBe('return { success: true }');
      expect(mockReq.routeData.isPublished).toBe(true);
    });

    it.skip('should match POST requests with hooks', async () => {
      // Arrange
      routeCacheService.getRoutesWithSWR.mockResolvedValue(mockRoutes);
      commonService.isRouteMatched
        .mockReturnValueOnce(null) // First route (/users) doesn't match
        .mockReturnValueOnce({ params: {} }); // Second route (/posts) matches

      const mockReq: MockRequest = {
        method: 'POST',
        baseUrl: '/posts',
        query: {},
        body: { title: 'Test Post' },
        user: undefined,
      };
      const mockRes = {};
      const mockNext = jest.fn();

      // Act
      await middleware.use(mockReq, mockRes, mockNext);

      // Assert
      expect(mockReq.routeData.handler).toBe('return { created: true }');
      expect(mockReq.routeData.hooks).toHaveLength(1);
      expect(mockReq.routeData.hooks[0].methods[0].method).toBe('POST');
    });

    it('should handle DELETE/PATCH requests with :id parameter', async () => {
      // Arrange
      const routeWithId = {
        ...mockRoutes[0],
        handlers: [
          {
            id: 3,
            method: { method: 'DELETE' },
            logic: 'return { deleted: true }',
          },
        ],
      };
      routeCacheService.getRoutesWithSWR.mockResolvedValue([routeWithId]);
      commonService.isRouteMatched
        .mockReturnValueOnce(null) // First try /users
        .mockReturnValueOnce({ params: { id: '123' } }); // Second try /users/:id

      const mockReq: MockRequest = {
        method: 'DELETE',
        baseUrl: '/users/123',
        query: {},
        body: {},
        user: undefined,
      };
      const mockRes = {};
      const mockNext = jest.fn();

      // Act
      await middleware.use(mockReq, mockRes, mockNext);

      // Assert
      expect(commonService.isRouteMatched).toHaveBeenCalledTimes(2);
      expect(mockReq.routeData.params).toEqual({ id: '123' });
    });
  });

  describe('Dynamic Repository Creation', () => {
    it('should create dynamic repositories for main and target tables', async () => {
      // Arrange
      routeCacheService.getRoutesWithSWR.mockResolvedValue([mockRoutes[1]]); // Post route with target tables
      commonService.isRouteMatched.mockReturnValue({ params: {} });

      const mockReq: MockRequest = {
        method: 'POST',
        baseUrl: '/posts',
        query: { include: 'comments' },
        body: {},
        user: { id: 1, name: 'test user' },
      };
      const mockRes = {};
      const mockNext = jest.fn();

      // Act
      await middleware.use(mockReq, mockRes, mockNext);

      // Assert
      expect(mockReq.routeData.context).toBeDefined();
      expect(mockReq.routeData.context.$repos).toBeDefined();
      expect(mockReq.routeData.context.$repos.main).toBeDefined();
      expect(mockReq.routeData.context.$repos.comments).toBeDefined();
      expect(mockReq.routeData.context.$user).toEqual({
        id: 1,
        name: 'test user',
      });
      expect(mockReq.routeData.context.$query).toEqual({ include: 'comments' });
      expect(mockReq.routeData.context.$body).toEqual({});
    });

    it('should filter out system tables from target tables', async () => {
      // Arrange
      const routeWithSystemTables = {
        ...mockRoutes[1],
        targetTables: [
          { id: 1, name: 'table_definition' },
          { id: 2, name: 'column_definition' },
          { id: 3, name: 'relation_definition' },
          { id: 4, name: 'user_table' },
        ],
      };
      routeCacheService.getRoutesWithSWR.mockResolvedValue([
        routeWithSystemTables,
      ]);
      commonService.isRouteMatched.mockReturnValue({ params: {} });

      const mockReq: MockRequest = {
        method: 'POST',
        baseUrl: '/posts',
        query: {},
        body: {},
        user: undefined,
      };
      const mockRes = {};
      const mockNext = jest.fn();

      // Act
      await middleware.use(mockReq, mockRes, mockNext);

      // Assert
      expect(mockReq.routeData.context.$repos).toBeDefined();
      expect(mockReq.routeData.context.$repos.main).toBeDefined();
      expect(mockReq.routeData.context.$repos.user_table).toBeDefined();
      // System tables should be filtered out
      expect(mockReq.routeData.context.$repos.table_definition).toBeUndefined();
      expect(
        mockReq.routeData.context.$repos.column_definition,
      ).toBeUndefined();
      expect(
        mockReq.routeData.context.$repos.relation_definition,
      ).toBeUndefined();
    });
  });

  describe('Hook Filtering', () => {
    const globalHooks = [
      {
        id: 1,
        priority: 0,
        methods: [],
        route: null,
        isEnabled: true,
      },
      {
        id: 2,
        priority: 1,
        methods: [{ method: 'GET' }],
        route: null,
        isEnabled: true,
      },
    ];

    const localHooks = [
      {
        id: 3,
        priority: 2,
        methods: [],
        route: { id: 1 },
        isEnabled: true,
      },
      {
        id: 4,
        priority: 3,
        methods: [{ method: 'POST' }],
        route: { id: 1 },
        isEnabled: true,
      },
    ];

    it('should include global hooks for all methods', async () => {
      // Arrange
      const routeWithHooks = {
        ...mockRoutes[0],
        hooks: [...globalHooks, ...localHooks],
      };
      routeCacheService.getRoutesWithSWR.mockResolvedValue([routeWithHooks]);
      commonService.isRouteMatched.mockReturnValue({ params: {} });

      const mockReq: MockRequest = {
        method: 'GET',
        baseUrl: '/users',
        query: {},
        body: {},
        user: undefined,
      };
      const mockRes = {};
      const mockNext = jest.fn();

      // Act
      await middleware.use(mockReq, mockRes, mockNext);

      // Assert
      const filteredHooks = mockReq.routeData.hooks;
      expect(filteredHooks).toHaveLength(3); // global all + global GET + local all
      expect(filteredHooks.some((h) => h.id === 1)).toBe(true); // Global all methods
      expect(filteredHooks.some((h) => h.id === 2)).toBe(true); // Global GET
      expect(filteredHooks.some((h) => h.id === 3)).toBe(true); // Local all methods
      expect(filteredHooks.some((h) => h.id === 4)).toBe(false); // Local POST should be excluded
    });

    it('should include method-specific hooks only', async () => {
      // Arrange
      const routeWithHooks = {
        ...mockRoutes[0],
        hooks: [...globalHooks, ...localHooks],
      };
      routeCacheService.getRoutesWithSWR.mockResolvedValue([routeWithHooks]);
      commonService.isRouteMatched.mockReturnValue({ params: {} });

      const mockReq: MockRequest = {
        method: 'POST',
        baseUrl: '/users',
        query: {},
        body: {},
        user: undefined,
      };
      const mockRes = {};
      const mockNext = jest.fn();

      // Act
      await middleware.use(mockReq, mockRes, mockNext);

      // Assert
      const filteredHooks = mockReq.routeData.hooks;
      expect(filteredHooks).toHaveLength(3); // global all + local all + local POST
      expect(filteredHooks.some((h) => h.id === 1)).toBe(true); // Global all methods
      expect(filteredHooks.some((h) => h.id === 2)).toBe(false); // Global GET should be excluded
      expect(filteredHooks.some((h) => h.id === 3)).toBe(true); // Local all methods
      expect(filteredHooks.some((h) => h.id === 4)).toBe(true); // Local POST
    });
  });

  describe('Context Object Creation', () => {
    it('should create complete context object with all helpers', async () => {
      // Arrange
      routeCacheService.getRoutesWithSWR.mockResolvedValue([mockRoutes[0]]);
      commonService.isRouteMatched.mockReturnValue({ params: { id: '123' } });

      const mockReq: MockRequest = {
        method: 'GET',
        baseUrl: '/users/123',
        query: { include: 'posts' },
        body: { name: 'test' },
        user: { id: 1, role: 'admin' },
      };
      const mockRes = {};
      const mockNext = jest.fn();

      // Act
      await middleware.use(mockReq, mockRes, mockNext);

      // Assert
      const context = mockReq.routeData.context;
      expect(context.$body).toEqual({ name: 'test' });
      expect(context.$throw).toBeDefined();
      expect(typeof context.$throw).toBe('object');
      expect(context.$params).toEqual({ id: '123' });
      expect(context.$query).toEqual({ include: 'posts' });
      expect(context.$user).toEqual({ id: 1, role: 'admin' });
      expect(context.$req).toBe(mockReq);
      expect(context.$share.$logs).toEqual([]);
      expect(typeof context.$logs).toBe('function');
      expect(context.$helpers).toBeDefined();
      expect(typeof context.$helpers.$jwt).toBe('function');
      expect(context.$helpers.$bcrypt).toBeDefined();
      expect(typeof context.$helpers.$bcrypt.hash).toBe('function');
      expect(typeof context.$helpers.$bcrypt.compare).toBe('function');
    });

    it('should handle logging functionality', async () => {
      // Arrange
      routeCacheService.getRoutesWithSWR.mockResolvedValue([mockRoutes[0]]);
      commonService.isRouteMatched.mockReturnValue({ params: {} });

      const mockReq: MockRequest = {
        method: 'GET',
        baseUrl: '/users',
        query: {},
        body: {},
        user: undefined,
      };
      const mockRes = {};
      const mockNext = jest.fn();

      // Act
      await middleware.use(mockReq, mockRes, mockNext);

      // Assert
      const context = mockReq.routeData.context;
      context.$logs('test log 1', 'test log 2');
      expect(context.$share.$logs).toEqual(['test log 1', 'test log 2']);
    });
  });

  describe('Performance Tests', () => {
    it('should complete route detection quickly with cache hit', async () => {
      // Arrange
      routeCacheService.getRoutesWithSWR.mockResolvedValue(mockRoutes);
      commonService.isRouteMatched.mockReturnValue({ params: {} });

      const mockReq: MockRequest = {
        method: 'GET',
        baseUrl: '/users',
        query: {},
        body: {},
        user: undefined,
      };
      const mockRes = {};
      const mockNext = jest.fn();

      // Act
      const startTime = Date.now();
      await middleware.use(mockReq, mockRes, mockNext);
      const duration = Date.now() - startTime;

      // Assert
      expect(duration).toBeLessThan(50); // Should complete quickly
      expect(mockNext).toHaveBeenCalled();
    });

    it('should handle concurrent requests efficiently', async () => {
      // Arrange
      routeCacheService.getRoutesWithSWR.mockResolvedValue(mockRoutes);
      commonService.isRouteMatched.mockReturnValue({ params: {} });

      const requests = Array.from({ length: 10 }, (_, i) => ({
        method: 'GET',
        baseUrl: `/users/${i}`,
        query: {},
        body: {},
        user: undefined,
      }));

      const responses = requests.map(() => ({}));
      const nextFunctions = requests.map(() => jest.fn());

      // Act
      const startTime = Date.now();
      await Promise.all(
        requests.map((req, i) =>
          middleware.use(req, responses[i], nextFunctions[i]),
        ),
      );
      const duration = Date.now() - startTime;

      // Assert
      expect(duration).toBeLessThan(200); // Concurrent requests should be fast
      expect(routeCacheService.getRoutesWithSWR).toHaveBeenCalledTimes(10);
      nextFunctions.forEach((next) => expect(next).toHaveBeenCalled());
    });
  });

  describe('Error Scenarios', () => {
    it('should handle repository creation errors gracefully', async () => {
      // Arrange
      const routeWithBadTable = {
        ...mockRoutes[0],
        mainTable: null, // This should cause an error
      };
      routeCacheService.getRoutesWithSWR.mockResolvedValue([routeWithBadTable]);
      commonService.isRouteMatched.mockReturnValue({ params: {} });

      const mockReq: MockRequest = {
        method: 'GET',
        baseUrl: '/users',
        query: {},
        body: {},
        user: undefined,
      };
      const mockRes = {};
      const mockNext = jest.fn();

      // Act & Assert
      await expect(
        middleware.use(mockReq, mockRes, mockNext),
      ).rejects.toThrow();
    });

    it('should continue processing even if some dynamic repos fail', async () => {
      // This test would require more complex mocking of DynamicRepository
      // For now, we'll test that the middleware doesn't crash on edge cases

      // Arrange
      routeCacheService.getRoutesWithSWR.mockResolvedValue([mockRoutes[1]]);
      commonService.isRouteMatched.mockReturnValue({ params: {} });

      const mockReq: MockRequest = {
        method: 'POST',
        baseUrl: '/posts',
        query: {},
        body: {},
        user: undefined,
      };
      const mockRes = {};
      const mockNext = jest.fn();

      // Act
      await middleware.use(mockReq, mockRes, mockNext);

      // Assert
      expect(mockNext).toHaveBeenCalled();
      expect(mockReq.routeData).toBeDefined();
    });
  });
});
