import { Test, TestingModule } from '@nestjs/testing';
import { RouteCacheService } from '../../../src/infrastructure/cache/services/route-cache.service';
import { DataSourceService } from '../../../src/core/database/data-source/data-source.service';
import { CacheService } from '../../../src/infrastructure/cache/services/cache.service';
import { Logger } from '@nestjs/common';

describe('RouteCacheService - SWR Pattern Tests', () => {
  let service: RouteCacheService;
  let dataSourceService: jest.Mocked<DataSourceService>;
  let cacheService: jest.Mocked<CacheService>;

  const mockRoutes = [
    { id: 1, path: '/test', isEnabled: true },
    { id: 2, path: '/api', isEnabled: true },
  ];

  beforeEach(async () => {
    const mockDataSourceService = {
      getRepository: jest.fn().mockReturnValue({
        find: jest.fn().mockResolvedValue([]),
        createQueryBuilder: jest.fn().mockReturnValue({
          leftJoinAndSelect: jest.fn().mockReturnThis(),
          where: jest.fn().mockReturnThis(),
          getMany: jest.fn().mockResolvedValue(mockRoutes),
        }),
      }),
    };

    const mockCacheService = {
      get: jest.fn(),
      set: jest.fn().mockResolvedValue(undefined),
      acquire: jest.fn().mockResolvedValue(true),
      release: jest.fn().mockResolvedValue(true),
    };

    const module: TestingModule = await Test.createTestingModule({
      providers: [
        RouteCacheService,
        { provide: DataSourceService, useValue: mockDataSourceService },
        { provide: CacheService, useValue: mockCacheService },
      ],
    }).compile();

    service = module.get<RouteCacheService>(RouteCacheService);
    dataSourceService = module.get(DataSourceService);
    cacheService = module.get(CacheService);

    // Spy on logger to suppress logs during tests
    jest.spyOn(Logger.prototype, 'log').mockImplementation();
    jest.spyOn(Logger.prototype, 'warn').mockImplementation();
    jest.spyOn(Logger.prototype, 'error').mockImplementation();
    jest.spyOn(Logger.prototype, 'debug').mockImplementation();
  });

  afterEach(() => {
    jest.clearAllMocks();
  });

  describe('Cache Hit Scenario', () => {
    it('should return cached routes without additional Redis calls when cache is fresh', async () => {
      // Arrange: Fresh cache exists
      redisLockService.get.mockResolvedValueOnce(mockRoutes);

      // Act
      const result = await service.getRoutesWithSWR();

      // Assert
      expect(result).toEqual(mockRoutes);
      expect(redisLockService.get).toHaveBeenCalledTimes(1);
      expect(redisLockService.get).toHaveBeenCalledWith('global-routes');
      expect(dataSourceService.getRepository).not.toHaveBeenCalled();
    });

    it('should warn when Redis is slow but still return cached data', async () => {
      // Arrange: Simulate slow Redis
      redisLockService.get.mockImplementation(
        () =>
          new Promise((resolve) => setTimeout(() => resolve(mockRoutes), 20)),
      );

      const warnSpy = jest.spyOn(Logger.prototype, 'warn').mockImplementation();

      // Act
      const result = await service.getRoutesWithSWR();

      // Assert
      expect(result).toEqual(mockRoutes);
      expect(warnSpy).toHaveBeenCalledWith(
        expect.stringContaining('⚠️ Cache hit but Redis slow:'),
      );
    });
  });

  describe('SWR Pattern - Cache Miss with Stale Data', () => {
    it('should serve stale data immediately and trigger background revalidation', async () => {
      // Arrange
      redisLockService.get
        .mockResolvedValueOnce(null) // Cache miss
        .mockResolvedValueOnce(mockRoutes) // Stale data exists
        .mockResolvedValueOnce(false); // Not currently revalidating

      // Act
      const result = await service.getRoutesWithSWR();

      // Assert
      expect(result).toEqual(mockRoutes);
      expect(redisLockService.get).toHaveBeenCalledWith('global-routes');
      expect(redisLockService.get).toHaveBeenCalledWith('stale:routes');
      expect(redisLockService.get).toHaveBeenCalledWith('revalidating:routes');
      expect(redisLockService.acquire).toHaveBeenCalledWith(
        'revalidating:routes',
        'true',
        30000,
      );
    });

    it('should skip background revalidation if already revalidating', async () => {
      // Arrange
      redisLockService.get
        .mockResolvedValueOnce(null) // Cache miss
        .mockResolvedValueOnce(mockRoutes) // Stale data exists
        .mockResolvedValueOnce(true); // Already revalidating

      // Act
      const result = await service.getRoutesWithSWR();

      // Assert
      expect(result).toEqual(mockRoutes);
      expect(redisLockService.acquire).not.toHaveBeenCalled();
    });

    it('should handle parallel Redis calls efficiently', async () => {
      // Arrange
      redisLockService.get
        .mockResolvedValueOnce(null) // Cache miss
        .mockImplementation(async (key) => {
          if (key === 'stale:routes') return mockRoutes;
          if (key === 'revalidating:routes') return false;
          return null;
        });

      const startTime = Date.now();

      // Act
      const result = await service.getRoutesWithSWR();

      // Assert
      const duration = Date.now() - startTime;
      expect(result).toEqual(mockRoutes);
      expect(duration).toBeLessThan(50); // Should be fast due to parallel calls
    });
  });

  describe('Cold Start - No Cache, No Stale Data', () => {
    it('should fetch from database when no cache or stale data exists', async () => {
      // Arrange
      redisLockService.get
        .mockResolvedValueOnce(null) // Cache miss
        .mockResolvedValueOnce(null) // No stale data
        .mockResolvedValueOnce(false); // Not revalidating

      // Act
      const result = await service.getRoutesWithSWR();

      // Assert
      expect(result).toEqual(mockRoutes);
      expect(dataSourceService.getRepository).toHaveBeenCalledTimes(2); // For routes and hooks
      expect(redisLockService.acquire).toHaveBeenCalledWith(
        'global-routes',
        mockRoutes,
        60000,
      );
      expect(redisLockService.set).toHaveBeenCalledWith(
        'stale:routes',
        mockRoutes,
        0,
      );
    });

    it('should handle database errors gracefully during cold start', async () => {
      // Arrange
      redisLockService.get.mockResolvedValue(null);
      dataSourceService.getRepository.mockImplementation(() => {
        throw new Error('Database connection failed');
      });

      // Act & Assert
      await expect(service.getRoutesWithSWR()).rejects.toThrow(
        'Database connection failed',
      );
    });
  });

  describe('Background Revalidation', () => {
    it('should acquire lock successfully and revalidate cache', async () => {
      // Arrange
      redisLockService.get
        .mockResolvedValueOnce(null) // Cache miss
        .mockResolvedValueOnce(mockRoutes) // Stale data exists
        .mockResolvedValueOnce(false); // Not revalidating

      redisLockService.acquire.mockResolvedValueOnce(true); // Lock acquired

      // Act
      const result = await service.getRoutesWithSWR();

      // Wait for background task to potentially complete
      await new Promise((resolve) => setTimeout(resolve, 50));

      // Assert
      expect(result).toEqual(mockRoutes);
      expect(redisLockService.acquire).toHaveBeenCalledWith(
        'revalidating:routes',
        'true',
        30000,
      );
      expect(redisLockService.release).toHaveBeenCalledWith(
        'revalidating:routes',
        'true',
      );
    });

    it('should skip revalidation if lock acquisition fails', async () => {
      // Arrange
      redisLockService.get
        .mockResolvedValueOnce(null) // Cache miss
        .mockResolvedValueOnce(mockRoutes) // Stale data exists
        .mockResolvedValueOnce(false); // Not revalidating

      redisLockService.acquire.mockResolvedValueOnce(false); // Lock acquisition failed

      // Act
      const result = await service.getRoutesWithSWR();
      await new Promise((resolve) => setTimeout(resolve, 50));

      // Assert
      expect(result).toEqual(mockRoutes);
      expect(redisLockService.release).not.toHaveBeenCalled();
    });

    it('should handle revalidation errors and still release lock', async () => {
      // Arrange
      redisLockService.get
        .mockResolvedValueOnce(null) // Cache miss
        .mockResolvedValueOnce(mockRoutes) // Stale data exists
        .mockResolvedValueOnce(false); // Not revalidating

      redisLockService.acquire.mockResolvedValueOnce(true);
      dataSourceService.getRepository.mockImplementation(() => {
        throw new Error('Revalidation failed');
      });

      const errorSpy = jest
        .spyOn(Logger.prototype, 'error')
        .mockImplementation();

      // Act
      const result = await service.getRoutesWithSWR();
      await new Promise((resolve) => setTimeout(resolve, 100));

      // Assert
      expect(result).toEqual(mockRoutes); // Still serve stale data
      expect(errorSpy).toHaveBeenCalledWith(
        expect.stringContaining('❌ Failed to reload route cache:'),
        expect.any(String),
      );
      expect(redisLockService.release).toHaveBeenCalled();
    });
  });

  describe('Manual Cache Reload', () => {
    it('should reload cache and update both main and stale cache', async () => {
      // Act
      await service.reloadRouteCache();

      // Assert
      expect(dataSourceService.getRepository).toHaveBeenCalledTimes(2);
      expect(redisLockService.set).toHaveBeenCalledWith(
        'global-routes',
        mockRoutes,
        60000,
      );
      expect(redisLockService.set).toHaveBeenCalledWith(
        'stale:routes',
        mockRoutes,
        0,
      );
    });

    it('should handle manual reload errors gracefully', async () => {
      // Arrange
      dataSourceService.getRepository.mockImplementation(() => {
        throw new Error('Manual reload failed');
      });

      const errorSpy = jest
        .spyOn(Logger.prototype, 'error')
        .mockImplementation();

      // Act
      await service.reloadRouteCache();

      // Assert
      expect(errorSpy).toHaveBeenCalledWith(
        expect.stringContaining('❌ Failed to reload route cache:'),
        expect.any(String),
      );
    });
  });

  describe('Redis Connection Issues', () => {
    it('should handle Redis timeout during cache check', async () => {
      // Arrange
      redisLockService.get.mockImplementation(
        () =>
          new Promise((_, reject) =>
            setTimeout(() => reject(new Error('Redis timeout')), 100),
          ),
      );

      // Act & Assert
      await expect(service.getRoutesWithSWR()).rejects.toThrow('Redis timeout');
    });

    it('should handle Redis errors during stale data fetch', async () => {
      // Arrange
      redisLockService.get
        .mockResolvedValueOnce(null) // Cache miss
        .mockRejectedValueOnce(new Error('Redis connection lost'));

      // Act & Assert
      await expect(service.getRoutesWithSWR()).rejects.toThrow(
        'Redis connection lost',
      );
    });
  });

  describe('Performance Tests', () => {
    it('should complete cache hits in under 10ms', async () => {
      // Arrange
      redisLockService.get.mockResolvedValueOnce(mockRoutes);

      // Act
      const startTime = Date.now();
      const result = await service.getRoutesWithSWR();
      const duration = Date.now() - startTime;

      // Assert
      expect(result).toEqual(mockRoutes);
      expect(duration).toBeLessThan(10);
    });

    it('should handle concurrent requests efficiently', async () => {
      // Arrange
      redisLockService.get.mockResolvedValue(mockRoutes);

      // Act
      const startTime = Date.now();
      const promises = Array.from({ length: 10 }, () =>
        service.getRoutesWithSWR(),
      );
      const results = await Promise.all(promises);
      const duration = Date.now() - startTime;

      // Assert
      expect(results).toHaveLength(10);
      expect(results.every((result) => result === mockRoutes)).toBe(true);
      expect(duration).toBeLessThan(50); // Should be fast due to caching
    });

    it('should handle SWR pattern under load', async () => {
      // Arrange: Cache miss, stale data available
      redisLockService.get
        .mockResolvedValue(null) // Cache miss
        .mockResolvedValueOnce(mockRoutes) // Stale data for first call
        .mockResolvedValue(false); // Not revalidating

      // Simulate multiple concurrent requests during cache expiry
      const promises = Array.from({ length: 5 }, () =>
        service.getRoutesWithSWR(),
      );

      // Act
      const startTime = Date.now();
      const results = await Promise.all(promises);
      const duration = Date.now() - startTime;

      // Assert
      expect(results).toHaveLength(5);
      expect(duration).toBeLessThan(100); // Should be fast due to SWR
      expect(redisLockService.get).toHaveBeenCalledWith('stale:routes');
    });
  });

  describe('Edge Cases and Data Validation', () => {
    it('should handle empty routes from database', async () => {
      // Arrange
      const emptyRoutes = [];
      redisLockService.get.mockResolvedValue(null);
      (dataSourceService.getRepository as jest.Mock).mockReturnValue({
        find: jest.fn().mockResolvedValue([]),
        createQueryBuilder: jest.fn().mockReturnValue({
          leftJoinAndSelect: jest.fn().mockReturnThis(),
          where: jest.fn().mockReturnThis(),
          getMany: jest.fn().mockResolvedValue(emptyRoutes),
        }),
      } as any);

      // Act
      const result = await service.getRoutesWithSWR();

      // Assert
      expect(result).toEqual(emptyRoutes);
      expect(redisLockService.set).toHaveBeenCalledWith(
        'stale:routes',
        emptyRoutes,
        0,
      );
    });

    it('should handle corrupted stale data gracefully', async () => {
      // Arrange: When stale data exists but is corrupted, it should fallback to DB
      redisLockService.get
        .mockResolvedValueOnce(null) // Cache miss
        .mockResolvedValueOnce(null) // No stale data (simulating corruption as null)
        .mockResolvedValueOnce(false); // Not revalidating

      // Act
      const result = await service.getRoutesWithSWR();

      // Assert
      expect(result).toEqual(mockRoutes); // Should fallback to DB
      expect(dataSourceService.getRepository).toHaveBeenCalled();
    });

    it('should handle TTL edge cases correctly', async () => {
      // Arrange: Cache about to expire
      redisLockService.get
        .mockResolvedValueOnce(null) // Cache just expired
        .mockResolvedValueOnce(mockRoutes) // Stale data available
        .mockResolvedValueOnce(false); // Not revalidating

      // Act
      const result = await service.getRoutesWithSWR();

      // Assert
      expect(result).toEqual(mockRoutes);
      expect(redisLockService.acquire).toHaveBeenCalledWith(
        'revalidating:routes',
        'true',
        30000, // 30 second lock TTL
      );
    });
  });

  describe('Multi-instance Safety', () => {
    it('should handle multiple instances trying to revalidate simultaneously', async () => {
      // Arrange: Multiple instances hit cache expiry at same time
      redisLockService.get
        .mockResolvedValueOnce(null) // Cache miss - instance 1
        .mockResolvedValueOnce(mockRoutes) // Stale data - instance 1
        .mockResolvedValueOnce(false) // Not revalidating - instance 1
        .mockResolvedValueOnce(null) // Cache miss - instance 2
        .mockResolvedValueOnce(mockRoutes) // Stale data - instance 2
        .mockResolvedValueOnce(true); // Already revalidating - instance 2

      redisLockService.acquire
        .mockResolvedValueOnce(true) // Instance 1 gets lock
        .mockResolvedValueOnce(false); // Instance 2 doesn't get lock

      // Act
      const [result1, result2] = await Promise.all([
        service.getRoutesWithSWR(),
        service.getRoutesWithSWR(),
      ]);

      // Assert
      expect(result1).toEqual(mockRoutes);
      expect(result2).toEqual(mockRoutes);
      expect(redisLockService.acquire).toHaveBeenCalledTimes(1); // Only first call should acquire
    });
  });
});
