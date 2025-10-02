import { Test, TestingModule } from '@nestjs/testing';
import { RouteCacheService } from '../../src/infrastructure/cache/services/route-cache.service';
import { DataSourceService } from '../../src/core/database/data-source/data-source.service';
import { CacheService } from '../../src/infrastructure/cache/services/cache.service';
import { Logger } from '@nestjs/common';

describe('RouteCacheService - Stress Testing', () => {
  let service: RouteCacheService;
  let dataSourceService: jest.Mocked<DataSourceService>;
  let cacheService: jest.Mocked<CacheService>;

  const mockRoutes = Array.from({ length: 100 }, (_, i) => ({
    id: i + 1,
    path: `/route${i}`,
    isEnabled: true,
    hooks: [],
  }));

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

    // Suppress logs during stress tests
    jest.spyOn(Logger.prototype, 'log').mockImplementation();
    jest.spyOn(Logger.prototype, 'warn').mockImplementation();
    jest.spyOn(Logger.prototype, 'error').mockImplementation();
    jest.spyOn(Logger.prototype, 'debug').mockImplementation();
  });

  afterEach(() => {
    jest.clearAllMocks();
  });

  describe('High Concurrency Cache Hits', () => {
    it('should handle 1000 concurrent cache hit requests', async () => {
      // Arrange
      redisLockService.get.mockResolvedValue(mockRoutes);

      // Act
      const startTime = Date.now();
      const promises = Array.from({ length: 1000 }, () =>
        service.getRoutesWithSWR(),
      );

      const results = await Promise.all(promises);
      const duration = Date.now() - startTime;

      // Assert
      expect(results).toHaveLength(1000);
      expect(results.every((result) => result === mockRoutes)).toBe(true);
      expect(duration).toBeLessThan(5000); // Should complete within 5 seconds
      expect(redisLockService.get).toHaveBeenCalledTimes(1000);
      expect(dataSourceService.getRepository).not.toHaveBeenCalled();
    });

    it('should handle 500 concurrent requests with varying Redis latency', async () => {
      // Arrange: Simulate varying Redis response times
      redisLockService.get.mockImplementation(
        () =>
          new Promise((resolve) =>
            setTimeout(() => resolve(mockRoutes), Math.random() * 50),
          ),
      );

      // Act
      const startTime = Date.now();
      const promises = Array.from({ length: 500 }, () =>
        service.getRoutesWithSWR(),
      );

      const results = await Promise.all(promises);
      const duration = Date.now() - startTime;

      // Assert
      expect(results).toHaveLength(500);
      expect(results.every((result) => result === mockRoutes)).toBe(true);
      expect(duration).toBeLessThan(10000); // Should handle latency gracefully
    });
  });

  describe('Cache Miss Burst Load', () => {
    it('should handle burst of cache misses with stale data serving', async () => {
      // Arrange: Cache miss but stale data available
      redisLockService.get.mockImplementation((key) => {
        if (key === 'global-routes') return Promise.resolve(null); // Cache miss
        if (key === 'stale:routes') return Promise.resolve(mockRoutes); // Stale data
        if (key === 'revalidating:routes') return Promise.resolve(false); // Not revalidating
        return Promise.resolve(null);
      });

      // Only one background revalidation should happen
      redisLockService.acquire.mockResolvedValueOnce(true);

      // Act
      const startTime = Date.now();
      const promises = Array.from({ length: 200 }, () =>
        service.getRoutesWithSWR(),
      );

      const results = await Promise.all(promises);
      const duration = Date.now() - startTime;

      // Assert
      expect(results).toHaveLength(200);
      expect(duration).toBeLessThan(3000); // Should serve stale data quickly
      expect(results.every((result) => result === mockRoutes)).toBe(true);
    });

    it('should handle cold start scenario under load', async () => {
      // Arrange: No cache, no stale data
      redisLockService.get.mockResolvedValue(null);

      // Act
      const startTime = Date.now();
      const promises = Array.from({ length: 50 }, () =>
        service.getRoutesWithSWR(),
      );

      const results = await Promise.all(promises);
      const duration = Date.now() - startTime;

      // Assert
      expect(results).toHaveLength(50);
      expect(results.every((result) => result === mockRoutes)).toBe(true);
      expect(duration).toBeLessThan(5000); // Cold start should still be reasonable
      expect(dataSourceService.getRepository).toHaveBeenCalled();
    });
  });

  describe('Background Revalidation Stress', () => {
    it('should handle multiple concurrent revalidation attempts gracefully', async () => {
      // Arrange: Cache miss scenario that triggers revalidation
      redisLockService.get.mockImplementation((key) => {
        if (key === 'global-routes') return Promise.resolve(null); // Cache miss
        if (key === 'stale:routes') return Promise.resolve(mockRoutes); // Stale data
        if (key === 'revalidating:routes') return Promise.resolve(false); // Not revalidating
        return Promise.resolve(null);
      });

      // Only first request should get the lock
      redisLockService.acquire.mockResolvedValueOnce(true);

      // Act
      const promises = Array.from({ length: 100 }, () =>
        service.getRoutesWithSWR(),
      );

      const results = await Promise.all(promises);
      await new Promise((resolve) => setTimeout(resolve, 100)); // Wait for background tasks

      // Assert
      expect(results).toHaveLength(100);
      expect(results.every((result) => result === mockRoutes)).toBe(true);
    });

    it('should handle revalidation lock timeout scenarios', async () => {
      // Arrange: Simulate stuck revalidation scenario
      redisLockService.get.mockImplementation((key) => {
        if (key === 'global-routes') return Promise.resolve(null); // Cache miss
        if (key === 'stale:routes') return Promise.resolve(mockRoutes); // Stale data
        if (key === 'revalidating:routes') return Promise.resolve(true); // Already revalidating
        return Promise.resolve(null);
      });

      // Act
      const promises = Array.from({ length: 100 }, () =>
        service.getRoutesWithSWR(),
      );

      const results = await Promise.all(promises);

      // Assert
      expect(results).toHaveLength(100);
      expect(results.every((result) => result === mockRoutes)).toBe(true);
      expect(redisLockService.acquire).not.toHaveBeenCalled(); // No new revalidation attempts
    });
  });

  describe('Redis Connection Stress', () => {
    it('should handle intermittent Redis failures', async () => {
      // Arrange: Simulate random Redis failures
      let callCount = 0;
      redisLockService.get.mockImplementation(() => {
        callCount++;
        if (callCount % 10 === 0) {
          return Promise.reject(new Error('Redis connection lost'));
        }
        return Promise.resolve(mockRoutes);
      });

      // Act
      const promises = Array.from({ length: 100 }, () =>
        service.getRoutesWithSWR().catch((error) => ({ error: error.message })),
      );

      const results = await Promise.all(promises);

      // Assert
      const successes = results.filter((result) => !('error' in result));
      const failures = results.filter((result) => 'error' in result);

      expect(successes.length).toBeGreaterThan(80); // Most should succeed
      expect(failures.length).toBeGreaterThan(0); // Some should fail
      expect(failures.every((f) => f.error === 'Redis connection lost')).toBe(
        true,
      );
    });

    it('should handle Redis timeout under high load', async () => {
      // Arrange: Simulate slow Redis responses
      redisLockService.get.mockImplementation(
        () =>
          new Promise((resolve, reject) => {
            const delay = Math.random() * 1000; // 0-1000ms delay
            setTimeout(() => {
              if (delay > 800) {
                reject(new Error('Redis timeout'));
              } else {
                resolve(mockRoutes);
              }
            }, delay);
          }),
      );

      // Act
      const promises = Array.from({ length: 200 }, () =>
        service.getRoutesWithSWR().catch((error) => ({ error: error.message })),
      );

      const results = await Promise.all(promises);

      // Assert
      const successes = results.filter((result) => !('error' in result));
      const timeouts = results.filter(
        (result) => 'error' in result && result.error === 'Redis timeout',
      );

      expect(successes.length).toBeGreaterThan(100); // Most should succeed
      expect(timeouts.length).toBeGreaterThan(0); // Some should timeout
    });
  });

  describe('Memory and Resource Management', () => {
    it('should not accumulate background tasks', async () => {
      // Arrange: Track background task behavior
      redisLockService.get.mockImplementation((key) => {
        if (key === 'global-routes') return Promise.resolve(null); // Cache miss
        if (key === 'stale:routes') return Promise.resolve(mockRoutes); // Stale data
        if (key === 'revalidating:routes') return Promise.resolve(false); // Not revalidating
        return Promise.resolve(null);
      });

      redisLockService.acquire.mockResolvedValueOnce(true);

      // Act
      const promises = Array.from({ length: 50 }, () =>
        service.getRoutesWithSWR(),
      );

      await Promise.all(promises);
      await new Promise((resolve) => setTimeout(resolve, 100)); // Wait for background tasks

      // Assert: Background revalidation should work
      expect(redisLockService.set).toHaveBeenCalled(); // Background task updated cache
    });

    it('should handle large route datasets efficiently', async () => {
      // Arrange: Very large route dataset
      const largeRouteSet = Array.from({ length: 10000 }, (_, i) => ({
        id: i + 1,
        path: `/route${i}`,
        isEnabled: true,
        hooks: Array.from({ length: 10 }, (_, j) => ({ id: j, priority: j })),
        handlers: Array.from({ length: 5 }, (_, k) => ({
          id: k,
          logic: `handler${k}`,
        })),
      }));

      redisLockService.get.mockResolvedValue(largeRouteSet);

      // Act
      const startTime = Date.now();
      const promises = Array.from({ length: 100 }, () =>
        service.getRoutesWithSWR(),
      );

      const results = await Promise.all(promises);
      const duration = Date.now() - startTime;

      // Assert
      expect(results).toHaveLength(100);
      expect(results[0]).toHaveLength(10000);
      expect(duration).toBeLessThan(2000); // Should handle large datasets efficiently
    });
  });

  describe('Performance Benchmarks', () => {
    it('should maintain consistent performance under sustained load', async () => {
      // Arrange
      redisLockService.get.mockResolvedValue(mockRoutes);

      const rounds = 5;
      const requestsPerRound = 200;
      const durations: number[] = [];

      // Act: Run multiple rounds of sustained load
      for (let round = 0; round < rounds; round++) {
        const startTime = Date.now();

        const promises = Array.from({ length: requestsPerRound }, () =>
          service.getRoutesWithSWR(),
        );

        await Promise.all(promises);
        const duration = Date.now() - startTime;
        durations.push(duration);

        // Small delay between rounds
        await new Promise((resolve) => setTimeout(resolve, 100));
      }

      // Assert: Performance should be consistent across rounds
      const avgDuration =
        durations.reduce((a, b) => a + b, 0) / durations.length;
      const maxDuration = Math.max(...durations);
      const minDuration = Math.min(...durations);

      expect(avgDuration).toBeLessThan(1000); // Average should be under 1s
      expect(maxDuration - minDuration).toBeLessThan(500); // Variance should be reasonable
    });

    it.skip('should scale linearly with request count', async () => {
      // Arrange
      redisLockService.get.mockResolvedValue(mockRoutes);

      const testSizes = [50, 100, 200, 400];
      const results: { size: number; duration: number; throughput: number }[] =
        [];

      // Act: Test different load sizes
      for (const size of testSizes) {
        const startTime = Date.now();

        const promises = Array.from({ length: size }, () =>
          service.getRoutesWithSWR(),
        );

        await Promise.all(promises);
        const duration = Math.max(Date.now() - startTime, 1); // Avoid divide by zero
        const throughput = size / (duration / 1000); // requests per second

        results.push({ size, duration, throughput });
      }

      // Assert: Throughput should remain relatively consistent
      const throughputs = results.map((r) => r.throughput);
      const avgThroughput =
        throughputs.reduce((a, b) => a + b, 0) / throughputs.length;

      expect(avgThroughput).toBeGreaterThan(50); // At least 50 req/s

      // All throughputs should be reasonable (avoid divide by zero)
      throughputs.forEach((tp) => {
        expect(tp).toBeGreaterThan(0);
        expect(tp).toBeLessThan(100000); // Higher upper bound for cache hits
      });
    });
  });

  describe('Edge Case Stress Testing', () => {
    it('should handle rapid cache expiry scenarios', async () => {
      // Arrange: Simulate cache expiring very frequently
      let cacheHit = true;
      redisLockService.get.mockImplementation(() => {
        cacheHit = !cacheHit; // Alternate between hit and miss
        if (cacheHit) {
          return Promise.resolve(mockRoutes);
        } else {
          return Promise.resolve(null);
        }
      });

      // Stale data always available
      redisLockService.get.mockResolvedValueOnce(mockRoutes);
      redisLockService.get.mockResolvedValue(false); // Not revalidating

      // Act
      const promises = Array.from({ length: 100 }, () =>
        service.getRoutesWithSWR(),
      );

      const results = await Promise.all(promises);

      // Assert
      expect(results).toHaveLength(100);
      expect(results.every((result) => result === mockRoutes)).toBe(true);
    });

    it('should handle database slowdown during high load', async () => {
      // Arrange: Cache miss scenario with slow database
      redisLockService.get.mockResolvedValue(null);

      dataSourceService.getRepository.mockReturnValue({
        find: jest.fn().mockImplementation(
          () => new Promise((resolve) => setTimeout(() => resolve([]), 200)), // 200ms delay
        ),
        createQueryBuilder: jest.fn().mockReturnValue({
          leftJoinAndSelect: jest.fn().mockReturnThis(),
          where: jest.fn().mockReturnThis(),
          getMany: jest.fn().mockImplementation(
            () =>
              new Promise((resolve) =>
                setTimeout(() => resolve(mockRoutes), 200),
              ), // 200ms delay
          ),
        }),
      } as any);

      // Act
      const startTime = Date.now();
      const promises = Array.from({ length: 20 }, () =>
        service.getRoutesWithSWR(),
      );

      const results = await Promise.all(promises);
      const duration = Date.now() - startTime;

      // Assert
      expect(results).toHaveLength(20);
      expect(results.every((result) => result === mockRoutes)).toBe(true);
      expect(duration).toBeGreaterThanOrEqual(200); // Should take at least as long as DB delay
      expect(duration).toBeLessThan(5000); // But not excessively long
    });
  });
});
