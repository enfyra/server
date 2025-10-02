import { Test, TestingModule } from '@nestjs/testing';
import { CacheService } from '../../src/infrastructure/cache/services/cache.service';
import { RedisService } from '@liaoliaots/nestjs-redis';

describe('Simple Monitoring and Metrics', () => {
  let cacheService: CacheService;
  let mockRedis: any;

  beforeEach(async () => {
    mockRedis = {
      set: jest.fn(),
      get: jest.fn(),
      del: jest.fn(),
      eval: jest.fn(),
      pttl: jest.fn(),
    };

    const mockRedisService = {
      getOrNil: jest.fn().mockReturnValue(mockRedis),
    };

    const module: TestingModule = await Test.createTestingModule({
      providers: [
        CacheService,
        { provide: RedisService, useValue: mockRedisService },
      ],
    }).compile();

    cacheService = module.get<CacheService>(CacheService);

    // Suppress console logs during tests
    jest.spyOn(console, 'log').mockImplementation();
    jest.spyOn(console, 'error').mockImplementation();
  });

  afterEach(() => {
    jest.clearAllMocks();
  });

  describe('Performance Metrics', () => {
    it('should measure operation response time', async () => {
      mockRedis.get.mockImplementation(
        () =>
          new Promise((resolve) =>
            setTimeout(() => resolve('"test-data"'), 50),
          ),
      );

      const startTime = Date.now();
      const result = await redisLockService.get('test-key');
      const responseTime = Date.now() - startTime;

      expect(result).toBe('test-data');
      expect(responseTime).toBeGreaterThan(40);
      expect(responseTime).toBeLessThan(100);
    });

    it('should track memory usage during operations', async () => {
      const initialMemory = process.memoryUsage();

      mockRedis.set.mockResolvedValue('OK');
      mockRedis.pttl.mockResolvedValue(30000);

      // Perform multiple operations
      const promises = Array.from({ length: 100 }, (_, i) =>
        redisLockService.acquire(`key-${i}`, `value-${i}`, 1000),
      );

      await Promise.all(promises);

      const finalMemory = process.memoryUsage();
      const memoryIncrease = finalMemory.heapUsed - initialMemory.heapUsed;

      // Memory increase should be reasonable (less than 10MB)
      expect(memoryIncrease).toBeLessThan(10 * 1024 * 1024);
    });

    it('should measure CPU usage during intensive operations', async () => {
      const startCPU = process.cpuUsage();

      mockRedis.get.mockImplementation(() => {
        // Simulate some CPU work
        let sum = 0;
        for (let i = 0; i < 10000; i++) {
          sum += Math.random();
        }
        return Promise.resolve(`"result-${sum}"`);
      });

      await Promise.all(
        Array.from({ length: 50 }, () => redisLockService.get('cpu-test')),
      );

      const cpuUsage = process.cpuUsage(startCPU);
      const totalCPU = cpuUsage.user + cpuUsage.system;

      expect(totalCPU).toBeGreaterThan(0);

      // Convert to milliseconds
      const cpuTimeMs = totalCPU / 1000;
      expect(cpuTimeMs).toBeLessThan(1000);
    });
  });

  describe('Error Rate Monitoring', () => {
    it('should track success and failure rates', async () => {
      let successCount = 0;
      let failureCount = 0;

      mockRedis.set.mockImplementation(() => {
        // 80% success rate
        if (Math.random() > 0.2) {
          successCount++;
          return Promise.resolve('OK');
        } else {
          failureCount++;
          return Promise.reject(new Error('Redis error'));
        }
      });

      mockRedis.pttl.mockResolvedValue(30000);

      const results = await Promise.allSettled(
        Array.from({ length: 100 }, (_, i) =>
          redisLockService.acquire(`key-${i}`, `value-${i}`, 1000),
        ),
      );

      const actualSuccesses = results.filter(
        (r) => r.status === 'fulfilled',
      ).length;
      const actualFailures = results.filter(
        (r) => r.status === 'rejected',
      ).length;

      expect(actualSuccesses).toBeGreaterThan(60); // At least 60% success
      expect(actualFailures).toBeGreaterThan(0); // Some failures expected
      expect(actualSuccesses + actualFailures).toBe(100);

      // Calculate success rate
      const successRate = actualSuccesses / 100;
      expect(successRate).toBeGreaterThan(0.6);
    });

    it('should categorize different error types', async () => {
      const errorTypes = {
        timeout: 0,
        connection: 0,
        other: 0,
      };

      mockRedis.set.mockImplementation(() => {
        const errorType = Math.random();

        if (errorType < 0.3) {
          errorTypes.timeout++;
          return Promise.reject(new Error('Operation timeout'));
        } else if (errorType < 0.6) {
          errorTypes.connection++;
          return Promise.reject(new Error('Connection refused'));
        } else if (errorType < 0.8) {
          errorTypes.other++;
          return Promise.reject(new Error('Unknown error'));
        } else {
          return Promise.resolve('OK');
        }
      });

      mockRedis.pttl.mockResolvedValue(30000);

      await Promise.allSettled(
        Array.from({ length: 50 }, (_, i) =>
          redisLockService.acquire(`error-key-${i}`, 'value', 1000),
        ),
      );

      const totalErrors =
        errorTypes.timeout + errorTypes.connection + errorTypes.other;
      expect(totalErrors).toBeGreaterThan(0);

      // Should have different types of errors
      expect(errorTypes.timeout).toBeGreaterThan(0);
      expect(errorTypes.connection).toBeGreaterThan(0);
    });
  });

  describe('Throughput Monitoring', () => {
    it('should measure operations per second', async () => {
      mockRedis.get.mockResolvedValue('"cached-data"');

      const operationCount = 200;
      const startTime = Date.now();

      await Promise.all(
        Array.from({ length: operationCount }, (_, i) =>
          redisLockService.get(`throughput-key-${i}`),
        ),
      );

      const duration = Date.now() - startTime;
      const operationsPerSecond = (operationCount / duration) * 1000;

      expect(operationsPerSecond).toBeGreaterThan(100); // At least 100 ops/sec
      expect(duration).toBeLessThan(5000); // Should complete within 5 seconds
    });

    it('should handle concurrent load gracefully', async () => {
      mockRedis.set.mockImplementation(
        () => new Promise((resolve) => setTimeout(() => resolve('OK'), 10)),
      );
      mockRedis.pttl.mockResolvedValue(30000);

      const concurrentOperations = 50;
      const startTime = Date.now();

      const results = await Promise.allSettled(
        Array.from({ length: concurrentOperations }, (_, i) =>
          redisLockService.acquire(`concurrent-${i}`, 'value', 5000),
        ),
      );

      const duration = Date.now() - startTime;
      const successfulOperations = results.filter(
        (r) => r.status === 'fulfilled',
      ).length;

      expect(successfulOperations).toBe(concurrentOperations);
      expect(duration).toBeLessThan(2000); // Should handle concurrency well
    });
  });

  describe('Cache Metrics', () => {
    it('should track cache hit and miss rates', async () => {
      let hits = 0;
      let misses = 0;

      mockRedis.get.mockImplementation((key) => {
        // 70% hit rate
        if (Math.random() > 0.3) {
          hits++;
          return Promise.resolve(`"cached-${key}"`);
        } else {
          misses++;
          return Promise.resolve(null);
        }
      });

      const operations = Array.from(
        { length: 100 },
        (_, i) => redisLockService.get(`cache-key-${i % 20}`), // 20 unique keys with repeats
      );

      await Promise.all(operations);

      const totalOperations = hits + misses;
      const hitRate = hits / totalOperations;
      const missRate = misses / totalOperations;

      expect(totalOperations).toBe(100);
      expect(hitRate).toBeGreaterThan(0.5); // At least 50% hit rate
      expect(hitRate + missRate).toBeCloseTo(1, 2); // Should sum to ~1
    });

    it('should monitor cache size and evictions', async () => {
      let cacheSize = 0;
      let evictions = 0;
      const maxCacheSize = 50;

      mockRedis.set.mockImplementation(() => {
        if (cacheSize >= maxCacheSize) {
          evictions++;
          cacheSize = maxCacheSize - 1; // Simulate eviction
        }
        cacheSize++;
        return Promise.resolve('OK');
      });

      mockRedis.del.mockImplementation(() => {
        if (cacheSize > 0) {
          cacheSize--;
          evictions++;
        }
        return Promise.resolve(1);
      });

      // Fill cache beyond capacity
      for (let i = 0; i < 75; i++) {
        await redisLockService.set(`item-${i}`, { data: i }, 60000);

        // Occasionally trigger manual evictions
        if (i % 20 === 0 && i > 0) {
          await redisLockService.deleteKey(`old-item-${i - 20}`);
        }
      }

      expect(evictions).toBeGreaterThan(0);
      expect(cacheSize).toBeLessThanOrEqual(maxCacheSize);
    });
  });

  describe('Health Check Metrics', () => {
    it('should monitor service health and availability', async () => {
      const healthChecks = [];

      mockRedis.get.mockImplementation(() => {
        const responseTime = Math.random() * 100; // 0-100ms
        const isHealthy = responseTime < 80; // Healthy if under 80ms

        healthChecks.push({
          timestamp: Date.now(),
          responseTime,
          isHealthy,
          status: isHealthy ? 'healthy' : 'degraded',
        });

        if (isHealthy) {
          return Promise.resolve('"health-ok"');
        } else {
          return Promise.reject(new Error('Service degraded'));
        }
      });

      // Perform health checks
      await Promise.allSettled(
        Array.from({ length: 20 }, () => redisLockService.get('health-check')),
      );

      const healthyChecks = healthChecks.filter((hc) => hc.isHealthy).length;
      const degradedChecks = healthChecks.filter((hc) => !hc.isHealthy).length;
      const averageResponseTime =
        healthChecks.reduce((sum, hc) => sum + hc.responseTime, 0) /
        healthChecks.length;

      expect(healthChecks).toHaveLength(20);
      expect(healthyChecks).toBeGreaterThan(0);
      expect(averageResponseTime).toBeLessThan(100);

      // Calculate availability percentage
      const availability = (healthyChecks / healthChecks.length) * 100;
      expect(availability).toBeGreaterThan(0);
    });
  });
});
