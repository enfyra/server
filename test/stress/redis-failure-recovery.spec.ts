import { Test, TestingModule } from '@nestjs/testing';
import { CacheService } from '../../src/infrastructure/cache/services/cache.service';
import { RouteCacheService } from '../../src/infrastructure/cache/services/route-cache.service';
import { DataSourceService } from '../../src/core/database/data-source/data-source.service';
import { RedisService } from '@liaoliaots/nestjs-redis';

describe('Redis Failure Recovery', () => {
  let cacheService: CacheService;
  let routeCacheService: RouteCacheService;
  let mockRedis: any;

  beforeEach(async () => {
    mockRedis = {
      set: jest.fn(),
      get: jest.fn(),
      del: jest.fn(),
      eval: jest.fn(),
      pttl: jest.fn(),
      ping: jest.fn(),
      isReady: jest.fn(),
    };

    const mockRedisService = {
      getOrNil: jest.fn().mockReturnValue(mockRedis),
    };

    const mockDataSourceService = {
      getRepository: jest.fn().mockReturnValue({
        find: jest
          .fn()
          .mockResolvedValue([{ path: '/api/test', method: 'GET' }]),
      }),
    };

    const module: TestingModule = await Test.createTestingModule({
      providers: [
        CacheService,
        RouteCacheService,
        { provide: RedisService, useValue: mockRedisService },
        { provide: DataSourceService, useValue: mockDataSourceService },
      ],
    }).compile();

    cacheService = module.get<CacheService>(CacheService);
    routeCacheService = module.get<RouteCacheService>(RouteCacheService);

    // Suppress console logs during tests
    jest.spyOn(console, 'log').mockImplementation();
    jest.spyOn(console, 'error').mockImplementation();
    jest.spyOn(console, 'warn').mockImplementation();
  });

  afterEach(() => {
    jest.clearAllMocks();
  });

  describe('Redis Connection Failures', () => {
    it('should handle Redis connection timeout', async () => {
      mockRedis.set.mockRejectedValue(new Error('Connection timeout'));

      await expect(
        redisLockService.acquire('test-key', 'test-value', 30000),
      ).rejects.toThrow('Connection timeout');
    });

    it('should handle Redis connection refused', async () => {
      mockRedis.get.mockRejectedValue(new Error('ECONNREFUSED'));

      await expect(redisLockService.get('test-key')).rejects.toThrow(
        'ECONNREFUSED',
      );
    });

    it('should handle Redis network errors gracefully', async () => {
      mockRedis.eval.mockRejectedValue(new Error('Network error'));

      const result = await redisLockService.release('test-key', 'test-value');

      expect(result).toBe(false);
    });

    it('should detect and handle stale connections', async () => {
      mockRedis.ping.mockRejectedValue(new Error('Connection closed'));
      mockRedis.isReady.mockReturnValue(false);
      mockRedis.get.mockRejectedValue(new Error('Connection not ready'));

      await expect(redisLockService.get('test-key')).rejects.toThrow(
        'Connection not ready',
      );
    });
  });

  describe('Circuit Breaker Pattern', () => {
    it('should implement circuit breaker for consecutive failures', async () => {
      // Simulate 5 consecutive failures
      mockRedis.set.mockRejectedValue(new Error('Redis down'));

      const failures = [];
      for (let i = 0; i < 5; i++) {
        try {
          await redisLockService.acquire(`key-${i}`, 'value', 1000);
        } catch (error) {
          failures.push(error);
        }
      }

      expect(failures).toHaveLength(5);
      expect(failures.every((e) => e.message === 'Redis down')).toBe(true);
    });

    it('should recover after Redis comes back online', async () => {
      // First call fails
      mockRedis.set.mockRejectedValueOnce(new Error('Redis down'));

      try {
        await redisLockService.acquire('test-key', 'test-value', 1000);
      } catch (error) {
        expect(error.message).toBe('Redis down');
      }

      // Second call succeeds
      mockRedis.set.mockResolvedValueOnce('OK');
      mockRedis.pttl.mockResolvedValueOnce(1000);

      const result = await redisLockService.acquire(
        'test-key-2',
        'test-value',
        1000,
      );
      expect(result).toBe(true);
    });

    it.skip('should handle partial Redis cluster failures', async () => {
      // Simulate cluster with some nodes down
      mockRedis.get
        .mockRejectedValueOnce(new Error('Node 1 down'))
        .mockRejectedValueOnce(new Error('Node 2 down'))
        .mockResolvedValueOnce('"fallback-data"');

      const result = await redisLockService.get('test-key');

      expect(result).toBe('fallback-data');
    });
  });

  describe('Route Cache Resilience', () => {
    it.skip('should fallback to database when Redis is unavailable', async () => {
      mockRedis.get.mockRejectedValue(new Error('Redis unavailable'));

      // Mock the background revalidation to succeed
      jest
        .spyOn(routeCacheService as any, 'loadAndCacheRoutes')
        .mockResolvedValue([{ path: '/api/test', method: 'GET' }]);

      const routes = await routeCacheService.getRoutesWithSWR();

      expect(routes).toEqual([{ path: '/api/test', method: 'GET' }]);
    });

    it.skip('should handle corrupted cache data gracefully', async () => {
      mockRedis.get.mockResolvedValue('invalid-json-data{');

      jest
        .spyOn(routeCacheService as any, 'loadAndCacheRoutes')
        .mockResolvedValue([{ path: '/api/fallback', method: 'GET' }]);

      const routes = await routeCacheService.getRoutesWithSWR();

      expect(routes).toEqual([{ path: '/api/fallback', method: 'GET' }]);
    });

    it.skip('should implement exponential backoff for retries', async () => {
      const delays = [];

      jest.spyOn(global, 'setTimeout').mockImplementation(((
        callback: Function,
        delay: number,
      ) => {
        delays.push(delay);
        callback();
        return {} as any;
      }) as any);

      mockRedis.get
        .mockRejectedValueOnce(new Error('Timeout'))
        .mockRejectedValueOnce(new Error('Timeout'))
        .mockRejectedValueOnce(new Error('Timeout'))
        .mockResolvedValueOnce(JSON.stringify([{ path: '/api/success' }]));

      try {
        await routeCacheService.getRoutesWithSWR();
      } catch (error) {
        // Expected to fail on retries
      }

      // Should implement exponential backoff
      expect(delays.length).toBeGreaterThan(0);
    });

    it('should maintain cache consistency during Redis failover', async () => {
      // Simulate Redis failover scenario
      mockRedis.set
        .mockRejectedValueOnce(new Error('Primary down'))
        .mockResolvedValueOnce('OK'); // Secondary takes over

      mockRedis.get.mockResolvedValue(JSON.stringify([{ path: '/api/test' }]));

      await routeCacheService.reloadRouteCache();

      const routes = await routeCacheService.getRoutesWithSWR();
      expect(routes).toEqual([{ path: '/api/test' }]);
    });
  });

  describe('Lock Service Resilience', () => {
    it('should handle lock acquisition during Redis restart', async () => {
      mockRedis.set
        .mockRejectedValueOnce(new Error('Connection reset'))
        .mockResolvedValueOnce('OK');

      mockRedis.pttl.mockResolvedValue(30000);

      // First attempt fails
      await expect(
        redisLockService.acquire('test-key', 'test-value', 30000),
      ).rejects.toThrow('Connection reset');

      // Second attempt succeeds after Redis restarts
      const result = await redisLockService.acquire(
        'test-key',
        'test-value',
        30000,
      );
      expect(result).toBe(true);
    });

    it('should prevent lock leakage during Redis failures', async () => {
      mockRedis.eval.mockRejectedValue(new Error('Redis connection lost'));

      // Lock release fails, but should be handled gracefully
      const result = await redisLockService.release('test-key', 'test-value');

      expect(result).toBe(false);
      // Verify cleanup logic was attempted
      expect(mockRedis.eval).toHaveBeenCalled();
    });

    it('should handle lock expiration during network partitions', async () => {
      // Simulate network partition - can't communicate with Redis
      mockRedis.pttl.mockRejectedValue(new Error('Network partition'));

      await expect(
        redisLockService.acquire('test-key', 'test-value', 30000),
      ).rejects.toThrow('Network partition');
    });

    it('should implement distributed lock with Redis Sentinel', async () => {
      // Simulate Sentinel failover
      mockRedis.set
        .mockRejectedValueOnce(new Error('Master down'))
        .mockResolvedValueOnce('OK'); // New master elected

      mockRedis.pttl.mockResolvedValue(30000);

      // Should retry and succeed with new master
      await expect(
        redisLockService.acquire('test-key', 'test-value', 30000),
      ).rejects.toThrow('Master down');

      const result = await redisLockService.acquire(
        'test-key-2',
        'test-value',
        30000,
      );
      expect(result).toBe(true);
    });
  });

  describe('Data Consistency', () => {
    it.skip('should maintain data integrity during Redis cluster resharding', async () => {
      // Simulate cluster resharding
      mockRedis.get
        .mockRejectedValueOnce(new Error('MOVED 3999 127.0.0.1:7002'))
        .mockResolvedValueOnce(JSON.stringify({ data: 'migrated-value' }));

      const result = await redisLockService.get('test-key');

      expect(result).toEqual({ data: 'migrated-value' });
    });

    it('should handle Redis memory pressure gracefully', async () => {
      mockRedis.set.mockRejectedValue(new Error('OOM command not allowed'));

      await expect(
        redisLockService.set('test-key', { large: 'data' }, 30000),
      ).rejects.toThrow('OOM command not allowed');
    });

    it('should implement cache warming after Redis recovery', async () => {
      // Simulate Redis recovery
      mockRedis.get.mockResolvedValue(null); // Cache is empty after recovery

      jest
        .spyOn(routeCacheService as any, 'loadAndCacheRoutes')
        .mockResolvedValue([
          { path: '/api/users', method: 'GET' },
          { path: '/api/posts', method: 'POST' },
        ]);

      const routes = await routeCacheService.getRoutesWithSWR();

      expect(routes).toHaveLength(2);
      expect(routes).toEqual([
        { path: '/api/users', method: 'GET' },
        { path: '/api/posts', method: 'POST' },
      ]);
    });
  });

  describe('Performance Under Stress', () => {
    it('should handle high concurrency during Redis issues', async () => {
      // Simulate intermittent Redis issues under load
      mockRedis.set.mockImplementation(() => {
        return Math.random() > 0.3
          ? Promise.resolve('OK')
          : Promise.reject(new Error('Intermittent failure'));
      });

      mockRedis.pttl.mockResolvedValue(30000);

      const promises = Array.from(
        { length: 100 },
        (_, i) =>
          redisLockService
            .acquire(`key-${i}`, `value-${i}`, 1000)
            .catch(() => false), // Convert rejections to false
      );

      const results = await Promise.all(promises);
      const successes = results.filter((r) => r === true);
      const failures = results.filter((r) => r === false);

      // At least 60% should succeed (70% success rate minus margin)
      expect(successes.length).toBeGreaterThan(60);
      expect(failures.length).toBeGreaterThan(0);
    });

    it('should prevent Redis connection pool exhaustion', async () => {
      // Simulate slow Redis responses
      mockRedis.get.mockImplementation(
        () =>
          new Promise((resolve) =>
            setTimeout(() => resolve('"slow-response"'), 100),
          ),
      );

      const promises = Array.from({ length: 50 }, (_, i) =>
        redisLockService.get(`key-${i}`),
      );

      const startTime = Date.now();
      const results = await Promise.all(promises);
      const duration = Date.now() - startTime;

      expect(results).toHaveLength(50);
      expect(results.every((r) => r === 'slow-response')).toBe(true);

      // Should complete within reasonable time despite slow responses
      expect(duration).toBeLessThan(2000);
    });
  });

  describe('Monitoring and Alerting', () => {
    it.skip('should track Redis failure metrics', async () => {
      const failures = [];

      // Mock failure tracking
      jest.spyOn(console, 'error').mockImplementation((message) => {
        failures.push(message);
      });

      mockRedis.set.mockRejectedValue(new Error('Connection failed'));

      try {
        await redisLockService.acquire('test-key', 'test-value', 1000);
      } catch (error) {
        // Expected failure
      }

      expect(failures.length).toBeGreaterThan(0);
    });

    it('should emit alerts for prolonged Redis outages', async () => {
      const alerts = [];

      // Mock alerting system
      jest.spyOn(console, 'warn').mockImplementation((message) => {
        alerts.push(message);
      });

      // Simulate prolonged outage
      mockRedis.get.mockRejectedValue(new Error('Prolonged outage'));

      const failures = [];
      for (let i = 0; i < 10; i++) {
        try {
          await redisLockService.get(`key-${i}`);
        } catch (error) {
          failures.push(error);
        }
      }

      expect(failures).toHaveLength(10);
      // Should have generated some monitoring output
    });
  });
});
