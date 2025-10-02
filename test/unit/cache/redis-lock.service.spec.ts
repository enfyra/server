import { Test, TestingModule } from '@nestjs/testing';
import { CacheService } from '../../../src/infrastructure/cache/services/cache.service';
import { RedisService } from '@liaoliaots/nestjs-redis';

describe('CacheService', () => {
  let service: CacheService;
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

    service = module.get<CacheService>(CacheService);

    // Suppress console logs during tests
    jest.spyOn(console, 'log').mockImplementation();
  });

  afterEach(() => {
    jest.clearAllMocks();
  });

  describe('acquire', () => {
    it('should acquire lock successfully', async () => {
      mockRedis.set.mockResolvedValue('OK');
      mockRedis.pttl.mockResolvedValue(30000);

      const result = await service.acquire('test-key', 'test-value', 30000);

      expect(result).toBe(true);
      expect(mockRedis.set).toHaveBeenCalledWith(
        'test-key',
        'test-value',
        'PX',
        30000,
        'NX',
      );
    });

    it('should fail to acquire existing lock', async () => {
      mockRedis.set.mockResolvedValue(null);
      mockRedis.pttl.mockResolvedValue(15000);

      const result = await service.acquire('test-key', 'test-value', 30000);

      expect(result).toBe(false);
    });

    it('should handle complex objects', async () => {
      const complexValue = {
        id: 1,
        data: ['a', 'b'],
        nested: { key: 'value' },
      };
      mockRedis.set.mockResolvedValue('OK');
      mockRedis.pttl.mockResolvedValue(30000);

      const result = await service.acquire('test-key', complexValue, 30000);

      expect(result).toBe(true);
      expect(mockRedis.set).toHaveBeenCalledWith(
        'test-key',
        JSON.stringify(complexValue),
        'PX',
        30000,
        'NX',
      );
    });
  });

  describe('release', () => {
    it('should release lock successfully', async () => {
      mockRedis.eval.mockResolvedValue(1);

      const result = await service.release('test-key', 'test-value');

      expect(result).toBe(true);
      expect(mockRedis.eval).toHaveBeenCalled();
    });

    it('should fail to release non-matching lock', async () => {
      mockRedis.eval.mockResolvedValue(0);

      const result = await service.release('test-key', 'wrong-value');

      expect(result).toBe(false);
    });

    it('should handle complex objects in release', async () => {
      const complexValue = { id: 1, data: 'test' };
      mockRedis.eval.mockResolvedValue(1);

      const result = await service.release('test-key', complexValue);

      expect(result).toBe(true);
    });
  });

  describe('get', () => {
    it('should retrieve stored value', async () => {
      const testData = { id: 1, name: 'test' };
      mockRedis.get.mockResolvedValue(JSON.stringify(testData));

      const result = await service.get('test-key');

      expect(result).toEqual(testData);
    });

    it('should return null for non-existent key', async () => {
      mockRedis.get.mockResolvedValue(null);

      const result = await service.get('test-key');

      expect(result).toBeNull();
    });

    it('should handle string values', async () => {
      mockRedis.get.mockResolvedValue('simple-string');

      const result = await service.get('test-key');

      expect(result).toBe('simple-string');
    });

    it('should handle malformed JSON gracefully', async () => {
      mockRedis.get.mockResolvedValue('invalid-json{');

      const result = await service.get('test-key');

      expect(result).toBe('invalid-json{');
    });
  });

  describe('set', () => {
    it('should set value with TTL', async () => {
      mockRedis.set.mockResolvedValue('OK');
      mockRedis.pttl.mockResolvedValue(30000);

      await service.set('test-key', { data: 'test' }, 30000);

      expect(mockRedis.set).toHaveBeenCalledWith(
        'test-key',
        '{"data":"test"}',
        'PX',
        30000,
      );
    });

    it('should set value without TTL', async () => {
      mockRedis.set.mockResolvedValue('OK');

      await service.set('test-key', { data: 'test' }, 0);

      expect(mockRedis.set).toHaveBeenCalledWith('test-key', '{"data":"test"}');
    });
  });

  describe('exists', () => {
    it('should check if value exists and matches', async () => {
      const testValue = { id: 1 };
      mockRedis.get.mockResolvedValue(JSON.stringify(testValue));

      const result = await service.exists('test-key', testValue);

      expect(result).toBe(true);
    });

    it('should return false for non-matching values', async () => {
      mockRedis.get.mockResolvedValue('{"id":2}');

      const result = await service.exists('test-key', { id: 1 });

      expect(result).toBe(false);
    });

    it('should return false for non-existent keys', async () => {
      mockRedis.get.mockResolvedValue(null);

      const result = await service.exists('test-key', { id: 1 });

      expect(result).toBe(false);
    });
  });

  describe('deleteKey', () => {
    it('should delete key successfully', async () => {
      mockRedis.del.mockResolvedValue(1);

      await service.deleteKey('test-key');

      expect(mockRedis.del).toHaveBeenCalledWith('test-key');
    });
  });

  describe('setNoExpire', () => {
    it('should set value without expiration', async () => {
      mockRedis.set.mockResolvedValue('OK');

      await service.setNoExpire('test-key', { data: 'test' });

      expect(mockRedis.set).toHaveBeenCalledWith('test-key', '{"data":"test"}');
    });
  });

  describe('Error Handling', () => {
    it('should handle Redis connection errors', async () => {
      mockRedis.set.mockRejectedValue(new Error('Redis connection failed'));

      await expect(service.acquire('test-key', 'value', 30000)).rejects.toThrow(
        'Redis connection failed',
      );
    });

    it('should handle malformed Lua script responses', async () => {
      mockRedis.eval.mockResolvedValue('unexpected');

      const result = await service.release('test-key', 'value');

      expect(result).toBe(false);
    });
  });

  describe('Performance', () => {
    it('should handle high concurrency operations', async () => {
      mockRedis.set.mockResolvedValue('OK');
      mockRedis.pttl.mockResolvedValue(30000);

      const promises = Array.from({ length: 100 }, (_, i) =>
        service.acquire(`key-${i}`, `value-${i}`, 30000),
      );

      const results = await Promise.all(promises);

      expect(results.every((r) => r === true)).toBe(true);
      expect(mockRedis.set).toHaveBeenCalledTimes(100);
    });
  });
});
