import { describe, expect, it, vi } from 'vitest';
import { RateLimitService } from '../../src/engines/cache';

describe('RateLimitService', () => {
  it('keeps blocked rate-limit keys on a Redis TTL', async () => {
    const redis = {
      eval: vi.fn(async () => [0, 0, Date.now() + 1000, 1]),
    };
    const service = new RateLimitService({
      redis: redis as any,
      envService: { get: () => 'app-a' } as any,
    });

    await service.check('route:/orders', { maxRequests: 1, perSeconds: 60 });

    const script = redis.eval.mock.calls[0]?.[0] as string;
    expect(script).toContain(
      "else\n      redis.call('PEXPIRE', key, window * 1000)",
    );
  });

  it('repairs lifecycle TTL when reading rate-limit status', async () => {
    const operations: Array<{ name: string; args: any[] }> = [];
    const pipeline = {
      zremrangebyscore: vi.fn((...args: any[]) => {
        operations.push({ name: 'zremrangebyscore', args });
        return pipeline;
      }),
      zcard: vi.fn((...args: any[]) => {
        operations.push({ name: 'zcard', args });
        return pipeline;
      }),
      zrange: vi.fn((...args: any[]) => {
        operations.push({ name: 'zrange', args });
        return pipeline;
      }),
      pexpire: vi.fn((...args: any[]) => {
        operations.push({ name: 'pexpire', args });
        return pipeline;
      }),
      exec: vi.fn(async () => [
        [null, 0],
        [null, 1],
        [null, ['member', String(Date.now())]],
        [null, 1],
      ]),
    };
    const service = new RateLimitService({
      redis: { pipeline: () => pipeline } as any,
      envService: { get: () => 'app-a' } as any,
    });

    await service.status('route:/orders', {
      maxRequests: 5,
      perSeconds: 60,
    });

    expect(pipeline.pexpire).toHaveBeenCalledWith(
      'app-a:rl:route:/orders',
      60000,
    );
    expect(operations.map((item) => item.name)).toEqual([
      'zremrangebyscore',
      'zcard',
      'zrange',
      'pexpire',
    ]);
  });
});
