import { describe, expect, it, vi } from 'vitest';
import { UserRevocationService } from '../../src/domain/auth/services/user-revocation.service';

describe('UserRevocationService', () => {
  it('writes an authorization revision before publishing invalidation', async () => {
    const order: string[] = [];
    const cacheService = {
      set: vi.fn(async () => {
        order.push('revision');
      }),
    };
    const redisPubSubService = {
      publish: vi.fn(async () => {
        order.push('publish');
      }),
    };
    const service = new UserRevocationService({
      cacheService: cacheService as any,
      redisPubSubService: redisPubSubService as any,
      queryBuilderService: {} as any,
    });

    await service.publish('user-1');

    expect(order).toEqual(['revision', 'publish']);
    expect(cacheService.set).toHaveBeenCalledWith(
      'auth:user:user-1:revision',
      expect.any(String),
      365 * 24 * 60 * 60 * 1_000,
    );
    expect(redisPubSubService.publish).toHaveBeenCalledWith('user:revoked', {
      userId: 'user-1',
    });
  });
});
