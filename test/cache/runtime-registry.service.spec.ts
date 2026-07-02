import { describe, expect, it, vi } from 'vitest';
import { RuntimeRegistryService } from '../../src/engines/cache';
import { CACHE_IDENTIFIERS } from '../../src/shared/utils/cache-events.constants';

describe('RuntimeRegistryService', () => {
  it('publishes an activated snapshot from a cache service', async () => {
    const service = new RuntimeRegistryService();

    await service.publishFromCache(CACHE_IDENTIFIERS.FLOW, {
      getCacheAsync: vi.fn(async () => [{ id: 1, name: 'demo' }]),
    });

    expect(service.getSnapshot(CACHE_IDENTIFIERS.FLOW)).toEqual(
      expect.objectContaining({
        identifier: CACHE_IDENTIFIERS.FLOW,
        version: 1,
        data: [{ id: 1, name: 'demo' }],
      }),
    );
  });

  it('publishes only after the caller supplies the rebuilt cache view', async () => {
    const flowCacheService = {
      getCacheAsync: vi.fn(async () => [{ id: 2, name: 'after-reload' }]),
    };
    const service = new RuntimeRegistryService();

    service.init();
    await service.publishFromCache(CACHE_IDENTIFIERS.FLOW, flowCacheService);

    expect(service.getSnapshot(CACHE_IDENTIFIERS.FLOW)?.data).toEqual([
      { id: 2, name: 'after-reload' },
    ]);
  });

  it('records failed publish attempts explicitly', async () => {
    const service = new RuntimeRegistryService();

    await expect(
      service.publishFromCache(CACHE_IDENTIFIERS.PACKAGE, {
        getCacheAsync: vi.fn(async () => {
          throw new Error('cache unavailable');
        }),
      }),
    ).rejects.toThrow('cache unavailable');

    expect(service.getEntry(CACHE_IDENTIFIERS.PACKAGE)).toEqual(
      expect.objectContaining({
        status: 'failed',
        error: 'cache unavailable',
      }),
    );
  });
});
