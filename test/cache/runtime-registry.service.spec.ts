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
    expect(service.getActiveData(CACHE_IDENTIFIERS.FLOW)).toEqual([
      { id: 2, name: 'after-reload' },
    ]);
    expect(service.requireActiveData(CACHE_IDENTIFIERS.FLOW)).toEqual([
      { id: 2, name: 'after-reload' },
    ]);
  });

  it('fails clearly when active data is required before activation', () => {
    const service = new RuntimeRegistryService();

    expect(() => service.requireActiveData(CACHE_IDENTIFIERS.FLOW)).toThrow(
      'Runtime cache flow is not activated',
    );
  });

  it('publishes metadata caches through the metadata active-view API', async () => {
    const metadataCacheService = {
      getMetadata: vi.fn(async () => ({
        tables: new Map([['enfyra_user', { name: 'enfyra_user' }]]),
        tablesList: [{ name: 'enfyra_user' }],
        version: 1,
        timestamp: new Date('2026-07-02T00:00:00.000Z'),
      })),
    };
    const service = new RuntimeRegistryService();

    await service.publishFromCache(
      CACHE_IDENTIFIERS.METADATA,
      metadataCacheService,
    );

    expect(service.getSnapshot<any>(CACHE_IDENTIFIERS.METADATA)?.data).toEqual(
      expect.objectContaining({
        tablesList: [{ name: 'enfyra_user' }],
      }),
    );
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
