import { EventEmitter2 } from 'eventemitter2';
import { describe, expect, it, vi } from 'vitest';
import {
  FlowCacheService,
  PackageCacheService,
  WebsocketCacheService,
} from '../../src/engines/cache';
import { CACHE_IDENTIFIERS } from '../../src/shared/utils/cache-events.constants';

function registrySnapshot(identifier: string, data: unknown) {
  return {
    getSnapshot: vi.fn((requested: string) =>
      requested === identifier
        ? {
            identifier,
            version: 1,
            activatedAt: '2026-07-02T00:00:00.000Z',
            data,
          }
        : undefined,
    ),
  };
}

describe('runtime registry read bridge', () => {
  it('serves package names from the active registry snapshot', async () => {
    const queryBuilderService = {
      find: vi.fn(async () => {
        throw new Error('DB should not be read');
      }),
    };
    const service = new PackageCacheService({
      queryBuilderService: queryBuilderService as any,
      eventEmitter: new EventEmitter2(),
      packageCdnLoaderService: {} as any,
      runtimeRegistryService: registrySnapshot(CACHE_IDENTIFIERS.PACKAGE, [
        'lodash',
      ]) as any,
      lazyRef: {} as any,
    });

    await expect(service.getPackages()).resolves.toEqual(['lodash']);
    expect(queryBuilderService.find).not.toHaveBeenCalled();
  });

  it('serves flow lookups from the active registry snapshot', async () => {
    const queryBuilderService = {
      find: vi.fn(async () => {
        throw new Error('DB should not be read');
      }),
    };
    const service = new FlowCacheService({
      queryBuilderService: queryBuilderService as any,
      eventEmitter: new EventEmitter2(),
      runtimeRegistryService: registrySnapshot(CACHE_IDENTIFIERS.FLOW, [
        {
          id: 7,
          name: 'scheduled-flow',
          triggerType: 'schedule',
          steps: [],
        },
      ]) as any,
    });

    await expect(service.getFlowByName('scheduled-flow')).resolves.toEqual(
      expect.objectContaining({ id: 7 }),
    );
    expect(queryBuilderService.find).not.toHaveBeenCalled();
  });

  it('serves websocket gateway lookups from the active registry snapshot', async () => {
    const queryBuilderService = {
      find: vi.fn(async () => {
        throw new Error('DB should not be read');
      }),
    };
    const service = new WebsocketCacheService({
      queryBuilderService: queryBuilderService as any,
      eventEmitter: new EventEmitter2(),
      runtimeRegistryService: registrySnapshot(CACHE_IDENTIFIERS.WEBSOCKET, [
        {
          id: 3,
          path: '/chat',
          isEnabled: true,
          events: [],
        },
      ]) as any,
    });

    await expect(service.getGatewayByPath('/chat')).resolves.toEqual(
      expect.objectContaining({ id: 3 }),
    );
    expect(queryBuilderService.find).not.toHaveBeenCalled();
  });
});
