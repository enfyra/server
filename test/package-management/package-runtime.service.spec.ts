import { EventEmitter2 } from 'eventemitter2';
import { describe, expect, it, vi } from 'vitest';
import { PackageRuntimeService } from '../../src/engines/cache';
import {
  CACHE_EVENTS,
  CACHE_IDENTIFIERS,
} from '../../src/shared/utils/cache-events.constants';

function createRuntime(options?: {
  packages?: any[];
  loadPackage?: (name: string, version: string) => Promise<void>;
}) {
  const eventEmitter = new EventEmitter2();
  const queryBuilderService = {
    find: vi.fn(async () => ({ data: options?.packages ?? [] })),
    update: vi.fn(async () => undefined),
  };
  const packageCdnLoaderService = {
    isLoaded: vi.fn(() => false),
    loadPackage: vi.fn(options?.loadPackage ?? (async () => undefined)),
  };
  const dynamicWebSocketGateway = {
    server: {},
    emitToNamespace: vi.fn(),
  };
  const service = new PackageRuntimeService({
    eventEmitter,
    queryBuilderService: queryBuilderService as any,
    packageCdnLoaderService: packageCdnLoaderService as any,
    lazyRef: { dynamicWebSocketGateway } as any,
  });

  return {
    eventEmitter,
    queryBuilderService,
    packageCdnLoaderService,
    dynamicWebSocketGateway,
    service,
  };
}

describe('PackageRuntimeService', () => {
  it('waits for system ready before preloading packages after cache publish', async () => {
    const { eventEmitter, packageCdnLoaderService, service } = createRuntime({
      packages: [
        { id: 1, name: 'lodash', version: '4.17.21', status: 'installed' },
      ],
    });

    service.init();
    eventEmitter.emit(CACHE_EVENTS.RUNTIME_CACHE_ACTIVATED, {
      identifier: CACHE_IDENTIFIERS.PACKAGE,
    });
    expect(packageCdnLoaderService.loadPackage).not.toHaveBeenCalled();

    eventEmitter.emit(CACHE_EVENTS.SYSTEM_READY);
    eventEmitter.emit(CACHE_EVENTS.RUNTIME_CACHE_ACTIVATED, {
      identifier: CACHE_IDENTIFIERS.PACKAGE,
    });

    await vi.waitFor(() => {
      expect(packageCdnLoaderService.loadPackage).toHaveBeenCalledWith(
        'lodash',
        '4.17.21',
      );
    });
    expect(service.getStatus().lastPreload).toEqual(
      expect.objectContaining({ status: 'ok', loaded: 1, failed: 0 }),
    );
  });

  it('marks package preload as degraded when a package fails to load', async () => {
    const { eventEmitter, queryBuilderService, service } = createRuntime({
      packages: [
        { id: 2, name: 'broken-package', version: '1.0.0', status: 'failed' },
      ],
      loadPackage: async () => {
        throw new Error('cdn missing');
      },
    });

    service.init();
    eventEmitter.emit(CACHE_EVENTS.SYSTEM_READY);
    eventEmitter.emit(CACHE_EVENTS.RUNTIME_CACHE_ACTIVATED, {
      identifier: CACHE_IDENTIFIERS.PACKAGE,
    });

    await vi.waitFor(() => {
      expect(service.getStatus().lastPreload).toEqual(
        expect.objectContaining({ status: 'degraded', loaded: 0, failed: 1 }),
      );
    });
    expect(queryBuilderService.update).toHaveBeenCalledWith(
      'enfyra_package',
      2,
      expect.objectContaining({ status: 'failed' }),
    );
  });
});
