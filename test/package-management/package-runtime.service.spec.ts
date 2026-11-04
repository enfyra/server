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
  it('omits preload history until a preload has started', () => {
    const { service } = createRuntime();
    expect(service.getStatus()).not.toHaveProperty('lastPreload');
  });

  it('preloads installed packages before system ready and defers recovery packages', async () => {
    const { eventEmitter, packageCdnLoaderService, service } = createRuntime({
      packages: [
        { id: 1, name: 'lodash', version: '4.17.21', status: 'installed' },
        { id: 2, name: 'node-ssh', version: '13.2.1', status: 'failed' },
      ],
    });

    await service.init();

    await vi.waitFor(() => {
      expect(packageCdnLoaderService.loadPackage).toHaveBeenCalledWith(
        'lodash',
        '4.17.21',
      );
    });
    expect(packageCdnLoaderService.loadPackage).not.toHaveBeenCalledWith(
      'node-ssh',
      '13.2.1',
    );

    eventEmitter.emit(CACHE_EVENTS.SYSTEM_READY);

    await vi.waitFor(() => {
      expect(packageCdnLoaderService.loadPackage).toHaveBeenCalledWith(
        'node-ssh',
        '13.2.1',
      );
    });
    expect(service.getStatus().lastPreload).toEqual(
      expect.objectContaining({ status: 'ok' }),
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

    await service.init();
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

  it('invalidates the package cache when a recovered package rejoins the installed set', async () => {
    const invalidations: any[] = [];
    const { eventEmitter, service } = createRuntime({
      packages: [
        { id: 1, name: 'undici', version: '8.10.0', status: 'failed' },
      ],
    });
    eventEmitter.on(CACHE_EVENTS.INVALIDATE, (payload: any) => {
      invalidations.push(payload);
    });

    await service.init();
    await vi.waitFor(() => {
      expect(service.getStatus().preloadRunning).toBe(false);
      expect(service.getStatus().lastPreload?.completedAt).toBeDefined();
    });
    expect(invalidations).toEqual([]);

    eventEmitter.emit(CACHE_EVENTS.SYSTEM_READY);

    await vi.waitFor(() => {
      expect(service.getStatus().lastPreload).toEqual(
        expect.objectContaining({ status: 'ok', loaded: 1 }),
      );
    });
    expect(invalidations).toEqual([
      expect.objectContaining({
        table: 'enfyra_package',
        action: 'reload',
        scope: 'full',
      }),
    ]);
  });

  it('invalidates the package cache when an installed package drops out', async () => {
    const invalidations: any[] = [];
    const { eventEmitter, packageCdnLoaderService, service } = createRuntime({
      packages: [
        { id: 1, name: 'lodash', version: '4.17.21', status: 'installed' },
      ],
    });
    eventEmitter.on(CACHE_EVENTS.INVALIDATE, (payload: any) => {
      invalidations.push(payload);
    });

    await service.init();
    await vi.waitFor(() => {
      expect(service.getStatus().preloadRunning).toBe(false);
      expect(service.getStatus().lastPreload?.completedAt).toBeDefined();
    });
    packageCdnLoaderService.loadPackage.mockRejectedValue(
      new Error('cdn missing'),
    );
    eventEmitter.emit(CACHE_EVENTS.RUNTIME_CACHE_ACTIVATED, {
      identifier: CACHE_IDENTIFIERS.PACKAGE,
    });

    await vi.waitFor(() => {
      expect(invalidations.length).toBeGreaterThan(0);
    });
    expect(invalidations[0]).toEqual(
      expect.objectContaining({ table: 'enfyra_package', scope: 'full' }),
    );
  });

  it('does not invalidate the package cache when the installed set is unchanged', async () => {
    const invalidations: any[] = [];
    const { eventEmitter, service } = createRuntime({
      packages: [
        { id: 1, name: 'lodash', version: '4.17.21', status: 'installed' },
      ],
    });
    eventEmitter.on(CACHE_EVENTS.INVALIDATE, (payload: any) => {
      invalidations.push(payload);
    });

    await service.init();
    await vi.waitFor(() => {
      expect(service.getStatus().lastPreload).toEqual(
        expect.objectContaining({ status: 'ok', loaded: 1 }),
      );
    });

    expect(invalidations).toEqual([]);
  });
});
