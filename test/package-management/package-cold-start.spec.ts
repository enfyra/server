import { EventEmitter2 } from 'eventemitter2';
import { describe, expect, it, vi } from 'vitest';
import { PackageRuntimeService } from '../../src/engines/cache';
import { CACHE_EVENTS } from '../../src/shared/utils/cache-events.constants';

function createRuntime(packages: any[]) {
  const eventEmitter = new EventEmitter2();
  const queryBuilderService = {
    find: vi.fn(async (options: any) => ({
      data: options.limit === -1 ? packages : packages.slice(0, 10),
    })),
    update: vi.fn(async () => undefined),
  };
  const packageCdnLoaderService = {
    isLoaded: vi.fn(() => false),
    loadPackage: vi.fn(async (_name: string, _version: string) => undefined),
  };
  const service = new PackageRuntimeService({
    eventEmitter,
    queryBuilderService: queryBuilderService as any,
    packageCdnLoaderService: packageCdnLoaderService as any,
    lazyRef: { dynamicWebSocketGateway: {} } as any,
  });
  return {
    service,
    eventEmitter,
    queryBuilderService,
    packageCdnLoaderService,
  };
}

const installedPackage = {
  id: 1,
  name: 'cold-start-package',
  version: '1.0.0',
  status: 'installed',
};

describe('package cold-start readiness', () => {
  it('awaits installed artifacts before initialization can finish', async () => {
    const { service, packageCdnLoaderService } = createRuntime([
      installedPackage,
    ]);
    let release!: () => void;
    const blocked = new Promise<void>((resolve) => {
      release = resolve;
    });
    packageCdnLoaderService.loadPackage.mockImplementation(async () => blocked);
    let initialized = false;
    const pending = service.init();
    expect(service.init()).toBe(pending);
    const initialization = pending.then(() => {
      initialized = true;
    });
    try {
      await vi.waitFor(() => {
        expect(packageCdnLoaderService.loadPackage).toHaveBeenCalled();
      });
      expect(initialized).toBe(false);
    } finally {
      release();
      await initialization;
    }
    expect(initialized).toBe(true);
  });

  it('uses an existing artifact without fetching or writing shared status', async () => {
    const { service, queryBuilderService, packageCdnLoaderService } =
      createRuntime([installedPackage]);
    packageCdnLoaderService.isLoaded.mockReturnValue(true);
    await service.init();
    expect(packageCdnLoaderService.loadPackage).not.toHaveBeenCalled();
    expect(queryBuilderService.update).not.toHaveBeenCalled();
    expect(service.getStatus()).toMatchObject({
      preloadRunning: false,
      lastPreload: { status: 'ok' },
    });
  });

  it('propagates the initial package query failure', async () => {
    const { service, queryBuilderService } = createRuntime([installedPackage]);
    const failure = new Error('package query unavailable');
    queryBuilderService.find.mockRejectedValue(failure);
    await expect(service.init()).rejects.toBe(failure);
    expect(service.getStatus()).toMatchObject({
      preloadRunning: false,
      lastPreload: { status: 'degraded' },
    });
  });

  it('rejects startup on an unavailable installed package without changing shared membership', async () => {
    const {
      service,
      eventEmitter,
      queryBuilderService,
      packageCdnLoaderService,
    } = createRuntime([installedPackage]);
    packageCdnLoaderService.loadPackage.mockRejectedValue(
      new Error('cdn unavailable'),
    );
    const invalidation = vi.fn();
    eventEmitter.on(CACHE_EVENTS.INVALIDATE, invalidation);
    const initialization = Promise.resolve(service.init());
    await expect(initialization).rejects.toThrow(/cold-start-package/);
    expect(queryBuilderService.update).not.toHaveBeenCalled();
    expect(invalidation).not.toHaveBeenCalled();
    expect(service.getStatus()).toMatchObject({
      preloadRunning: false,
      lastPreload: { status: 'degraded' },
    });
  });

  it('prepares every enabled installed package beyond the default query page', async () => {
    const packages = Array.from({ length: 12 }, (_, index) => ({
      ...installedPackage,
      id: index + 1,
      name: `cold-start-package-${index}`,
    }));
    const { service, packageCdnLoaderService } = createRuntime(packages);
    await service.init();
    await vi.waitFor(() => {
      expect(service.getStatus().lastPreload?.status).toBe('ok');
    });
    expect(packageCdnLoaderService.loadPackage).toHaveBeenCalledTimes(12);
  });

  it('repairs a recovery status when the artifact already exists without fetching it again', async () => {
    const {
      service,
      eventEmitter,
      queryBuilderService,
      packageCdnLoaderService,
    } = createRuntime([{ ...installedPackage, status: 'failed' }]);
    packageCdnLoaderService.isLoaded.mockReturnValue(true);
    const invalidation = vi.fn();
    eventEmitter.on(CACHE_EVENTS.INVALIDATE, invalidation);
    await service.init();
    await vi.waitFor(() => {
      expect(service.getStatus().lastPreload?.status).toBe('ok');
    });
    eventEmitter.emit(CACHE_EVENTS.SYSTEM_READY);
    await vi.waitFor(() => {
      expect(queryBuilderService.update).toHaveBeenCalledWith(
        'enfyra_package',
        1,
        { status: 'installed', lastError: null },
      );
      expect(service.getStatus().lastPreload?.status).toBe('ok');
    });
    expect(service.getStatus().systemReady).toBe(true);
    expect(packageCdnLoaderService.loadPackage).not.toHaveBeenCalled();
    expect(invalidation).toHaveBeenCalledTimes(1);
  });
});
