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
  return { service, eventEmitter, queryBuilderService, packageCdnLoaderService };
}

const installedPackage = {
  id: 1,
  name: 'cold-start-package',
  version: '1.0.0',
  status: 'installed',
};

async function waitForPreload(service: PackageRuntimeService) {
  await vi.waitFor(() => {
    expect(service.getStatus().preloadRunning).toBe(false);
    expect(service.getStatus().lastPreload?.completedAt).toBeDefined();
  });
}

describe('non-blocking parallel package preload', () => {
  it('returns immediately and starts installed downloads before system ready', async () => {
    const { service, packageCdnLoaderService } = createRuntime([installedPackage]);
    let release!: () => void;
    const blocked = new Promise<void>((resolve) => { release = resolve; });
    packageCdnLoaderService.loadPackage.mockImplementation(async () => blocked);
    let initialized = false;
    const initialization = Promise.resolve(service.init()).then(() => { initialized = true; });
    try {
      await vi.waitFor(() => expect(packageCdnLoaderService.loadPackage).toHaveBeenCalledTimes(1));
      expect(initialized).toBe(true);
      expect(service.getStatus()).toMatchObject({ systemReady: false, preloadRunning: true });
    } finally {
      release();
      await initialization;
      await waitForPreload(service);
    }
  });

  it('does not wait for the metadata query and initializes listeners only once', async () => {
    const { service, queryBuilderService, eventEmitter } = createRuntime([]);
    let release!: () => void;
    const blocked = new Promise<void>((resolve) => { release = resolve; });
    queryBuilderService.find.mockImplementation(async () => { await blocked; return { data: [] }; });
    let initialized = false;
    const initialization = Promise.resolve(service.init()).then(() => { initialized = true; });
    service.init();
    try {
      await vi.waitFor(() => expect(queryBuilderService.find).toHaveBeenCalledTimes(1));
      expect(initialized).toBe(true);
      expect(eventEmitter.listenerCount(CACHE_EVENTS.SYSTEM_READY)).toBe(1);
      expect(eventEmitter.listenerCount(CACHE_EVENTS.RUNTIME_CACHE_ACTIVATED)).toBe(1);
    } finally {
      release();
      await initialization;
      await waitForPreload(service);
    }
  });

  it('uses an existing artifact without fetching or writing shared status', async () => {
    const { service, queryBuilderService, packageCdnLoaderService } = createRuntime([installedPackage]);
    packageCdnLoaderService.isLoaded.mockReturnValue(true);
    service.init();
    await waitForPreload(service);
    expect(packageCdnLoaderService.loadPackage).not.toHaveBeenCalled();
    expect(queryBuilderService.update).not.toHaveBeenCalled();
    expect(service.getStatus().lastPreload?.status).toBe('ok');
  });

  it('reports a background query failure without rejecting initialization', async () => {
    const { service, queryBuilderService } = createRuntime([installedPackage]);
    queryBuilderService.find.mockRejectedValue(new Error('package query unavailable'));
    await expect(Promise.resolve(service.init())).resolves.toBeUndefined();
    await waitForPreload(service);
    expect(service.getStatus().lastPreload).toMatchObject({ status: 'degraded', error: 'package query unavailable' });
  });

  it('reports unavailable installed packages independently of boot', async () => {
    const { service, eventEmitter, queryBuilderService, packageCdnLoaderService } = createRuntime([installedPackage]);
    packageCdnLoaderService.loadPackage.mockRejectedValue(new Error('cdn unavailable'));
    const invalidation = vi.fn();
    eventEmitter.on(CACHE_EVENTS.INVALIDATE, invalidation);
    await expect(Promise.resolve(service.init())).resolves.toBeUndefined();
    await waitForPreload(service);
    expect(queryBuilderService.update).toHaveBeenCalledWith('enfyra_package', 1, { status: 'failed', lastError: 'cdn unavailable' });
    expect(invalidation).toHaveBeenCalledTimes(1);
    expect(service.getStatus().lastPreload).toMatchObject({ status: 'degraded', failed: 1 });
  });

  it('prepares every enabled installed package beyond the default query page', async () => {
    const packages = Array.from({ length: 12 }, (_, index) => ({ ...installedPackage, id: index + 1, name: `cold-start-package-${index}` }));
    const { service, packageCdnLoaderService } = createRuntime(packages);
    service.init();
    await waitForPreload(service);
    expect(packageCdnLoaderService.loadPackage).toHaveBeenCalledTimes(12);
  });

  it('runs three preparations concurrently without waiting for system ready', async () => {
    const packages = Array.from({ length: 8 }, (_, index) => ({ ...installedPackage, id: index + 1, name: `parallel-package-${index}` }));
    const { service, packageCdnLoaderService } = createRuntime(packages);
    let release!: () => void;
    const blocked = new Promise<void>((resolve) => { release = resolve; });
    let active = 0;
    let maximum = 0;
    packageCdnLoaderService.loadPackage.mockImplementation(async () => {
      active++;
      maximum = Math.max(maximum, active);
      try { await blocked; } finally { active--; }
    });
    service.init();
    try {
      await vi.waitFor(() => expect(maximum).toBe(3));
      expect(packageCdnLoaderService.loadPackage).toHaveBeenCalledTimes(3);
    } finally {
      release();
      await waitForPreload(service);
    }
    expect(maximum).toBe(3);
    expect(packageCdnLoaderService.loadPackage).toHaveBeenCalledTimes(8);
  });

  it('continues queued packages after one background package fails', async () => {
    const packages = Array.from({ length: 8 }, (_, index) => ({ ...installedPackage, id: index + 1, name: `failure-package-${index}` }));
    const { service, packageCdnLoaderService } = createRuntime(packages);
    packageCdnLoaderService.loadPackage.mockImplementation(async (name) => {
      if (name === 'failure-package-0') throw new Error('fixture unavailable');
    });
    service.init();
    await waitForPreload(service);
    expect(packageCdnLoaderService.loadPackage).toHaveBeenCalledTimes(8);
    expect(service.getStatus().lastPreload).toMatchObject({ status: 'degraded', loaded: 7, failed: 1 });
  });

  it('defers recovery rows until ready and reuses their existing artifacts', async () => {
    const { service, eventEmitter, queryBuilderService, packageCdnLoaderService } = createRuntime([{ ...installedPackage, status: 'failed' }]);
    packageCdnLoaderService.isLoaded.mockReturnValue(true);
    const invalidation = vi.fn();
    eventEmitter.on(CACHE_EVENTS.INVALIDATE, invalidation);
    service.init();
    await waitForPreload(service);
    expect(queryBuilderService.update).not.toHaveBeenCalled();
    eventEmitter.emit(CACHE_EVENTS.SYSTEM_READY);
    await vi.waitFor(() => expect(queryBuilderService.update).toHaveBeenCalledWith('enfyra_package', 1, { status: 'installed', lastError: null }));
    await waitForPreload(service);
    expect(packageCdnLoaderService.loadPackage).not.toHaveBeenCalled();
    expect(invalidation).toHaveBeenCalledTimes(1);
  });
});
