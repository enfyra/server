import { afterAll, afterEach, describe, expect, it, vi } from 'vitest';
import * as fs from 'fs';
import * as path from 'path';

const tempRoot = vi.hoisted(() => {
  const fs = require('fs');
  const os = require('os');
  const path = require('path');
  return fs.mkdtempSync(
    path.join(os.tmpdir(), 'enfyra-cdn-publication-test-'),
  ) as string;
});

vi.mock('os', async (importOriginal) => ({
  ...(await importOriginal<typeof import('os')>()),
  tmpdir: () => tempRoot,
}));

import { IsolatedExecutorService } from '@enfyra/kernel';
import { EventEmitter2 } from 'eventemitter2';
import { PackageCdnLoaderService } from '../../src/engines/cache/services/package-cdn-loader.service';
import { PackageRuntimeService } from '../../src/engines/cache/services/package-runtime.service';
import { PackageCacheService } from '../../src/engines/cache/services/package-cache.service';
import { RuntimeRegistryService } from '../../src/engines/cache/services/runtime-registry.service';
import { CACHE_IDENTIFIERS } from '../../src/shared/utils/cache-events.constants';

const cacheDir = path.join(tempRoot, 'enfyra-pkg-cache');

function mockBundleFetch(name: string, source: () => Promise<string>) {
  const fetchMock = vi.fn(async (url: string | URL | Request) => {
    const href = String(url);
    if (href.endsWith(`/${name}@1.0.0/package.json`)) {
      return new Response('{ "dependencies": {} }');
    }
    if (href.endsWith(`/${name}@1.0.0?bundle&target=node`)) {
      return new Response(await source());
    }
    return new Response('', { status: 404 });
  });
  vi.stubGlobal('fetch', fetchMock);
  return fetchMock;
}

afterEach(() => {
  vi.unstubAllGlobals();
  vi.restoreAllMocks();
});

afterAll(() => {
  fs.rmSync(tempRoot, { recursive: true, force: true });
});

describe('package artifact publication', () => {
  it('executes the first package call after cold preparation with the installed kernel', async () => {
    const loader = new PackageCdnLoaderService();
    const eventEmitter = new EventEmitter2();
    const registry = new RuntimeRegistryService({ eventEmitter });
    const row = {
      id: 1,
      name: 'first-call-fixture',
      version: '1.0.0',
      status: 'installed',
    };
    const queryBuilderService = {
      find: vi.fn(async () => ({ data: [row] })),
      update: vi.fn(async () => undefined),
    };
    const deps = {
      eventEmitter,
      queryBuilderService: queryBuilderService as any,
      packageCdnLoaderService: loader,
      lazyRef: { dynamicWebSocketGateway: {} } as any,
    };
    const runtime = new PackageRuntimeService(deps);
    const cache = new PackageCacheService(deps);
    mockBundleFetch(
      row.name,
      async () => 'export default { value: () => 42 };',
    );
    await runtime.init();
    await cache.reload(false);
    await registry.publishFromCache(CACHE_IDENTIFIERS.PACKAGE, cache);
    const executor = new IsolatedExecutorService({
      packageCacheService: { getPackages: async () => registry.getPackages() },
      packageCdnLoaderService: loader,
      tuning: {
        maxConcurrentWorkers: 1,
        isolatesPerWorker: 1,
        isolateMemoryLimitMb: 64,
      },
    });
    try {
      const value = await executor.run(
        'return await $ctx.$pkgs["first-call-fixture"].value();',
        {
          $body: {},
          $query: {},
          $params: {},
          $share: { $logs: [] },
          $helpers: {},
          $cache: {},
          $repos: {},
          $user: null,
        } as any,
        5000,
      );
      expect(value).toBe(42);
      expect(queryBuilderService.update).not.toHaveBeenCalled();
    } finally {
      await executor.onDestroy();
    }
  });

  it('shares a concurrent load within one service', async () => {
    const loader = new PackageCdnLoaderService();
    let release!: () => void;
    const blocked = new Promise<void>((resolve) => {
      release = resolve;
    });
    const fetchMock = mockBundleFetch('singleflight-fixture', async () => {
      await blocked;
      return 'export default 42;';
    });
    const requests = [
      loader.loadPackage('singleflight-fixture', '1.0.0'),
      loader.loadPackage('singleflight-fixture', '1.0.0'),
    ];
    release();
    const outcomes = await Promise.allSettled(requests);
    expect(outcomes.every((outcome) => outcome.status === 'fulfilled')).toBe(
      true,
    );
    expect(
      fetchMock.mock.calls.filter(([url]) => String(url).includes('?bundle')),
    ).toHaveLength(1);
  });

  it('does not replace an artifact published by another loader', async () => {
    const first = new PackageCdnLoaderService();
    const second = new PackageCdnLoaderService();
    let calls = 0;
    let release!: () => void;
    const blocked = new Promise<void>((resolve) => {
      release = resolve;
    });
    mockBundleFetch('concurrent-publisher-fixture', async () => {
      calls++;
      if (calls === 2) await blocked;
      return `export default ${calls};`;
    });
    const firstLoad = first.loadPackage(
      'concurrent-publisher-fixture',
      '1.0.0',
    );
    const secondLoad = second.loadPackage(
      'concurrent-publisher-fixture',
      '1.0.0',
    );
    const settled = Promise.allSettled([firstLoad, secondLoad]);
    try {
      const descriptor = await firstLoad;
      const original = fs.readFileSync(descriptor.filePath, 'utf-8');
      release();
      const outcomes = await settled;
      expect(outcomes.every((outcome) => outcome.status === 'fulfilled')).toBe(
        true,
      );
      expect(fs.readFileSync(descriptor.filePath, 'utf-8')).toBe(original);
    } finally {
      release();
      await settled;
    }
  });

  it('settles started dependency writes before discarding a failed candidate', async () => {
    const loader = new PackageCdnLoaderService();
    let release!: () => void;
    const blocked = new Promise<void>((resolve) => {
      release = resolve;
    });
    let slowStarted = false;
    vi.stubGlobal(
      'fetch',
      vi.fn(async (url: string | URL | Request) => {
        const href = String(url);
        if (href.endsWith('/package.json'))
          return new Response('{ "dependencies": {} }');
        if (
          href.endsWith('/failed-dependency-fixture@1.0.0?bundle&target=node')
        ) {
          return new Response(
            'import "/bad-dependency@1.0.0/node/main.mjs"; import "/slow-dependency@1.0.0/node/main.mjs"; export default 42;',
          );
        }
        if (href.includes('/slow-dependency@')) {
          slowStarted = true;
          await blocked;
          return new Response('export default 42;');
        }
        return new Response('', { status: 503 });
      }),
    );
    let settled = false;
    const loading = loader
      .loadPackage('failed-dependency-fixture', '1.0.0')
      .finally(() => {
        settled = true;
      });
    const rejection = expect(loading).rejects.toThrow(/503/);
    try {
      await vi.waitFor(() => {
        expect(slowStarted).toBe(true);
      });
      await new Promise((resolve) => setImmediate(resolve));
      expect(settled).toBe(false);
    } finally {
      release();
      await rejection;
      await new Promise((resolve) => setImmediate(resolve));
    }
    expect(fs.readdirSync(path.join(cacheDir, '.staging'))).toEqual([]);
  });

  it('cleans a failed candidate and permits a new attempt', async () => {
    const loader = new PackageCdnLoaderService();
    let failed = true;
    mockBundleFetch('failed-candidate-fixture', async () => {
      if (failed) throw new Error('transport unavailable');
      return 'export default 42;';
    });
    await expect(
      loader.loadPackage('failed-candidate-fixture', '1.0.0'),
    ).rejects.toThrow('transport unavailable');
    expect(loader.isLoaded('failed-candidate-fixture', '1.0.0')).toBe(false);
    expect(
      fs.readdirSync(cacheDir).filter((entry) => entry.includes('.tmp-')),
    ).toEqual([]);
    const stagingDir = path.join(cacheDir, '.staging');
    if (fs.existsSync(stagingDir))
      expect(fs.readdirSync(stagingDir)).toEqual([]);
    failed = false;
    await expect(
      loader.loadPackage('failed-candidate-fixture', '1.0.0'),
    ).resolves.toMatchObject({ name: 'failed-candidate-fixture' });
  });
});
