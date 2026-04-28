import { afterEach, describe, expect, it, vi } from 'vitest';
import * as fs from 'fs';
import { EventEmitter2 } from 'eventemitter2';
import { PackageCacheService, PackageCdnLoaderService } from '../../src/engines/cache';

function response(body: string, ok = true, status = 200) {
  return {
    ok,
    status,
    statusText: ok ? 'OK' : 'Error',
    text: async () => body,
  } as Response;
}

describe('PackageCdnLoaderService', () => {
  afterEach(() => {
    vi.restoreAllMocks();
  });

  it('rewrites esm.sh absolute CDN imports to local cached modules before import', async () => {
    const loader = new PackageCdnLoaderService();
    loader.invalidateAll();

    const fetchMock = vi.fn(async (url: string | URL | Request) => {
      const href = String(url);
      if (href.endsWith('/demo-pkg@1.0.0?bundle&target=node')) {
        return response(`
          import "/demo-dep@1.0.0?target=node";
          export { default } from "/demo-pkg@1.0.0/node/demo.bundle.mjs";
        `);
      }
      if (href.endsWith('/demo-dep@1.0.0?target=node')) {
        return response(`
          import "/../build/Release/native-addon.node?target=node";
          globalThis.__enfyraTestDepLoaded = true;
          export const dep = 1;
        `);
      }
      if (href.endsWith('/demo-pkg@1.0.0/node/demo.bundle.mjs')) {
        return response(`
          export default {
            value: globalThis.__enfyraTestDepLoaded ? 42 : 0
          };
        `);
      }
      return response('not found', false, 404);
    });
    vi.stubGlobal('fetch', fetchMock);

    await (loader as any).fetchAndWriteBundle('demo-pkg', '1.0.0');
    const source = loader.getPackageSources(['demo-pkg'])[0];
    const code = fs.readFileSync(source.filePath, 'utf-8');
    const packageDir = source.filePath.replace(/\/main\.mjs$/, '');

    expect(source.filePath).toContain('demo_pkg@1.0.0/main.mjs');
    expect(fs.existsSync(`${packageDir}/manifest.json`)).toBe(true);
    expect(fs.existsSync(`${packageDir}/deps`)).toBe(true);
    expect(code).toContain('./deps/');
    expect(code).not.toContain('file://');
    expect(code).not.toContain('"/demo-dep@1.0.0?target=node"');
    expect(code).not.toContain('"/demo-pkg@1.0.0/node/demo.bundle.mjs"');
    expect(fetchMock).toHaveBeenCalledTimes(3);
  });
});

describe('PackageCacheService CDN preload', () => {
  it('retries packages left in installing state and marks them installed', async () => {
    const queryBuilderService = {
      find: vi.fn(async () => ({
        data: [
          {
            id: 7,
            name: 'node-ssh',
            version: '13.2.1',
            status: 'installing',
          },
        ],
      })),
      update: vi.fn(async () => undefined),
    };
    const packageCdnLoaderService = {
      isLoaded: vi.fn(() => false),
      loadPackage: vi.fn(async () => ({})),
    };
    const packageCache = new PackageCacheService({
      queryBuilderService: queryBuilderService as any,
      eventEmitter: new EventEmitter2(),
      packageCdnLoaderService: packageCdnLoaderService as any,
      lazyRef: { dynamicWebSocketGateway: {} } as any,
    });

    await (packageCache as any).preloadPackagesFromCdn();

    expect(packageCdnLoaderService.loadPackage).toHaveBeenCalledWith(
      'node-ssh',
      '13.2.1',
    );
    expect(queryBuilderService.update).toHaveBeenCalledWith(
      'package_definition',
      { where: [{ field: 'id', operator: '=', value: 7 }] },
      { status: 'installed', lastError: null },
    );
  });
});
