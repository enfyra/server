import { afterEach, describe, expect, it, vi } from 'vitest';
import { PackageCdnLoaderService } from 'src/engine/cache';

function response(body: string, ok = true, status = 200) {
  return {
    ok,
    status,
    statusText: ok ? 'OK' : 'Error',
    text: async () => body,
  } as Response;
}

describe('PackageCdnLoaderService worker runtime fallback', () => {
  afterEach(() => {
    vi.restoreAllMocks();
  });

  it('keeps Node-runtime candidates available for worker proxy when Node import validation fails', async () => {
    const loader = new PackageCdnLoaderService();
    loader.invalidateAll();

    vi.stubGlobal(
      'fetch',
      vi.fn(async () =>
        response(`
          import missing from "missing-runtime-dep";
          export default { missing };
        `),
      ),
    );

    const mod = await loader.loadPackage('worker-only-demo', '1.0.0');
    expect(mod).toEqual({
      __enfyraRuntime: 'worker',
      name: 'worker-only-demo',
      version: '1.0.0',
    });

    const sources = loader.getPackageSources(['worker-only-demo']);
    expect(sources[0]).toMatchObject({
      name: 'worker-only-demo',
      safeName: 'worker_only_demo',
    });
    expect(sources[0].fileUrl).toContain('file://');
  });
});
