import { afterAll, afterEach, describe, expect, it, vi } from 'vitest';
import * as fs from 'fs';

const tempRoot = vi.hoisted(() => {
  const fs = require('fs');
  const os = require('os');
  const path = require('path');
  return fs.mkdtempSync(
    path.join(os.tmpdir(), 'enfyra-package-fallback-test-'),
  ) as string;
});

vi.mock('os', async (importOriginal) => ({
  ...(await importOriginal<typeof import('os')>()),
  tmpdir: () => tempRoot,
}));

afterAll(() => {
  fs.rmSync(tempRoot, { recursive: true, force: true });
});
import { PackageCdnLoaderService } from '../../src/engines/cache';

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
    vi.unstubAllGlobals();
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

    const descriptor = await loader.loadPackage('worker-only-demo', '1.0.0');
    expect(descriptor).toMatchObject({
      name: 'worker-only-demo',
      version: '1.0.0',
      safeName: 'worker_only_demo',
    });

    const sources = loader.getPackageSources(['worker-only-demo']);
    expect(sources[0]).toMatchObject({
      name: 'worker-only-demo',
      safeName: 'worker_only_demo',
    });
    expect(sources[0].fileUrl).toContain('file://');
  });
});
