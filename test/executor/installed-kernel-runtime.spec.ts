import { createRequire } from 'node:module';
import { readFileSync, realpathSync } from 'node:fs';
import path from 'node:path';
import { afterEach, describe, expect, it } from 'vitest';
import { IsolatedExecutorService } from '@enfyra/kernel';

const require = createRequire(import.meta.url);
let service: IsolatedExecutorService | undefined;

afterEach(() => {
  service?.onDestroy();
  service = undefined;
});

describe('installed Kernel native runtime', () => {
  it('resolves the published Kernel and a single native dependency without a local alias', () => {
    const server = JSON.parse(
      readFileSync(new URL('../../package.json', import.meta.url), 'utf8'),
    );
    expect(server.dependencies['@enfyra/kernel']).toBe(
      'patch:@enfyra/kernel@npm%3A2.0.25#~/.yarn/patches/@enfyra-kernel-npm-2.0.25-a0a542b5c4.patch',
    );
    expect(server.dependencies['isolated-vm']).toBeUndefined();
    const kernelRoot = path.dirname(
      path.dirname(require.resolve('@enfyra/kernel')),
    );
    expect(realpathSync(kernelRoot)).toBe(
      path.resolve('node_modules/@enfyra/kernel'),
    );
    const kernel = JSON.parse(
      readFileSync(path.join(kernelRoot, 'package.json'), 'utf8'),
    );
    expect(kernel.version).toBe('2.0.25');
    expect(kernel.dependencies['isolated-vm']).toBe(
      'npm:@enfyra/isolated-vm@7.0.7',
    );
    const nativeRequire = createRequire(path.join(kernelRoot, 'package.json'));
    expect(nativeRequire.resolve('isolated-vm')).toBe(
      require.resolve('isolated-vm'),
    );
    expect(nativeRequire('isolated-vm/package.json').version).toBe('7.0.7');
    nativeRequire('isolated-vm');
    const addons = Object.keys(require.cache).filter(
      (filename) =>
        filename.endsWith('.node') && filename.includes('isolated-vm'),
    );
    expect(addons).toHaveLength(1);
  });

  it('keeps task realms isolated and preserves binary views through the installed executor', async () => {
    service = new IsolatedExecutorService({
      packageCacheService: { getPackages: async () => [] },
      packageCdnLoaderService: { getPackageSources: () => [] },
      tuning: {
        maxConcurrentWorkers: 1,
        isolatesPerWorker: 2,
        isolateMemoryLimitMb: 64,
      },
    });
    const result = await service.run(
      `
      globalThis.installedKernelSentinel = 'private';
      return {
        bytes: new Uint8Array([0, 72, 105, 255]).subarray(1, 3),
        processType: typeof process,
        temporalType: typeof Temporal,
      };
    `,
      {},
      5000,
    );
    expect(Array.from(result.bytes)).toEqual([72, 105]);
    expect(result.processType).toBe('undefined');
    if (Number(process.versions.node.split('.')[0]) === 26)
      expect(result.temporalType).toBe('object');
    expect(
      await service.run('return typeof installedKernelSentinel;', {}, 5000),
    ).toBe('undefined');
    expect(service.getMetrics().crashesTotal).toBe(0);
  });
});
